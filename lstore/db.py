import os
import msgpack
from lstore.config import BUFFERPOOL_SIZE
from lstore.config import PAGE_SIZE
from lstore.table import Table, Record
from datetime import datetime


class Database:
    def __init__(self):
        self.tables = []
        self.path = None
        self.bufferpool = None
        self.bufferpool_size = BUFFERPOOL_SIZE

    def open(self, path):
        """
        Opens the database at the specified path.
        """
        self.path = path

        # Create the directory if it doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)

        # Initialize the bufferpool
        self.bufferpool = Bufferpool(self.bufferpool_size, self.path)

        # Load database metadata if it exists
        metadata_path = os.path.join(path, "db_metadata.msg")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as f:
                table_metadata = msgpack.unpackb(f.read(), raw=False)

            # Recreate tables from metadata
            for table_info in table_metadata:
                name = table_info["name"]
                table = self.create_table(
                    name, table_info["num_columns"], table_info["key"]
                )

                # Load table data if the directory exists
                table_path = os.path.join(path, name)
                if os.path.exists(table_path):
                    self.load_table_data(table, table_info)

    def close(self):
        """
        Saves the current state of the database to disk and closes it.
        """
        if not self.path:
            raise Exception("Database is not open")

        # Save table metadata and data
        table_metadata = []
        for table in self.tables:
            table_info = {
                "name": table.name,
                "num_columns": table.num_columns,
                "key": table.key,
            }
            table_metadata.append(table_info)
            self.save_table_data(table)

        # Save database metadata
        metadata_path = os.path.join(self.path, "db_metadata.msg")
        with open(metadata_path, "wb") as f:
            f.write(msgpack.packb(table_metadata, use_bin_type=True))

        # Flush all bufferpool pages to disk
        if self.bufferpool:
            self.bufferpool.reset()

        # Clear in-memory state
        self.tables = []
        self.path = None
        self.bufferpool = None

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        # Check if the table already existsdef create_table(self, name, num_columns, key):
        """
        Creates a new table with a reference to the database.
        """
        # Check if the table already exists
        for table in self.tables:
            if table.name == name:
                raise Exception(f"Table {name} already exists")

        # Create a new table
        table = Table(name, num_columns, key)

        # Give the table a reference to this database
        table.database = self

        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        # Check if the table exists and delete it
        for i, table in enumerate(self.tables):
            if table.name == name:
                self.tables.pop(i)
                return

        raise Exception(f"Table {name} does not exist")

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        # Check if the table exists and return it
        for table in self.tables:
            if table.name == name:
                return table

        raise Exception(f"Table {name} does not exist")

    # Need to implement later
    def load_table_data(self, table, table_info):
        """
        Loads a table's data from disk.
        """
        table_path = os.path.join(self.path, table.name)

        # Create table directory if it doesn't exist
        if not os.path.exists(table_path):
            os.makedirs(table_path)
            return  # New table, nothing to load

        # Load table metadata
        table_meta_path = os.path.join(table_path, "tb_metadata.msg")
        if os.path.exists(table_meta_path):
            with open(table_meta_path, "rb") as f:
                metadata = msgpack.unpackb(f.read(), raw=False)

            table.num_columns = metadata["num_columns"]
            table.key = metadata["key"]

        # Iterate over the Page Directory and rebuild the Index via Insertion
        for x in range(table.num_columns):
            table.index.create_index(x)

        # Load Page Directory and rebuild them
        page_directory_path = os.path.join(table_path, "pg_directory.msg")
        if os.path.exists(page_directory_path):
            with open(page_directory_path, "rb") as f:
                pg_data = msgpack.unpackb(f.read(), raw=False)

            for rid_str, columns in zip(pg_data["rid"], pg_data["data"]):
                rid = tuple(rid_str)
                key = columns[table.key]
                record = Record(rid, key, columns)
                table.page_directory[rid] = record

                table.index.insert(key, rid)

    # Need to implement later
    def save_table_data(self, table):
        table_path = os.path.join(self.path, table.name)
        os.makedirs(table_path, exist_ok=True)

        metadata = {
            "name": table.name,
            "num_columns": table.num_columns,
            "key": table.key,
            "num_pages": sum(
                len(pr.base_pages) + len(pr.tail_pages) for pr in table.page_ranges
            ),
        }

        # Save table metadata
        with open(os.path.join(table_path, "tb_metadata.msg"), "wb") as f:
            f.write(msgpack.packb(metadata, use_bin_type=True))

        # Save each page separately
        for pr_idx, page_range in enumerate(table.page_ranges):
            for page_idx, page in enumerate(page_range.base_pages):
                page_id = f"base_{pr_idx}_{page_idx}"
                self.save_page(table, page, page_id)

            for page_idx, page in enumerate(page_range.tail_pages):
                page_id = f"tail_{pr_idx}_{page_idx}"
                self.save_page(table, page, page_id)

        # Save page directory separately
        pg_directory = {
            "rid": list(table.page_directory.keys()),
            "data": [record.columns for record in table.page_directory.values()],
        }
        with open(os.path.join(table_path, "pg_directory.msg"), "wb") as f:
            f.write(msgpack.packb(pg_directory, use_bin_type=True))

    def save_page(self, table, page, page_id):
        """Helper function to save a single page."""
        table_path = os.path.join(self.path, table.name)
        page_path = os.path.join(table_path, f"{page_id}.msg")

        page_data = {
            "columns": [col.data for col in page.pages],
            "tps": getattr(page, "tps", None),
        }

        with open(page_path, "wb") as f:
            f.write(msgpack.packb(page_data, use_bin_type=True))


class Bufferpool:
    def __init__(self, size, path):
        self.size = size  # maximum number of pages in memory
        self.path = path  # database path
        self.pages = {}  # page_id -> (page_data, is_dirty)
        self.page_paths = {}  # page_id -> disk_path
        self.pins = {}  # page_id -> pin count
        self.access_times = {}  # page_id -> last_access_time
        self.access_counter = 0  # counter for tracking access order

    def get_page(self, page_id, table_name, num_columns=None):
        """
        Get a page from the bufferpool. If not in memory, load from disk.
        Returns the page data and pins the page.
        """
        try:
            composite_key = (table_name, page_id)
            print(f"Getting page {page_id} for table {table_name}")
            
            # If page is in bufferpool, access it and update access time
            if composite_key in self.pages:
                self.access_counter += 1
                self.access_times[composite_key] = self.access_counter
                self.pins[composite_key] = self.pins.get(composite_key, 0) + 1
                print(f"Page found in bufferpool, pin count: {self.pins[composite_key]}")
                return self.pages[composite_key][0]  # Return page_data

            print(f"Page not in bufferpool, will load from disk")
            # If bufferpool is full, evict pages until space is available
            eviction_attempts = 0
            while len(self.pages) >= self.size and eviction_attempts < 5:
                try:
                    self.evict_page()
                    print(f"Evicted a page, bufferpool size: {len(self.pages)}")
                except Exception as e:
                    print(f"Eviction attempt {eviction_attempts} failed: {e}")
                    eviction_attempts += 1
                    
                    # If no pages can be evicted after several attempts, force eviction
                    if eviction_attempts >= 5:
                        print("Forcing eviction of least pinned page")
                        if self.pages:
                            # Find the page with the lowest pin count
                            min_pin_page = min(self.pins, key=self.pins.get)
                            print(f"Forcing eviction of page {min_pin_page} with pin count {self.pins[min_pin_page]}")
                            self.write_dirty(min_pin_page, self.pages[min_pin_page][0])
                            del self.pages[min_pin_page]
                            del self.page_paths[min_pin_page]
                            del self.pins[min_pin_page]
                            del self.access_times[min_pin_page]
                        else:
                            print("No pages to evict, but bufferpool reports full - resetting")
                            self.pages.clear()
                            self.pins.clear()
                            self.access_times.clear()

            # Construct the disk file path
            page_path = self._construct_page_path(table_name, page_id)
            self.page_paths[composite_key] = page_path
            print(f"Page path: {page_path}")

            # Load page from disk if it exists, otherwise create empty page
            if os.path.exists(page_path):
                try:
                    with open(page_path, "rb") as f:
                        page_data = msgpack.unpackb(f.read(), raw=False)
                    print(f"Loaded page from disk")
                except Exception as e:
                    print(f"Error reading page from disk: {e}")
                    page_data = self._create_empty_page(num_columns)
            else:
                print(f"Page not found on disk, creating empty page")
                page_data = self._create_empty_page(num_columns)

            # Insert the page into the bufferpool
            self.pages[composite_key] = (page_data, False)  # Not dirty initially
            self.pins[composite_key] = 1  # Pin on load
            self.access_counter += 1
            self.access_times[composite_key] = self.access_counter

            return page_data
        except Exception as e:
            print(f"Error in get_page: {e}")
            import traceback
            traceback.print_exc()
            
            # Return a minimal empty page to avoid crashes
            return self._create_empty_page(num_columns)

    def set_page(self, page_id, table_name, page_data, num_columns=None):
        """
        Update or insert a page in the bufferpool and mark it as dirty.
        """
        # If bufferpool is full, evict pages until space is available
        while len(self.pages) >= self.size:
            try:
                self.evict_page()
            except Exception:
                # If no pages can be evicted, forcibly evict a page
                if self.pages:
                    # Find the page with the lowest pin count (even if it's 1)
                    min_pin_page = min(self.pins, key=self.pins.get)
                    self.write_dirty(min_pin_page, self.pages[min_pin_page][0])
                    del self.pages[min_pin_page]
                    del self.page_paths[min_pin_page]
                    del self.pins[min_pin_page]
                    del self.access_times[min_pin_page]
                else:
                    raise Exception("Cannot evict any pages from bufferpool")

        # Check if page exists in bufferpool
        if page_id in self.pages:
            self.pages[page_id] = (page_data, True)  # Mark as dirty
            self.access_counter += 1
            self.access_times[page_id] = self.access_counter
            self.pins[page_id] = self.pins.get(page_id, 0) + 1
            return

        # Construct page path and store it
        page_path = self._construct_page_path(table_name, page_id)
        self.page_paths[page_id] = page_path

        # Add page to bufferpool
        self.pages[page_id] = (page_data, True)  # Mark as dirty
        self.pins[page_id] = 1
        self.access_counter += 1
        self.access_times[page_id] = self.access_counter

    def evict_page(self):
        """
        Evict the least recently used unpinned page.
        If the page is dirty, write it to disk first.
        """
        # Find the least recently used unpinned page
        lru_page = None
        oldest_access = float("inf")

        for pid, access_time in self.access_times.items():
            if self.pins.get(pid, 0) == 0 and access_time < oldest_access:
                oldest_access = access_time
                lru_page = pid

        if lru_page is None:
            raise Exception("No unpinned page available for eviction.")

        # If the page is dirty, write it to disk
        page_data, is_dirty = self.pages[lru_page]
        if is_dirty:
            self.write_dirty(lru_page, page_data)

        # Remove the page from the bufferpool
        del self.pages[lru_page]
        del self.page_paths[lru_page]
        del self.pins[lru_page]
        del self.access_times[lru_page]

    def unpin_page(self, page_id, table_name = None):
        """
        Decrement the pin count for a page.
        The page can be evicted only when pin count is 0.
        """
        if page_id in self.pins and self.pins[page_id] > 0:
            self.pins[page_id] -= 1

    def _create_empty_page(self, num_columns):
        """Create an empty page data structure with the expected format."""
        return {
            "columns": [[] for _ in range(num_columns)] if num_columns else [],
            "indirection": [],
            "rid": [],
            "timestamp": [],
            "schema_encoding": [],
            "tps": None,
        }

    def write_dirty(self, page_id, page_data):
        """
        Write a dirty page back to disk.
        """
        if page_id in self.page_paths:
            path = self.page_paths[page_id]
            # Ensure directory exists
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # Serialize and write data
            with open(path, "wb") as f:
                f.write(msgpack.packb(page_data, use_bin_type=True))

            # Mark page as clean
            if page_id in self.pages:
                self.pages[page_id] = (page_data, False)

    def reset(self):
        """
        Write all dirty pages to disk and clear the bufferpool.
        """
        for pid, (page_data, is_dirty) in self.pages.items():
            if is_dirty:
                self.write_dirty(pid, page_data)

        # Clear all bufferpool data structures
        self.pages.clear()
        self.page_paths.clear()
        self.pins.clear()
        self.access_times.clear()
        self.access_counter = 0

    def _construct_page_path(self, table_name, page_id):
        """
        Standardize page path construction.
        """
        # Create a structured path for different page types
        if isinstance(page_id, tuple):
            # Handle structured page IDs (e.g., (range_id, page_type, page_num))
            page_type = page_id[0]
            if page_type == "base":
                return os.path.join(
                    self.path, table_name, f"base_{page_id[1]}_{page_id[2]}.msg"
                )
            elif page_type == "tail":
                return os.path.join(
                    self.path, table_name, f"tail_{page_id[1]}_{page_id[2]}.msg"
                )

        # Default case for simple page IDs
        return os.path.join(self.path, table_name, f"{page_id}.msg")
