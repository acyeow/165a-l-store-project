import os
import msgpack
from lstore.config import BUFFERPOOL_SIZE
from lstore.table import Table
import datetime

class Database():

    def __init__(self):
        self.tables = []
        self.path = None
        self.bufferpool = None
        self.bufferpool_size = BUFFERPOOL_SIZE

    def open(self, path):
        """
        Opens the database from the specified path.
        If the database does not exist, it initializes an empty database.
        """
        self.path = path

        # Create the directory if it doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)
            return 

        self.bufferpool = Bufferpool(self.bufferpool_size)

        # Load the database metadata (list of tables) / USING MSG INSTEAD OF PICKLE
        metadata_path = os.path.join(path, "db_metadata.msg")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as f:
                table_metadata = msgpack.unpackb(f.read(), raw=False)

            # Reconstruct tables from metadata
            for table_info in table_metadata:
                name = table_info['name']
                
                # Check if table already exists in memory / had an issue with dupes (ie. running the tester twice)
                existing_table = None
                for table in self.tables:
                    if table.name == name:
                        existing_table = table
                        break
                
                # Only create the table if it doesn't already exist
                if existing_table is None:
                    num_columns = table_info['num_columns']
                    key = table_info['key']
                    
                    # Create table instance
                    table = Table(name, num_columns, key)
                    self.tables.append(table)
                    
                # Load table data from disk
                self.load_table_data(table, table_info)

    def close(self):
        """
        Saves the current state of the database to disk and closes it.
        """
        if not self.path:
            raise Exception("Database is not open")
        
        #Contains the metadata of each table in the database
        table_metadata = []
        for table in self.tables:
            table_info = {
                'name': table.name,
                'num_columns': table.num_columns,
                'key': table.key,
            }
            table_metadata.append(table_info)
            
            # Save table data
            self.save_table_data(table)

        # Save table metadata / USING MSG INSTEAD OF PICKLE
        metadata_path = os.path.join(self.path, "db_metadata.msg")
        with open(metadata_path, "wb") as f:
            f.write(msgpack.packb(table_metadata, use_bin_type=True))

        # Clear in-memory state
        # Flush bufferpool (function inside bufferpool)
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
        # Check if the table already exists
        for table in self.tables:
            if table.name == name:
                raise Exception(f"Table {name} already exists")

        # Create a new table and add it to the list of tables
        table = Table(name, num_columns, key)
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
    
    #Need to implement later
    def load_table_data(self, table, table_info):
        table_path = os.path.join(self.path, table.name)

        if not os.path.exists(table_path):
            os.makedirs(table_path)
        
        pass

    #Need to implement later
    def save_table_data(self, table):
        #Set up file structure .../table/page_ranges/
        table_path = os.path.join(self.path, table.name)
        page_ranges_path = os.path.join(table_path, "page_ranges")
        os.makedirs(page_ranges_path, exist_ok=True)
        
        #Contains the metadata of the specific table
        #Save table metadata in .../table/
        metadata = {
            'name' : table.name,
            'num_columns': table.num_columns,
            'key': table.key,
            #planning on saving the page ranges as individual files to store the data
            #this should help verify that the amount of data matches the given metadata
            'page_range_count': len(table.page_ranges)
        }
        with open(os.path.join(table_path, "tb_metadata.msg"), "wb") as f:
            f.write(msgpack.packb(metadata, use_bin_type=True))
        
        # Save each page range
        for pr_index, page_range in enumerate(table.page_ranges):
            pr_data = {'base_pages': [], 'tail_pages': []}
            
            # Helper function to serialize a page
            def serialize_page(page, page_type):
            #I haven't figured out how to format the saved data
                return 0
            
            # Serialize base pages
            for base_page in page_range.base_pages:
                pr_data['base_pages'].append(serialize_page(base_page, 'base'))
            
            # Serialize tail pages
            for tail_page in page_range.tail_pages:
                pr_data['tail_pages'].append(serialize_page(tail_page, 'tail'))
            
            # Save page range to file
            pr_file_path = os.path.join(page_ranges_path, f"page_range_{pr_index}.msg")
            with open(pr_file_path, "wb") as f:
                f.write(msgpack.packb(pr_data, use_bin_type=True))

class Bufferpool:
    def __init__(self, size, path):
        self.size = size # bufferpool size
        self.path = path # database path
        self.pages = {}  # page_id -> (page_data, is_dirty)
        self.page_paths = {}  # page_id -> disk_path
        self.pins = {} # page_id -> pins in place (int)
        self.access_times = {}  # page_id -> last_access_time
        self.access_counter = 0

    # Function for getting a page from the buffer pool and disk
    def get_page(self, page_id, table_name, column_index):
        # If page is in the bufferpool, access it and update access time and pin it
        if page_id in self.pages[0]:
            self.access_times[page_id] = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            self.pins[page_id] = self.pins.get(page_id, 0) + 1
            return self.pages[0][page_id]

        # If the page is not in the bufferpool, load it from the disk and make space if necessary
        if len(self.pages[0]) >= self.size:
            self.evict_page

        page_path = os.path.join()
        self.page_paths[page_id] = page_path

    # returns page_data

    def set_page(self, page_id, table_name, column_index):
        # sets a page and returns nothing

    # Function for unpinning a page after done with it
    def unpin_page(self, page_id):
        # returns nothing and unpins a page

    # Function for evicting a page from the bufferpool
    def evict_page(self):
        # evicts a page using lru and returns nothing

    # Function for evicting all the pages from a table from
    # Call this in drop_table()
    def evict_table(self, table_name):
        # Evicts multiple pages for a single table and returns nothing

    # Function for writing a dirty page back into disk
    def write_dirty(self, page_id, page_data):
        # Writes the dirty page back into disk and returns nothing

    # Write all dirty pages back into disk and evict all pages
    # Call this function when the database closes
    def reset(self):
        # returns nothing and resets bufferpool