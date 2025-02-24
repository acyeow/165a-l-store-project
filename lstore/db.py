import os
import msgpack
from lstore.config import BUFFERPOOL_SIZE
from lstore.table import Table

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
        metadata_path = os.path.join(path, "metadata.msg")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as f:
                table_metadata = msgpack.unpackb(f.read(), raw=False)

            # Reconstruct tables from metadata
            for table_info in table_metadata:
                name = table_info['name']
                num_columns = table_info['num_columns']
                key_index = table_info['key_index']
                
                # Create table instance
                table = Table(name, num_columns, key_index)
                self.tables.append(table)
                
                # Load table data from disk
                self.load_table_data(table, table_info)

    def close(self):
        """
        Saves the current state of the database to disk and closes it.
        """
        if not self.path:
            raise Exception("Database is not open")
        
        table_metadata = []
        for table in self.tables:
            table_info = {
                'name': table.name,
                'num_columns': table.num_columns,
                'key_index': table.key_index,
            }
            table_metadata.append(table_info)
            
            # Save table data
            self._save_table_data(table)

        # Save table metadata / USING MSG INSTEAD OF PICKLE
        metadata_path = os.path.join(self.path, "metadata.msg")
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
    def create_table(self, name, num_columns, key_index):
        # Check if the table already exists
        for table in self.tables:
            if table.name == name:
                raise Exception(f"Table {name} already exists")

        # Create a new table and add it to the list of tables
        table = Table(name, num_columns, key_index)
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
    def load_table_data():
        pass

    #Need to implement later
    def save_table_data():
        pass

class Bufferpool:
    def __init__(self, size):
        self.size = size
        self.pages = {}  # page_id -> (page_data, is_dirty)
        self.page_paths = {}  # page_id -> disk_path
        self.access_times = {}  # page_id -> last_access_time
        self.access_counter = 0