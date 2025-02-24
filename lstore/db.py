import os
import pickle
from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        self.path = None

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

        # Load the database metadata (list of tables)
        metadata_path = os.path.join(path, "metadata.pkl")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as f:
                table_metadata = pickle.load(f)

            # Reconstruct tables from metadata
            for name, num_columns, key_index in table_metadata:
                table = Table(name, num_columns, key_index)
                table.load_from_disk(path)  # Assume Table has a method to load data from disk
                self.tables.append(table)

    def close(self):
        """
        Saves the current state of the database to disk and closes it.
        """
        if not self.path:
            raise Exception("Database is not open")

        # Save table metadata
        metadata_path = os.path.join(self.path, "metadata.pkl")
        table_metadata = [(table.name, table.num_columns, table.key_index) for table in self.tables]
        with open(metadata_path, "wb") as f:
            pickle.dump(table_metadata, f)

        # Save each table's data to disk
        for table in self.tables:
            table.save_to_disk(self.path)  # Assume Table has a method to save data to disk

        # Clear in-memory state
        self.tables = []
        self.path = None

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
