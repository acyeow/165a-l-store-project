from lstore.table import Table, Record
from lstore.index import Index, BTreeNode, BTree


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            #This is if the the record doesn't exist; unsure what is 2PL
            existing_records = self.select(primary_key, self.table.key, [1]*self.table.num_columns)
            if not existing_records:
                return False

            #Unsure for now
            base_record = existing_records[0]
            #Need to figure out how to tell which column is the indrection column to reset
            #THis should mark as deleted
            base_record.columns[1] = -1  
            return True
        except Exception as e:
            print(f"Delete failed: {e}")
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns): #Looking at exam_tester insert(1,2,3,4)
        try:
            #Check formatting is consistent (same # of col)
            if len(columns) != self.table.num_columns:
                return False

            #Check if the data already exists
            key_column = columns[self.table.key]
            existing_records = self.select(key_column, self.table.key, [1])
            if existing_records:  
                return False
            
            #Initially all 0's
            schema_encoding = '0' * self.table.num_columns
            #Get RID from table
            rid = self.table.current_rid
            indirection = rid

            #Need to make a record using insert_record from table.py, unsure how to format a record
            all_columns = [rid, indirection, schema_encoding, columns]
            record = Record(rid, key_column, all_columns)

            return self.table.insert_record(record)
        
        except Exception as e:
            print(f"Insert failed: {e}")
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            #Check if record exists
            existing_records = self.select(primary_key, self.table.key, [1]*self.table.num_columns)
            if not existing_records:
                return False

            base_record = existing_records[0]
            rid = base_record.rid

            #Check if format matches
            updated_columns = list(columns)
            if len(updated_columns) != self.table.num_columns:
                return False

            #Create tail page using the update_record function in table.py
            return self.table.update_record(rid, updated_columns)
        except Exception as e:
            print(f"Update failed: {e}")
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
