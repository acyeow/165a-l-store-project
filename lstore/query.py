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
    #When a record is deleted, the base record will be invalidated by setting its RID and all its tail records to a special value (-1). 
    """
    def delete(self, primary_key):
        try:
            #This is if the the record doesn't exist; unsure what is 2PL
            existing_records = self.select(primary_key, self.table.key, [1]*self.table.num_columns)
            if not existing_records:
                return False

            #If the record exists, 
            base_record = existing_records[0]
            current_rid = base_record.rid
            #Latest Tail Page
            current_indirection = base_record.columns[1]

            #If the base page isn't the latest version
            if current_indirection != current_rid:
                #Traverse the tail pages (Most recent to oldest)
                while current_indirection != base_record.rid:

                    #Get tail record
                    #tail_record = 
                    #Set RID to -1
                    #tail_record.columns[0] = -1
                    #Move to next tail via indirection column
                    #current_indirection = tail_record.columns[1]

                    pass
            
            #Set base page RID to -1
            base_record.columns[0] = -1  
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

            #Check if the data already exists (dupe key)
            key_column = columns[self.table.key]
            existing_records = self.select(key_column, self.table.key, [1]*self.table.num_columns)
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

            if not self.table.insert_record(record):
                return False
            
            #Put into index
            
            return True
        
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
        result = []
        # Use helper function to obtain RIDs matching the search key
        rids = self.table.lookup_by_value(search_key, search_key_index)
        
        for rid in rids:
            # Retrieve the latest version
            record = self.table.get_latest_record(rid)
            
            # If the record is locked by TPL
            if hasattr(record, 'locked') and record.locked:
                return False
            
            projected_values = []
            # Assume that len(projected_columns_index) equals the number of columns in record.values.
            for i, flag in enumerate(projected_columns_index):
                if flag == 1:
                    projected_values.append(record.values[i])
            new_record = Record(projected_values)
            result.append(new_record)
    
        return result

    
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
    try:
        total = 0
        found_records = False

        # Iterate through all keys in the range
        for key in range(start_range, end_range + 1):
            # Use the index to find the RID(s) associated with the key
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue  # Skip if no records match the key

            for rid in rids:
                # Fetch the record from the page directory
                record = self.table.get_record(rid)
                if not record:
                    continue  # Skip if the record doesn't exist

                # Add the value of the specified column to the total
                total += record.columns[aggregate_column_index]
                found_records = True

        if not found_records:
            return False  # No records found in the range

        return total

    except Exception as e:
        print(f"Sum failed: {e}")
        return False

    
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
    try:
        total = 0
        found_records = False

        # Iterate through all keys in the range
        for key in range(start_range, end_range + 1):
            # Use the index to find the RID(s) associated with the key
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue  # Skip if no records match the key

            for rid in rids:
                # Fetch the record from the page directory
                record = self.table.get_record(rid)
                if not record:
                    continue  # Skip if the record doesn't exist

                # Fetch the specified version of the record
                versioned_columns = self.__fetch_versioned_columns(record, relative_version)
                if not versioned_columns:
                    continue  # Skip if the version doesn't exist

                # Add the value of the specified column to the total
                total += versioned_columns[aggregate_column_index]
                found_records = True

        if not found_records:
            return False  # No records found in the range

        return total

    except Exception as e:
        print(f"Sum version failed: {e}")
        return False


    
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
