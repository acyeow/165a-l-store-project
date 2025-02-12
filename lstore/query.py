from lstore.table import Table, Record, INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN
from lstore.index import Index, BTreeNode, BTree
from time import process_time

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
            existing_records = self.select(primary_key, self.table.key, [1, 1, 1, 1] + [1]*self.table.num_columns)
            if not existing_records:
                print(f"Delete failed: No record found for key {primary_key}.")
                return False

            #If the record exists, 
            base_record = existing_records[0]

            current_record = base_record

            if base_record is None or len(base_record.columns) < self.table.num_columns + 4:
                print(f"Delete failed: Base record for key {primary_key} is invalid.")
                return False

            if len(base_record.columns) < self.table.num_columns + 4:
                print(f"Delete failed: Base record for key {primary_key} is missing columns. Found {len(base_record.columns)}, expected {self.table.num_columns + 4}.")
                return False

            print(f"Deleting record with primary key {primary_key} (RID {base_record.rid})")

            while (len(current_record.columns) > INDIRECTION_COLUMN and current_record.columns[INDIRECTION_COLUMN] not in (None, -1)):
                tail_rid = current_record.columns[INDIRECTION_COLUMN]

                if tail_rid is None or not isinstance(tail_rid, int):
                    print(f"Delete failed: Tail RID {tail_rid} is invalid.")
                    break

                tail_record = self.table.get_record_by_rid(tail_rid)


                if tail_record is None or len(tail_record.columns) < self.table.num_columns + 4:
                    print(f"Delete failed: Tail record {tail_rid} is invalid.")
                    break

                tail_record.columns[RID_COLUMN] = -1
                tail_record.columns[INDIRECTION_COLUMN] = -1
                current_record = tail_record
            
            #Set base page RID to -1
            if len(base_record.columns) > RID_COLUMN:
                base_record.columns[RID_COLUMN] = -1
            if len(base_record.columns) > INDIRECTION_COLUMN:
                base_record.columns[INDIRECTION_COLUMN] = -1

            print(f"Base record (RID {base_record.rid}) deleted successfully.")

            # Removing records from the index
            rid = base_record.rid
            for column_index in range(self.table.num_columns):
                if column_index < len(base_record.columns):
                    col_val = base_record.columns[column_index]
                    # Check if column is indexed
                    if col_val is not None and self.table.index.indices[column_index] is not None:
                        self.table.index.indices[column_index].delete(col_val)

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

            if self.table.key >= len(columns):
                return False

            #Check if the data already exists (dupe key)
            key_column = columns[self.table.key]
            existing_records = self.select(key_column, self.table.key, [1]*self.table.num_columns)
            if existing_records:
                return False
            
            #Initially all 0's
            schema_encoding = 0
            #Get RID from table
            rid = self.table.get_next_rid()
            indirection = -1
            timestamp = int(process_time())

            #Need to make a record using insert_record from table.py, unsure how to format a record
            metadata = [indirection, rid, timestamp, schema_encoding]
            data = list(columns)
            all_columns = metadata + data
            record = Record(rid, columns[self.table.key], all_columns)

            if not self.table.insert_record(record):
                return False

            if len(self.table.index.indices) < self.table.num_columns:
                return False
            
            #Put into index
            for column_index in range(self.table.num_columns):
                if column_index >= len(self.table.index.indices) or self.table.index.indices[column_index] is None:
                     continue
                col_val = columns[column_index]
                if isinstance(self.table.index.indices[column_index], BTree):
                    self.table.index.indices[column_index].insert(col_val, rid)

            #print(f"✅ Inserted record {rid}")
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
                    projected_values.append(record.columns[i])
            new_record = Record(record.rid, record.key, projected_values)
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
        # We can use relative_version to indicate how many tail records we need to traverse
        # We can do version == 0: return base record, version == 1: return first tail record; version == 2 return second tail record, etc

        result = []

        rids = self.table.lookup_by_value(search_key, search_key_index)
        for rid in rids:
            current_record = self.table.get_record_by_rid(rid)
            if current_record is None:
                continue

            # Traverse the tail chain 'version' times
            for _ in range(relative_version):
                # Check if there is an update
                if current_record.columns[0] in (None, -1):
                    # If no update, break loop
                    break
                tail_rid = current_record.columns[0]
                tail_record = self.table.get_record_by_rid(tail_rid)
                if tail_record is None:
                    break
                current_record = tail_record
            
            # Check if 'locked'
            if hasattr(current_record, "locked") and current_record.locked:
                return False
            
            # Apply projection to select the only needed columns from record
            projected_cols = [col_value for flag, col_value in zip(projected_columns_index, current_record.columns) if flag == 1]
            
            # Construct a new Record object with the same rid and key but only the projected columns
            projected_record = Record(current_record.rid, current_record.key, projected_cols)
            result.append(projected_record)
        
        return result

    
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
            #When a record is updated, update the index to point towards tail page instead?

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
