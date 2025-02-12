from datetime import datetime

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon successful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # Get the RID of the record
        primary_key_column = 0 
        rid = self.table.index.locate(primary_key_column, primary_key)
        if not rid:
            return False
        rid = rid[0]
        # Set the record to empty
        self.table.page_ranges[rid[0]].base_pages[rid[1]].indirection[rid[2]] = ["empty"]
        return True

    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # Get the current time
        start_time = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Initialize the schema encoding to all 0s
        schema_encoding = '0' * self.table.num_columns  
        
        # Insert the record
        self.table.insert_record(start_time, schema_encoding, *columns)
        
        return True

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
        # Get the RID of the record
        rid = self.table.index.locate(search_key_index, search_key)
        if not rid:
            return []
        rid = rid[0]
        
        # Get the record
        rid = self.table.page_ranges[rid[0]].base_pages[rid[1]].indirection[rid[2]]
        
        # Use the index to finc the record
        record = self.table.find_record(search_key, rid, projected_columns_index)
        
        return [record]

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retrieve.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        # Get the RID of the record
        rid = self.table.index.locate(search_key_index, search_key)
        if not rid:
            return []
        rid = rid[0]
        
        # Save the base RID
        base_rid = rid
        
        # Get the record
        rid = self.table.page_ranges[rid[0]].base_pages[rid[1]].indirection[rid[2]] 
        
        # Navigate to the desired version
        while relative_version < 0:
            if rid[3] == 'b':
                if rid != base_rid:
                    rid = self.table.page_ranges[rid[0]].base_pages[rid[1]].indirection[rid[2]]
            else:
                rid = self.table.page_ranges[rid[0]].tail_pages[rid[1]].indirection[rid[2]]
            relative_version += 1
            
        # Use the index to find the record
        record = self.table.find_record(search_key, rid, projected_columns_index)
        
        return [record]

    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # Get the RID of the record
        rid = self.table.index.locate(self.table.key, primary_key)
        if not rid:
            return False
        rid = rid[0]
        
        # Unpack the RID data
        page_range_index, page_index, record_index, _ = rid
        
        # Get the current record
        current_rid = self.table.page_ranges[page_range_index].base_pages[page_index].indirection[record_index]
        
        # Find the record
        record = self.table.find_record(primary_key, current_rid, [1] * self.table.num_columns)
        
        # Check that we have space in the tail page
        page_range = self.table.page_ranges[page_range_index]
        if not page_range.tail_pages or not page_range.tail_pages[-1].has_capacity():
            page_range.add_tail_page(self.table.num_columns)
        
        # Insert the new record in the tail page
        current_tp = page_range.num_tail_pages - 1
        tail_page = page_range.tail_pages[current_tp]
        tail_page.insert_tail_page_record(*columns, record=record)
        tail_page.indirection.append(rid)
        
        # Update the base page indirection
        new_record_index = tail_page.num_records - 1
        update_rid = (page_range_index, current_tp, new_record_index, 't')
        tail_page.rid.append(update_rid)
        page_range.base_pages[page_index].indirection[record_index] = update_rid
        
        # Update the schema encoding
        for i in range(self.table.num_columns):
            if tail_page.schema_encoding[new_record_index][i] == 1:
                page_range.base_pages[page_index].schema_encoding[record_index][i] = 1
        
        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        # Get RIDs in range
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        if not rids:
            return False

        total_sum = 0
        processed_keys = set()
        collected_values = []  # Store (key, value) pairs for debugging
        
        for rid in rids:
            # Get base record
            base_page = self.table.page_ranges[rid[0]].base_pages[rid[1]]
            
            # Read key
            key_bytes = base_page.pages[self.table.key].data[rid[2]*8:(rid[2]+1)*8]
            key_value = int.from_bytes(key_bytes, byteorder='big')
            
            if key_value < start_range or key_value > end_range:
                continue
                
            if key_value in processed_keys:
                continue
                
            processed_keys.add(key_value)
            
            # Get latest version through indirection
            current_rid = base_page.indirection[rid[2]]
            
            try:
                # Get value from appropriate page
                if current_rid[3] == 't':  # Tail page
                    page = self.table.page_ranges[current_rid[0]].tail_pages[current_rid[1]]
                else:  # Base page
                    page = base_page
                
                value_bytes = page.pages[aggregate_column_index].data[current_rid[2]*8:(current_rid[2]+1)*8]
                value = int.from_bytes(value_bytes, byteorder='big')
                
                total_sum += value
                collected_values.append((key_value, value))
                
            except Exception as e:
                continue
        
        return total_sum
                
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retrieve.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    
    """
    
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        if not rids:
            return False

        total_sum = 0
        processed_keys = set()
        collected_values = []

        for rid in rids:
            # Get base record
            base_rid = rid
            base_page = self.table.page_ranges[rid[0]].base_pages[rid[1]]
            
            # Read key
            key_bytes = base_page.pages[self.table.key].data[rid[2]*8:(rid[2]+1)*8]
            key_value = int.from_bytes(key_bytes, byteorder='big')
            
            if key_value < start_range or key_value > end_range or key_value in processed_keys:
                continue
                
            processed_keys.add(key_value)
            
            # Navigate to desired version
            current_rid = base_page.indirection[rid[2]]
            version_count = relative_version
            
            while version_count < 0:
                if current_rid[3] == 'b':  # Base record
                    if current_rid != base_rid:  # Not original base
                        current_rid = self.table.page_ranges[current_rid[0]].base_pages[current_rid[1]].indirection[current_rid[2]]
                else:  # Tail record
                    current_rid = self.table.page_ranges[current_rid[0]].tail_pages[current_rid[1]].indirection[current_rid[2]]
                version_count += 1

            # Read value
            try:
                if current_rid[3] == 't':
                    page = self.table.page_ranges[current_rid[0]].tail_pages[current_rid[1]]
                else:
                    page = base_page
                    
                value_bytes = page.pages[aggregate_column_index].data[current_rid[2]*8:(current_rid[2]+1)*8]
                value = int.from_bytes(value_bytes, byteorder='big')
                
                total_sum += value
                collected_values.append((key_value, value))
                
            except Exception as e:
                print(f"DEBUG: Error reading value: {e}")
                continue
        
        return total_sum

    """
    increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True if increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)
        if r:
            r = r[0]
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False