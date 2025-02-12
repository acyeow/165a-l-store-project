from lstore.table import Table, Record
from lstore.index import Index
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
        primary_key_column = 0 
        rid = self.table.index.locate(primary_key_column, primary_key)
        if not rid:
            return False
        rid = rid[0]  # Assuming locate returns a list of RIDs
        self.table.page_ranges[rid[0]].base_pages[rid[1]].indirection[rid[2]] = ["empty"]
        return True

    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        start_time = datetime.now().strftime("%Y%m%d%H%M%S")
        schema_encoding = '0' * self.table.num_columns  # Add '0000...' for schema_encoding
        self.table.insert_record(start_time, schema_encoding, *columns)  # Call function in Table.py to insert record
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
        rid = self.table.index.locate(search_key_index, search_key)
        if not rid:
            return []
        rid = rid[0]  
        rid = self.table.page_ranges[rid[0]].base_pages[rid[1]].indirection[rid[2]]
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
        rid = self.table.index.locate(search_key_index, search_key)
        if not rid:
            return []
        rid = rid[0]  
        base_rid = rid
        rid = self.table.page_ranges[rid[0]].base_pages[rid[1]].indirection[rid[2]] 
        while relative_version < 0:
            if rid[3] == 'b':
                if rid != base_rid:
                    rid = self.table.page_ranges[rid[0]].base_pages[rid[1]].indirection[rid[2]]
            else:
                rid = self.table.page_ranges[rid[0]].tail_pages[rid[1]].indirection[rid[2]]
            relative_version += 1
        record = self.table.find_record(search_key, rid, projected_columns_index)
        return [record]

    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        rid = self.table.index.locate(self.table.key, primary_key)
        if not rid:
            return False
        rid = rid[0] 
        page_range_index, page_index, record_index, _ = rid
        current_rid = self.table.page_ranges[page_range_index].base_pages[page_index].indirection[record_index]
        record = self.table.find_record(primary_key, current_rid, [1] * self.table.num_columns)
        
        page_range = self.table.page_ranges[page_range_index]
        if not page_range.tail_pages or not page_range.tail_pages[-1].has_capacity():
            page_range.add_tail_page(self.table.num_columns)
        
        current_tp = page_range.num_tail_pages - 1
        tail_page = page_range.tail_pages[current_tp]
        tail_page.insert_tail_page_record(*columns, record=record)
        tail_page.indirection.append(rid)
        
        new_record_index = tail_page.num_records - 1
        update_rid = (page_range_index, current_tp, new_record_index, 't')
        tail_page.rid.append(update_rid)
        page_range.base_pages[page_index].indirection[record_index] = update_rid
        
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
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        if not rids:
            return False

        total_sum = 0
        for rid in rids:
            while rid[3] != 'b':
                rid = self.table.page_ranges[rid[0]].tail_pages[rid[1]].indirection[rid[2]]
            record = self.table.find_record(rid[0], rid, [1] * self.table.num_columns)
            total_sum += record.columns[aggregate_column_index]

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
        for rid in rids:
            current_rid = self._get_relative_rid(rid, relative_version)
            record = self.table.find_record(rid[0], current_rid, [1] * self.table.num_columns)
            total_sum += record.columns[aggregate_column_index]

        return total_sum

    def _get_relative_rid(self, base_rid, relative_version):
        current_rid = self.table.page_ranges[base_rid[0]].base_pages[base_rid[1]].indirection[base_rid[2]]
        while relative_version < 0:
            page_range = self.table.page_ranges[current_rid[0]]
            if current_rid[3] == 'b' and current_rid != base_rid:
                current_rid = page_range.base_pages[current_rid[1]].indirection[current_rid[2]]
            else:
                current_rid = page_range.tail_pages[current_rid[1]].indirection[current_rid[2]]
            relative_version += 1
        return current_rid

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