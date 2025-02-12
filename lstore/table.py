from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        #Columns[0] = Indirection
        #Columns[1] = RID
        #Columns[2] = Timestamp
        #Columns[3] = Schema Encoding
        self.columns = columns

class PageRangeFinder:
    def __init__(self):
        self.page_ranges = {}

    def add_page_range(self, start, end):
        for rid in range(start, end + 1):
            self.page_ranges[rid] = (start, end)

    def find_page_range(self, rid):
        return self.page_ranges.get(rid, None)

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.page_ranges = []
        self.current_rid = 0

    def create_page_range(self, is_base=True):
        from lstore.page import PageRange
        start_rid = self.current_rid
        end_rid = start_rid + 999
        page_range = PageRange(start_rid, end_rid, self.num_columns, is_base)
        self.page_ranges.append(page_range)
        return page_range

    def get_page_range(self, rid):
        for page_range in self.page_ranges:
            if page_range.start_rid <= rid <= page_range.end_rid:
                return page_range
        return None
    
    def get_next_rid(self):
        next_rid = self.current_rid
        self.current_rid += 1
        return next_rid

    def insert_record(self, record):
        page_range = self.get_page_range(record.rid)
        if page_range is None or not page_range.is_base or not all(page_list[-1].has_capacity() for page_list in page_range.pages):
            page_range = self.create_page_range(is_base=True)
        
        success = page_range.add_record(self, record.rid, record)
        return success

    def update_record(self, rid, updated_columns):

        tail_page_range = self.get_page_range(self.current_rid)
        if tail_page_range is None or tail_page_range.is_base:
            tail_page_range = self.create_page_range(is_base=False)

        base_record = self.get_record_by_rid(rid)

        while base_record.columns[1] not in (None, -1) and base_record.columns[1] != rid:
            base_record = self.get_record_by_rid(base_record.columns[1])

        if base_record is None:
            return False
        if base_record.columns[1] not in (None, -1):
            base_record = self.get_record_by_rid(base_record.columns[1])
        new_tail_rid = self.current_rid
        new_tail_values = base_record.columns.copy()

        for i, column_value in enumerate(updated_columns):
            if column_value is not None:
                new_tail_values[i] = column_value

        new_tail_record = Record(new_tail_rid, base_record.key, new_tail_values)
        if tail_page_range.add_tail_record(new_tail_record):
            self.page_directory[rid].get_record(rid).columns[1] = new_tail_rid
            self.page_directory[new_tail_rid] = tail_page_range
            self.current_rid += 1
            return True
        return False

    def update_schema_encoding(self, schema_encoding, updated_columns):
        for i, column in enumerate(updated_columns):
            if column is not None:
                schema_encoding |= (1 << i)
        return schema_encoding
    
    def lookup_by_value(self, search_key, search_key_index):
  
        rids = []
        
        # Iterate over each PageRange
        for page_range in self.page_ranges:
            # Only considers base records
            if not page_range.is_base:
                continue
            
            # We can assume that all columns in a base page range will have the same num_records so 
            # we will use the first column to find how many records are stored
            num_records = page_range.pages[0][0].num_records
            
            # Computes RID and checks value
            for offset in range(num_records):
                rid = page_range.start_rid + offset
                # Get latest
                record = self.get_latest_record(rid)
                if record is None:
                    continue
                # Compare the record's value at the search_key_index with the search_key
                if record.columns[search_key_index] == search_key:
                    rids.append(rid)
        
        return rids
    
    def get_record_by_rid(self, rid):
        
        # Look up the page range that contains this RID
        page_range = self.page_directory.get(rid, None)
        if page_range is None:
            return None
        
        return page_range.get_record(rid)
    
    def get_latest_record(self, rid):

        base_record = self.get_record_by_rid(rid)
        if base_record is None:
            return None

        # If no update, its indirection pointer is assumed to be -1 or None
        if base_record.columns[1] in (None, -1):
            return base_record

        latest_record = base_record
        visited_rids = set()

        # While the current record has a valid indirection pointer, get its tail record
        while latest_record.columns[1] not in (None, -1):
            tail_rid = latest_record.columns[1]

            # Detect infinite loop
            if tail_rid in visited_rids or tail_rid == rid:
                print(f"Infinite loop: RID {tail_rid} is repeating.")
                base_record.columns[1] = -1
                break
            visited_rids.add(tail_rid)

            tail_record = self.get_record_by_rid(tail_rid)
            if tail_record is None:
                break
            latest_record = tail_record

        return latest_record
    
    def __merge(self):
        print("merge is happening")
        pass

 
