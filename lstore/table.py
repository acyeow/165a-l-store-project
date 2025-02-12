from lstore.index import Index
from lstore.page_range import PageRange
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

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
        
        # Initialize the first page range
        self.add_page_range(num_columns)

    def find_current_base_page(self):
        # Find the first base page with capacity
        for page_range in self.page_ranges:
            for base_page in page_range.base_pages:
                if base_page.has_capacity():
                    return page_range, base_page

        # If no base page has capacity, create a new base page
        if not self.page_ranges[-1].has_capacity():
            self.add_page_range(self.num_columns)
        self.page_ranges[-1].add_base_page(self.num_columns)
        
        return self.page_ranges[-1], self.page_ranges[-1].base_pages[-1]

    def create_rid(self):
        # Get the current base page
        page_range, base_page = self.find_current_base_page()
        
        # Create a new rid for a base page record
        rid = (self.page_ranges.index(page_range), page_range.base_pages.index(base_page), base_page.num_records, 'b')
        base_page.rid[base_page.num_records] = rid
        
        return rid

    def find_record(self, key, rid, projected_columns_index):
        record = []
        # Get the page type
        page_type = 'base_pages' if rid[3] == 'b' else 'tail_pages'
        # Get the pages
        pages = getattr(self.page_ranges[rid[0]], page_type)[rid[1]].pages

        # Get the record from the pages
        for i, projected in enumerate(projected_columns_index):
            if projected == 1:
                bytearray = pages[i].data
                value = int.from_bytes(bytearray[rid[2] * 8:rid[2] * 8 + 8], byteorder='big')
                record.append(value)

        # Return the record
        return Record(key, rid, record)

    def insert_record(self, start_time, schema_encoding, *columns):
        # Get the current base page
        page_range, base_page = self.find_current_base_page()
        
        # Create a new rid for a base page record
        rid = self.create_rid()
        
        # Insert the record into the base page
        indirection = rid
        base_page.insert_base_page_record(rid, start_time, schema_encoding, indirection, *columns)
        
        # Create a new record
        self.page_directory[rid] = Record(rid, columns[self.key], columns)
        
        # Insert the record into the index
        key = columns[0]
        self.index.insert(key, rid)
        
        return True

    def add_page_range(self, num_columns):
        # Create a new page range
        new_page_range = PageRange(num_columns)
        self.page_ranges.append(new_page_range)

    def __merge(self):
        print("merge is happening")
        pass