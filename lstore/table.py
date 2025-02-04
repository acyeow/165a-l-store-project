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

    def insert_record(self, record):
        page_range = self.get_page_range(self.current_rid)
        if page_range is None or not page_range.is_base or not page_range.has_capacity():
            page_range = self.create_page_range(is_base=True)
        if page_range.add_record(record):
            self.page_directory[record.rid] = page_range
            self.current_rid += 1
            return True
        return False

    def update_record(self, rid, updated_columns):
        record = self.page_directory[rid]
        page_range = self.get_page_range(self.current_rid)
        if page_range is None or page_range.is_base:
            page_range = self.create_page_range(is_base=False)
        new_tail_record = Record(self.current_rid, record.key, updated_columns)
        if page_range.add_tail_record(new_tail_record):
            record.indirection = new_tail_record.rid
            record.schema_encoding = self.update_schema_encoding(record.schema_encoding, updated_columns)
            self.page_directory[new_tail_record.rid] = page_range
            self.current_rid += 1
            return True
        return False

    def update_schema_encoding(self, schema_encoding, updated_columns):
        for i, column in enumerate(updated_columns):
            if column is not None:
                schema_encoding |= (1 << i)
        return schema_encoding
    
    def __merge(self):
        print("merge is happening")
        pass

 
