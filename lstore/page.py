PAGE_SIZE = 4096
DATA_SIZE = 8
RECORDS_PER_PAGE = PAGE_SIZE // DATA_SIZE

class LogicalPage:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value):
        # Only handle 8-byte integers for milestone 1
        if not isinstance(value, int):
            raise ValueError("Value must be an integer")
        if value.bit_length() > 64:
            raise OverflowError("int too big to convert")
        value_bytes = value.to_bytes(8, byteorder='big')
        
        start = self.num_records * 8
        end = (self.num_records + 1) * 8
        self.data[start:end] = value_bytes
        self.num_records += 1

    def read(self, index, num_values):
        values = []
        for i in range(num_values):
            start = (index + i) * 8
            end = (index + i + 1) * 8
            value_bytes = self.data[start:end]
            value = int.from_bytes(value_bytes, byteorder='big')
            values.append(value)
        return values

# compressed, read-only pages
class BasePage:
    def __init__(self, num_cols):
        self.rid = [None] * RECORDS_PER_PAGE
        self.num_cols = num_cols
        self.num_records = 0
        self.indirection = []
        self.schema_encoding = []
        self.start_time = []
        
        self.pages = []
        
        for _ in range(self.num_cols):
            self.pages.append(LogicalPage())
    
    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE
    
    def insert_base_page_record(self, rid, start_time, schema_encoding, indirection, *columns):
        if not self.has_capacity():
            return False
        for i in range(self.num_cols):
            self.pages[i].write(columns[i])
    
        self.rid.append(rid)
        self.start_time.append(start_time)
        self.schema_encoding.append(schema_encoding)
        self.indirection.append(indirection)
        self.num_records += 1
        return True
        

# uncompressed, append-only updates
class TailPage:
    def __init__(self, num_cols):
        self.rid = [None] * RECORDS_PER_PAGE
        self.num_cols = num_cols
        self.num_records = 0
        self.indirection = []
        self.schema_encoding = []
        
        self.pages = []
        
        for _ in range(self.num_cols):
            self.pages.append(LogicalPage())
    
    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE
    
    def insert_tail_page_record(self, *columns, record):
        schema = ''
        for i in range(self.num_cols):
            if columns[i] is not None:
                schema += '1'
                self.pages[i].write(columns[i])
            else:
                schema += '0'
                self.pages[i].write(record.columns[i])
                
        self.schema_encoding.append(schema)
        self.num_records += 1
        return True