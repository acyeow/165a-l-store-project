from lstore.table import Record

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        # Assuming each record is 8 bytes
        return self.num_records < 4096 // 8

    def write(self, value):
        # If there is space in the page, write the value
        if self.has_capacity():
            # Write the value to the page
            self.data[self.num_records * 8: (self.num_records + 1) * 8] = value.to_bytes(8, 'big')
            # Increment the number of records in the page
            self.num_records += 1
            return True
        # If there is no space in the page, return False
        return False
    
    def read(self, index):
        # If the index is within the number of records in the page, return the value at the index
        if index < self.num_records:
            return int.from_bytes(self.data[index * 8:(index + 1) * 8], 'big')
        # If the index is out of range, return None
        return None

# compressed, read-only pages
class BasePage(Page):
    def __init__(self):
        super().__init__()

# uncompressed, append-only updates
class TailPage(Page):
    def __init__(self):
        super().__init__()
  
# upcompressed, append-only inserts      
class TailLevelTailPage(Page):
    def __init__(self):
        super().__init__()
        
class PageRange:
    def __init__(self, start_rid, end_rid, num_columns, is_base=True):
        self.start_rid = start_rid
        self.end_rid = end_rid
        self.is_base = is_base
        if is_base:
            self.pages = [BasePage() for _ in range(num_columns)]
        else:
            self.pages = [TailPage() for _ in range(num_columns)]
            self.current_tail_page = [TailPage() for _ in range(num_columns)]

    def has_capacity(self):
        return all(page.has_capacity() for page in self.pages)

    def add_record(self, record):
        if not self.is_base:
            raise ValueError("Cannot add base record to tail page range")
        for i, column_value in enumerate(record.columns):
            if not self.pages[i].write(column_value):
                return False
        return True

    def add_tail_record(self, record):
        if self.is_base:
            raise ValueError("Cannot add tail record to base page range")
        for i, column_value in enumerate(record.columns):
            if column_value is not None:
                if not self.current_tail_page[i].write(column_value):
                    self.current_tail_page[i] = TailPage()
                    self.pages[i].append(self.current_tail_page[i])
                    self.current_tail_page[i].write(column_value)
        return True

    def get_record(self, rid):
        # Basically the current record's position
        offset = rid - self.start_rid
        record_columns = []
        num_columns = len(self.pages)
        
        for i in range(num_columns):
            # if self.pages[i] have been converted to a list
            if self.is_base:
                # Base page range, one page per column
                value = self.pages[i].read(offset)
            else:
                # if self.pages[i] is not a list, wrap it in a list
                if isinstance(self.pages[i], list):
                    page_list = self.pages[i]
                else:
                    page_list = [self.pages[i]]
                    
                # Find the page that contains the record at this offset
                index = offset
                value = None
                for page in page_list:
                    # Record is in this page
                    if index < page.num_records:
                        value = page.read(index)
                        break
                    else:
                        #Record is not in the page, subtract num_records from this page to go to next page in list
                        index -= page.num_records
            record_columns.append(value)
        
        # Record constructor needs key but since PageRange does not store the key, we can just pass None for now
        return Record(rid, None, record_columns)