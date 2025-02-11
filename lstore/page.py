from lstore.table import Record, Table

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        # Assuming each record is 8 bytes
        max_records = 512
        return self.num_records < max_records

    def write(self, value):
        # If there is space in the page, write the value
        if self.has_capacity():
            # Check for string inputs and non-int inputs
            if isinstance(value, str):
                value = int.from_bytes(value.encode('utf-8'), 'big')
            elif not isinstance(value, int):
                print(f"Unsupported data type: {type(value)}")
                return False

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
        self.num_columns = num_columns

		# Each pagerange holds 16 base pages and tail pages are appended
        if is_base:
            self.pages = [[BasePage()] for _ in range(num_columns)]
        else:
            self.pages = [[TailPage()] for _ in range(num_columns)]
            self.current_tail_page = [self.pages[i][-1] for _ in range(num_columns)]

    def has_capacity(self):
        return all(page.has_capacity() for page in self.pages)

    def add_record(self, table, rid, record):
        if not self.is_base:
            raise ValueError("Cannot add base record to tail page range")

        for i, column_value in enumerate(record.columns):
            if not isinstance(self.pages[i], list):
                self.pages[i] = [self.pages[i]]

            if not self.pages[i][-1].write(column_value):
                # Create a new base page and assign it
                new_page = BasePage()
                self.pages[i].append(new_page)
                new_page.write(column_value)

            table.page_directory[rid] = self

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
        num_columns = self.num_columns

        for i in range(num_columns):
            # if self.pages[i] have been converted to a list
            if self.is_base:
                if len(self.pages[i]) > 0:
                    last_page = self.pages[i][-1]
                    if 0 <= offset < last_page.num_records:
                        value = last_page.read(offset)
                # Base page range, one page per column

            else:
                if not isinstance(self.pages[i], list) or len(self.pages[i]) == 0:
                    print("ERROR: Column {i} has no valid pages for RID {rid}")
                    continue

                # if self.pages[i] is not a list, wrap it in a list
                if isinstance(self.pages[i], list):
                    page_list = self.pages[i]
                else:
                    page_list = [self.pages[i]]

                # Find the page that contains the record at this offset
                index = offset
                value = None

                # Read from newest to oldest
                for page in reversed(page_list):
                    # Record is in this page
                    if index < page.num_records:
                        value = page.read(index)
                        break
                    #Record is not in the page, subtract num_records from this page to go to next page in list
                    index -= page.num_records
            record_columns.append(value)

        # Record constructor needs key but since PageRange does not store the key, we can just pass None for now
        return Record(rid, None, record_columns)