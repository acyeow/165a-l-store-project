class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        # Assuing each record is 8 bytes
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
    
class BasePage(Page):
    def __init__(self):
        super().__init__()

class TailPage(Page):
    def __init__(self):
        super().__init__()

