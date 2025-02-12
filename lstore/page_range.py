from lstore.page import BasePage, TailPage

class PageRange:
    def __init__(self, num_cols):
       self.base_pages = []
       self.tail_pages = []
       self.num_base_pages = 0
       self.num_tail_pages = 0
       
    def has_capacity(self):
        return self.num_base_pages < 512
    
    def create_page_range(self, num_records):
        self.id = num_records + 1
        
    def add_base_page(self, num_cols):
        if self.has_capacity():
            self.base_pages.append(BasePage(num_cols))
            self.num_base_pages += 1
            
    def add_tail_page(self, num_cols):
        self.tail_pages.append(TailPage(num_cols))
        self.num_tail_pages += 1