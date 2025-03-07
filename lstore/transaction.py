from lstore.table import Table, Record
from lstore.index import Index
import os

from threading import Lock

class Transaction:
    def __init__(self, transaction_id, buffer_pool, lock_manager):
        self.transaction_id = transaction_id  # Unique ID for transaction
        self.queries = []  # List to store queries and their arguments
        self.rollback_operations = []  # List to store operations for rollback
        self.buffer_pool = buffer_pool  # Reference to buffer pool
        self.lock_manager = lock_manager  # Reference to lock manager
        self.locks_held = set()  # Set to track locks held by this transaction
        self.log_lock = Lock()  # Lock for thread-safe log writes

    def add_query(self, query, table, *args):
        # Store query and its arguments as well as current state for potential rollback
        self.queries.append((query, table, args))
        self.rollback_operations.append((table.get_record, args[0]))

    def run(self):
        # Get locks, abort if fail
        for query, table, args in self.queries:
            record_id = args[0]  # Assuming first argument is record ID
            if not self.lock_manager.acquire_lock(self.transaction_id, record_id):
                return self.abort() 
        # Do queries, abort if fail
        for query, table, args in self.queries:
            result = query(*args)
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        # This function returns false if something is aborted
        # Roll back changes by executing rollback operations in reverse order
        for operation, args in reversed(self.rollback_operations):
            operation(*args)  

        for record_id in self.locks_held:
            self.lock_manager.release_lock(self.transaction_id, record_id)
        self.queries = []
        self.rollback_operations = []
        self.locks_held = set()
        return False

    def commit(self):
        # This function returns true if commit succeeds
        self._write_to_transaction_log()
        self._flush_dirty_pages()

        for record_id in self.locks_held:
            self.lock_manager.release_lock(self.transaction_id, record_id)
        self.queries = []
        self.rollback_operations = []
        self.locks_held = set()
        return True

    def _write_to_transaction_log(self):
        # Writes to the log to save all transactions
        with self.log_lock:  
            with open("transaction_log.txt", "a") as log_file:
                for query, table, args in self.queries:
                    log_file.write(f"Transaction {self.transaction_id}: Query {query.__name__}, Table {table.name}, Args {args}\n")
                # Flush log to disk to ensure durability
                os.fsync(log_file.fileno())

    def _flush_dirty_pages(self):
        # Sends modified pages to disk
        for query, table, args in self.queries:
            # Get dirty pages for table affected by query
            dirty_pages = self.buffer_pool.get_dirty_pages_for_table(table)
            for page in dirty_pages:
                page.write_to_disk()
                self.buffer_pool.mark_page_clean(page)

