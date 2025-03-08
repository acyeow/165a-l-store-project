from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
from lstore.db import LockManager
import os
from threading import Lock

class Transaction:
    def __init__(self, transaction_id, buffer_pool, lock_manager):
        self.transaction_id = transaction_id  # Unique ID for transaction
        self.queries = []  # List to store queries and their arguments
        self.rollback_operations = []  # List of tuples to store operations for rollback
        self.buffer_pool = buffer_pool  # Reference to buffer pool
        self.lock_manager = lock_manager  # Reference to lock manager
        self.locks_held = set()  # Set to track locks held by this transaction
        self.mutex = Lock()  # Mutex Lock for thread-safe log writes
        self._deleted_records = {} # Deleted records for rollback

    def add_query(self, query, table, *args):
        with self.mutex:
            # Store querying changes and its arguments as well as current state for potential rollback
            self.queries.append((query, table, args))
            if query.__name__ in ["update", "insert", "delete"]:
                self.rollback_operations.append((self._get_rollback_operation(query, table, args), args))

    # Helper function for possible rollback
    def _get_rollback_operation(self, query, table, args):
        # Store the operations
        if query.__name__ == "insert":
            return lambda key: Query(table).delete(key)
        elif query.__name__ == "update":
            return lambda key: self._restore_previous_version(table, key)
        elif query.__name__ == "delete":
            return lambda key: self._restore_deleted_record(table, key)
        return lambda *args: None

    # Helper function for update rollback
    def _restore_previous_version(self, table, key):
        with self.mutex:
            columns = self._get_record_columns(table, key)
            if columns is not None:
                Query(table).update(key, *columns)


    # Helper function for delete rollback
    def _restore_deleted_record(self, table, key):
        with self.mutex:
            if key in self._deleted_records:
                Query(table).insert(*self._deleted_records[key])

    # Get the columns of a record given its primary key.
    def _get_record_columns(self, table, key):
        rids = table.index.locate(table.key, key)
        if not rids or rids[0] not in table.page_directory:
            return None
        base_location = table.page_directory[rids[0]]
        page_id, slot = base_location
        base_page = self.buffer_pool.get_page(page_id, table.name, table.num_columns)
        return [base_page["columns"][i][slot] for i in range(table.num_columns)]

    def run(self):
        with self.mutex:
            # Get locks, abort if fail
            for query, table, args in self.queries:
                record_id = args[0]  # Assuming first argument is record ID
                if not self.lock_manager.acquire_lock(self.transaction_id, record_id):
                    self.abort()
                    return False

            # Do queries, abort if fail
            for query, table, args in self.queries:
                # Store the query if it is delete
                if query.__name__ == "delete":
                    columns = self._get_record_columns(table, args[0])
                    if columns is not None:
                        self._deleted_records[args[0]] = columns

                # Store the query if it is insert
                elif query.__name__ == "insert":
                    self._deleted_records[args[0]] = args

                result = query(*args)
                if result is False:
                    self.abort()
                    return False
            return self.commit()

    def abort(self):
        # This function returns false if something is aborted
        # Roll back changes by executing rollback operations in reverse order
        with self.mutex:
            for operation, args in reversed(self.rollback_operations):
                operation(args[0])

            for record_id in self.locks_held:
                self.lock_manager.release_lock(self.transaction_id, record_id)

            self.queries.clear()
            self.rollback_operations.clear()
            self.locks_held.clear()
            self._deleted_records.clear()
            return False

    def commit(self):
        # This function returns true if commit succeeds
        with self.mutex:
            self._write_to_transaction_log()
            self._flush_dirty_pages()

            for record_id in self.locks_held:
                self.lock_manager.release_lock(self.transaction_id, record_id)
            self.queries.clear()
            self.rollback_operations.clear()
            self.locks_held.clear()
            self._deleted_records.clear()
            return True

    def _write_to_transaction_log(self):
        # Writes to the log to save all transactions
        with self.mutex:
            with open("transaction_log.txt", "a") as log_file:
                for query, table, args in self.queries:
                    log_file.write(f"Transaction {self.transaction_id}: Query {query.__name__}, Table {table.name}, Args {args}\n")
                # Flush log to disk to ensure durability
                os.fsync(log_file.fileno())

    def _flush_dirty_pages(self):
        # Flush all dirty pages if safe
        with self.mutex:
            self.buffer_pool.reset()
