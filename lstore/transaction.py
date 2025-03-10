from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
from lstore.db import LockManager
import os
from threading import Lock


class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self, transaction_id=None, buffer_pool=None, lock_manager=None):
        self.transaction_id = transaction_id if transaction_id is not None else id(self)  # Unique ID for transaction
        self.queries = []  # List to store queries and their arguments
        self.rollback_operations = []  # List of tuples to store operations for rollback
        self.buffer_pool = buffer_pool  # Reference to buffer pool
        self.lock_manager = lock_manager  # Reference to lock manager
        self.locks_held = set()  # Set to track locks held by this transaction
        self.mutex = Lock()  # Mutex Lock for thread-safe log writes
        self._deleted_records = {} # Deleted records for rollback

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, table, *args):
        # Initialize transaction ID if not already done
        if not hasattr(self, "transaction_id") or self.transaction_id is None:
            self.transaction_id = id(self)

        # Initialize locks_held if not already done
        if not hasattr(self, "locks_held"):
            self.locks_held = set()

        # Get lock manager from table if not already set
        if not hasattr(self, "lock_manager") or self.lock_manager is None:
            if hasattr(table, "database") and table.database is not None:
                self.lock_manager = table.database.lock_manager

        # Store the query and args
        self.queries.append((query, table, args))

        # Store rollback information if needed
        if hasattr(self, "rollback_operations") and query.__name__ in [
            "update",
            "insert",
            "delete",
        ]:
            # Implement rollback operation storage
            pass

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
        if (
            self.buffer_pool is None
            and hasattr(table, "database")
            and table.database is not None
        ):
            self.buffer_pool = table.database.bufferpool
        rids = table.index.locate(table.key, key)
        if not rids or rids[0] not in table.page_directory:
            return None
        record = table.page_directory[rids[0]]
        return record.columns

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        print(f"TX{self.transaction_id}: Running {len(self.queries)} queries")
        
        # Initialize transaction components
        if not hasattr(self, 'transaction_id') or self.transaction_id is None:
            self.transaction_id = id(self)
        
        if not self.queries:
            return False
        
        # Get lock manager and initialize locks_held if needed
        if not hasattr(self, 'lock_manager') or self.lock_manager is None:
            try:
                query, table, args = self.queries[0]
                if hasattr(table, 'database') and table.database is not None:
                    self.lock_manager = table.database.lock_manager
            except Exception as e:
                print(f"TX{self.transaction_id}: Lock manager init error")
        
        if not hasattr(self, 'locks_held'):
            self.locks_held = set()
        
        try:
            # Execute all queries
            for query, table, args in self.queries:
                try:
                    result = query(*args)
                    if result is False:
                        print(f"TX{self.transaction_id}: Query failed")
                        return self.abort()
                except Exception as e:
                    print(f"TX{self.transaction_id}: Query error: {str(e)[:50]}...")
                    return self.abort()
            
            print(f"TX{self.transaction_id}: Committing")
            return self.commit()
        except Exception as e:
            print(f"TX{self.transaction_id}: Execution error")
            return self.abort()

    def commit(self):
        """
        Commit the transaction, making all changes permanent
        """
        try:
            # Release all locks
            if self.lock_manager:
                for record_id in self.locks_held:
                    try:
                        self.lock_manager.release_lock(self.transaction_id, record_id)
                    except Exception as e:
                        print(f"Error releasing lock on {record_id}: {e}")
        except Exception as e:
            print(f"Error releasing locks during commit: {e}")

        # Clear transaction state
        self.queries.clear()
        self.rollback_operations.clear()
        self.locks_held.clear()
        return True

    def abort(self):
        """
        Abort the transaction and roll back any changes.
        """
        print(f"Aborting transaction {self.transaction_id}")

        # Release all locks
        if hasattr(self, "lock_manager") and self.lock_manager:
            for record_id in self.locks_held:
                try:
                    self.lock_manager.release_lock(self.transaction_id, record_id)
                except Exception as e:
                    print(f"Error releasing lock on {record_id}: {e}")

        # Clear transaction state
        self.queries = []
        if hasattr(self, "rollback_operations"):
            self.rollback_operations = []
        self.locks_held = set()

        return False

    def _write_to_transaction_log(self):
        # Writes to the log to save all transactions
        with self.mutex:
            with open("transaction_log.txt", "a") as log_file:
                for query, table, args in self.queries:
                    log_file.write(
                        f"Transaction {self.transaction_id}: Query {query.__name__}, Table {table.name}, Args {args}\n"
                    )
                # Flush log to disk to ensure durability
                os.fsync(log_file.fileno())

    def _flush_dirty_pages(self):
        # Flush all dirty pages if safe
        with self.mutex:
            self.buffer_pool.reset()
