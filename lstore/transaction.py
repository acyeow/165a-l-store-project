from lstore.table import Table, Record
from lstore.index import Index
import os
from threading import Lock

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, transaction_id=None, buffer_pool=None, lock_manager=None):
        self.transaction_id = transaction_id if transaction_id is not None else id(self)
        self.queries = []
        self.rollback_operations = []
        self.buffer_pool = buffer_pool
        self.lock_manager = lock_manager
        self.locks_held = set()
        self.mutex = Lock()
        self._deleted_records = {}

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # Ensure transaction_id is set
        if not hasattr(self, 'transaction_id') or self.transaction_id is None:
            self.transaction_id = id(self)
        
        # Check if there are any queries to run
        if not self.queries:
            return False
        
        # Get lock manager from the first query's table if not already set
        if not hasattr(self, 'lock_manager') or self.lock_manager is None:
            try:
                # Extract table from the queries list based on your storage format
                query, args = self.queries[0]
                # Assuming the table is the first argument
                if args and hasattr(args[0], 'database'):
                    first_table = args[0]
                    if hasattr(first_table, 'database') and first_table.database is not None:
                        self.lock_manager = first_table.database.lock_manager
            except Exception as e:
                print(f"Error initializing lock manager: {e}")
        
        # Initialize locks_held if not already
        if not hasattr(self, 'locks_held'):
            self.locks_held = set()
        
        try:
            # Execute all queries
            for query, args in self.queries:
                try:
                    result = query(*args)
                    if result is False:
                        return self.abort()
                except Exception as e:
                    print(f"Error executing query: {e}")
                    return self.abort()
            
            return self.commit()
        except Exception as e:
            print(f"Error in transaction execution: {e}")
            return self.abort()
    
    def abort(self):
        """
        Abort the transaction and roll back any changes
        """
        print(f"Aborting transaction {self.transaction_id}")
        try:
            # Release all locks first
            if self.lock_manager:
                for record_id in self.locks_held:
                    try:
                        self.lock_manager.release_lock(self.transaction_id, record_id)
                        print(f"Released lock on record {record_id}")
                    except Exception as e:
                        print(f"Error releasing lock on {record_id}: {e}")
        except Exception as e:
            print(f"Error releasing locks during abort: {e}")
        
        # Clear transaction state
        self.queries.clear()
        self.rollback_operations.clear()
        self.locks_held.clear()
        self._deleted_records.clear()
        return False

    def commit(self):
        """
        Commit the transaction, making all changes permanent
        """
        print(f"Committing transaction {self.transaction_id}")
        try:
            # Release all locks
            if self.lock_manager:
                for record_id in self.locks_held:
                    try:
                        self.lock_manager.release_lock(self.transaction_id, record_id)
                        print(f"Released lock on record {record_id}")
                    except Exception as e:
                        print(f"Error releasing lock on {record_id}: {e}")
        except Exception as e:
            print(f"Error releasing locks during commit: {e}")
            
        # Clear transaction state
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
