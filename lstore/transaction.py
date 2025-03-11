from lstore.table import Table, Record
from lstore.index import Index
import os

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        pass

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
        with self.mutex:
            # Ensure transaction_id is set
            if self.transaction_id is None:
                self.transaction_id = id(self)
            
            # Ensure buffer_pool and lock_manager are available
            if not self.queries:
                print(f"Transaction {self.transaction_id}: No queries to execute")
                return False
                
            if self.buffer_pool is None or self.lock_manager is None:
                _, first_table, _ = self.queries[0]
                if self.buffer_pool is None and hasattr(first_table, 'database'):
                    self.buffer_pool = first_table.database.bufferpool
                if self.lock_manager is None and hasattr(first_table, 'database'):
                    self.lock_manager = first_table.database.lock_manager
                    
            if self.lock_manager is None:
                print(f"Transaction {self.transaction_id}: Lock manager not available")
                return False
            
            try:    
                # Get locks, abort if fail
                for query, table, args in self.queries:
                    record_id = args[0]  # Assuming first argument is record ID
                    operation = query.__name__
                    
                    if not self.lock_manager.acquire_lock(self.transaction_id, record_id, operation):
                        print(f"Transaction {self.transaction_id}: Failed to acquire {operation} lock on {record_id}")
                        self.abort()
                        return False
                    self.locks_held.add(record_id)
                
                # Execute all queries
                for i, (query, table, args) in enumerate(self.queries):
                    # Execute the query
                    result = query(*args)
                    
                    if result is False:
                        print(f"Transaction {self.transaction_id}: Query {i+1} failed")
                        self.abort()
                        return False
                
                # All queries succeeded, commit
                return self.commit()
                
            except Exception as e:
                print(f"Transaction {self.transaction_id}: Error during execution: {e}")
                import traceback
                traceback.print_exc()
                self.abort()
                return False

    
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
