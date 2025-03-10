from lstore.table import Table, Record
from lstore.index import Index

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
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        self.queries.append((query, args))
        # use grades_table for aborting
=======
=======
>>>>>>> Stashed changes
        # Initialize transaction ID if not already done
        if not hasattr(self, 'transaction_id') or self.transaction_id is None:
            self.transaction_id = id(self)
        
        # Initialize locks_held if not already done
        if not hasattr(self, 'locks_held'):
            self.locks_held = set()
            
        # Get lock manager from table if not already set
        if not hasattr(self, 'lock_manager') or self.lock_manager is None:
            if hasattr(table, 'database') and table.database is not None:
                self.lock_manager = table.database.lock_manager
        
        # Store the query and args
        self.queries.append((query, table, args))
        
        # Store rollback information if needed
        if hasattr(self, 'rollback_operations') and query.__name__ in ["update", "insert", "delete"]:
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
        if self.buffer_pool is None and hasattr(table, 'database') and table.database is not None:
            self.buffer_pool = table.database.bufferpool
        rids = table.index.locate(table.key, key)
        if not rids or rids[0] not in table.page_directory:
            return None
        record = table.page_directory[rids[0]]
        return record.columns
>>>>>>> Stashed changes

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()
=======
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
>>>>>>> Stashed changes
=======
=======
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
>>>>>>> Stashed changes

    
    def abort(self):
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        #TODO: do roll-back and any other necessary operations
=======
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
>>>>>>> Stashed changes
        return False

    
    def commit(self):
<<<<<<< Updated upstream
        # TODO: commit to database
        return True

=======
        """
        Commit the transaction, making all changes permanent
        """
        print(f"Committing transaction {self.transaction_id}")
=======
        """
        Abort the transaction and roll back any changes.
        """
        print(f"Aborting transaction {self.transaction_id}")
        
        # Release all locks
        if hasattr(self, 'lock_manager') and self.lock_manager:
            for record_id in self.locks_held:
                try:
                    self.lock_manager.release_lock(self.transaction_id, record_id)
                except Exception as e:
                    print(f"Error releasing lock on {record_id}: {e}")
        
        # Clear transaction state
        self.queries = []
        if hasattr(self, 'rollback_operations'):
            self.rollback_operations = []
        self.locks_held = set()
        
        return False
    def commit(self):
        """
        Commit the transaction, making all changes permanent
        """
>>>>>>> Stashed changes
=======

    def abort(self):
        """
        Abort the transaction and roll back any changes.
        """
        print(f"Aborting transaction {self.transaction_id}")
        
        # Release all locks
        if hasattr(self, 'lock_manager') and self.lock_manager:
            for record_id in self.locks_held:
                try:
                    self.lock_manager.release_lock(self.transaction_id, record_id)
                except Exception as e:
                    print(f"Error releasing lock on {record_id}: {e}")
        
        # Clear transaction state
        self.queries = []
        if hasattr(self, 'rollback_operations'):
            self.rollback_operations = []
        self.locks_held = set()
        
        return False
    def commit(self):
        """
        Commit the transaction, making all changes permanent
        """
>>>>>>> Stashed changes
        try:
            # Release all locks
            if self.lock_manager:
                for record_id in self.locks_held:
                    try:
                        self.lock_manager.release_lock(self.transaction_id, record_id)
<<<<<<< Updated upstream
<<<<<<< Updated upstream
                        print(f"Released lock on record {record_id}")
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
                    except Exception as e:
                        print(f"Error releasing lock on {record_id}: {e}")
        except Exception as e:
            print(f"Error releasing locks during commit: {e}")
<<<<<<< Updated upstream
<<<<<<< Updated upstream
            
=======
                
>>>>>>> Stashed changes
=======
                
>>>>>>> Stashed changes
        # Clear transaction state
        self.queries.clear()
        self.rollback_operations.clear()
        self.locks_held.clear()
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        self._deleted_records.clear()
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
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
>>>>>>> Stashed changes
