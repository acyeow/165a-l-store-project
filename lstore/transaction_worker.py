<<<<<<< Updated upstream
from lstore.table import Table, Record
from lstore.index import Index
=======
from threading import Thread
import traceback
import time
>>>>>>> Stashed changes

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

<<<<<<< Updated upstream
    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
=======
    def __init__(self, transactions=[]):
        """
        Creates a TransactionWorker object
        """
        self.stats = []  # List to store result of each transaction
        self.transactions = transactions.copy()  # Copy the list to avoid reference issues
        self.result = 0  # Number of transactions that committed successfully
        self.thread = None  # Thread object for running transactions
        self.completed = False  # Flag to track completion

    def add_transaction(self, t):
        """
        Adds a transaction to list of transactions to be executed
        """
>>>>>>> Stashed changes
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
<<<<<<< Updated upstream
        pass
        # here you need to create a thread and call __run
    
=======
        """
        Starts execution of all transactions in a separate thread
        """
        print(f"Starting worker thread with {len(self.transactions)} transactions")
        self.thread = Thread(target=self.__run)
        self.thread.daemon = True  # Make thread daemon so it doesn't block program exit
        self.thread.start()
>>>>>>> Stashed changes

    """
    Waits for the worker to finish
    """
    def join(self):
<<<<<<< Updated upstream
        pass


    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

=======
        """
        Waits for worker thread to finish execution with timeout
        """
        if self.thread:
            print("Waiting for worker thread to complete...")
            start_time = time.time()
            max_wait = 10  # Maximum wait time in seconds
            
            while self.thread.is_alive() and time.time() - start_time < max_wait:
                self.thread.join(0.5)  # Check every 0.5 seconds
                print(f"Worker still running... ({time.time() - start_time:.1f}s)")
            
            if self.thread.is_alive():
                print("WARNING: Thread did not complete within timeout")
            else:
                print("Worker thread completed successfully")

    def __run(self):
        """
        Private method to execute all transactions and record their results
        """
        try:
            print(f"Worker started, processing {len(self.transactions)} transactions")
<<<<<<< Updated upstream
<<<<<<< Updated upstream
            transaction_count = len(self.transactions)
            success_count = 0
            
            for i, transaction in enumerate(self.transactions):
                try:
                    print(f"Starting transaction {i+1}/{transaction_count}")
                    # Make sure transaction has an ID
                    if transaction.transaction_id is None:
=======
=======
>>>>>>> Stashed changes
            
            for i, transaction in enumerate(self.transactions):
                try:
                    print(f"Starting transaction {i+1}/{len(self.transactions)}")
                    
                    # Ensure transaction has an ID
                    if not hasattr(transaction, 'transaction_id') or transaction.transaction_id is None:
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
                        transaction.transaction_id = id(transaction)
                    
                    # Run the transaction
                    result = transaction.run()
                    
                    # Record the result
<<<<<<< Updated upstream
<<<<<<< Updated upstream
                    self.stats.append(result is True)
                    if result is True:
                        success_count += 1
=======
                    self.stats.append(result)
>>>>>>> Stashed changes
=======
                    self.stats.append(result)
>>>>>>> Stashed changes
                    
                    print(f"Transaction {i+1} completed with result: {result}")
                except Exception as e:
                    print(f"Error executing transaction {i+1}: {e}")
                    import traceback
                    traceback.print_exc()
                    self.stats.append(False)
<<<<<<< Updated upstream
<<<<<<< Updated upstream
            
            self.result = success_count
            print(f"Worker completed, {success_count}/{transaction_count} transactions committed")
            self.completed = True
=======
=======
>>>>>>> Stashed changes
                    
                    # Important: Make sure locks are released even if transaction fails
                    if hasattr(transaction, 'abort'):
                        transaction.abort()
            
            # Calculate number of successful transactions
            self.result = len([s for s in self.stats if s])
            print(f"Worker completed, {self.result}/{len(self.transactions)} transactions committed")
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
        except Exception as e:
            print(f"Catastrophic error in worker thread: {e}")
            import traceback
            traceback.print_exc()
<<<<<<< Updated upstream
<<<<<<< Updated upstream
            self.completed = True  # Mark as completed even on error
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
