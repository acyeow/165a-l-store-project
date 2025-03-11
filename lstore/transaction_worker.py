from threading import Thread
import traceback
import time

class TransactionWorker:
    """
    Manage and execute multiple transactions concurrently using threads
    """

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
        self.transactions.append(t)

    def run(self):
        """
        Starts execution of all transactions in a separate thread
        """
        print(f"Starting worker thread with {len(self.transactions)} transactions")
        self.thread = Thread(target=self.__run)
        self.thread.daemon = True  # Make thread daemon so it doesn't block program exit
        self.thread.start()

    def join(self):
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
            transaction_count = len(self.transactions)
            success_count = 0
            
            for i, transaction in enumerate(self.transactions):
                try:
                    print(f"Starting transaction {i+1}/{transaction_count}")
                    # Make sure transaction has an ID
                    if transaction.transaction_id is None:
                        transaction.transaction_id = id(transaction)
                    
                    # Run the transaction
                    result = transaction.run()
                    
                    # Record the result
                    self.stats.append(result is True)
                    if result is True:
                        success_count += 1
                    
                    print(f"Transaction {i+1} completed with result: {result}")
                except Exception as e:
                    print(f"Error executing transaction {i+1}: {e}")
                    import traceback
                    traceback.print_exc()
                    self.stats.append(False)
            
            self.result = success_count
            print(f"Worker completed, {success_count}/{transaction_count} transactions committed")
            self.completed = True
        except Exception as e:
            print(f"Catastrophic error in worker thread: {e}")
            import traceback
            traceback.print_exc()
            self.completed = True  # Mark as completed even on error