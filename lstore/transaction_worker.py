from threading import Thread
import traceback
import time

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

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

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        """
        Starts execution of all transactions in a separate thread
        """
        print(f"Starting worker thread with {len(self.transactions)} transactions")
        self.thread = Thread(target=self.__run)
        self.thread.daemon = True  # Make thread daemon so it doesn't block program exit
        self.thread.start()

    """
    Waits for the worker to finish
    """
    def join(self):
        pass

    def __run(self):
        """
        Private method to execute all transactions and record their results
        """
        try:
            print(f"Worker started, processing {len(self.transactions)} transactions")
            
            for i, transaction in enumerate(self.transactions):
                try:
                    print(f"Starting transaction {i+1}/{len(self.transactions)}")
                    
                    # Ensure transaction has an ID
                    if not hasattr(transaction, 'transaction_id') or transaction.transaction_id is None:
                        transaction.transaction_id = id(transaction)
                    
                    # Run the transaction
                    result = transaction.run()
                    
                    # Record the result
                    self.stats.append(result)
                    
                    print(f"Transaction {i+1} completed with result: {result}")
                except Exception as e:
                    print(f"Error executing transaction {i+1}: {e}")
                    import traceback
                    traceback.print_exc()
                    self.stats.append(False)
                    
                    # Important: Make sure locks are released even if transaction fails
                    if hasattr(transaction, 'abort'):
                        transaction.abort()
            
            # Calculate number of successful transactions
            self.result = len([s for s in self.stats if s])
            print(f"Worker completed, {self.result}/{len(self.transactions)} transactions committed")
        except Exception as e:
            print(f"Catastrophic error in worker thread: {e}")
            import traceback
            traceback.print_exc()
