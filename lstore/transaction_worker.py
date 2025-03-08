from threading import Thread
from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:
    """
    Manage and execute multiple transactions concurrently using threads
    """

    def __init__(self, transactions=[]):
        """
        Creates a TransactionWorker object

        transactions (list): A list of transactions to be executed by this worker
        """
        self.stats = []  # List to store  result of each transaction (True if committed, False if aborted)
        self.transactions = transactions  # List of transactions to be executed
        self.result = 0  # Number of transactions that committed successfully
        self.thread = None  # Thread object for running transactions concurrently

    def add_transaction(self, t):
        """
        Adds a transaction to list of transactions to be executed

        t is the transaction to be added
        """
        self.transactions.append(t)

    def run(self):
        """
        Starts execution of all transactions in a separate thread
        """
        self.thread = Thread(target=self.__run)  # Create a thread to run transactions
        self.thread.start()  

    def join(self):
        """
        Waits for worker thread to finish execution
        """
        if self.thread:
            self.thread.join()  # Wait for thread to complete

    def __run(self):
        """
        Private method to execute all transactions and record their results
        """
        for transaction in self.transactions:
            # Execute each transaction and record whether it committed or aborted
            self.stats.append(transaction.run())
        # Calculate number of transactions that committed successfully
        self.result = len(list(filter(lambda x: x, self.stats)))
