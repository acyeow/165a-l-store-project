from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# creating grades table
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_transactions = 100
num_threads = 8

# create index on the non primary columns
try:
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

keys = []
records = {}
seed(3562901)

# array of insert transactions
insert_transactions = []

for i in range(number_of_transactions):
    insert_transactions.append(Transaction())

for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    t = insert_transactions[i % number_of_transactions]
    t.add_query(query.insert, grades_table, *records[key])

transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker())
    
for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(insert_transactions[i])



# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()


# After all workers finish, add this debug code
print(f"Page directory contains {len(grades_table.page_directory)} records")
print(f"Index contains keys: {len(grades_table.index.indices.get(0, {}).keys())}")

# After all transaction workers have joined
print("\n--- TRANSACTION SUMMARY ---")
success_count = sum(worker.result for worker in transaction_workers)
print(f"Transactions committed: {success_count}/{number_of_transactions}")

print("\n--- INDEX INTEGRITY CHECK ---")
successful_keys = []
failed_keys = []
for key in keys:
    rids = grades_table.index.locate(0, key)
    if rids:
        successful_keys.append(key)
    else:
        failed_keys.append(key)

print(f"Keys in index: {len(successful_keys)}/{len(keys)}")
if failed_keys:
    print(f"First 5 missing keys: {failed_keys[:5]}")

print(f"\n--- PAGE DIRECTORY CHECK ---")
pg_dir_keys = set()
for rid, record in grades_table.page_directory.items():
    pg_dir_keys.add(record.key)
print(f"Records in page directory: {len(grades_table.page_directory)}")
print(f"Unique keys in page directory: {len(pg_dir_keys)}")


# # Check inserted records using select query in the main thread outside workers
# for key in successful_keys:  # Only check keys that were successfully inserted
#     record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
#     error = False
#     for i, column in enumerate(record.columns):
#         if column != records[key][i]:
#             error = True
#     if error:
#         print('select error on', key, ':', record.columns, ', correct:', records[key])
#     else:
#         pass
# print("Select finished")


# # Wait for workers to finish
# for i in range(num_threads):
#     transaction_workers[i].join()

# # Check table consistency
# print("\nChecking table consistency...")
# grades_table.check_index_consistency()

# # Count total records inserted
# total_inserted = sum(worker.result for worker in transaction_workers)
# print(f"Total records successfully inserted: {total_inserted}")
# print(f"Expected records: {len(keys)}")

# # Check page directory
# print(f"Records in page directory: {len(grades_table.page_directory)}")

# # Check for each key directly
# for key in keys:
#     if grades_table.index.locate(0, key):
#         print(f"Key {key} found in index")
#     else:
#         print(f"Key {key} NOT found in index")
    
#     found = False
#     for rid, record in grades_table.page_directory.items():
#         if record.key == key:
#             found = True
#             break
    
#     if found:
#         print(f"Key {key} found in page directory")
#     else:
#         print(f"Key {key} NOT found in page directory")

db.close()
