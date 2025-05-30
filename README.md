# L-Store: Hybrid Transactional/Analytical Database

<div align="center">
  <img src="https://img.shields.io/badge/python-3.7%2B-blue" alt="Python 3.7+">
  <img src="https://img.shields.io/badge/license-Apache%202.0-green" alt="License">
  <img src="https://img.shields.io/badge/storage-column--oriented-orange" alt="Storage">
  <img src="https://img.shields.io/badge/transactions-ACID-red" alt="ACID">
</div>

## Overview

L-Store is a high-performance, hybrid transactional/analytical processing (HTAP) database system based on the [L-Store research paper](https://arxiv.org/pdf/1601.04084). This implementation provides a database that excels at both OLTP (Online Transaction Processing) and OLAP (Online Analytical Processing) workloads, eliminating the traditional separation between transactional and analytical systems.

## Key Features

- **Hybrid OLTP/OLAP Architecture**: Single system optimized for both transactional and analytical workloads
- **Multi-Version Concurrency Control**: Historical query support with efficient version management
- **Column-Oriented Storage**: Optimized for analytical queries with column-wise data organization
- **ACID Transactions**: Full support for ACID properties with 2-phase locking
- **Efficient Data Updates**: Update-in-place functionality without blocking analytical queries
- **Buffer Pool Management**: Memory-optimized page access with LRU eviction policy
- **B+ Tree Indexing**: Fast record lookups with dynamic index creation

## Technical Architecture

### Storage Model

L-Store uses a unique storage architecture combining:

- **Page Ranges**: Logical grouping of base and tail pages
- **Base Pages**: Compressed, read-optimized, columnar storage for baseline record data
- **Tail Pages**: Append-only, update storage that maintains historical versions
- **Indirection**: Efficient record versioning through indirection pointers

### Transaction Management

The implementation includes a robust transaction system that provides:

- **Concurrency Control**: Thread-safe operations with fine-grained locking
- **Transaction Workers**: Multi-threaded transaction execution
- **Lock Manager**: Resource management to prevent conflicts and deadlocks
- **Rollback Capability**: Automatic transaction abort with state restoration

### Query Engine

L-Store supports a variety of query operations:

- `SELECT`: Retrieve records with filtering and projection
- `INSERT`: Add new records with automatic key validation
- `UPDATE`: Modify existing records with versioning
- `DELETE`: Remove records while maintaining referential integrity
- `SUM`: Aggregate data across records with version awareness

### Performance Optimization

- **Bufferpool**: In-memory caching to minimize disk I/O
- **Merge Operation**: Background process to consolidate updates for read optimization
- **Indexing**: Dynamic index creation on any column
- **Version Selection**: Efficient access to historical data

## Implementation Details

### Core Components

- **Table**: Manages records, page allocation and schema
- **Page**: Fixed-size storage unit with logical organization
- **Index**: B+ Tree implementation for efficient lookups
- **Query**: Operations interface for data manipulation
- **Transaction**: ACID-compliant multi-operation units of work
- **Bufferpool**: Memory management for page data

### Technical Highlights

- **Copy-on-Write**: Non-destructive updates for analytical consistency
- **Lazy Merging**: Background consolidation of updates for read performance
- **Lineage-based Versioning**: Efficient tracking of record update history
- **Adaptive Page Management**: Dynamic allocation of storage resources
- **Thread-Safe Operations**: Lock-based concurrency with deadlock prevention

## Getting Started

### Prerequisites

```bash
python 3.7+
```

### Installation

```bash
pip install -r requirements.txt
```

### Basic Usage

```python
from lstore.db import Database
from lstore.query import Query

# Create database instance
db = Database()
db.open('./mydb')

# Create table with 5 columns, primary key at index 0
grades_table = db.create_table('Grades', 5, 0)

# Create query object
query = Query(grades_table)

# Insert a record
query.insert(1, 90, 85, 95, 100)

# Select a record
record = query.select(1, 0, [1, 1, 1, 1, 1])[0]
print(record)

# Update a record
query.update(1, None, 92, None, None, None)

# Aggregate records
sum_result = query.sum(1, 50, 1)  # Sum of column 1 for keys 1-50

# Close database
db.close()
```

### Transaction Support

```python
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

# Create transaction
transaction = Transaction()

# Add operations to transaction
transaction.add_query(query.select, grades_table, 1, 0, [1, 1, 1, 1, 1])
transaction.add_query(query.update, grades_table, 1, None, 95, None, None, None)

# Create and run transaction worker
worker = TransactionWorker()
worker.add_transaction(transaction)
worker.run()
worker.join()
```

## Project Structure

```
lstore/
├── __init__.py        # Package initialization
├── config.py          # Configuration parameters
├── db.py              # Database management and bufferpool
├── index.py           # B+ Tree indexing
├── page.py            # Page structure and operations
├── page_range.py      # Page range management
├── query.py           # Query operations
├── table.py           # Table and record management
├── transaction.py     # Transaction handling
└── transaction_worker.py  # Concurrent transaction execution
```

## Future Enhancements

- Query optimizer for complex analytical workloads
- Distributed storage and processing capabilities
- Recovery management for system failures
- Extended SQL interface
- Enhanced compression techniques
