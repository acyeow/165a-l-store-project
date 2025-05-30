# L-Store: HTAP Database

<div align="center">
  <img src="https://img.shields.io/badge/python-3.7%2B-blue" alt="Python 3.7+">
  <img src="https://img.shields.io/badge/license-Apache%202.0-green" alt="License">
  <img src="https://img.shields.io/badge/storage-column--oriented-orange" alt="Storage">
  <img src="https://img.shields.io/badge/transactions-ACID-red" alt="ACID">
</div>

## 📋 Overview

L-Store is a high-performance database system that eliminates the traditional divide between transactional (OLTP) and analytical (OLAP) processing. Based on the [L-Store research paper](https://arxiv.org/pdf/1601.04084), this implementation delivers:

- Real-time analytics on the latest transactional data
- High throughput for both read and write operations
- Columnar storage with efficient version management
- Multi-version concurrency control without read/write contentions

Unlike traditional systems that require separate engines or data copies for OLTP and OLAP workloads, L-Store uses a novel lineage-based architecture to support both workloads simultaneously in a single unified engine.

## 🚀 Key Features

### Architecture

- **Unified Storage Model**: Single representation for both transactional and analytical workloads
- **Lineage-based Updates**: Contention-free update mechanism over native columnar storage
- **2-Hop Access Guarantee**: Fast point queries with at most 2-hop access to the latest version
- **Lazy Background Merging**: Asynchronous merging process that doesn't block ongoing transactions

### Storage & Indexing

- **Columnar Layout**: Optimized for analytical workloads
- **Base & Tail Pages**: Read-optimized base data with append-only updates
- **Range Partitioning**: Efficient update clustering and merge processing
- **B+ Tree Indexing**: Fast record lookups with efficient version tracking

## 📐 Technical Architecture

### Storage Organization

L-Store uses a unique storage architecture that separates data into:

- **Base Pages**: Compressed, read-optimized pages containing the baseline version of records
- **Tail Pages**: Append-only pages storing updates to base records
- **Page Directory**: Maps record identifiers to physical locations
- **Indirection Column**: Efficiently tracks record versions through forward/backward pointers

### Meta-Data Columns

Each table contains several meta-data columns to support the lineage-based architecture:

- **Indirection Column**: Stores pointers to the latest version of records
- **Schema Encoding Column**: Bitmap indicating which columns have been updated
- **Start Time Column**: Timestamps for tracking record versions
- **Last Updated Time Column**: Populated after merge operations

### Update Mechanism

L-Store's unique update approach:

1. Creates tail records for changed values
2. Preserves the original values in separate tail records
3. Updates the indirection pointer in base records
4. Maintains all versions for historical queries

### Merge Process

The contention-free merge process:

1. Operates only on stable data (committed records)
2. Consolidates base pages with recent updates asynchronously
3. Creates new merged pages without blocking ongoing transactions
4. Updates page directory pointers atomically
5. Applies epoch-based page deallocation after query completion

## 📂 Project Structure

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

## 🔮 Future Directions

- Query optimizer for complex analytical workloads
- Distributed storage and processing capabilities
- Recovery management for system failures
- Extended SQL interface
- Enhanced compression techniques
