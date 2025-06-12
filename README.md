# L-Store

## Overview

L-Store is a high-performance database system that eliminates the traditional divide between transactional (OLTP) and analytical (OLAP) processing.
This is a reimplementation of the [L-Store research paper](https://arxiv.org/pdf/1601.04084).

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
