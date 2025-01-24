"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially
        self.table = table
        self.indices = [{} for _ in range(table.num_columns)]

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # If column is out of range or no index exists for the column, return empty list
        if column >= len(self.indices) or self.indices[column] is None:
            return []
        # Return the list of RIDs for the given value in the column, empty list if value not found
        return self.indices[column].get(value, [])

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # If column is out of range or no index exists for the column, return empty list
        if column >= len(self.indices) or self.indices[column] is None:
            return []
        # Return the list of RIDs for the given range in the column, empty list if no values in the range
        return [rid for value, rid in self.indices[column].items() if begin <= value <= end]

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        # If column is out of range, do nothing
        if column_number >= len(self.indices):
            return
        # Create index for the column
        for rid, record in self.table.page_directory.items():
            value = record.columns[column_number]
            if value not in self.indices[column_number]:
                self.indices[column_number][value] = []
            self.indices[column_number][value].append(rid)
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        # If column is out of range, do nothing
        if column_number >= len(self.indices):
            return
        # Drop index for the column
        self.indices[column_number] = None
