"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

# B-Tree Implementation for indexing
# Node structure: leaf status, keys, and children
class BTreeNode:
	def __init__(self, leaf=False):
		self.leaf = leaf
		self.keys = []
		self.children = []

# B-Tree
class BTree:
	def __init__(self, t):
		# Start with an empty node, root is the root node and t is the minimum degree of the tree
		self.root = BTreeNode(True)
		self.t = t

	# Search operation
	def search(self, node, key):
		i = 0

		# Search keys until the key is found
		while (i < len(node.keys)) and (node.keys[i] < key):
			i += 1

		# return the node if the key matches
		if (i < len(node.keys)) and (node.keys[i] == key):
			return node.keys[i]

		# If the key does not exist and the node is a leaf, the return none
		if node.leaf:
			return None

		# keep searching deeper if the node isn't found
		return self.search(node.children[i], key)

	# Traverse operation
	def traverse(self, node, begin=None, end=None, result=[]):
		# if the node does not exist, then return the root
		if node is None:
			return result

        # traverse through the nodes from left to right
		for i in range(len(node.keys)):
			if not node.leaf:
				self.traverse(node.children[i], begin, end, result)

			if (begin is None or node.keys[i] >= begin) and (end is None or node.keys[i] <= end):
				result.append(node.keys[i])

		if not node.leaf:
			self.traverse(node.children[-1], begin, end, result)
		return result

	# Insertion operation
	def insert(self, key):
		# check the node to insert into and split it if full
		root = self.root

		if len(root.keys) == (self.t * 2) - 1:
			new = BTreeNode(False)
			new.children.append(root)
			self.split(new, 0, root)
			self.root = new

		# insert non full
		self.insert_non_full(self.root, key)


	# Insertion operation for non-full nodes
	def insert_non_full(self, node, key):
		i = len(node.keys) - 1

		if node.leaf:
			node.keys.append(None)
			while i >= 0 and key < node.keys[i]:
				node.keys[i + 1] = node.keys[i]
				i -= 1
			node.keys[i + 1] = key

		else:
			while i >= 0 and key < node.keys[i]:
				i -= 1
			i += 1

			if len(node.children[i].keys) == (2 * self.t) - 1:
				self.split(node, i, node.children[i])
				if key > node.keys[i]:
					i += 1

			self.insert_non_full(node.children[i], key)


	# Split function for splitting full nodes
	def split(self, parent, i, child):
		# split the keys from the middle
		t = self.t
		new = BTreeNode(child.leaf)
		parent.keys.insert(i, child.keys[t - 1])
		parent.children.insert(i + 1, new)
		new.keys = child.keys[t:(2 * t) - 1]
		child.keys = child.keys[0:t - 1]

		if not child.leaf:
			new.children = child.children[t:(2 * t)]
			child.children = child.children[0:t]

	# Deletion operation
	def delete(self, key):
		if not self.root:
			return

		self.delete_internal(self.root, key)

		if not self.root.keys and not self.root.leaf:
			self.root = self.root.children[0]

	# Helper function for deletion operation
	def delete_internal(self, node, key):
		i = 0

		while i < len(node.keys) and key > node.keys[i]:
			i += 1

		if i < len(node.keys) and node.keys[i] == key:
			if node.leaf:
				node.keys.pop(i)

			else:
				node.keys[i] = self.get_predecessor(node.children[i])
				self.delete_internal(node.children[i], node.keys[i])

		elif not node.leaf:
			self.delete_internal(node.children[i], key)

	# Operation for getting predecessor node
	def get_predecessor(self, node):
		# keep going down the tree until you hit the leaf, then return the node before the leaf
		while not node.leaf:
			node = node.children[-1]

		return node.keys[-1]

class Index:

    def __init__(self, table, t = 3):
        # One index for each table. All are empty initially
        self.table = table
        self.t = t
        self.indices = [{} for _ in range(table.num_columns)]

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # If column is out of range or no index exists for the column, return empty list
        if column >= len(self.indices) or self.indices[column] is None:
            return []
        # Return the list of RIDs for the given value in the column, empty list if value not found
        return self.indices[column].search(self.indices[column].root, value)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # If column is out of range or no index exists for the column, return empty list
        if column >= len(self.indices) or self.indices[column] is None:
            return []
        # Return the list of RIDs for the given range in the column, empty list if no values in the range
        return self.indices[column].traverse(self.indices[column].root, begin, end, [])

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        # If column is out of range, do nothing
        if column_number >= len(self.indices):
            return
        # Create a B-Tree
        self.indices[column_number] = BTree(self.t)
        # Create index for the column
        for rid, record in self.table.page_directory.items():
            value = record.columns[column_number]
            self.indices[column_number].insert(value)
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        # If column is out of range, do nothing
        if column_number >= len(self.indices):
            return
        # Drop index for the column
        self.indices[column_number] = None

	# Delete a value from the index
	def delete(self, column, value):
        if column < len(self.indices) and self.indices[column] is not None:
			self.indices[column].delete(value)