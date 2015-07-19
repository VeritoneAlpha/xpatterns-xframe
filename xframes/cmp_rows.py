
#    Row comparison wrapper for ascending or descending comparison.


class CmpRows(object):
    """ Comparison wrapper for a row.

    Rows can be sorted on one or more columns, and each one
    may be ascending or descending.  
    This class wraps the row, remembers the column indexes used for
    comparing the rows, and each ones ascending/descending flag.
    It provides the needed comparison functions for sorting.

    Rows are assumed to be indexable collections of values.
    Values may be any python type that itself is comparable.
    The underlying python comparison functions are used on these values.
    """
    def __init__(self, row, indexes, ascending):
        """ Instantiate a wrapped row. """
        self.row = row
        self.indexes = indexes
        self.ascending = ascending

    def less(self, other):
        """ True if self is less than other.  

        Comparison is reversed when a row is marked descending.
        """
        for index, ascending in zip(self.indexes, self.ascending):
            left = self.row[index]
            right = other.row[index]
            if left < right: return ascending
            if left > right: return not ascending
        return False

    def greater(self, other):
        """ True if self is greater than other.  

        Comparison is reversed when a row is marked descending.
        """
        for index, ascending in zip(self.indexes, self.ascending):
            left = self.row[index]
            right = other.row[index]
            if left > right: return ascending
            if left < right: return not ascending
        return False

    def equal(self, other):
        """ True when self is equal to other.

        Only comparison fields are used in this test.
        """
        for index in self.indexes:
            left = self.row[index]
            right = other.row[index]
            if left > right: return False
            if left < right: return False
        return True
    
    # These are the comparison interface
    def __lt__(self, other):
        return self.less(other)

    def __gt__(self, other):
        return self.greater(other)

    def __eq__(self, other):
        return self.equal(other)

    def __le__(self, other):
        return self.less(other) or self.equal(other)

    def __ge__(self, other):
        return self.greater(other) or self.equal(other)

    def __ne__(self, other):
        return not self.equal(other)
