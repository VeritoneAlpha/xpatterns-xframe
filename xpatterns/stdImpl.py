"""
Provides shared implementation functions for stdXArrayImpl and stdXFrameImpl
"""

from pyspark import StorageLevel

# Safe version of zip.
# This requires that left and right RDDs be of the same length, but
#  not the same partition structure
def safeZip(left, right):
    try:
        res = left.zip(right)
    except ValueError:
        ix_left = left.zipWithIndex().map(lambda row: (row[1], row[0]))
        ix_right = right.zipWithIndex().map(lambda row: (row[1], row[0]))
        res = ix_left.join(ix_right)
        res = res.sortByKey()
        res = res.map(lambda kv: kv[1], preservesPartitioning=True)
    res.persist(StorageLevel.MEMORY_AND_DISK)
    return res

def infer_type_of_list(data):
    """
    Look through a list and get its data type.
    Use the first type, and check to make sure the rest are of that type.
    Missing values are skipped.
    """
    candidate = None
    for d in data:
        if d is None: continue
        d_type = type(d)
        if candidate is None: candidate = d_type
        if d_type != candidate: 
            raise TypeError('mixed types in list: {} {}'.format(d_type, candidate))
    return candidate

class StdImpl:
    pass
