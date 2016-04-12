"""
Wrapper for RDD.

Wrapped functions allow entry and exit tracing and keeps perf counts.
"""


# This class includes only functions that are actually called in the impl classes.
# If new RDD functions are called, they must be added here.


import pyspark
from pyspark import RDD

from xframes.traced_object import TracedObject


# noinspection PyPep8Naming,PyProtectedMember
class XRdd(TracedObject):

    def __init__(self, rdd=None, structure_id=None):
        """
        Create a new XRdd.

        Note
        ----
        The zip operation is only allowed on RDDs that have the same number of partitions, and the same
        number of elsments in each partition.  Some operations preserve this property, and others
        do not.  We can still accomplish the zip, but it is more expensive, so it is desirable
        to know when we can safely call zip.  The structure_id is meant for that purpose.
        Normally, the structure_id of an RDD is its RDD id.  If another RDD is derived from
        it, and that one has the same structure, then the derived RDD get's its parent's
        structure id.  Zip operates only on RDDs that share a structure_id.
        """
        self._rdd = rdd
        self.id = rdd.id()
        self.structure_id = structure_id if structure_id else self.id

    @staticmethod
    def is_rdd(rdd):
        return isinstance(rdd, RDD)

    @staticmethod
    def is_dataframe(rdd):
        return isinstance(rdd, pyspark.sql.DataFrame)

    # getters
    def RDD(self):
        return self._rdd

    # actions
    def name(self):
        self._entry()
        res = self._rdd.name()
        return res

    def get_id(self):
        self._entry()
        return self.id

    def get_structure_id(self):
        self._entry()
        return self.structure_id

    def count(self):
        self._entry()
        res = self._rdd.count()
        return res

    def take(self, n):
        self._entry(n=n)
        res = self._rdd.take(n)
        return res

    def takeOrdered(self, num, key=None):
        self._entry(num=num)
        res = self._rdd.takeOrdered(num, key)
        return res

    def collect(self):
        self._entry()
        res = self._rdd.collect()
        return res

    def first(self):
        self._entry()
        res = self._rdd.first()
        return res
        
    def max(self):
        self._entry()
        res = self._rdd.max()
        return res

    def min(self):
        self._entry()
        res = self._rdd.min()
        return res

    def sum(self):
        self._entry()
        res = self._rdd.sum()
        return res

    def mean(self):
        self._entry()
        res = self._rdd.mean()
        return res

    def stdev(self):
        self._entry()
        res = self._rdd.stdev()
        return res

    def sampleStdev(self):
        self._entry()
        res = self._rdd.sampleStdev()
        return res

    def variance(self):
        self._entry()
        res = self._rdd.variance()
        return res

    def sampleVariance(self):
        self._entry()
        res = self._rdd.sampleVariance()
        return res

    def aggregate(self, zeroValue, seqOp, combOp):
        self._entry()
        res = self._rdd.aggregate(zeroValue, seqOp, combOp)
        return res

    def reduce(self, fn):
        self._entry()
        res = self._rdd.reduce(fn)
        return res

    def toDebugString(self):
        self._entry()
        res = self._rdd.toDebugString()
        return res
        
    def persist(self, storage_level):
        self._entry(storage_level=storage_level)
        self._rdd.persist(storage_level)

    def unpersist(self):
        self._entry()
        self._rdd.unpersist()

    def saveAsPickleFile(self, path):
        self._entry(path=path)
        self._rdd.saveAsPickleFile(path)

    def saveAsTextFile(self, path):
        self._entry(path=path)
        self._rdd.saveAsTextFile(path)

    def stats(self):
        self._entry()
        res = self._rdd.stats()
        return res

    # transformations
    def repartition(self, number_of_partitions):
        self._entry()
        res = self._rdd.repartition(number_of_partitions)
        return XRdd(res)

    def map(self, fn, preserves_partitioning=False):
        self._entry(preserves_partitioning=preserves_partitioning)
        res = self._rdd.map(fn, preserves_partitioning)
        return XRdd(res, structure_id=self.structure_id)

    def mapPartitions(self, fn, preserves_partitioning=False):
        self._entry(preserves_partitioning=preserves_partitioning)
        res = self._rdd.mapPartitions(fn, preserves_partitioning)
        return XRdd(res, structure_id=self.structure_id)

    def mapPartitionsWithIndex(self, fn, preserves_partitioning=False):
        self._entry(preserves_partitioning=preserves_partitioning)
        res = self._rdd.mapPartitionsWithIndex(fn, preserves_partitioning)
        return XRdd(res, structure_id=self.structure_id)

    def mapValues(self, fn):
        self._entry()
        res = self._rdd.mapValues(fn)
        return XRdd(res, structure_id=self.structure_id)

    def flatMap(self, fn, preserves_partitioning=False):
        self._entry(preserves_partitioning=preserves_partitioning)
        res = self._rdd.flatMap(fn, preserves_partitioning)
        return XRdd(res, structure_id=self.structure_id)

    def basic_zip(self, other):
        # these are separate so they can have their own tracing
        self._entry()
        return self._rdd.zip(other._rdd)

    def safe_zip(self, other):
        # do the zip operation safely
        self._entry()
        ix_left = self._rdd.zipWithIndex().map(lambda row: (row[1], row[0]))
        ix_right = other._rdd.zipWithIndex().map(lambda row: (row[1], row[0]))
        return ix_left.join(ix_right).sortByKey().values()

    def zip(self, other):
        self._entry()
        if self.structure_id == other.structure_id:
            res = self.basic_zip(other)
            structure_id = self.structure_id
        else:
            res = self.safe_zip(other)
            structure_id = None
        # noinspection PyUnresolvedReferences
        res.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        return XRdd(res, structure_id=structure_id)

    def zipWithIndex(self):
        self._entry()
        res = self._rdd.zipWithIndex()
        return XRdd(res, structure_id=self.structure_id)

    def zipWithUniqueId(self):
        self._entry()
        res = self._rdd.zipWithUniqueId()
        return XRdd(res, structure_id=self.structure_id)

    def filter(self, fn):
        self._entry()
        res = self._rdd.filter(fn)
        return XRdd(res)

    def distinct(self):
        self._entry()
        res = self._rdd.distinct()
        return XRdd(res)

    def keys(self):
        self._entry()
        res = self._rdd.keys()
        return XRdd(res, structure_id=self.structure_id)
        
    def values(self):
        self._entry()
        res = self._rdd.values()
        return XRdd(res, self.structure_id)
        
    def sample(self, with_replacement, fraction, seed=None):
        self._entry(with_replacement=with_replacement, fraction=fraction, seed=seed)
        res = self._rdd.sample(with_replacement, fraction, seed)
        return XRdd(res)

    def union(self, other):
        self._entry()
        res = self._rdd.union(other._rdd)
        return XRdd(res)

    def groupByKey(self):
        self._entry()
        res = self._rdd.groupByKey()
        return XRdd(res)

    def cartesian(self, right):
        self._entry()
        res = self._rdd.cartesian(right._rdd)
        return XRdd(res)
        
    def join(self, right):
        self._entry()
        res = self._rdd.join(right._rdd)
        return XRdd(res)
        
    def leftOuterJoin(self, right):
        self._entry()
        res = self._rdd.leftOuterJoin(right._rdd)
        return XRdd(res)
        
    def rightOuterJoin(self, right):
        self._entry()
        res = self._rdd.rightOuterJoin(right._rdd)
        return XRdd(res)
        
    def fullOuterJoin(self, right):
        self._entry()
        res = self._rdd.fullOuterJoin(right._rdd)
        return XRdd(res)

    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        self._entry()
        res = self._rdd.sortBy(keyfunc, ascending, numPartitions)
        return XRdd(res)

    def sortByKey(self, ascending=True, numPartitions=None, keyfunc=lambda x: x):
        self._entry()
        res = self._rdd.sortByKey(ascending, numPartitions, keyfunc)
        return XRdd(res)
