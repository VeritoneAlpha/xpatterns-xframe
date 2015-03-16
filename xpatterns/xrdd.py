"""
Wrapper for RDD.

Wrapped functions allow entry and exit tracing and keeps perf counts.
"""

# This class includes only functions that are actually called in the impl classes.
# If new RDD functions are called, they must be added here.

import inspect

from pyspark.sql import *

class XRdd:
    entry_trace = False
    exit_trace = False
    perf_count = None

    def __init__(self, rdd):
        self._entry()
        self.rdd = rdd
        self._exit()

    def _entry(self, *args):
        """ Trace function entry. """
        stack = inspect.stack()
        caller = stack[1]
        called_by = stack[2]
        if XRdd.entry_trace:
            print 'enter RDD', caller[3], args, 'called by', called_by[3]
        if XRdd.perf_count is not None:
            my_fun = caller[3]
            if not my_fun in XRdd.perf_count:
                XRdd.perf_count[my_fun] = 0
            XRdd.perf_count[my_fun] += 1

    def _exit(self, *args):
        """ Trace function exit. """
        if XRdd.exit_trace:
            print 'exit RDD', inspect.stack()[1][3], args

    @classmethod
    def set_trace(cls, entry_trace=None, exit_trace=None):
        cls.entry_trace = entry_trace or cls.entry_trace
        cls.exit_trace = exit_trace or cls.exit_trace

    @classmethod
    def set_perf_count(cls, enable=True):
        if enable:
            cls.perf_count = {}
        else:
            cls.perf_count = None

    @classmethod
    def get_perf_count(cls):
        return cls.perf_count

    # actions
    def name(self):
        self._entry();
        res = self.rdd.name()
        self._exit()
        return res

    def count(self):
        self._entry();
        res = self.rdd.count()
        self._exit()
        return res

    def take(self, n):
        self._entry(n)
        res = self.rdd.take(n)
        self._exit()
        return res

    def takeOrdered(self, num, key=None):
        self._entry(num)
        res = self.rdd.takeOrdered(num, key)
        self._exit()
        return res

    def collect(self):
        self._entry()
        res = self.rdd.collect()
        self._exit()
        return res

    def first(self):
        self._entry()
        res = self.rdd.first()
        self._exit()
        return res
        
    def max(self):
        self._entry()
        res = self.rdd.max()
        self._exit()
        return res

    def min(self):
        self._entry()
        res = self.rdd.min()
        self._exit()
        return res

    def sum(self):
        self._entry()
        res = self.rdd.sum()
        self._exit()
        return res

    def mean(self):
        self._entry()
        res = self.rdd.mean()
        self._exit()
        return res

    def stdev(self):
        self._entry()
        res = self.rdd.stdev()
        self._exit()
        return res

    def sampleStdev(self):
        self._entry()
        res = self.rdd.sampleStdev()
        self._exit()
        return res

    def variance(self):
        self._entry()
        res = self.rdd.variance()
        self._exit()
        return res

    def sampleVariance(self):
        self._entry()
        res = self.rdd.sampleVariance()
        self._exit()
        return res

    def aggregate(self, zeroValue, seqOp, combOp):
        self._entry()
        res = self.rdd.aggregate(zeroValue, seqOp, combOp)
        self._exit()
        return res

    def reduce(self, fn):
        self._entry()
        res = self.rdd.reduce(fn)
        self._exit()
        return res

    def toDebugString(self):
        self._entry()
        res = self.rdd.toDebugString()
        self._exit()
        return res
        
    def persist(self, storage_level):
        self._entry(storage_level)
        self.rdd.persist(storage_level)
        self._exit()

    def unpersist(self):
        self._entry()
        self.rdd.unpersist()
        self._exit()

    def saveAsPickleFile(self, path):
        self._entry(path)
        self.rdd.saveAsPickleFile(path)
        self._exit()

    def saveAsTextFile(self, path):
        self._entry(path)
        self.rdd.saveAsTextFile(path)
        self._exit()


    # transformations
    def repartition(self, number_of_partitions):
        self._entry()
        res = self.rdd.repartition(number_of_partitions)
        self._exit()
        return XRdd(res)

    def map(self, fn, preservesPartitioning=False):
        self._entry(preservesPartitioning)
        res = self.rdd.map(fn, preservesPartitioning)
        self._exit()
        return XRdd(res)

    def mapPartitions(self, fn, preservesPartitioning=False):
        self._entry(preservesPartitioning)
        res = self.rdd.mapPartitions(fn, preservesPartitioning)
        self._exit()
        return XRdd(res)

    def mapValues(self, fn):
        self._entry()
        res = self.rdd.mapValues(fn)
        self._exit()
        return XRdd(res)

    def flatMap(self, fn, preservesPartitioning=False):
        self._entry(preservesPartitioning)
        res = self.rdd.flatMap(fn, preservesPartitioning)
        self._exit()
        return XRdd(res)

    def zipWithIndex(self):
        self._entry()
        res = self.rdd.zipWithIndex()
        self._exit()
        return XRdd(res)

    def zipWithUniqueId(self):
        self._entry()
        res = self.rdd.zipWithUniqueId()
        self._exit()
        return XRdd(res)

    def filter(self, fn):
        self._entry()
        res = self.rdd.filter(fn)
        self._exit()
        return XRdd(res)

    def distinct(self):
        self._entry()
        res = self.rdd.distinct()
        self._exit()
        return XRdd(res)

    def keys(self):
        self._entry()
        res = self.rdd.keys()
        self._exit()
        return XRdd(res)
        
    def values(self):
        self._entry()
        res = self.rdd.values()
        self._exit()
        return XRdd(res)
        
    def repartition(self, number_of_partitions):
        self._entry(number_of_partitions)
        res = self.rdd.repartition(number_of_partitions)
        self._exit()
        return XRdd(res)

    def sample(self, withReplacement, fraction, seed=None):
        self._entry(withReplacement, fraction, seed)
        res = self.rdd.sample(withReplacement, fraction, seed)
        self._exit()
        return XRdd(res)

    def zip(self, other):
        self._entry()
        res = self.rdd.zip(other.rdd)
        self._exit()
        return XRdd(res)

    def union(self, other):
        self._entry(other)
        res = self.rdd.union(other.rdd)
        self._exit()
        return XRdd(res)

    def groupByKey(self):
        self._entry()
        res = self.rdd.groupByKey()
        self._exit()
        return XRdd(res)

    def cartesian(self, right):
        self._entry()
        res = self.rdd.cartesian(right.rdd)
        self._exit()
        return XRdd(res)
        
    def join(self, right):
        self._entry()
        res = self.rdd.join(right.rdd)
        self._exit()
        return XRdd(res)
        
    def leftOuterJoin(self, right):
        self._entry()
        res = self.rdd.leftOuterJoin(right.rdd)
        self._exit()
        return XRdd(res)
        
    def rightOuterJoin(self, right):
        self._entry()
        res = self.rdd.rightOuterJoin(right.rdd)
        self._exit()
        return XRdd(res)
        
    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        self._entry()
        res = self.rdd.sortBy(keyfunc, ascending, numPartitions)
        self._exit()
        return XRdd(res)

    def sortByKey(self, ascending=True, numPartitions=None, keyfunc=lambda x: x):
        self._entry()
        res = self.rdd.sortByKey(ascending, numPartitions, keyfunc)
        self._exit()
        return XRdd(res)
