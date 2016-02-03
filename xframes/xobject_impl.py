"""
This object implements the base of the xframes inheritance hierarchy.
"""
from pyspark import RDD

from xframes.spark_context import spark_context, spark_sql_context, hive_context
from xframes.xrdd import XRdd


class XObjectImpl(object):
    """ Implementation for XObject. """

    def __init__(self, rdd):
        self._rdd = self._wrap_rdd(rdd)

    @staticmethod
    def _wrap_rdd(rdd):
        if rdd is None:
            return rdd
        if isinstance(rdd, RDD):
            return XRdd(rdd)
        if isinstance(rdd, XRdd):
            return rdd
        raise TypeError('type is not RDD')

    @staticmethod
    def spark_context():
        return spark_context()

    @staticmethod
    def spark_sql_context():
        return spark_sql_context()

    @staticmethod
    def hive_context():
        return hive_context()

    def get_rdd(self):
        return self._rdd

    def _replace_rdd(self, rdd):
        self._rdd = self._wrap_rdd(rdd)

    def dump_debug_info(self):
        return self._rdd.toDebugString()
