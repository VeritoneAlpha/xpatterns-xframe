"""
This object implements the base of the xframes inheritance hierarchy.
"""
import os

from pyspark import RDD

from xframes.spark_context import CommonSparkContext
from xframes.xrdd import XRdd
import xframes.fileio as fileio


class XObjectImpl(object):
    """ Implementation for XObject. """

    def __init__(self, rdd):
        self._rdd = self._wrap_rdd(rdd)

    @staticmethod
    def _wrap_rdd(rdd):
        if rdd is None:
            return None
        if isinstance(rdd, RDD):
            return XRdd(rdd)
        if isinstance(rdd, XRdd):
            return rdd
        raise TypeError('Type is not RDD')

    @staticmethod
    def spark_context():
        return CommonSparkContext.spark_context()

    @staticmethod
    def spark_sql_context():
        return CommonSparkContext.spark_sql_context()

    @staticmethod
    def hive_context():
        return CommonSparkContext.hive_context()

    @staticmethod
    def check_input_uri(uri):
        if ',' in uri:
            uri_list = uri.split(',')
        else:
            uri_list = [uri]
        for path in uri_list:
            if not fileio.exists(path):
                raise ValueError('Input file does not exist: {}'.format(path))

    @staticmethod
    def check_output_uri(uri):
        dirname = os.path.dirname(uri)
        if not fileio.exists(dirname):
            fileio.make_dir(dirname)
            if not fileio.exists(dirname):
                raise ValueError('Output directory does not exist: {}'.format(dirname))

    def _replace_rdd(self, rdd):
        self._rdd = self._wrap_rdd(rdd)

    def dump_debug_info(self):
        return self._rdd.toDebugString()
