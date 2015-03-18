"""
Provides shared implementation functions for XArrayImpl and XFrameImpl
"""

from pyspark import StorageLevel
from pyspark import SparkConf, SparkContext, SQLContext
import atexit

class Singleton:
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the
    class that should be a singleton.

    The decorated class can define one `__init__` function that
    takes only the `self` argument. Other than that, there are
    no restrictions that apply to the decorated class.

    To get the singleton instance, use the `Instance` method. Trying
    to use `__call__` will result in a `TypeError` being raised.

    Limitations: The decorated class cannot be inherited from.

    """

    def __init__(self, decorated):
        self._decorated = decorated

    def Instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.

        """
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)


# Context Defaults
#CLUSTER_URL = 'spark://ip-10-0-1-212:7077'
APP_NAME = 'xFrame'
SPARK_CORES_MAX = '8'
EXECUTOR_MEMORY = '2g'
CLUSTER_URL='local'

@Singleton
class CommonSparkContext:
    def __init__(self, cluster_url=CLUSTER_URL, app_name=APP_NAME, 
                 executor_memory=EXECUTOR_MEMORY, spark_cores_max=SPARK_CORES_MAX):
        conf = (SparkConf()
                .setMaster(cluster_url)
                .setAppName(app_name)
                .set("spark.cores-max", spark_cores_max)
                .set("spark.executor.memory", executor_memory))
        self._sc = SparkContext(conf=conf)
        self._sqlc = SQLContext(self._sc)
        atexit.register(self.close_context)

    def close_context(self):
        if self._sc:
            self._sc.stop()
            self._sc = None

    @property
    def sc(self):
        return self._sc

    @property
    def sqlc(self):
        return self._sqlc

# Safe version of zip.
# This requires that left and right RDDs be of the same length, but
#  not the same partition structure
# Try normal zip first, since it is much more efficient.
def safe_zip(left, right):
    try:
        res = left.zip(right)
    except ValueError:
        ix_left = left.zipWithIndex().map(lambda row: (row[1], row[0]))
        ix_right = right.zipWithIndex().map(lambda row: (row[1], row[0]))
        res = ix_left.join(ix_right)
        res = res.sortByKey()
        res = res.map(lambda kv: kv[1], preservesPartitioning=True)

    # do this to avoid exponential growth in lazy execution plan
    res.persist(StorageLevel.MEMORY_AND_DISK)
    return res

# TODO make this something that works with 'with'
def persist_temp(rdd):
    rdd.persist(StorageLevel.MEMORY_ONLY)

def persist_long(rdd):
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    
def infer_type_of_list(data):
    """
    Look through an iterable and get its data type.
    Use the first type, and check to make sure the rest are of that type.
    Missing values are skipped.
    """
    candidate = None
    for d in data:
        if d is None: continue
        d_type = type(d)
        if candidate is None: candidate = d_type
        if d_type != candidate: 
            numeric = (float, int, long)
            if d_type in numeric and candidate in numeric: continue
            raise TypeError('infer_type_of_list: mixed types in list: {} {}'.format(d_type, candidate))
    return candidate

def infer_type(rdd):
    h = rdd.take(100)      # partial action
    dtype = infer_type_of_list(h)
    return dtype
