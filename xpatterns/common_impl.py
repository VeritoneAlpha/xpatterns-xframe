"""
Provides shared implementation functions for XArrayImpl and XFrameImpl
"""

from xpatterns.environment import Environment

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

@Singleton
class CommonSparkContext:
    def __init__(self):
        """
        Create a spark context.

        The spark configuration is taken from $XPATTERNS_HOME/config.ini.

        Config Parameters
        -----------------
        cluster_url : str, optional
            The url of the spark cluster to use.  To use the local spark, give
            'local'.  To use a spark cluster with its master on a specific IP addredd,
            give the IP address or the hostname as in the following example:
            cluster_url=spark://my_spark_host:7077

        app_name : str, optional
            The app name is used on the job monitoring server, and for logging.

        cores_max : str, optional
            The maximum number of cores to use for execution.

        executor_memory : str, optional
            The amount of main memory to allocate to executors.  For example, '2g'.
        """

        env = Environment.create_default()
        cluster_url = env.get_config('spark', 'cluster_url', default='local')
        cores_max = env.get_config('spark', 'cores_max', default='8')
        executor_memory = env.get_config('spark', 'executor_memory', default='8g')
        app_name = env.get_config('spark', 'app_name', 'xFrame')
        conf = (SparkConf()
                .setMaster(cluster_url)
                .setAppName(app_name)
                .set("spark.cores-max", cores_max)
                .set("spark.executor.memory", executor_memory))
        self._sc = SparkContext(conf=conf)
        self._sqlc = SQLContext(self._sc)
        atexit.register(self.close_context)

    def close_context(self):
        if self._sc:
            self._sc.stop()
            self._sc = None

    def sc(self):
        return self._sc

    def sqlc(self):
        return self._sqlc

def spark_context():
    return CommonSparkContext.Instance().sc()

def spark_sql_context():
    return CommonSparkContext.Instance().sqlc()






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
def cache(rdd):
    rdd.persist(StorageLevel.MEMORY_ONLY)

def uncache(rdd):
    rdd.unpersist()

def persist(rdd):
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    
def unpersist(rdd):
    rdd.unpersist()

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
