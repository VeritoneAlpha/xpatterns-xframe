"""
Provides functions to create and maintain the spark context.
"""

import os
import atexit
from zipfile import PyZipFile
from tempfile import NamedTemporaryFile

from pyspark import SparkConf, SparkContext, SQLContext

from xpatterns.environment import Environment
from xpatterns.singleton import Singleton

# Context Defaults


# noinspection PyClassHasNoInit
class SparkInitContext():
    """
    Spark Context initialization.

    This may be used to initialize the spark context.
    If this mechanism is not used, then the spark context will be initialized
    using the config file the first time a context is needed.
    """
    cluster_url = None
    app_name = None
    cores_max = None
    executor_memory = None

    @staticmethod
    def set(**context):
        """
        Sets the spark context parameters, and then create a context.
        If the spark context has already been created, then this will have no effect.
        """
        if 'cluster_url' in context:
            SparkInitContext.cluster_url = context['cluster_url']
        if 'app_name' in context:
            SparkInitContext.app_name = context['app_name']
        if 'cores_max' in context:
            SparkInitContext.cores_max = context['cores_max']
        if 'executor_memory' in context:
            SparkInitContext.executor_memory = context['executor_memory']
        CommonSparkContext.Instance()

@Singleton
class CommonSparkContext(object):
    def __init__(self):
        """
        Create a spark context.

        The spark configuration is taken from $XPATTERNS_HOME/config.ini.

        Notes
        -----
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
        cluster_url = SparkInitContext.cluster_url or env.get_config('spark', 'cluster_url', default='local')
        cores_max = SparkInitContext.cores_max or env.get_config('spark', 'cores_max', default='8')
        executor_memory = SparkInitContext.executor_memory or env.get_config('spark', 'executor_memory', default='8g')
        app_name = SparkInitContext.app_name or env.get_config('spark', 'app_name', 'xFrame')
        conf = (SparkConf()
                .setMaster(cluster_url)
                .setAppName(app_name)
                .set("spark.cores-max", cores_max)
                .set("spark.executor.memory", executor_memory))
        self._sc = SparkContext(conf=conf)
        self._sqlc = SQLContext(self._sc)

        self.zip_path = self.build_zip()
        if self.zip_path:
            self._sc.addPyFile(self.zip_path)
        atexit.register(self.close_context)

    def close_context(self):
        if self._sc:
            self._sc.stop()
            self._sc = None
            if self.zip_path:
                os.remove(self.zip_path)

    def sc(self):
        return self._sc

    def sqlc(self):
        return self._sqlc

    @staticmethod
    def build_zip():
        if 'XPATTERNS_HOME' not in os.environ:
            return None
        tf = NamedTemporaryFile(suffix='.zip', delete=False)
        z = PyZipFile(tf, 'w')
        z.writepy(os.path.join(os.environ['XPATTERNS_HOME'], 'xpatterns'))
        z.close()
        return tf.name


def spark_context():
    """
    Returns the spark context.
    """
    return CommonSparkContext.Instance().sc()

def spark_sql_context():
    """
    Returns the spark sql context.
    """
    return CommonSparkContext.Instance().sqlc()

