"""
Provides functions to create and maintain the spark context.
"""

import os
import atexit
from zipfile import PyZipFile
from tempfile import NamedTemporaryFile

from pyspark import SparkConf, SparkContext, SQLContext

from xframes.environment import Environment
from xframes.singleton import Singleton

# Context Defaults


# noinspection PyClassHasNoInit
class SparkInitContext():
    """
    Spark Context initialization.

    This may be used to initialize the spark context.
    If this mechanism is not used, then the spark context will be initialized
    using the config file the first time a context is needed.
    """
    context = {}

    @staticmethod
    def set(**context):
        """
        Sets the spark context parameters, and then create a context.
        If the spark context has already been created, then this will have no effect.
        """
        SparkInitContext.context = context
        CommonSparkContext.Instance()

@Singleton
class CommonSparkContext(object):
    def __init__(self):
        """
        Create a spark context.

        The spark configuration is taken from $XFRAMES_HOME/config.ini or from
        the values set in SparkInitContext.set().

        Notes
        -----
        master : str, optional
            The url of the spark cluster to use.  To use the local spark, give
            'local'.  To use a spark cluster with its master on a specific IP addredd,
            give the IP address or the hostname as in the following example:
            master=spark://my_spark_host:7077

        app_name : str, optional
            The app name is used on the job monitoring server, and for logging.

        cores_max : str, optional
            The maximum number of cores to use for execution.

        executor_memory : str, optional
            The amount of main memory to allocate to executors.  For example, '2g'.
        """

        env = Environment.create_default()
        # TODO add any other properties in config.ini
        config_context = {'master': env.get_config('spark', 'master', default='local'),
                          'cores_max': env.get_config('spark', 'cores_max', default='8'),
                          'executor_memory': env.get_config('spark', 'executor_memory', default='8g'),
                          'app_name': env.get_config('spark', 'app_name', 'xFrame')}
        config_context.update(SparkInitContext.context)
        config_pairs = [(k, v) for k, v in config_context.iteritems()]
        self._config = (SparkConf().setMaster(config_context['master']).setAppName(config_context['app_name']).setAll(config_pairs))
        self._sc = SparkContext(conf=self._config)
        self._sqlc = SQLContext(self._sc)

        self.zip_path = self.build_zip()
        if self.zip_path:
            self._sc.addPyFile(self.zip_path)
        atexit.register(self.close_context)

    def config(self):
        return self._config

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
        if 'XFRAMES_HOME' not in os.environ:
            return None
        # This can fail at writepy if there is something wrong with the files
        #  in xframes.  Go ahead anyway, but things will probably fail of this job is
        #  distributed
        try:
            tf = NamedTemporaryFile(suffix='.zip', delete=False)
            z = PyZipFile(tf, 'w')
            z.writepy(os.environ['XFRAMES_HOME'])
            z.close()
            return tf.name
        except:
            print 'Zip file distribution failed -- workers will not get xframes code.'
            print 'Check for unexpected files in XFRAMES_HOME.'
            return None


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

