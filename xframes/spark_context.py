"""
Provides functions to create and maintain the spark context.
"""

import os
import atexit
from sys import stderr
from zipfile import PyZipFile
from tempfile import NamedTemporaryFile

from pyspark import SparkConf, SparkContext, SQLContext, HiveContext

from xframes.environment import Environment
from xframes.xrdd import XRdd


def get_xframes_home():
    import xframes
    return os.path.dirname(xframes.__file__)


def merge_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z

# CommonSparkContext wraps SparkContext, which must only be instantiated once in a program.
# This is used as a metaclass for CommonSparkContext, so that only one
#  instance is created.
class Singleton(type):
    def __init__(cls, name, bases, dictionary):
        super(Singleton, cls).__init__(name, bases, dictionary)
        cls.instance = None

    def __call__(cls, *args):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args)
        return cls.instance


# noinspection PyClassHasNoInit
class SparkInitContext:
    """
    Spark Context initialization.

    This may be used to initialize the spark context with the supplied values.
    If this mechanism is not used, then the spark context will be initialized
    using the config file the first time a context is needed.
    """
    context = {}

    @staticmethod
    def set(context):
        """
        Sets the spark context parameters, and then creates a context.
        If the spark context has already been created, then this will have no effect.

        Parameters
        ----------
        context : dict
            Dictionary of property/value pairs.  These are passed to spark as config parameters.
            If a config file is present, these parameters will override the parameters there.

        Notes
        -----
        The following values are the most commonly used.  They will be given default values if
        none are supplied in a configuration file.  Other values
        can be found in the spark configuration documentation.

        spark.master : str, optional
            The url of the spark cluster to use.  To use the local spark, give
            'local'.  To use a spark cluster with its master on a specific IP address,
            give the IP address or the hostname as in the following examples:

            spark.master=spark://my_spark_host:7077

            spark.master=mesos://my_mesos_host:5050

        app.name : str, optional
            The app name is used on the job monitoring server, and for logging.

        spark.cores.max : str, optional
            The maximum number of cores to use for execution.

        spark.executor.memory : str, optional
            The amount of main memory to allocate to executors.  For example, '2g'.
        """
        SparkInitContext.context = context
        CommonSparkContext()


class CommonSparkContext(object):
    __metaclass__ = Singleton

    def __init__(self):
        """
        Create a spark context.

        The spark configuration is taken from xframes/config.ini and from
        the values set in SparkInitContext.set() if this has been called.
        """

        # This reads from default.ini and then xframes/config.ini
        # if they exist.
        self._env = Environment.create()
        verbose = self._env.get_config('xframes', 'verbose', 'false').lower() == 'true'
        default_context = {'spark.master': 'local',
                           'app.name': 'xFrames'}
        # get values from [spark] section
        config_context = self._env.get_config_items('spark')
        context = merge_dicts(default_context, config_context)
        context = merge_dicts(context, SparkInitContext.context)
        config_pairs = [(k, v) for k, v in context.iteritems()]
        self._config = (SparkConf().setMaster(context['spark.master']).
                        setAppName(context['app.name']).
                        setAll(config_pairs))
        if verbose:
            print >> stderr, 'Spark Config:', config_pairs
        self._sc = SparkContext(conf=self._config)
        self._sqlc = SQLContext(self._sc)
        self._hivec = HiveContext(self._sc)

        if not context['spark.master'].startswith('local'):
            self.zip_path = self.build_zip()
            if self.zip_path:
                self._sc.addPyFile(self.zip_path)
        else:
            self.zip_path = None

        trace_flag = self._env.get_config('xframes', 'rdd-trace', 'false').lower() == 'true'
        XRdd.set_trace(trace_flag)
        atexit.register(self.close_context)

    def config(self):
        """
        Gets the configuration parameters used to initialize the spark context.

        Returns
        -------
        out : list
            A list of the key-value pairs stored as tuples, used to initialize the spark context.
        """
        props = self._config.getAll()
        return {prop[0]: prop[1] for prop in props}

    def close_context(self):
        if self._sc:
            self._sc.stop()
            self._sc = None
            if self.zip_path:
                os.remove(self.zip_path)

    def sc(self):
        """
        Gets the spark context.

        Returns
        -------
        out : SparkContext
            The spark context.  There is a single spark context per process.
        """

        return self._sc

    def sqlc(self):
        """
        Gets the spark sql context.

        Returns
        -------
        out : sql.SqlContext
            The spark sql context.
        """

        return self._sqlc

    def hivec(self):
        """
        Gets the hive context.

        Returns
        -------
        out : sql.HiveContext
            The hive context.
        """

        return self._hivec

    # noinspection PyBroadException
    @staticmethod
    def build_zip():
        # This can fail at writepy if there is something wrong with the files
        #  in xframes.  Go ahead anyway, but things will probably fail of this job is
        #  distributed
        try:
            tf = NamedTemporaryFile(suffix='.zip', delete=False)
            z = PyZipFile(tf, 'w')
            z.writepy(get_xframes_home())
            z.close()
            return tf.name
        except:
            print >>stderr, 'Zip file distribution failed -- workers will not get xframes code.'
            print >>stderr, 'Check for unexpected files in xframes directory.'
            return None

    @staticmethod
    def spark_context():
        """
        Returns the spark context.

        Returns
        -------
        out : pyspark.SparkContext
            The SparkContext object from spark.
        """

        return CommonSparkContext()._sc

    @staticmethod
    def spark_sql_context():
        """
        Returns the spark sql context.

        Returns
        -------
        out : pyspark.sql.SQLContext
            The SQLContext object from spark.
        """

        return CommonSparkContext()._sqlc

    @staticmethod
    def environment():
        return CommonSparkContext()._env
