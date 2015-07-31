"""
Provides functions to create and maintain the spark context.
"""

import os
import atexit
from sys import stderr
from zipfile import PyZipFile
from tempfile import NamedTemporaryFile

from pyspark import SparkConf, SparkContext, SQLContext

from xframes.environment import Environment
from xframes.singleton import Singleton

def get_xframes_home():
    import xframes
    return os.path.dirname(xframes.__file__)


# noinspection PyClassHasNoInit
class SparkInitContext():
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

        """
        SparkInitContext.context = context
        CommonSparkContext.Instance()

@Singleton
class CommonSparkContext(object):
    def __init__(self):
        """
        Create a spark context.

        The spark configuration is taken from xframes/config.ini and from
        the values set in SparkInitContext.set() if this has been called.

        Notes
        -----
        The following values are the most commonly used.  They will be given default values if
        none are supplied in a configuration file or through SparkInitContext.  Other values
        can be found in the spark configuration documentation.

        master : str, optional
            The url of the spark cluster to use.  To use the local spark, give
            'local'.  To use a spark cluster with its master on a specific IP address,
            give the IP address or the hostname as in the following examples:

            master=spark://my_spark_host:7077

            master=mesos://my_mesos_host:5050

        app_name : str, optional
            The app name is used on the job monitoring server, and for logging.

        cores_max : str, optional
            The maximum number of cores to use for execution.

        executor_memory : str, optional
            The amount of main memory to allocate to executors.  For example, '2g'.
        """

        def merge_dicts(x, y):
            z = x.copy()
            z.update(y)
            return z

        # This reads from default.ini and then xframes/config.ini
        # if they exist.
        env = Environment.create()
        default_context = {'master': 'local',
                           'cores_max': '4',
                           'executor_memory': '2g',
                           'app_name': 'xFrame'}
        # get values from [spark] section
        config_context = env.get_config_items('spark')
        context = merge_dicts(default_context, config_context)
        context = merge_dicts(context, SparkInitContext.context)
        config_pairs = [(k, v) for k, v in context.iteritems()]
        self._config = (SparkConf().setMaster(context['master']).
                        setAppName(context['app_name']).
                        setAll(config_pairs))
        self._sc = SparkContext(conf=self._config)
        self._sqlc = SQLContext(self._sc)

        if not context['master'].startswith('local'):
            self.zip_path = self.build_zip()
            if self.zip_path:
                self._sc.addPyFile(self.zip_path)
        else:
            self.zip_path = None
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


def common_spark_context():
    """
    Returns the common spark context.

    This is the xframes object that contains the actual spark context.

    Returns
    -------
    out : CommonSparkContext
        The xframes singleton containing the SparkContext.
    """

    return CommonSparkContext.Instance()

def spark_context():
    """
    Returns the spark context.

    Returns
    -------
    out : pyspark.SparkContext
        The SparkContext object from spark.
    """

    return CommonSparkContext.Instance().sc()

def spark_sql_context():
    """
    Returns the spark sql context.

    Returns
    -------
    out : pyspark.sql.SQLContext
        The SQLContext object from spark.
    """

    return CommonSparkContext.Instance().sqlc()

