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
        hdfs_user_name = self._env.get_config('hdfs', 'user', 'hdfs')
        os.environ['HADOOP_USER_NAME'] = hdfs_user_name
        default_context = {'spark.master': 'local[*]',
                           'spark.app.name': 'xFrames'}
        # get values from [spark] section
        config_context = self._env.get_config_items('spark')
        context = merge_dicts(default_context, config_context)
        context = merge_dicts(context, SparkInitContext.context)
        config_pairs = [(k, v) for k, v in context.iteritems()]
        self._config = (SparkConf().setAll(config_pairs))
        if verbose:
            print >> stderr, 'Spark Config:', config_pairs

        self._sc = SparkContext(conf=self._config)
        self._sqlc = SQLContext(self._sc)
        self._hivec = HiveContext(self._sc)
        self.zip_path = []
        self.version = self._sc.version.split('.')
        self.status_tracker = self._sc.statusTracker()
        if self.version >= [1, 4, 1]:
            self.application_id = self._sc.applicationId
        else:
            self.application_id = None

        if verbose:
            print 'Spark Version:', '.'.join(self.version)
            if self.application_id:
                print 'Application Id:', self.application_id

        if not context['spark.master'].startswith('local'):
            zip_path = self.build_zip(get_xframes_home())
            if zip_path:
                self._sc.addPyFile(zip_path)
                self.zip_path.append(zip_path)

        trace_flag = self._env.get_config('xframes', 'rdd-trace', 'false').lower() == 'true'
        XRdd.set_trace(trace_flag)
        atexit.register(self.close_context)

    def spark_add_files(self, dirs):
        """
        Adds python files in the given directory or directories.

        Parameters
        ----------
        dirs: str or list(str)
            If a str, the pathname to a directory containing a python module.
            If a list, then it is a list of such directories.

            The python files in each directory are compiled, packed into a zip, distributed to each
            spark slave, and placed in PYTHONPATH.

            This is only done if spark is deployed on a cluster.
        """
        props = self.config()
        if props.get('spark.master', 'local').startswith('local'):
            return
        if isinstance(dirs, basestring):
            dirs = [dirs]
        for path in dirs:
            zip_path = self.build_zip(path)
            if zip_path:
                self._sc.addPyFile(zip_path)
                self.zip_path.append(zip_path)

    def close_context(self):
        if self._sc:
            self._sc.stop()
            self._sc = None
            for zip_path in self.zip_path:
                os.remove(zip_path)

    def config(self):
        """
        Gets the configuration parameters used to initialize the spark context.

        Returns
        -------
        out : dict
            A dict of the properties used to initialize the spark context.
        """
        props = self._config.getAll()
        return {prop[0]: prop[1] for prop in props}

    def env(self):
        """
        Gets the config environment.

        Returns
        -------
        out : Environment
            The environment.  This contains all the values from the configuration file(s).
        """

        return self._env

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

    def jobs(self):
        """
        Get the spark job ID and info for the active jobs.

        This method would normally be called by another thread from the executing job.

        Returns
        -------
        out: map(job_id: job_info
            A map of the active job IDs and their corresponding job info
        """
        return {job_id: self.status_tracker.getJobInfo(job_id) for job_id in self.status_tracker.getActiveJobIds()}

    # noinspection PyBroadException
    @staticmethod
    def build_zip(module_dir):
        # This can fail at writepy if there is something wrong with the files
        #  in xframes.  Go ahead anyway, but things will probably fail if this job is
        #  distributed
        try:
            tf = NamedTemporaryFile(suffix='.zip', delete=False)
            z = PyZipFile(tf, 'w')
            z.writepy(module_dir)
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

        return CommonSparkContext().sc()

    @staticmethod
    def spark_config():
        """
        Returns the spark cofig parameters.

        Returns
        -------
        out : list
            A list of the key-value pairs stored as tuples, used to initialize the spark context.
        """

        return CommonSparkContext().config()

    @staticmethod
    def spark_sql_context():
        """
        Returns the spark sql context.

        Returns
        -------
        out : pyspark.sql.SQLContext
            The SQLContext object from spark.
        """

        return CommonSparkContext().sqlc()

    @staticmethod
    def environment():
        return CommonSparkContext().env()
