import xframes
import xframes.version
from xframes.xobject_impl import XObjectImpl


class XObject(object):
    """ 
    Common components for XFrame and XArray.
    """

    @staticmethod
    def version():
        """
        Returns the xframes library version.
        """
        return xframes.version.__version__

    @staticmethod
    def set_trace(entry_trace=None, exit_trace=None):
        """
        Set XFrame tracing.

        Turns on and off tracing of XFrame method calls.
        When entry tracing is on, a message is written when each XFrame method is entered,
        giving its imput parameters.  
        When exit tracing is on, then a message is written when each method exits.

        Parameters
        ----------
        entry_trace, boolean, optional
            If True, turn on entry tracing.  Defaults to False.

        exit_trace, boolean, optional
            If True, turns on exit tracing.  Defaults to False.

        """
        XObjectImpl.set_trace(entry_trace, exit_trace)

    @classmethod
    def init_context(cls, context):
        """
        Set the SparkContext parameters, and then create a context.
        The parameters listed below, as well as any other parameters allowed by SparkContext
        may be given.
        If the Spark context has already been created, then this will have no effect.

        Parameters
        ----------
        context: dict
            A dictionary of attribute/value pairs: spark configuration
            property names and values.

        Notes
        -----

        The most common properties are the following.

        master : str, optional
            The url of the spark cluster to use.  To use the local spark, give
            'local'.  To use a spark cluster with its master on a specific IP address,
            give the IP address or the hostname as in the following example:
            master=spark://my_spark_host:7077

        app_name : str, optional
            The app name is used on the job monitoring server, and for logging.

        cores_max : str, optional
            The maximum number of cores to use for execution.

        executor_memory : str, optional
            The amount of main memory to allocate to executors.  For example, '2g'.

        References
        ----------
        .. _Cofiguration: http://spark.apache.org/docs/latest/configuration.html
        """
        xframes.SparkInitContext().set(context)

    @staticmethod
    def spark_context():
        """
        Get the Spark context.

        Returns
        -------
        out : SparkContext
            The spark context.  
            If no context has been created yet, then one is created.
        """
        return XObjectImpl.spark_context()

    @staticmethod
    def spark_sql_context():
        """
        Get the spark sql context.

        Returns
        -------
        out : SparkSqlContext
            The spark sql context.  
            If no sql context has been created yet, then one is created.
        """
        return XObjectImpl.spark_sql_context()

#    def dump_debug_info(self):
#        return self.__impl__.dump_debug_info()
