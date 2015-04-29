from xpatterns.xobject_impl import XObjectImpl

XFRAMES_VERSION = '0.1.1'


class XObject(object):
    """ 
    Common components for XFrame and XArray.
    """

    @staticmethod
    def version():
        """
        Returns the xframes library version.
        """
        return XFRAMES_VERSION

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

    @staticmethod
    def spark_context():
        """
        Get the spark context.

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
