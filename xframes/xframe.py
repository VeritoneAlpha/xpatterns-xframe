"""
This module defines the XFrame class which provides the
ability to create, access and manipulate a remote scalable dataframe object.

XFrame acts similarly to pandas.DataFrame, but the data is immutable
and is stored as Spark RDDs.
"""

"""
Copyright (c) 2014, Dato, Inc.
All rights reserved.

Copyright (c) 2015, Atigeo, Inc.
All rights reserved.
"""

import array
from prettytable import PrettyTable
from textwrap import wrap
import datetime
import inspect
import time
import itertools
from sys import stderr

import numpy

from xframes.deps import pandas, HAS_PANDAS
from xframes.deps import dataframeplus, HAS_DATAFRAME_PLUS
from xframes.xobject import XObject
from xframes.xframe_impl import XFrameImpl
from xframes.xplot import XPlot
from xframes.xarray_impl import infer_type_of_list
from xframes.util import make_internal_url, classify_type
from xframes.xarray import XArray
import xframes

__all__ = ['XFrame']

FOOTER_STRS = ['Note: Only the head of the XFrame is printed.  You can use ',
               'print_rows(num_rows=m, num_columns=n) to print more rows and columns.']

LAZY_FOOTER_STRS = ['Note: Only the head of the XFrame is printed. This XFrame is lazily ',
                    'evaluated.  You can use len(xf) to force materialization.']

MAX_ROW_WIDTH = 80
HTML_MAX_ROW_WIDTH = 120


# noinspection PyUnresolvedReferences
class XFrame(XObject):
    """
    A tabular, column-mutable dataframe object that can scale to big data. 
    XFrame is able to hold data that are much larger than the machine's main
    memory. The data in XFrame is stored row-wise in a Spark RDD.
    Each row of the RDD is a list, whose elements correspond
    to the values in each column.  The column names and types are stored in the XFrame
    instance, and give the mapping to the row list.

    An XFrame can be constructed from the following data
    formats:

    * csv file (comma separated value)
    * xframe directory archive (A directory where an xframe was saved
      previously)
    * a spark RDD plus the column names and types
    * a spark.DataFrame
    * general text file (with csv parsing options, See :py:meth:`read_csv()`)
    * parquet file
    * a Python dictionary
    * pandas.DataFrame
    * JSON
    * Apache Avro

    and from the following sources:

    * your local file system
    * the XFrames Server's file system
    * HDFS
    * Amazon S3
    * HTTP(S).

    Only basic examples of construction are covered here. For more information
    and examples, please see the `User Guide`


    Parameters
    ----------
    data : array | pandas.DataFrame | spark.rdd | spark.DataFrame | string | dict, optional
        The actual interpretation of this field is dependent on the `format`
        parameter. If `data` is an array, Pandas DataFrame or Spark RDD, the contents are
        stored in the XFrame. If `data` is an object supporting iteritems, then is is handled
        like a dictionary.  If `data` is an object supporting iteration, then the values
        are iterated to form the XFrame.  If `data` is a string, it is interpreted as a
        file. Files can be read from local file system or urls (local://,
        hdfs://, s3://, http://, or remote://).

    format : string, optional
        Format of the data. The default, "auto" will automatically infer the
        input data format. The inference rules are simple: If the data is an
        array or a dataframe, it is associated with 'array' and 'dataframe'
        respectively. If the data is a string, it is interpreted as a file, and
        the file extension is used to infer the file format. The explicit
        options are:

        - "auto"
        - "array"
        - "dict"
        - "xarray"
        - "pandas.dataframe"
        - "csv"
        - "tsv"
        - "psv"
        - "parquet"
        - "rdd"
        - "spark.dataframe"
        - "xframe"

    verbose : bool, optional
        If True, print the progress while reading a file.

    Notes
    -----
    The following functionality is currently not implemented.
        - pack_columns data types except list, array, and dict
        - groupby quantile
        - split_datetime

    See Also
    --------
    xframes.XFrame.read_csv:
        Create a new XFrame from a csv file. Preferred for text and CSV formats,
        because it has a lot more options for controlling the parser.

    xframes.XFrame.read_parquet`
        Read an XFrame from a parquet file.

    xframes.XFrame.from_rdd
        Create a new XFrame from a Spark RDD or Spark DataFrame.  
        Column names and types can be specified if a spark RDD is given; otherwise 
        they are taken from the DataFrame.

    xframes.XFrame.save
        Save an XFrame in a file for later use within XFrames or Spark.

    xframes.XFrame.load
        Load an XFrame from a file.  The filename extension is used to determine the
        file format.

    xframes.XFrame.set_trace
        Controls entry and exit tracing.

    xframes.XFrame.spark_context
        Returns the spark context.

    xframes.XFrame.spark_sql_context
        Returns the spark sql context.

    Examples
    --------
    Create an XFrame from a Python dictionary.

    >>> from xframes import XFrame
    >>> sf = XFrame({'id':[1,2,3], 'val':['A','B','C']})
    >>> sf
    Columns:
        id  int
        val str
    Rows: 3
    Data:
       id  val
    0  1   A
    1  2   B
    2  3   C

    Create an XFrame from a remote CSV file.

    >>> url = 'http://testdatasets.s3-website-us-west-2.amazonaws.com/users.csv.gz'
    >>> xf = XFrame.read_csv(url,
    ...     delimiter=',', header=True, comment_char="#",
    ...     column_type_hints={'user_id': int})
    """

    # noinspection PyShadowingBuiltins
    def __init__(self, data=None, format='auto', impl=None, verbose=False):
        """__init__(data=list(), format='auto')
        Construct a new XFrame from a url, a pandas.DataFrame or a Spark RDD or DataFrame.
        """
        if impl:
            self.__impl__ = impl
            return

        _format = self._classify_auto(data) if format == 'auto' else format
        # print >>stderr, 'format', _format

        if _format == 'pandas.dataframe':
            self.__impl__ = XFrameImpl.load_from_pandas_dataframe(data)
        elif _format == 'xframe_obj':
            self.__impl__ = XFrameImpl(data.to_rdd(), data.column_names(), data.column_types())
        elif _format == 'xarray':
            self.__impl__ = XFrameImpl().add_column(data.__impl__, '')
        elif _format == 'array':
            if len(data) > 0:
                unique_types = set([type(x) for x in data if x is not None])
                if len(unique_types) == 1 and XArray in unique_types:
                    xf = XFrameImpl()
                    for arr in data:
                        xf.add_column(arr.__impl__, '')
                    self.__impl__ = xf
                elif XArray in unique_types:
                    raise ValueError('Cannot create XFrame from mix of regular values and XArrays.')
                else:
                    self.__impl__ = XFrameImpl().add_column(XArray(data).__impl__, '')
            else:
                self.__impl__ = XFrameImpl()
        elif _format == 'iter':
            self.__impl__ = XFrameImpl().add_column(XArray(data).__impl__, '')
        elif _format == 'dict':
            xf = XFrameImpl()
            for key, val in iter(sorted(data.iteritems())):
                if type(val) == XArray:
                    xf.add_column(val.__impl__, key)
                else:
                    xf.add_column(XArray(val).__impl__, key)
            self.__impl__ = xf
        elif _format == 'iteritems':
            xf = XFrameImpl()
            for key, val in iter(sorted(data.iteritems())):
                if not hasattr(val, '__iter__'):
                    raise TypeError('Iterator values must be iterable.')
                xf.add_column(XArray(val).__impl__, key)
            self.__impl__ = xf
        elif _format == 'csv':
            url = make_internal_url(data)
            tmpxf = XFrame.read_csv(url, delimiter=',', header=True, verbose=verbose)
            self.__impl__ = tmpxf.__impl__
        elif _format == 'tsv':
            url = make_internal_url(data)
            tmpxf = XFrame.read_csv(url, delimiter='\t', header=True, verbose=verbose)
            self.__impl__ = tmpxf.__impl__
        elif _format == 'psv':
            url = make_internal_url(data)
            tmpxf = XFrame.read_csv(url, delimiter='|', header=True, verbose=verbose)
            self.__impl__ = tmpxf.__impl__
        elif _format == 'parquet':
            url = make_internal_url(data)
            tmpxf = XFrame.read_parquet(url)
            self.__impl__ = tmpxf.__impl__
        elif _format == 'xframe':
            url = make_internal_url(data)
            self.__impl__ = XFrameImpl.load_from_xframe_index(url)
        elif _format == 'spark.dataframe':
            self.__impl__ = XFrameImpl.load_from_spark_dataframe(data)
        elif _format == 'rdd':
            self.__impl__ = XFrameImpl.load_from_rdd(data)
        elif _format == 'empty':
            self.__impl__ = XFrameImpl()
        else:
            raise ValueError('Unknown input type: {}.'.format(format))
        if self.__impl__ is None:
            raise ValueError('Constructor failed')

    @staticmethod
    def _classify_auto(data):
        if HAS_PANDAS and isinstance(data, pandas.DataFrame):
            return 'pandas.dataframe'
        if isinstance(data, XArray):
            return 'xarray'
        if isinstance(data, XFrame):
            return 'xframe_obj'
        if hasattr(data, 'iteritems'):
            return 'iteritems'
        if hasattr(data, '__iter__'):
            return 'iter'
        if data is None:
            return 'empty'
        if str(type(data)) == "<class 'pyspark.sql.dataframe.DataFrame'>":
            return 'spark.dataframe'
        if str(type(data)) == "<class 'pyspark.rdd.RDD'>":
            return 'rdd'
        if isinstance(data, str) or isinstance(data, unicode):
            if data.endswith(('.csv', '.csv.gz')):
                return 'csv'
            if data.endswith(('.tsv', '.tsv.gz')):
                return 'tsv'
            if data.endswith(('.psv', '.psv.gz')):
                return 'psv'
            if data.endswith('.parquet'):
                return 'parquet'
            if data.endswith(('.txt', '.txt.gz')):
                print 'Assuming file is csv. For other delimiters, use `XFrame.read_csv`.'
                return 'csv'
            else:
                return 'xframe'
        raise ValueError('Cannot infer input type for data {}.'.format(date))

    @classmethod
    def set_max_row_width(cls, width):
        """
        Set the maximum display width for printing.

        Parameters
        ----------
        width : int
            The maximum width of the table when printing.
        """
        global MAX_ROW_WIDTH
        MAX_ROW_WIDTH = width

    @classmethod
    def set_html_max_row_width(cls, width):
        """
        Set the maximum display width for displaying in HTML.

        Parameters
        ----------
        width : int
            The maximum width of the table when printing in html.
        """
        global HTML_MAX_ROW_WIDTH
        HTML_MAX_ROW_WIDTH = width

    @classmethod
    def set_footer_strs(cls, footer_strs):
        """
        Set the footer printed beneath tables.

        Parameters
        ----------
        footer_strs : list
            A list of strings.  Each string is a separate line, printed beneath
            a table.  This footer is used when the length of the table
            is known.  To disable printing the footer, pass an empty list.
        """
        global FOOTER_STRS
        if type(footer_strs) is not list:
            raise TypeError('Footer strs must be a list')
        FOOTER_STRS = footer_strs

    @classmethod
    def set_lazy_footer_strs(cls, footer_strs):
        """
        Set the footer printed beneath tables when the length is unknown.

        Parameters
        ----------
        footer_strs : list
            A list of strings.  Each string is a separate line, printed beneath
            a table.  This footer is used when the length of the table
            is not known because the XFrame has not been evaluated.
            To disable printing the footer, pass an empty list.
        """
        global LAZY_FOOTER_STRS
        if type(footer_strs) is not list:
            raise TypeError('Footer strs must be a list')
        LAZY_FOOTER_STRS = footer_strs

    @staticmethod
    def _infer_column_types_from_lines(first_rows, na_values):
        if len(first_rows.column_names()) < 1:
            print >>stderr, 'Insufficient number of columns to perform type inference.'
            raise RuntimeError('Insufficient columns.')
        if len(first_rows) < 1:
            print >>stderr, 'Insufficient number of rows to perform type inference.'
            raise RuntimeError('Insufficient rows.')

        col_names = first_rows.column_names()

        def row_as_array(row, col_names):
            return [row[col] for col in col_names]
        head = [row_as_array(row, col_names) for row in first_rows]

        def infer_type(col, na_values):
            col = [val for val in col if val not in na_values]
            types = [classify_type(val) for val in col if val is not None]
            unique_types = set(types)
            if len(unique_types) == 1:
                dtype = types[0]
            elif unique_types == {int, float}:
                dtype = float
            else: 
                dtype = str
            return dtype
        n_cols = len(head[0])
        cols = [[row[i] for row in head] for i in range(n_cols)]
        types = [infer_type(col, na_values) for col in cols]

        # special handling for '\n'
#        if delimiter == '\n' and len(column_type_hints) != 1:
#          column_type_hints = [str]

        column_type_hints = types
        return column_type_hints

    @classmethod
    def load(cls, filename):
        """
        Load an XFrame. The filename extension is used to determine the format
        automatically. This function is particularly useful for XFrames previously
        saved in binary format. For CSV imports the :py:meth:`~xframes.XFrame.read_csv` function
        provides greater control. If the XFrame is in binary format, `filename` is
        actually a directory, created when the XFrame is saved.

        Parameters
        ----------
        filename : string
            Location of the file to load. Can be a local path or a remote URL.

        Returns
        -------
        out : XFrame

        See Also
        --------
        xframes.XFrame.save
            Saves the XFrame to a file.

        xframes.XFrame.read_csv
            Allows more control over csv parsing.

        Examples
        --------
        >>> sf = xframes.XFrame({'id':[1,2,3], 'val':['A','B','C']})
        >>> sf.save('my_xframe')        # 'my_xframe' is a directory
        >>> sf_loaded = xframes.XFrame.load('my_xframe')
        """
        sf = cls(data=filename)
        return sf

    @classmethod
    def _read_csv_impl(cls,
                       url,
                       delimiter=',',
                       header=True,
                       error_bad_lines=False,
                       comment_char='',
                       escape_char='\\',
                       double_quote=True,
                       quote_char='\"',
                       skip_initial_space=True,
                       column_type_hints=None,
                       na_values=None,
                       nrows=None,
                       verbose=False,
                       store_errors=True):
        """
        Constructs an XFrame from a CSV file or a path to multiple CSVs, and
        returns a pair containing the XFrame and optionally
        (if store_errors=True) a dict of filenames to XArrays
        indicating for each file, what are the incorrectly parsed lines
        encountered.

        Parameters
        ----------
        store_errors : bool
            If true, the output errors dict will be filled.

        See `read_csv` for the rest of the parameters.
        """
        na_values = na_values or '[NA]'
        parsing_config = dict()
        parsing_config['delimiter'] = delimiter
        parsing_config['use_header'] = header
        parsing_config['continue_on_failure'] = not error_bad_lines
        parsing_config['comment_char'] = comment_char
        parsing_config['escape_char'] = escape_char
        parsing_config['double_quote'] = double_quote
        parsing_config['quote_char'] = quote_char
        parsing_config['skip_initial_space'] = skip_initial_space
        parsing_config['store_errors'] = store_errors
        if type(na_values) is str:
            na_values = [na_values]
        if na_values is not None and len(na_values) > 0:
            parsing_config['na_values'] = na_values

        if nrows is not None:
            parsing_config['row_limit'] = nrows

        impl = XFrameImpl()
        internal_url = make_internal_url(url)

        # Attempt to automatically detect the column types. Either produce a
        # list of types; otherwise default to all str types.
        column_type_inference_was_used = False
        if column_type_hints is None:
            try:
                # Get the first 100 rows (using all the desired arguments).
                # first row may be excluded (based on heder setting)
                first_rows = xframes.XFrame.read_csv(
                    url,
                    nrows=100,
                    column_type_hints=str,
                    header=header,
                    delimiter=delimiter,
                    comment_char=comment_char,
                    escape_char=escape_char,
                    double_quote=double_quote,
                    quote_char=quote_char,
                    skip_initial_space=skip_initial_space)
                column_type_hints = XFrame._infer_column_types_from_lines(first_rows, na_values)
                typelist = '[' + ','.join(t.__name__ for t in column_type_hints) + ']'
                if verbose:
                    print >>stderr, '------------------------------------------------------'
                    print >>stderr, 'Inferred types from first line of file as '
                    print >>stderr, 'column_type_hints=' + typelist
                    print >>stderr, 'If parsing fails due to incorrect types, you can correct'
                    print >>stderr, 'the inferred type list above and pass it to read_csv in'
                    print >>stderr, 'the column_type_hints argument'
                    print >>stderr, '------------------------------------------------------'
                column_type_inference_was_used = True
            except Exception as e:
                # If the above fails, default back to str for all columns.
                if verbose:
                    print >>stderr, 'Error', type(e), e
                    print >>stderr, 'Could not detect types. Using str for each column.'
                column_type_hints = str

        if type(column_type_hints) is type:
            type_hints = {'__all_columns__': column_type_hints}
        elif type(column_type_hints) is list:
            type_hints = dict(zip(['__X%d__' % i for i in range(len(column_type_hints))], column_type_hints))
        elif type(column_type_hints) is dict:
            type_hints = column_type_hints
        else:
            raise TypeError("Invalid type for column_type_hints. Must be a 'dict, 'list' or a single type.")

        try:
            errors = impl.load_from_csv(internal_url, parsing_config, type_hints)
        except IOError:
            if column_type_inference_was_used:
                # try again
                print >>stderr, 'Unable to parse the file with automatic type inference.'
                print >>stderr, 'Defaulting to column_type_hints=str'
                type_hints = {'__all_columns__': str}
                try:
                    errors = impl.load_from_csv(internal_url, parsing_config, type_hints)
                except:
                    raise
            else:
                raise

        return cls(impl=impl), {f: XArray(impl=es) for (f, es) in errors.iteritems()}

    @classmethod
    def read_csv_with_errors(cls,
                             url,
                             delimiter=',',
                             header=True,
                             comment_char='',
                             escape_char='\\',
                             double_quote=True,
                             quote_char='\"',
                             skip_initial_space=True,
                             column_type_hints=None,
                             na_values=None,
                             nrows=None,
                             verbose=False):
        """
        Constructs an XFrame from a CSV file or a path to multiple CSVs, and
        returns a pair containing the XFrame and a dict of filenames to XArrays
        indicating for each file, what are the incorrectly parsed lines
        encountered.

        Parameters
        ----------
        url : string
            Location of the CSV file or directory to load. If URL is a directory
            or a "glob" pattern, all matching files will be loaded.

        delimiter : string, optional
            This describes the delimiter used for parsing csv files. Must be a
            single character.

        header : bool, optional
            If true, uses the first row as the column names. Otherwise use the
            default column names: 'X1, X2, ...'.

        comment_char : string, optional
            The character which denotes that the
            remainder of the line is a comment.

        escape_char : string, optional
            Character which begins a C escape sequence

        double_quote : bool, optional
            If True, two consecutive quotes in a string are parsed to a single
            quote.

        quote_char : string, optional
            Character sequence that indicates a quote.

        skip_initial_space : bool, optional
            Ignore extra spaces at the start of a field

        column_type_hints : None, type, list[type], dict[string, type], optional
            This provides type hints for each column. By default, this method
            attempts to detect the type of each column automatically.

            Supported types are int, float, str, list, dict, and array.array.

            * If a single type is provided, the type will be
              applied to all columns. For instance, column_type_hints=float
              will force all columns to be parsed as float.
            * If a list of types is provided, the types applies
              to each column in order, e.g.[int, float, str]
              will parse the first column as int, second as float and third as
              string.
            * If a dictionary of column name to type is provided,
              each type value in the dictionary is applied to the key it
              belongs to.
              For instance {'user':int} will hint that the column called "user"
              should be parsed as an integer, and the rest will default to
              string.

        na_values : str | list of str, optional
            A string or list of strings to be interpreted as missing values.

        nrows : int, optional
            If set, only this many rows will be read from the file.

        verbose : bool, optional
            If True, print the progress while reading files.

        Returns
        -------
        out : tuple
            The first element is the XFrame with good data. The second element
            is a dictionary of filenames to XArrays indicating for each file,
            what are the incorrectly parsed lines encountered.

        See Also
        --------
        xframes.XFrame.read_csv
            Reads csv without error controls.
        xframes.XFrame
            The constructor can read csv files, but is not configurable.

        Examples
        --------
        >>> bad_url = 'https://s3.amazonaws.com/gl-testdata/bad_csv_example.csv'
        >>> (xf, bad_lines) = xframes.XFrame.read_csv_with_errors(bad_url)
        >>> xf
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [98 rows x 3 columns]

        >>> bad_lines
        {'https://s3.amazonaws.com/gl-testdata/bad_csv_example.csv': dtype: str
         Rows: 1
         ['x,y,z,a,b,c']}
        """
        na_values = na_values or '[NA]'
        return cls._read_csv_impl(url,
                                  delimiter=delimiter,
                                  header=header,
                                  error_bad_lines=False,  # we are storing errors,
                                                          # thus we must not fail
                                                          # on bad lines
                                  comment_char=comment_char,
                                  escape_char=escape_char,
                                  double_quote=double_quote,
                                  quote_char=quote_char,
                                  skip_initial_space=skip_initial_space,
                                  column_type_hints=column_type_hints,
                                  na_values=na_values,
                                  nrows=nrows,
                                  verbose=verbose,
                                  store_errors=True)

    @classmethod
    def read_csv(cls,
                 url,
                 delimiter=',',
                 header=True,
                 error_bad_lines=False,
                 comment_char='',
                 escape_char='\\',
                 double_quote=True,
                 quote_char='\"',
                 skip_initial_space=True,
                 column_type_hints=None,
                 na_values=None,
                 nrows=None,
                 verbose=False):
        """
        Constructs an XFrame from a CSV file or a path to multiple CSVs.

        Parameters
        ----------
        url : string
            Location of the CSV file or directory to load. If URL is a directory
            or a "glob" pattern, all matching files will be loaded.

        delimiter : string, optional
            This describes the delimiter used for parsing csv files. Must be a
            single character.

        header : bool, optional
            If true, uses the first row as the column names. Otherwise use the
            default column names : 'X1, X2, ...'.

        error_bad_lines : bool
            If true, will fail upon encountering a bad line. If false, will
            continue parsing skipping lines which fail to parse correctly.
            A sample of the first 10 encountered bad lines will be printed.

        comment_char : string, optional
            The character which denotes that the remainder of the line is a
            comment.

        escape_char : string, optional
            Character which begins a C escape sequence

        double_quote : bool, optional
            If True, two consecutive quotes in a string are parsed to a single
            quote.

        quote_char : string, optional
            Character sequence that indicates a quote.

        skip_initial_space : bool, optional
            Ignore extra spaces at the start of a field

        column_type_hints : None, type, list[type], dict[string, type], optional
            This provides type hints for each column. By default, this method
            attempts to detect the type of each column automatically.

            Supported types are int, float, str, list, dict, and array.array.

            * If a single type is provided, the type will be
              applied to all columns. For instance, column_type_hints=float
              will force all columns to be parsed as float.
            * If a list of types is provided, the types applies
              to each column in order, e.g.[int, float, str]
              will parse the first column as int, second as float and third as
              string.
            * If a dictionary of column name to type is provided,
              each type value in the dictionary is applied to the key it
              belongs to.
              For instance {'user':int} will hint that the column called "user"
              should be parsed as an integer, and the rest will default to
              string.

        na_values : str | list of str, optional
            A string or list of strings to be interpreted as missing values.

        nrows : int, optional
            If set, only this many rows will be read from the file.

        verbose : bool, optional
            If True, print the progress while reading files.

        Returns
        -------
        out : XFrame

        See Also
        --------
        xframes.XFrame.read_csv_with_errors
            Allows more control over errors.

        xframes.XFrame
            The constructor can read csv files, but is not configurable.

        Examples
        --------

        Read a regular csv file, with all default options, automatically
        determine types:

        >>> url = 'http://s3.amazonaws.com/gl-testdata/rating_data_example.csv'
        >>> xf = xframes.XFrame.read_csv(url)
        >>> xf
        Columns:
          user_id int
          movie_id  int
          rating  int
        Rows: 10000
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10000 rows x 3 columns]

        Read only the first 100 lines of the csv file:

        >>> xf = xframes.XFrame.read_csv(url, nrows=100)
        >>> xf
        Columns:
          user_id int
          movie_id  int
          rating  int
        Rows: 100
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [100 rows x 3 columns]

        Read all columns as str type

        >>> xf = xframes.XFrame.read_csv(url, column_type_hints=str)
        >>> xf
        Columns:
          user_id  str
          movie_id  str
          rating  str
        Rows: 10000
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10000 rows x 3 columns]

        Specify types for a subset of columns and leave the rest to be str.

        >>> xf = xframes.XFrame.read_csv(url,
        ...                               column_type_hints={
        ...                               'user_id':int, 'rating':float
        ...                               })
        >>> xf
        Columns:
          user_id str
          movie_id  str
          rating  float
        Rows: 10000
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |  3.0   |
        |  25907  |   1663   |  3.0   |
        |  25923  |   1663   |  3.0   |
        |  25924  |   1663   |  3.0   |
        |  25928  |   1663   |  2.0   |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10000 rows x 3 columns]

        Not treat first line as header:

        >>> xf = xframes.XFrame.read_csv(url, header=False)
        >>> xf
        Columns:
          X1  str
          X2  str
          X3  str
        Rows: 10001
        +---------+----------+--------+
        |    X1   |    X2    |   X3   |
        +---------+----------+--------+
        | user_id | movie_id | rating |
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10001 rows x 3 columns]

        Treat '3' as missing value:

        >>> xf = xframes.XFrame.read_csv(url, na_values=['3'], column_type_hints=str)
        >>> xf
        Columns:
          user_id str
          movie_id  str
          rating  str
        Rows: 10000
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |  None  |
        |  25907  |   1663   |  None  |
        |  25923  |   1663   |  None  |
        |  25924  |   1663   |  None  |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10000 rows x 3 columns]

        Throw error on parse failure:

        >>> bad_url = 'https://s3.amazonaws.com/gl-testdata/bad_csv_example.csv'
        >>> xf = xframes.XFrame.read_csv(bad_url, error_bad_lines=True)
        RuntimeError: Runtime Exception. Unable to parse line "x,y,z,a,b,c"
        Set error_bad_lines=False to skip bad lines
        """

        na_values = na_values or ['NA']
        ret = cls._read_csv_impl(url,
                                 delimiter=delimiter,
                                 header=header,
                                 error_bad_lines=error_bad_lines,
                                 comment_char=comment_char,
                                 escape_char=escape_char,
                                 double_quote=double_quote,
                                 quote_char=quote_char,
                                 skip_initial_space=skip_initial_space,
                                 column_type_hints=column_type_hints,
                                 na_values=na_values,
                                 nrows=nrows,
                                 verbose=verbose,
                                 store_errors=False)
        return ret[0]

    @classmethod
    def read_text(cls,
                  url,
                  delimiter=None,
                  nrows=None,
                  verbose=False):
        """
        Constructs an XFrame from a text file or a path to multiple text files.

        Parameters
        ----------
        url : string
            Location of the text file or directory to load. If URL is a directory
            or a "glob" pattern, all matching files will be loaded.

        delimiter : string, optional
            This describes the delimiter used for separating records. Must be a
            single character.  Defaults to newline.

        nrows : int, optional
            If set, only this many rows will be read from the file.

        verbose : bool, optional
            If True, print the progress while reading files.

        Returns
        -------
        out : XFrame

        Examples
        --------

        Read a regular text file, with default options.

        >>> url = 'http://s3.amazonaws.com/gl-testdata/rating_data_example.csv'
        >>> xf = xframes.XFrame.read_text(url)
        >>> xf
        +-------
        | text |
        +---------+
        |  25904  |
        |  25907  |
        |  25923  |
        |  25924  |
        |  25928  |
        |   ...   |
        +---------+
        [10000 rows x 1 column]

        Read only the first 100 lines of the text file:

        >>> xf = xframes.XFrame.read_text(url, nrows=100)
        >>> xf
        Rows: 100
        +---------+
        |  25904  |
        |  25907  |
        |  25923  |
        |  25924  |
        |  25928  |
        |   ...   |
        +---------+
        [100 rows x 1 columns]

        Read using a given delimiter.

        >>> xf = xframes.XFrame.read_text(url, delimiter='.')
        >>> xf
        Rows: 250
        +---------+
        |  25904  |
        |  25907  |
        |  25923  |
        |  25924  |
        |  25928  |
        |   ...   |
        +---------+
        [250 rows x 1 columns]


        """
        impl = XFrameImpl()
        impl.read_from_text(url,
                            delimiter=delimiter,
                            nrows=nrows,
                            verbose=verbose)
        return cls(impl=impl)

    @classmethod
    def read_parquet(cls, url):
        """
        Constructs an XFrame from a parquet file.

        Parameters
        ----------
        url : string
            Location of the parquet file to load. 

        verbose : bool, optional
            If True, print the progress while reading the file.

        Returns
        -------
        out : XFrame

        See Also
        --------
        xframes.XFrame
            The constructor can read parquet files.

        """
        impl = XFrameImpl()
        impl.load_from_parquet(url)
        return cls(impl=impl)

    def dump_debug_info(self):
        """
        Print information about the Spark RDD associated with this XFrame.
        """
        return self.__impl__.dump_debug_info()

    def __get_column_description__(self):
        colnames = self.column_names()
        coltypes = self.column_types()
        ret = 'Columns:\n'
        if len(colnames) > 0:
            for i in range(len(colnames)):
                ret = ret + '\t' + colnames[i] + '\t' + coltypes[i].__name__ + '\n'
            ret += '\n'
        else:
            ret += '\tNone\n\n'
        return ret

    def __get_pretty_tables__(self, wrap_text=False, max_row_width=MAX_ROW_WIDTH,
                              max_column_width=30, max_columns=20,
                              max_rows_to_display=60):
        """
        Returns a list of pretty print tables representing the current XFrame.
        If the number of columns is larger than max_columns, the last pretty
        table will contain an extra column of "...".

        Parameters
        ----------
        wrap_text : bool, optional

        max_row_width : int, optional
            Max number of characters per table.

        max_column_width : int, optional
            Max number of characters per column.

        max_columns : int, optional
            Max number of columns per table.

        max_rows_to_display : int, optional
            Max number of rows to display.

        Returns
        -------
        out : list[PrettyTable]
        """
        headxf = self.head(max_rows_to_display)
        if headxf.shape == (0, 0):
            return [PrettyTable()]

        def _truncate_str(s, wrap_str=False):
            """
            Truncate and optionally wrap the input string as unicode, replace
            unconvertible character with a diamond ?.
            """
            s = repr(s)
            # repr adds the escape characters. but also adds quotes around
            # the string
            if len(s) >= 2:
                s = s[1:-1]
            if len(s) <= max_column_width:
                return unicode(s, errors='replace')
            else:
                # if wrap_str is true, wrap the text and take at most 2 rows
                if wrap_str:
                    wrapped_lines = wrap(s, max_column_width)
                    ret = '\n'.join(wrapped_lines[:2])
                    last_line = wrapped_lines[:2][-1]
                    if len(last_line) >= max_column_width or len(wrapped_lines) > 2:
                        space_left = max_column_width - len(last_line)
                        space_truncate = max(0, 4 - space_left)
                        if space_truncate > 0:
                            ret = ret[:-space_truncate] + ' ...'
                        else:
                            ret += ' ...'
                else:
                    ret = s[:max_column_width]
                    ret = ret[:-4] + ' ...'
                return unicode(ret, errors='replace')
            # end of truncate_str

        columns = self.column_names()[:max_columns]
        columns.reverse()  # reverse the order of columns and we will pop from the end

        num_column_of_last_table = 0
        row_of_tables = []
        # let's build a list of tables with max_columns
        # each table should satisfy, max_row_width, and max_column_width
        while len(columns) > 0:
            tbl = PrettyTable()
            table_width = 0
            num_column_of_last_table = 0
            while len(columns) > 0:
                col = columns.pop()
                # check the max length of element in the column
                header = _truncate_str(col, wrap_text)
                if len(headxf) > 0:
                    col_width = min(max_column_width, max(max(len(str(x)) for x in headxf[col]), len(header) + 3))
                else:
                    col_width = max_column_width
                if table_width + col_width < max_row_width:
                    # truncate the header if necessary
                    tbl.add_column(header, [_truncate_str(str(x), wrap_text) for x in headxf[col]])
                    table_width = str(tbl).find('\n')
                    num_column_of_last_table += 1
                else:
                    # the column does not fit in the current table, push it back to columns
                    columns.append(col)
                    break
            tbl.align = 'c'
            row_of_tables.append(tbl)

        # add a column of all "..." if there are more columns than displayed
        if self.num_columns() > max_columns:
            row_of_tables[-1].add_column('...', ['...'] * len(headxf))
            num_column_of_last_table += 1

        # add a row of all "..." if there are more rows than displayed
        if self.__has_size__() and self.num_rows() > headxf.num_rows():
            row_of_tables[-1].add_row(['...'] * num_column_of_last_table)
        return row_of_tables

    def print_rows(self, num_rows=10, num_columns=40, max_column_width=30, max_row_width=MAX_ROW_WIDTH):
        """
        Print the first rows and columns of the XFrame in human readable format.

        Parameters
        ----------
        num_rows : int, optional
            Number of rows to print.

        num_columns : int, optional
            Number of columns to print.

        max_column_width : int, optional
            Maximum width of a column. Columns use fewer characters if possible.

        max_row_width : int, optional
            Maximum width of a printed row. Columns beyond this width wrap to a
            new line. `max_row_width` is automatically reset to be the
            larger of itself and `max_column_width`.

        See Also
        --------
        xframes.XFrame.head
            Returns the first part of a XFrame.

        xframes.XFrame.tail
            Returns the last part of an XFrame.
        """

        max_row_width = max(max_row_width, max_column_width + 1)

        row_of_tables = self.__get_pretty_tables__(wrap_text=False,
                                                   max_rows_to_display=num_rows,
                                                   max_columns=num_columns,
                                                   max_column_width=max_column_width,
                                                   max_row_width=max_row_width)
        footer = '[%d rows x %d columns]\n' % self.shape
        print '\n'.join([str(tb) for tb in row_of_tables]) + '\n' + footer

    def __str__(self, num_rows=10, footer=True):
        """
        Returns a string containing the first 10 elements of the frame, along
        with a description of the frame.
        """
        max_rows_to_display = num_rows

        row_of_tables = self.__get_pretty_tables__(wrap_text=False, 
                                                   max_rows_to_display=max_rows_to_display,
                                                   max_row_width=MAX_ROW_WIDTH)
        if not footer:
            return '\n'.join([str(tb) for tb in row_of_tables])

        if self.__has_size__():
            footer = '[%d rows x %d columns]\n' % self.shape
            if self.num_rows() > max_rows_to_display:
                footer += '\n'.join(FOOTER_STRS)
        else:
            footer = '[? rows x %d columns]\n' % self.num_columns()
            footer += '\n'.join(LAZY_FOOTER_STRS)
        return '\n'.join([str(tb) for tb in row_of_tables]) + '\n' + footer

    def _repr_html_(self):
        max_rows_to_display = 10

        row_of_tables = self.__get_pretty_tables__(wrap_text=True, 
                                                   max_row_width=HTML_MAX_ROW_WIDTH,
                                                   max_columns=40, 
                                                   max_column_width=25, 
                                                   max_rows_to_display=max_rows_to_display)
        if self.__has_size__():
            footer = '[%d rows x %d columns]<br/>' % self.shape
            if self.num_rows() > max_rows_to_display:
                footer += '<br/>'.join(FOOTER_STRS)
        else:
            footer = '[? rows x %d columns]<br/>' % self.num_columns()
            footer += '<br/>'.join(LAZY_FOOTER_STRS)
        begin = '<div style="max-height:1000px;max-width:1500px;overflow:auto;">'
        end = '\n</div>'
        return begin + '\n'.join([tb.get_html_string(format=True) for tb in row_of_tables]) + '\n' + footer + end

    def __nonzero__(self):
        """
        Returns true if the frame is not empty.
        """
        return self.num_rows() != 0

    def __len__(self):
        """
        Returns the number of rows of the xframe.
        """
        return self.num_rows()

    def __copy__(self):
        """
        Returns a shallow copy of the xframe.
        """
        return self.select_columns(self.column_names())

    def _row_selector(self, other):
        """
        Where other is an XArray of identical length as the current Frame,
        this returns a selection of a subset of rows in the current XFrame
        where the corresponding row in the selector is non-zero.
        """
        if type(other) is XArray:
            return XFrame(impl=self.__impl__.logical_filter(other.__impl__))

    def width(self):
        """
        Diagnostic: the number of elements in each tuple of the RDD.
        """
        return XArray(impl=self.__impl__.width())

    def num_rows(self):
        """
        The number of rows in this XFrame.

        Returns
        -------
        out : int
            Number of rows in the XFrame.

        See Also
        --------
        xframes.XFrame.num_columns
            Returns the number of columns.
        """
        return self.__impl__.num_rows()

    def num_columns(self):
        """
        The number of columns in this XFrame.

        Returns
        -------
        out : int
            Number of columns in the XFrame.

        See Also
        --------
        xframes.XFrame.num_rows
            Returns the number of rows.
        """
        return self.__impl__.num_columns()

    def column_names(self):
        """
        The name of each column in the XFrame.

        Returns
        -------
        out : list[string]
            Column names of the XFrame.

        See Also
        --------
        xframes.XFrame.rename
            Renames the columns.
        """
        return self.__impl__.column_names()

    def column_types(self):
        """
        The type of each column in the XFrame.

        Returns
        -------
        out : list[type]
            Column types of the XFrame.

        See Also
        --------
        xframes.XFrame.dtype
            This is a synonym for column_types.
        """
        return self.__impl__.dtype()

    def dtype(self):
        """
        The type of each column in the XFrame.

        Returns
        -------
        out : list[type]
            Column types of the XFrame.

        See Also
        --------
        xframes.XFrame.column_types
            This is a synonym for dtype.
        """
        return self.column_types()

    def head(self, n=10):
        """
        The first n rows of the XFrame.

        Parameters
        ----------
        n : int, optional
            The number of rows to fetch.

        Returns
        -------
        out : XFrame
            A new XFrame which contains the first n rows of the current XFrame

        See Also
        --------
        xframes.XFrame.tail
            Returns the last part of the XFrame.

        xframes.XFrame.print_rows
            Prints the XFrame.
        """
        return XFrame(impl=self.__impl__.head(n))

    def tail(self, n=10):
        """
        The last n rows of the XFrame.

        Parameters
        ----------
        n : int, optional
            The number of rows to fetch.

        Returns
        -------
        out : XFrame
            A new XFrame which contains the last n rows of the current XFrame

        See Also
        --------
        xframes.XFrame.head
            Returns the first part of the XFrame.

        xframes.XFrame.print_rows
            Prints the XFrame.
        """
        return XFrame(impl=self.__impl__.tail(n))

    def to_pandas_dataframe(self):
        """
        Convert this XFrame to pandas.DataFrame.

        This operation will construct a pandas.DataFrame in memory. Care must
        be taken when size of the returned object is big.

        Returns
        -------
        out : pandas.DataFrame
            The dataframe which contains all rows of XFrame
        """
        if not HAS_PANDAS:
            raise TypeError('Pandas not found in PYTHONPATH.')
        df = pandas.DataFrame()
        for i in range(self.num_columns()):
            column_name = self.column_names()[i]
            df[column_name] = list(self[column_name])
            if len(df[column_name]) == 0:
                df[column_name] = df[column_name].astype(self.column_types()[i])
        return df

    def to_dataframeplus(self):
        """
        Convert this XFrame to xpatterns DataFramePlus

        This operation will construct a DataFramePlus in memory. Care must
        be taken when size of the returned object is big.

        Returns
        -------
        out : DataFramePlus
            The dataframe which contains all rows of XFrame
        """
        if not HAS_DATAFRAME_PLUS:
            raise TypeError('DataFramePlus not found in PYTHONPATH.')
        df = pandas.DataFrame()
        for i in range(self.num_columns()):
            column_name = self.column_names()[i]
            df[column_name] = list(self[column_name])
            if len(df[column_name]) == 0:
                df[column_name] = df[column_name].astype(self.column_types()[i])
        return dataframeplus.DataFramePlus(df)

    def to_rdd(self):
        """
        Convert the current XFrame to a Spark RDD.  The RDD consists of tuples
        containing the column data.  No conversion is necessary: the internal RDD is
        returned.

        Returns
        -------
        out : spark.RDD
            The spark RDD that is used to represent the XFrame.

        See Also
        --------
        from_rdd
            Converts from a Spark RDD.
        """
        return self.__impl__.to_rdd()

    def to_spark_dataframe(self, table_name=None, number_of_partitions=4):
        """
        Convert the current XFrame to a Spark DataFrame.

        Parameters
        ----------
        table_name: str, optional
            If given, name the temporary table.

        number_of_partitions: int, optional
            The number of partitions to create.

        Returns
        -------
        out: spark.DataFrame
        """
        return self.__impl__.to_spark_dataframe(table_name, 
                                                number_of_partitions=number_of_partitions)

    @classmethod
    def from_rdd(cls, rdd, column_names=None, column_types=None):
        """
        Create a XFrame from a spark RDD or spark DataFrame.  The data should be:
        * an RDD of tuples
        * Each tuple should be of the same length.
        * Each "column" should be of a uniform type.

        Parameters
        ----------
        rdd: spark.RDD or spark.DataFrame
            Data used to populate the XFrame

        column_names : list of string, optional
            The column names to use.  Ignored for Spark DataFrames.

        column_types : list of type, optional
            The column types to use.  Ignored for Spark DataFrames.

        Returns
        -------
        out : XFrame

        See Also
        --------
        to_rdd
            Converts to a Spark RDD.
        """
        check_res = rdd.take(1)

        if len(check_res) > 0 \
                and check_res[0].__class__.__name__ == 'Row' \
                and rdd.__class__.__name__ != 'DataFrame':
            raise Exception('Conversion from RDD(pyspark.sql.Row) to XFrame not supported. ' +
                            'Please call inferSchema(RDD) first.')
        xf = cls()
        if XFrameImpl.is_dataframe(rdd):
            xf.__impl__ = XFrameImpl.load_from_spark_dataframe(rdd)
        elif XFrameImpl.is_rdd(rdd):
            xf.__impl__ = XFrameImpl.load_from_rdd(rdd, column_names, column_types)
        else:
            raise ValueError('Argument is not an RDD.')
        return xf

    def apply(self, fn, dtype=None, seed=None):
        """
        Transform each row to an :class:`~xframes.XArray` according to a
        specified function. Returns a new XArray of `dtype` where each element
        in this XArray is transformed by `fn(x)` where `x` is a single row in
        the xframe represented as a dictionary.  The `fn` should return
        exactly one value which can be cast into type `dtype`. If `dtype` is
        not specified, the first 100 rows of the XFrame are used to make a guess
        of the target data type.

        Parameters
        ----------
        fn : function
            The function to transform each row of the XFrame. The return
            type should be convertible to `dtype` if `dtype` is not None.

        dtype : data type, optional
            The `dtype` of the new XArray. If None, the first 100
            elements of the array are used to guess the target
            data type.

        seed : int, optional
            Used as the seed if a random number generator is included in `fn`.

        Returns
        -------
        out : XArray
            The XArray transformed by fn.  Each element of the XArray is of
            type `dtype`

        Examples
        --------
        Concatenate strings from several columns:

        >>> xf = xframes.XFrame({'user_id': [1, 2, 3], 'movie_id': [3, 3, 6],
                                  'rating': [4, 5, 1]})
        >>> xf.apply(lambda x: str(x['user_id']) + str(x['movie_id']) + str(x['rating']))
        dtype: str
        Rows: 3
        ['134', '235', '361']
        """
        if not inspect.isfunction(fn):
            raise TypeError('Input must be a function.')
        rows = self.__impl__.head_as_list(10)
        names = self.__impl__.column_names()
        dryrun = [fn(dict(zip(names, row))) for row in rows]
        if dtype is None:
            dtype = infer_type_of_list(dryrun)

        if not seed:
            seed = int(time.time())

        return XArray(impl=self.__impl__.transform(fn, dtype, seed))

    def transform_col(self, col, fn=None, dtype=None, seed=None):
        """
        Transform a single column according to a specified function. 
        The remaining columns are not modified.
        The type of the transformed column types becomes `dtype`, with
        the new value being the result of `fn(x)`, where `x` is a single row in
        the XFrame represented as a dictionary.  The `fn` should return
        exactly one value which can be cast into type `dtype`. If `dtype` is
        not specified, the first 100 rows of the XFrame are used to make a guess
        of the target data type.

        Parameters
        ----------
        col : string
            The name of the column to transform.

        fn : function, optional
            The function to transform each row of the XFrame. The return
            type should be convertible to `dtype` if `dtype` is not None.
            If the function is not given, an identity function is used.

        dtype : dtype, optional
            The column data type of the new XArray. If None, the first 100
            elements of the array are used to guess the target
            data type.

        seed : int, optional
            Used as the seed if a random number generator is included in `fn`.

        Returns
        -------
        out : XFrame
            An XFrame with the given column transformed by the function and cast to the given type.

        Examples
        --------
        Translate values in a column:

        >>> xf = xframes.XFrame({'user_id': [1, 2, 3], 'movie_id': [3, 3, 6],
                                  'rating': [4, 5, 1]})
        >>> xf.transform_col('rating', lambda row: row['rating'] * 2)


        Cast values in a column to a different type

        >>> xf = xframes.XFrame({'user_id': [1, 2, 3], 'movie_id': [3, 3, 6],
                                  'rating': [4, 5, 1]})
        >>> xf.transform_col('user_id', dtype=str)

        """
        if fn is None:
            fn = lambda row: row[col]
        elif not inspect.isfunction(fn):
            raise TypeError('Input must be a function.')
        rows = self.__impl__.head_as_list(10)
        names = self.__impl__.column_names()
        # do the dryrun so we can see some diagnostic output
        dryrun = [fn(dict(zip(names, row))) for row in rows]
        if dtype is None:
            dtype = infer_type_of_list(dryrun)
        if not seed:
            seed = int(time.time())

        return XFrame(impl=self.__impl__.transform_col(col, fn, dtype, seed))

    def transform_cols(self, cols, fn=None, dtypes=None, seed=None):
        """
        Transform multiple columns according to a specified function. 
        The remaining columns are not modified.
        The type of the transformed column types are given by  `dtypes`, with
        the new values being the result of `fn(x)` where `x` is a single row in
        the xframe represented as a dictionary.  The `fn` should return
        a value for each element of cols, which can be cast into the corresponding `dtype`.
        If `dtypes` is not specified, the first 100 rows of the XFrame are
        used to make a guess of the target data types.

        Parameters
        ----------
        cols : list of string
            The names of the column to transform.

        fn : function, optional
            The function to transform each row of the XFrame. The return
            value should be a list of values, one for each column of cols.
            each type should be convertible to the corresponding `dtype` if `dtype` is not None.
            If the function is not given, an identity function is used.

        dtypes : list of dtype, optional
            The data types of the new columns. There must be one data type
            for each column in cols.  If not supplied, the first 100
            elements of the array are used to guess the target
            data types.

        seed : int, optional
            Used as the seed if a random number generator is included in `fn`.

        Returns
        -------
        out : XFrame
            An XFrame with the given columns transformed by the function and cast to the given types.

        Examples
        --------
        Translate values in a column:

        >>> xf = xframes.XFrame({'user_id': [1, 2, 3], 'movie_id': [3, 3, 6],
                                  'rating': [4, 5, 1]})
        >>> xf.transform_col(['movie_id', 'rating'], lambda row: [row['movie_id'] + 1, row['rating'] * 2])


        Cast types in several columns:

        >>> xf = xframes.XFrame({'user_id': [1, 2, 3], 'movie_id': [3, 3, 6],
                                  'rating': [4, 5, 1]})
        >>> xf.transform_col(['movie_id', 'rating'], dtype=[str, str])

        """
        if fn is None:
            fn = lambda row: [row[col] for col in cols]
        elif not inspect.isfunction(fn):
            raise TypeError('Input must be a function.')
        rows = self.__impl__.head_as_list(10)
        names = self.__impl__.column_names()
        # do the dryrun so we can see some diagnostic output
        dryrun = [fn(dict(zip(names, row))) for row in rows]
        if dtypes is None:
            if len(dryrun[0]) != len(cols):
                raise ValueError('Function return length must match number of cols.')
            dtypes = []
            for index in range(0, len(cols)):
                dryrun_col = [row[index] for row in dryrun]
                dtypes.append(infer_type_of_list(dryrun_col))
        else:
            if len(cols) != len(dtypes):
                raise ValueError('Length of cols and dtypes must match.')
        if not seed:
            seed = int(time.time())

        return XFrame(impl=self.__impl__.transform_cols(cols, fn, dtypes, seed))

    def detect_type(self, column_name):
        """
        If the column is of string type, and the values can safely be cast to int or float, then
        return the type to be cast to.  Uses the entire column to detect the type.

        Parameters
        ----------
        column_name : str
            The name of the column to cast.

        Returns
        -------
        out : type
            int or float: The column can be cast to this type.

            str: The column cannot be cast to one of the types above.

        Examples
        --------

        >>> xf = xpatterns.XFrame({'value': ['1', '2', '3']})
        >>> xf.detect_type('value')

        """
        column = self.__getitem__(column_name)
        if column.dtype() is not str:
            return str

        def classify_type(s):
            if type(s) != str:
                return 'expected str, got {}: {}'.format(type(s), s)
            if len(s) == 0:
                return ''
            if s.startswith('-'):
                s = s[1:]
            if s.isdigit(): return 'int'
            if s.replace('.', '', 1).isdigit(): return 'float'
            return 'str'

        types = list(column.apply(classify_type).unique())
        if len(types) == 2 and '' in types and 'int' in types:
            types = ['float']
        if '' in types: types.remove('')
        if len(types) != 1:
            return str
        if len(types) == 1 and types[0] == 'str':
            return str

        if len(types) == 1 and 'int' in types:
            return int
        if len(types) == 1 and 'float' in types:
            return float
        if len(types) == 2 and 'float' in types and 'int' in types:
            return float

        return str

    def detect_type_and_cast(self, column_name):
        """
        If the column is of string type, and the values can all be interpreted as
        integer or float values, then cast the column to the numerical type.

        Parameters
        ----------
        column_name : str
            The name of the column to cast.

        Examples
        --------

        >>> xf = xpatterns.XFrame({'value': ['1', '2', '3']})
        >>> xf.detect_type_and_cast('value')

        """
        new_type = self.detect_type(column_name)
        if new_type is None:
            return self

        def cast_int(row):
            val = row[column_name]
            if val is None:
                return [None]
            if len(val) == 0:
                return [None]
            try:
                return [int(val)]
            except ValueError:
                raise ValueError('Cast failed: (int) {}'.format(val))

        def cast_float(row):
            val = row[column_name]
            if val is None:
                return [None]
            if len(val) == 0:
                return [util.nan]
            try:
                return [float(val)]
            except ValueError:
                raise ValueError('Cast failed: (float) {}'.format(val))

        if new_type in [int]:
            return XFrame(impl=self.__impl__.transform_cols([column_name], cast_int, [int], 0))
        if new_type in [float]:
            return XFrame(impl=self.__impl__.transform_cols([column_name], cast_float, [float], 0))

        return self

    def flat_map(self, column_names, fn, column_types='auto', seed=None):
        """
        Map each row of the XFrame to multiple rows in a new XFrame via a
        function.

        The output of `fn` must have type ``list[list[...]]``.  Each inner list
        will be a single row in the new output, and the collection of these
        rows within the outer list make up the data for the output XFrame.
        All rows must have the same length and the same order of types to
        make sure the result columns are homogeneously typed.  For example, if
        the first element emitted into in the outer list by `fn` is
        ``[43, 2.3, 'string']``, then all other elements emitted into the outer
        list must be a list with three elements, where the first is an `int`,
        second is a `float`, and third is a `string`.  If `column_types` is not
        specified, the first 10 rows of the XFrame are used to determine the
        column types of the returned XFrame.

        Parameters
        ----------
        column_names : list[str]
            The column names for the returned XFrame.

        fn : function
            The function that maps each of the xframe row into multiple rows,
            returning ``list[list[...]]``.  All output rows must have the same
            length and order of types.

        column_types : list[type], optional
            The column types of the output XFrame. Default value
            will be automatically inferred by running `fn` on the first
            10 rows of the output.

        seed : int, optional
            Used as the seed if a random number generator is included in `fn`.

        Returns
        -------
        out : XFrame
            A new XFrame containing the results of the ``flat_map`` of the
            original XFrame.

        Examples
        ---------
        Repeat each row according to the value in the 'number' column.

        >>> xf = xframes.XFrame({'letter': ['a', 'b', 'c'],
        ...                       'number': [1, 2, 3]})
        >>> xf.flat_map(['number', 'letter'],
        ...             lambda x: [list(x.itervalues()) for _ in range(0, x['number'])])
        +--------+--------+
        | number | letter |
        +--------+--------+
        |   1    |   a    |
        |   2    |   b    |
        |   2    |   b    |
        |   3    |   c    |
        |   3    |   c    |
        |   3    |   c    |
        +--------+--------+
        [6 rows x 2 columns]
        """
        if not inspect.isfunction(fn):
            raise TypeError('Input must be a function')
        if not seed:
            seed = int(time.time())

        # determine the column_types
        if column_types == 'auto':
            types = set()
            rows = self.__impl__.head_as_list(10)
            names = self.__impl__.column_names()
            results = [fn(dict(zip(names, row))) for row in rows]
            if not (results is None or type(results) == list):
                raise TypeError('Output type of the lambda function must be a list of lists.')
            else:
                for rows in results:
                    if type(rows) is not list:
                        raise TypeError('Output type of the lambda function must be a list of lists.')
                    for row in rows:
                        if type(row) is not list:
                            raise TypeError('Output type of the lambda function must be a list of lists.')
                        types.add(tuple([type(v) for v in row]))
            if len(types) != 1:
                raise TypeError('Mapped rows must have the same length and types.')

            column_types = list(types.pop())

        assert type(column_types) is list
        if not len(column_types) == len(column_names):
            raise TypeError('Number of output columns must match the size of column names.')
        return XFrame(impl=self.__impl__.flat_map(fn, column_names, column_types, seed))

    def sample(self, fraction, seed=None):
        """
        Sample the current XFrame's rows.

        Parameters
        ----------
        fraction : float
            Approximate fraction of the rows to fetch. Must be between 0 and 1.
            The number of rows returned is approximately the fraction times the
            number of rows.

        seed : int, optional
            Seed for the random number generator used to sample.

        Returns
        -------
        out : XFrame
            A new XFrame containing sampled rows of the current XFrame.

        Examples
        --------
        Suppose we have an XFrame with 6,145 rows.

        >>> import random
        >>> xf = XFrame({'id': range(0, 6145)})

        Retrieve about 30% of the XFrame rows with repeatable results by
        setting the random seed.

        >>> len(xf.sample(.3, seed=5))
        1783
        """
        if not seed:
            seed = int(time.time())

        if fraction > 1 or fraction < 0:
            raise ValueError('Invalid sampling rate: {}.'.format(fraction))

        if self.num_rows() == 0 or self.num_columns() == 0:
            return self
        else:
            return XFrame(impl=self.__impl__.sample(fraction, seed))

    def random_split(self, fraction, seed=None):
        """
        Randomly split the rows of an XFrame into two XFrames. The first XFrame
        contains *M* rows, sampled uniformly (without replacement) from the
        original XFrame. *M* is approximately the fraction times the original
        number of rows. The second XFrame contains the remaining rows of the
        original XFrame.

        Parameters
        ----------
        fraction : float
            Approximate fraction of the rows to fetch for the first returned
            XFrame. Must be between 0 and 1.

        seed : int, optional
            Seed for the random number generator used to split.

        Returns
        -------
        out : tuple [XFrame]
            Two new XFrames.

        Examples
        --------
        Suppose we have an XFrame with 6,145 rows and we want to randomly split
        it into training and testing datasets with about a 70%/30% split.

        >>> xf = xframes.XFrame({'id': range(1024)})
        >>> xf_train, xf_test = xf.random_split(.9, seed=5)
        >>> print len(xf_test), len(xf_train)
        102 922
        """
        if fraction > 1 or fraction < 0:
            raise ValueError('Invalid sampling rate: {}.'.format(fraction))
        if self.num_rows() == 0 or self.num_columns() == 0:
            return XFrame(), XFrame()

        if not seed:
            seed = int(time.time())

        # The server side requires this to be an int, so cast if we can
        try:
            seed = int(seed)
        except ValueError:
            raise ValueError("The 'seed' parameter must be of type int.")

        impl_pair = self.__impl__.random_split(fraction, seed)
        return XFrame(data=[], impl=impl_pair[0]), XFrame(data=[], impl=impl_pair[1])

    def topk(self, column_name, k=10, reverse=False):
        """
        Get k rows according to the largest values in the given column. Result is
        sorted by `column_name` in the given order (default is descending).
        When `k` is small, `topk` is more efficient than `sort`.

        Parameters
        ----------
        column_name : string
            The column to sort on

        k : int, optional
            The number of rows to return

        reverse : bool, optional
            If True, return the top k rows in ascending order, otherwise, in
            descending order.

        Returns
        -------
        out : XFrame
            an XFrame containing the top k rows sorted by column_name.

        See Also
        --------
        xframes.XFrame.sort

        Examples
        --------
        >>> xf = xframes.XFrame({'id': range(1000)})
        >>> xf['value'] = -xf['id']
        >>> xf.topk('id', k=3)
        +--------+--------+
        |   id   |  value |
        +--------+--------+
        |   999  |  -999  |
        |   998  |  -998  |
        |   997  |  -997  |
        +--------+--------+
        [3 rows x 2 columns]

        >>> xf.topk('value', k=3)
        +--------+--------+
        |   id   |  value |
        +--------+--------+
        |   1    |  -1    |
        |   2    |  -2    |
        |   3    |  -3    |
        +--------+--------+
        [3 rows x 2 columns]
        """
        if type(column_name) is not str:
            raise TypeError('Column_name must be a string.')

        xf = self[self[column_name].topk_index(topk=k, reverse=reverse)]
        return xf.sort(column_name, ascending=reverse)

    def save(self, filename, file_format=None):
        """
        Save the XFrame to a file system for later use.

        Parameters
        ----------
        filename : string
            The location to save the XFrame. Either a local directory or a
            remote URL. If the format is 'binary', a directory will be created
            at the location which will contain the xframe.

        format : {'binary', 'csv', 'parquet'}, optional
            Format in which to save the XFrame. Binary saved XFrames can be
            loaded much faster and without any format conversion losses. If not
            given, will try to infer the format from filename given. If file
            name ends with 'csv' or '.csv.gz', then save as 'csv' format.
            If the file ends with 'parquet', then save as parquet file.
            Otherwise save as 'binary' format.

        See Also
        --------
        xframes.XFrame.load
        xframes.XFrame.XFrame

        Examples
        --------
        >>> # Save the xframe into binary format
        >>> xf.save('data/training_data_xframe')

        >>> # Save the xframe into csv format
        >>> xf.save('data/training_data.csv', file_format='csv')
        """

        if file_format is None:
            if filename.endswith(('.csv', '.csv.gz')):
                file_format = 'csv'
            elif filename.endswith('.parquet'):
                file_format = 'parquet'
            else:
                file_format = 'binary'
        else:
            if file_format is 'csv':
                if not filename.endswith(('.csv', '.csv.gz')):
                    filename += '.csv'
            elif file_format is 'parquet':
                if not filename.endswith('.parquet'):
                    filename += '.parquet'
            elif file_format is not 'binary':
                raise ValueError("Invalid format: {}. Supported formats are 'csv', 'parquet', and 'binary'."
                                 .format(file_format))

        # Save the XFrame
        url = make_internal_url(filename)

        if file_format is 'binary':
            self.__impl__.save(url)

        elif file_format is 'csv':
            if not filename.endswith(('.csv', '.csv.gz')):
                raise ValueError('File name must end with .csv or .csv.gz.')
            self.__impl__.save_as_csv(url)
        elif file_format is 'parquet':
            if not filename.endswith('.parquet'):
                raise ValueError('File name must wnd with .parquet.')
            self.__impl__.save_as_parquet(url, number_of_partitions=8)
        else:
            raise ValueError('Unsupported format: {}.'.format(file_format))

    def select_column(self, column_name):
        """
        Return an  :class:`~xframes.XArray` that corresponds with
        the given column name. Throws an exception if the column name is something other than a
        string or if the column name is not found.

        Subscripting an XFrame by a column name is equivalent to this function.

        Parameters
        ----------
        column_name : str
            The column name.

        Returns
        -------
        out : XArray
            The XArray that is referred by `column_name`.

        See Also
        --------
        xframes.XFrame.select_columns
            Returns multiple columns.

        Examples
        --------
        >>> xf = xframes.XFrame({'user_id': [1,2,3],
        ...                       'user_name': ['alice', 'bob', 'charlie']})
        >>> # This line is equivalent to `sa = xf['user_name']`
        >>> sa = xf.select_column('user_name')
        >>> sa
        dtype: str
        Rows: 3
        ['alice', 'bob', 'charlie']
        """
        if not isinstance(column_name, str):
            raise TypeError('Invalid column_name type: must be str.')
        return XArray(data=[], impl=self.__impl__.select_column(column_name))

    def select_columns(self, keylist):
        """
        Get XFrame composed only of the columns referred to in the given list of
        keys. Throws an exception if ANY of the keys are not in this XFrame or
        if `keylist` is anything other than a list of strings.

        Parameters
        ----------
        keylist : list[str]
            The list of column names.

        Returns
        -------
        out : XFrame
            A new XFrame that is made up of the columns referred to in
            `keylist` from the current XFrame.

        See Also
        --------
        xframes.XFrame.select_column
            Returns a single column.

        Examples
        --------
        >>> xf = xframes.XFrame({'user_id': [1,2,3],
        ...                       'user_name': ['alice', 'bob', 'charlie'],
        ...                       'zipcode': [98101, 98102, 98103]
        ...                      })
        >>> # This line is equivalent to `xf2 = xf[['user_id', 'zipcode']]`
        >>> xf2 = xf.select_columns(['user_id', 'zipcode'])
        >>> xf2
        +---------+---------+
        | user_id | zipcode |
        +---------+---------+
        |    1    |  98101  |
        |    2    |  98102  |
        |    3    |  98103  |
        +---------+---------+
        [3 rows x 2 columns]
        """
        if not hasattr(keylist, '__iter__'):
            raise TypeError('Keylist must be an iterable.')
        if not all([isinstance(x, str) for x in keylist]):
            raise TypeError('Invalid key type: must be str.')

        key_set = set(keylist)
        if len(key_set) != len(keylist):
            for key in key_set:
                if keylist.count(key) > 1:
                    raise ValueError("There are duplicate keys in key list: '{}'.".format(key))

        return XFrame(data=[], impl=self.__impl__.select_columns(keylist))

    def add_column(self, data, name=''):
        """
        Add a column to this XFrame. The length of the new column
        must match the length of the existing XFrame. This
        operation modifies the current XFrame in place and returns self. If no
        `name` is given, a default name is chosen.

        Parameters
        ----------
        data : XArray
            The 'column' of data to add.

        name : string, optional
            The name of the column. If no name is given, a default name is
            chosen.

        Returns
        -------
        out : XFrame
            The current XFrame.

        See Also
        --------
        xframes.XFrame.add_columns
            Adds multiple columns.

        Examples
        --------
        >>> xf = xframes.XFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        >>> sa = xframes.XArray(['cat', 'dog', 'fossa'])
        >>> # This line is equivalant to `xf['species'] = sa`
        >>> xf.add_column(sa, name='species')
        >>> xf
        +----+-----+---------+
        | id | val | species |
        +----+-----+---------+
        | 1  |  A  |   cat   |
        | 2  |  B  |   dog   |
        | 3  |  C  |  fossa  |
        +----+-----+---------+
        [3 rows x 3 columns]
        """
        # Check type for pandas dataframe or XArray?
        if not isinstance(data, XArray):
            raise TypeError('Must give column as XArray.')
        if not isinstance(name, str):
            raise TypeError('Invalid column name: must be str.')
        self.__impl__.add_column(data.__impl__, name)
        return self

    def add_columns(self, data, namelist=None):
        """
        Adds multiple columns to this XFrame. The length of the new columns
        must match the length of the existing XFrame. This
        operation modifies the current XFrame in place and returns self.

        Parameters
        ----------
        data : list of XArray or XFrame
            The columns to add.  If `data` is an XFrame, all columns in it are added.

        namelist : list of string, optional
            A list of column names. All names must be specified. `Namelist` is
            ignored if `data` is an XFrame.

        Returns
        -------
        out : XFrame
            The current XFrame.

        See Also
        --------
        xframes.XFrame.add_column
            adds one column

        Examples
        --------
        >>> xf = xframes.XFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        >>> xf2 = xframes.XFrame({'species': ['cat', 'dog', 'fossa'],
        ...                        'age': [3, 5, 9]})
        >>> xf.add_columns(xf2)
        >>> xf
        +----+-----+-----+---------+
        | id | val | age | species |
        +----+-----+-----+---------+
        | 1  |  A  |  3  |   cat   |
        | 2  |  B  |  5  |   dog   |
        | 3  |  C  |  9  |  fossa  |
        +----+-----+-----+---------+
        [3 rows x 4 columns]
        """
        datalist = data
        if isinstance(data, XFrame):
            other = data
            namelist = other.column_names()

            my_columns = set(self.column_names())
            for name in namelist:
                if name in my_columns:
                    raise ValueError("Column '{}' already exists in current XFrame.".format(name))
            self.__impl__.add_columns_frame(data)
            return self
        else:
            if not hasattr(datalist, '__iter__'):
                raise TypeError('Datalist must be an iterable.')
            if not hasattr(namelist, '__iter__'):
                raise TypeError('Namelist must be an iterable.')

            if not all([isinstance(x, XArray) for x in datalist]):
                raise TypeError('Must give column as XArray.')
            if not all([isinstance(x, str) for x in namelist]):
                raise TypeError("Invalid column name in list : must all be 'str'.")
            if len(namelist) != len(datalist):
                raise ValueError('Namelist length mismatch.')

            self.__impl__.add_columns_array(datalist, namelist)
            return self

    def remove_column(self, name):
        """
        Remove a column from this XFrame. This operation modifies the current
        XFrame in place and returns self.

        Parameters
        ----------
        name : string
            The name of the column to remove.

        Returns
        -------
        out : XFrame
            The XFrame with given column removed.

        Examples
        --------
        >>> xf = xframes.XFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        >>> # This is equivalent to `del xf['val']`
        >>> xf.remove_column('val')
        >>> xf
        +----+
        | id |
        +----+
        | 1  |
        | 2  |
        | 3  |
        +----+
        [3 rows x 1 columns]
        """
        if name not in self.column_names():
            raise KeyError('Cannot find column {}.'.format(name))
        self.__impl__.remove_column(name)
        return self

    def remove_columns(self, column_names):
        """
        Removes one or more columns from this XFrame. This operation modifies the current
        XFrame in place and returns self.

        Parameters
        ----------
        column_names : list or iterable
            A list or iterable of the column names.

        Returns
        -------
        out : XFrame
            The XFrame with given columns removed.

        Examples
        --------
        >>> xf = xframes.XFrame({'id': [1, 2, 3], 'val1': ['A', 'B', 'C'], 'val2': [10, 11, 12]})
        >>> xf.remove_columns(['val1', 'val2'])
        >>> xf
        +----+
        | id |
        +----+
        | 1  |
        | 2  |
        | 3  |
        +----+
        [3 rows x 1 columns]
        """
        if not hasattr(column_names, '__iter__'):
            raise TypeError('Column_names must be an iterable.')
        for name in column_names:
            if name not in self.column_names():
                raise KeyError('Cannot find column {}.'.format(name))
        self.__impl__.remove_columns(column_names)
        return self

    def swap_columns(self, column_1, column_2):
        """
        Swap the columns with the given names. This operation modifies the
        current XFrame in place and returns self.

        Parameters
        ----------
        column_1 : string
            Name of column to swap

        column_2 : string
            Name of other column to swap

        Returns
        -------
        out : XFrame
            The XFrame with swapped columns.

        Examples
        --------
        >>> xf = xframes.XFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        >>> xf.swap_columns('id', 'val')
        >>> xf
        +-----+-----+
        | val | id  |
        +-----+-----+
        |  A  |  1  |
        |  B  |  2  |
        |  C  |  3  |
        +----+-----+
        [3 rows x 2 columns]
        """
        if column_1 not in self.column_names():
            raise KeyError("Cannot find column '{}'.".format(column_1))
        if column_2 not in self.column_names():
            raise KeyError("Cannot find column '{}'.".format(column_2))

        self.__impl__.swap_columns(column_1, column_2)
        return self

    def rename(self, names):
        """
        Rename the given columns. `Names` can be a dict specifying
        the old and new names. This changes the names of the columns given as
        the keys and replaces them with the names given as the values.  Alternatively,
        `names` can be a list of the new column names.  In this case it must be
        the same length as the number of columns.  This
        operation modifies the current XFrame in place and returns self.

        Parameters
        ----------
        names : dict [string, string] | list [ string ]
            Dictionary of [old_name, new_name] or list of names

        Returns
        -------
        out : XFrame
            The current XFrame.

        See Also
        --------
        xframes.XFrame.column_names

        Examples
        --------
        >>> xf = XFrame({'X1': ['Alice','Bob'],
        ...              'X2': ['123 Fake Street','456 Fake Street']})
        >>> xf.rename({'X1': 'name', 'X2':'address'})
        >>> xf
        +-------+-----------------+
        |  name |     address     |
        +-------+-----------------+
        | Alice | 123 Fake Street |
        |  Bob  | 456 Fake Street |
        +-------+-----------------+
        [2 rows x 2 columns]
        """
        if type(names) not in [list, dict]:
            raise TypeError('Names must be a dictionary: oldname -> newname or a list of newname.')
        all_columns = set(self.column_names())
        if type(names) == dict:
            for k in names:
                if k not in all_columns:
                    raise ValueError("Cannot find column '{}' in the XFrame.".format(k))
            for k in names:
                self.__impl__.set_column_name(k, names[k])
        else:
            if len(names) != len(self.column_names()):
                raise ValueError('Names must be the same length as the number of columns ({})'
                                 .format(len(self.column_names())))
            self.__impl__.replace_column_names(names)
        return self

    def __getattr__(self, key):
        """
        This method allows the dot notation to refer to columns.

        It is a little simpler than the subscript notation, but it less general because the
        column name has to be a python constant rather than a variable, and because it has to be
        an acceptable python constant (cannot contain minus sign, cannot start with a number, etc.)
        It is also only an alternative to the "select_column" form of subscript notation.
        """
        # This should never see a key that corresponds to an existing attribute.
        return self.__getitem__(key)
        
    def __getitem__(self, key):
        """
        This method does things based on the type of `key`.

        If `key` is:
            * str
                Calls `select_column` on `key`
            * XArray
                Performs a logical filter.  Expects given XArray to be the same
                length as all columns in current XFrame.  Every row
                corresponding with an entry in the given XArray that is
                equivalent to False is filtered from the result.
            * int
                Returns a single row of the XFrame (the `key`th one) as a dictionary.
            * slice
                Returns an XFrame including only the sliced rows.
        """
        if type(key) is XArray:
            return self._row_selector(key)
        elif type(key) is list:
            return self.select_columns(key)
        elif type(key) is str:
            return self.select_column(key)
        elif type(key) is int:
            if key < 0:
                key += len(self)
            if key >= len(self):
                raise IndexError('XFrame index out of range.')
            return list(XFrame(impl=self.__impl__.copy_range(key, 1, key + 1)))[0]
        elif type(key) is slice:
            start = key.start
            stop = key.stop
            step = key.step
            if start is None:
                start = 0
            if stop is None:
                stop = len(self)
            if step is None:
                step = 1
            # handle negative indices
            if start < 0:
                start += len(self)
            if stop < 0:
                stop += len(self)
            return XFrame(impl=self.__impl__.copy_range(start, step, stop))
        else:
            raise TypeError('Invalid index type: must be XArray, ' +
                            "'int', 'list', slice, or 'str': ({})".format(type(key)))

    def __setattr__(self, key, value):
        """
        This method allows the dot notation to refer to columns on the left side.

        It is a little simpler than the subscript notation, but it less general because the
        column name has to be a python constant rather than a variable, and because it has to be
        an acceptable python constant (cannot contain minus sign, cannot start with a number, etc.)
        """
        if key.startswith('_'):
            self.__dict__[key] = value
        else:
            self.__setitem__(key, value)

    def __setitem__(self, key, value):
        """
        A wrapper around add_column(s).  Key can be either a list or a str.  If
        value is an XArray, it is added to the XFrame as a column.  If it is a
        constant value (int, str, or float), then a column is created where
        every entry is equal to the constant value.  Existing columns can also
        be replaced using this wrapper.

        """
        if type(key) is list:
            self.add_columns(value, key)
        elif type(key) is str:
            if type(value) is XArray:
                sa_value = value
            elif hasattr(value, '__iter__'):  # wrap list, array... to xarray
                sa_value = XArray(value)
            else:  # create an xarray  of constant value
                sa_value = XArray.from_const(value, self.num_rows())

            # set new column
            if key not in self.column_names():
                self.add_column(sa_value, key)
            else:
                # special case if replacing the only column.
                # server would fail the replacement if the new column has different
                # length than current one, which doesn't make sense if we are replacing
                # the only column. To support this, we call a different function in the 
                # implementation.
                single_column = (self.num_columns() == 1)
                if single_column:
                    self.__impl__.replace_single_column(sa_value)
                else:
                    self.__impl__.replace_selected_column(key, sa_value)

        else:
            raise TypeError('Cannot set column with key type {}.'.format(type(key)))

    def __delitem__(self, key):
        """
        Wrapper around remove_column.
        """
        self.remove_column(key)

    def __materialize__(self):
        """
        For an XFrame that is lazily evaluated, force the persistence of the
        XFrame to disk, committing all lazy evaluated operations.
        """
        self.__impl__.materialize()

    def __is_materialized__(self):
        """
        Returns whether or not the XFrame has been materialized.
        """
        return self.__impl__.is_materialized()

    def __has_size__(self):
        """
        Returns whether or not the size of the XFrame is known.
        """
        return self.__impl__.is_materialized()

    def __iter__(self):
        """
        Provides an iterator to the rows of the XFrame.
        """

        def generator():
            elems_at_a_time = 262144
            self.__impl__.begin_iterator()
            ret = self.__impl__.iterator_get_next(elems_at_a_time)
            column_names = self.column_names()
            while True:
                for j in ret:
                    yield dict(zip(column_names, j))
#                    yield j

                if len(ret) == elems_at_a_time:
                    ret = self.__impl__.iterator_get_next(elems_at_a_time)
                else:
                    break

        return generator()

    def append(self, other):
        """
        Add the rows of an XFrame to the end of this XFrame.

        Both XFrames must have the same set of columns with the same column
        names and column types.

        Parameters
        ----------
        other : XFrame
            Another XFrame whose rows are appended to the current XFrame.

        Returns
        -------
        out : XFrame
            The result XFrame from the append operation.

        Examples
        --------
        >>> xf = xframes.XFrame({'id': [4, 6, 8], 'val': ['D', 'F', 'H']})
        >>> xf2 = xframes.XFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        >>> xf = xf.append(xf2)
        >>> xf
        +----+-----+
        | id | val |
        +----+-----+
        | 4  |  D  |
        | 6  |  F  |
        | 8  |  H  |
        | 1  |  A  |
        | 2  |  B  |
        | 3  |  C  |
        +----+-----+
        [6 rows x 2 columns]
        """
        if type(other) is not XFrame:
            raise RuntimeError('XFrame append can only work with XFrame.')

        left_empty = len(self.column_names()) == 0
        right_empty = len(other.column_names()) == 0

        if left_empty and right_empty:
            return XFrame()

        if left_empty or right_empty:
            non_empty_xframe = self if right_empty else other
            return non_empty_xframe

        # check length of names 
        my_column_names = self.column_names()
        my_column_types = self.column_types()
        other_column_names = other.column_names()
        other_column_types = other.column_types()
        if len(my_column_names) != len(other_column_names):
            raise RuntimeError('Two XFrames must have the same number of columns.')

        # check if the order of column name is the same
        for i in range(len(my_column_names)):
            if other_column_names[i] != my_column_names[i]:
                raise RuntimeError('Column {} name is not the same in two XFrames, one is {} the other is {}.'.format(
                    my_column_names[i], my_column_names[i], other_column_names[i]))
            # check column type
            if my_column_types[i] != other_column_types[i]:
                raise RuntimeError('Column {} type is not the same in two XFrames, one is {} the other is {}.'.format(
                    my_column_names[i], my_column_types[i], other_column_types))

        return XFrame(impl=self.__impl__.append(other.__impl__))

    def groupby(self, key_columns, operations=None, *args):
        """
        Perform a group on the `key_columns` followed by aggregations on the
        columns listed in `operations`.

        The `operations` parameter is a dictionary that indicates which
        aggregation operators to use and which columns to use them on. The
        available operators are SUM, MAX, MIN, COUNT, MEAN, VARIANCE, STD, CONCAT,
        SELECT_ONE, ARGMIN, ARGMAX, and QUANTILE.
        See :mod:`~xframes.aggregate` for more detail on the aggregators.

        Parameters
        ----------
        key_columns : string | list[string]
            Column(s) to group by. Key columns can be of any type other than
            dictionary.

        operations : dict, list, optional
            Dictionary of columns and aggregation operations. Each key is a
            output column name and each value is an aggregator. This can also
            be a list of aggregators, in which case column names will be
            automatically assigned.

        *args
            All other remaining arguments will be interpreted in the same
            way as the operations argument.

        Returns
        -------
        out_xf : XFrame
            A new XFrame, with a column for each groupby column and each
            aggregation operation.

        See Also
        --------
        xframes.aggregate

        Examples
        --------
        Suppose we have an XFrame with movie ratings by many users.

        >>> import xframes.aggregate as agg
        >>> url = 'http://s3.amazonaws.com/gl-testdata/rating_data_example.csv'
        >>> xf = xframes.XFrame.read_csv(url)
        >>> xf
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |  25933  |   1663   |   4    |
        |  25934  |   1663   |   4    |
        |  25935  |   1663   |   4    |
        |  25936  |   1663   |   5    |
        |  25937  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10000 rows x 3 columns]

        Compute the number of occurrences of each user.

        >>> user_count = xf.groupby('user_id',
        ...                         {'count': agg.COUNT()})
        >>> user_count
        +---------+-------+
        | user_id | count |
        +---------+-------+
        |  62361  |   1   |
        |  30727  |   1   |
        |  40111  |   1   |
        |  50513  |   1   |
        |  35140  |   1   |
        |  42352  |   1   |
        |  29667  |   1   |
        |  46242  |   1   |
        |  58310  |   1   |
        |  64614  |   1   |
        |   ...   |  ...  |
        +---------+-------+
        [9852 rows x 2 columns]

        Compute the mean and standard deviation of ratings per user.

        >>> user_rating_stats = xf.groupby('user_id',
        ...                                {
        ...                                    'mean_rating': agg.MEAN('rating'),
        ...                                    'std_rating': agg.STD('rating')
        ...                                })
        >>> user_rating_stats
        +---------+-------------+------------+
        | user_id | mean_rating | std_rating |
        +---------+-------------+------------+
        |  62361  |     5.0     |    0.0     |
        |  30727  |     4.0     |    0.0     |
        |  40111  |     2.0     |    0.0     |
        |  50513  |     4.0     |    0.0     |
        |  35140  |     4.0     |    0.0     |
        |  42352  |     5.0     |    0.0     |
        |  29667  |     4.0     |    0.0     |
        |  46242  |     5.0     |    0.0     |
        |  58310  |     2.0     |    0.0     |
        |  64614  |     2.0     |    0.0     |
        |   ...   |     ...     |    ...     |
        +---------+-------------+------------+
        [9852 rows x 3 columns]

        Compute the movie with the minimum rating per user.

        >>> chosen_movies = xf.groupby('user_id',
        ...                            {
        ...                                'worst_movies': agg.ARGMIN('rating','movie_id')
        ...                            })
        >>> chosen_movies
        +---------+-------------+
        | user_id | worst_movies |
        +---------+-------------+
        |  62361  |     1663    |
        |  30727  |     1663    |
        |  40111  |     1663    |
        |  50513  |     1663    |
        |  35140  |     1663    |
        |  42352  |     1663    |
        |  29667  |     1663    |
        |  46242  |     1663    |
        |  58310  |     1663    |
        |  64614  |     1663    |
        |   ...   |     ...     |
        +---------+-------------+
        [9852 rows x 2 columns]

        Compute the movie with the max rating per user and also the movie with
        the maximum imdb-ranking per user.

        >>> xf['imdb-ranking'] = xf['rating'] * 10
        >>> chosen_movies = xf.groupby('user_id',
        ...         {('max_rating_movie','max_imdb_ranking_movie'):
        ...            agg.ARGMAX(('rating','imdb-ranking'),'movie_id')})
        >>> chosen_movies
        +---------+------------------+------------------------+
        | user_id | max_rating_movie | max_imdb_ranking_movie |
        +---------+------------------+------------------------+
        |  62361  |       1663       |          16630         |
        |  30727  |       1663       |          16630         |
        |  40111  |       1663       |          16630         |
        |  50513  |       1663       |          16630         |
        |  35140  |       1663       |          16630         |
        |  42352  |       1663       |          16630         |
        |  29667  |       1663       |          16630         |
        |  46242  |       1663       |          16630         |
        |  58310  |       1663       |          16630         |
        |  64614  |       1663       |          16630         |
        |   ...   |       ...        |          ...           |
        +---------+------------------+------------------------+
        [9852 rows x 3 columns]

        Compute the movie with the max rating per user.

        >>> chosen_movies = xf.groupby('user_id',
        ...         {'best_movies': agg.ARGMAX('rating','movie')})

        Compute the movie with the max rating per user and also the movie with the maximum imdb-ranking per user.

        >>> chosen_movies = xf.groupby('user_id',
        ...        {('max_rating_movie','max_imdb_ranking_movie'):
        ...                              agg.ARGMAX(('rating','imdb-ranking'),'movie')})

        Compute the count, mean, and standard deviation of ratings per (user,
        time), automatically assigning output column names.

        >>> xf['time'] = xf.apply(lambda x: (x['user_id'] + x['movie_id']) % 11 + 2000)
        >>> user_rating_stats = xf.groupby(['user_id', 'time'],
        ...                                [agg.COUNT(),
        ...                                 agg.MEAN('rating'),
        ...                                 agg.STDV('rating')])
        >>> user_rating_stats
        +------+---------+-------+---------------+----------------+
        | time | user_id | Count | Avg of rating | Stdv of rating |
        +------+---------+-------+---------------+----------------+
        | 2006 |  61285  |   1   |      4.0      |      0.0       |
        | 2000 |  36078  |   1   |      4.0      |      0.0       |
        | 2003 |  47158  |   1   |      3.0      |      0.0       |
        | 2007 |  34446  |   1   |      3.0      |      0.0       |
        | 2010 |  47990  |   1   |      3.0      |      0.0       |
        | 2003 |  42120  |   1   |      5.0      |      0.0       |
        | 2007 |  44940  |   1   |      4.0      |      0.0       |
        | 2008 |  58240  |   1   |      4.0      |      0.0       |
        | 2002 |   102   |   1   |      1.0      |      0.0       |
        | 2009 |  52708  |   1   |      3.0      |      0.0       |
        | ...  |   ...   |  ...  |      ...      |      ...       |
        +------+---------+-------+---------------+----------------+
        [10000 rows x 5 columns]


        The groupby function can take a variable length list of aggregation
        specifiers so if we want the count and the 0.25 and 0.75 quantiles of
        ratings:

        >>> user_rating_stats = xf.groupby(['user_id', 'time'], agg.COUNT(),
        ...                                {'rating_quantiles': agg.QUANTILE('rating',[0.25, 0.75])})
        >>> user_rating_stats
        +------+---------+-------+------------------------+
        | time | user_id | Count |    rating_quantiles    |
        +------+---------+-------+------------------------+
        | 2006 |  61285  |   1   | array('d', [4.0, 4.0]) |
        | 2000 |  36078  |   1   | array('d', [4.0, 4.0]) |
        | 2003 |  47158  |   1   | array('d', [3.0, 3.0]) |
        | 2007 |  34446  |   1   | array('d', [3.0, 3.0]) |
        | 2010 |  47990  |   1   | array('d', [3.0, 3.0]) |
        | 2003 |  42120  |   1   | array('d', [5.0, 5.0]) |
        | 2007 |  44940  |   1   | array('d', [4.0, 4.0]) |
        | 2008 |  58240  |   1   | array('d', [4.0, 4.0]) |
        | 2002 |   102   |   1   | array('d', [1.0, 1.0]) |
        | 2009 |  52708  |   1   | array('d', [3.0, 3.0]) |
        | ...  |   ...   |  ...  |          ...           |
        +------+---------+-------+------------------------+
        [10000 rows x 4 columns]

        To put all items a user rated into one list value by their star rating:

        >>> user_rating_stats = xf.groupby(["user_id", "rating"],
        ...                                {"rated_movie_ids": agg.CONCAT("movie_id")})
        >>> user_rating_stats
        +--------+---------+----------------------+
        | rating | user_id |     rated_movie_ids  |
        +--------+---------+----------------------+
        |   3    |  31434  | array('d', [1663.0]) |
        |   5    |  25944  | array('d', [1663.0]) |
        |   4    |  38827  | array('d', [1663.0]) |
        |   4    |  51437  | array('d', [1663.0]) |
        |   4    |  42549  | array('d', [1663.0]) |
        |   4    |  49532  | array('d', [1663.0]) |
        |   3    |  26124  | array('d', [1663.0]) |
        |   4    |  46336  | array('d', [1663.0]) |
        |   4    |  52133  | array('d', [1663.0]) |
        |   5    |  62361  | array('d', [1663.0]) |
        |  ...   |   ...   |         ...          |
        +--------+---------+----------------------+
        [9952 rows x 3 columns]

        To put all items and rating of a given user together into a dictionary
        value:

        >>> user_rating_stats = xf.groupby("user_id",
        ...                                {"movie_rating": agg.CONCAT("movie_id", "rating")})
        >>> user_rating_stats
        +---------+--------------+
        | user_id | movie_rating |
        +---------+--------------+
        |  62361  |  {1663: 5}   |
        |  30727  |  {1663: 4}   |
        |  40111  |  {1663: 2}   |
        |  50513  |  {1663: 4}   |
        |  35140  |  {1663: 4}   |
        |  42352  |  {1663: 5}   |
        |  29667  |  {1663: 4}   |
        |  46242  |  {1663: 5}   |
        |  58310  |  {1663: 2}   |
        |  64614  |  {1663: 2}   |
        |   ...   |     ...      |
        +---------+--------------+
        [9852 rows x 2 columns]
        """

        # TODO: groupby CONCAT produces unicode output from utf8 input
        # TODO: Preserve character encoding.

        operations = operations or {}
        # some basic checking first
        # make sure key_columns is a list
        if isinstance(key_columns, str):
            key_columns = [key_columns]
        # check that every column is a string, and is a valid column name
        my_column_names = self.column_names()
        my_column_types = self.column_types()
        key_columns_array = []
        for column in key_columns:
            if not isinstance(column, str):
                raise TypeError('Column name must be a string.')
            if column not in my_column_names:
                raise KeyError("Column '{}' does not exist in XFrame".format(column))
            col_type = my_column_types[my_column_names.index(column)]
            if col_type == dict:
                raise TypeError('Cannot group on a dictionary column.')
            key_columns_array.append(column)

        group_output_columns = []
        group_columns = []
        group_ops = []

        all_ops = [operations] + list(args)
        for op_entry in all_ops:
            # if it is not a dict, nor a list, it is just a single aggregator
            # element (probably COUNT). wrap it in a list so we can reuse the
            # list processing code
            operation = op_entry
            if not(isinstance(operation, list) or isinstance(operation, dict)):
                operation = [operation]
            if isinstance(operation, dict):
                # now sweep the dict and add to group_columns and group_ops
                for key in operation:
                    val = operation[key]
                    if type(val) is tuple:
                        (op, column) = val
                        if (op == '__builtin__argmax__' or op == '__builtin__argmin__') \
                                and (type(column[0]) is tuple) != (type(key) is tuple):
                            raise TypeError('Output column(s) and aggregate column(s) for ' +
                                            'aggregate operation should be either all tuple or all string.')

                        if (op == '__builtin__argmax__' or op == '__builtin__argmin__') and type(column[0]) is tuple:
                            for (col, output) in zip(column[0], key):
                                group_columns = group_columns + [[col, column[1]]]
                                group_ops = group_ops + [op]
                                group_output_columns = group_output_columns + [output]
                        else:
                            group_columns = group_columns + [column]
                            group_ops = group_ops + [op]
                            group_output_columns = group_output_columns + [key]
                    elif val == xframes.aggregate.COUNT:
                        group_output_columns = group_output_columns + [key]
                        val = xframes.aggregate.COUNT()
                        (op, column) = val
                        group_columns = group_columns + [column]
                        group_ops = group_ops + [op]
                    else:
                        raise TypeError("Unexpected type in aggregator definition of output column: '{}'"
                                        .format(key))
            elif isinstance(operation, list):
                # we will be using automatically defined column names
                for val in operation:
                    if type(val) is tuple:
                        (op, column) = val
                        if (op == '__builtin__argmax__' or op == '__builtin__argmin__') \
                                and type(column[0]) is tuple:
                            for col in column[0]:
                                group_columns = group_columns + [[col, column[1]]]
                                group_ops = group_ops + [op]
                                group_output_columns = group_output_columns + ['']
                        else:
                            group_columns = group_columns + [column]
                            group_ops = group_ops + [op]
                            group_output_columns = group_output_columns + ['']
                    elif val == xframes.aggregate.COUNT:
                        group_output_columns = group_output_columns + ['']
                        val = xframes.aggregate.COUNT()
                        (op, column) = val
                        group_columns = group_columns + [column]
                        group_ops = group_ops + [op]
                    else:
                        raise TypeError('Unexpected type in aggregator definition.')

        # let's validate group_columns and group_ops are valid
        for (cols, op) in zip(group_columns, group_ops):
            for col in cols:
                if not isinstance(col, str):
                    raise TypeError('Column name must be a string.')

            if not isinstance(op, str):
                raise TypeError('Operation type not recognized.')

            if op is not xframes.aggregate.COUNT()[0]:
                for col in cols:
                    if col not in my_column_names:
                        raise KeyError("Column '{}' does not exist in XFrame.".format(col))

        return XFrame(impl=self.__impl__.groupby_aggregate(key_columns_array,
                                                           group_columns,
                                                           group_output_columns,
                                                           group_ops))

    def join(self, right, on=None, how='inner'):
        """
        Merge two XFrames. Merges the current (left) XFrame with the given
        (right) XFrame using a SQL-style equi-join operation by columns.

        Parameters
        ----------
        right : XFrame
            The XFrame to join.

        on : str | list | dict, optional
            The column name(s) representing the set of join keys.  Each row that
            has the same value in this set of columns will be merged together.

            * If `on` is not given, the join keyd are all columns in the left and right
              XFrames that have the same name

            * If a string is given, this is interpreted as a join using one column,
              where both XFrames have the same column name.

            * If a list is given, this is interpreted as a join using one or
              more column names, where each column name given exists in both
              XFrames.

            * If a dict is given, each dict key is taken as a column name in the
              left XFrame, and each dict value is taken as the column name in
              right XFrame that will be joined together. e.g.
              {'left_col_name':'right_col_name'}.

        how : {'left', 'right', 'outer', 'inner'}, optional
            The type of join to perform.  'inner' is default.

            * inner: Equivalent to a SQL inner join.  Result consists of the
              rows from the two frames whose join key values match exactly,
              merged together into one XFrame.

            * left: Equivalent to a SQL left outer join. Result is the union
              between the result of an inner join and the rest of the rows from
              the left XFrame, merged with missing values.

            * right: Equivalent to a SQL right outer join.  Result is the union
              between the result of an inner join and the rest of the rows from
              the right XFrame, merged with missing values.

            * outer: Equivalent to a SQL full outer join. Result is
              the union between the result of a left outer join and a right
              outer join.

        Returns
        -------
        out : XFrame
            The joined XFrames.

        Examples
        --------
        >>> animals = xframes.XFrame({'id': [1, 2, 3, 4],
        ...                           'name': ['dog', 'cat', 'sheep', 'cow']})
        >>> sounds = xframes.XFrame({'id': [1, 3, 4, 5],
        ...                          'sound': ['woof', 'baa', 'moo', 'oink']})
        >>> animals.join(sounds, how='inner')
        +----+-------+-------+
        | id |  name | sound |
        +----+-------+-------+
        | 1  |  dog  |  woof |
        | 3  | sheep |  baa  |
        | 4  |  cow  |  moo  |
        +----+-------+-------+
        [3 rows x 3 columns]

        >>> animals.join(sounds, on='id', how='left')
        +----+-------+-------+
        | id |  name | sound |
        +----+-------+-------+
        | 1  |  dog  |  woof |
        | 3  | sheep |  baa  |
        | 4  |  cow  |  moo  |
        | 2  |  cat  |  None |
        +----+-------+-------+
        [4 rows x 3 columns]

        >>> animals.join(sounds, on=['id'], how='right')
        +----+-------+-------+
        | id |  name | sound |
        +----+-------+-------+
        | 1  |  dog  |  woof |
        | 3  | sheep |  baa  |
        | 4  |  cow  |  moo  |
        | 5  |  None |  oink |
        +----+-------+-------+
        [4 rows x 3 columns]

        >>> animals.join(sounds, on={'id':'id'}, how='outer')
        +----+-------+-------+
        | id |  name | sound |
        +----+-------+-------+
        | 1  |  dog  |  woof |
        | 3  | sheep |  baa  |
        | 4  |  cow  |  moo  |
        | 5  |  None |  oink |
        | 2  |  cat  |  None |
        +----+-------+-------+
        [5 rows x 3 columns]
        """
        available_join_types = ['left', 'right', 'outer', 'inner']

        if not isinstance(right, XFrame):
            raise TypeError('Can only join two XFrames.')

        if how not in available_join_types:
            raise ValueError('Invalid join type.')

        join_keys = dict()
        if on is None:
            left_names = self.column_names()
            right_names = right.column_names()
            common_columns = [name for name in left_names if name in right_names]
            for name in common_columns:
                join_keys[name] = name
        elif type(on) is str:
            join_keys[on] = on
        elif type(on) is list:
            for name in on:
                if type(name) is not str:
                    raise TypeError('Join keys must each be a str.')
                join_keys[name] = name
        elif type(on) is dict:
            join_keys = on
        else:
            raise TypeError("Must pass a 'str', 'list', or 'dict' of join keys.")

        return XFrame(impl=self.__impl__.join(right.__impl__, how, join_keys))

    def split_datetime(self, expand_column, column_name_prefix=None, limit=None, tzone=False):
        """
        Splits a datetime column of XFrame to multiple columns, with each value in a
        separate column. Returns a new XFrame with the expanded column replaced with
        a list of new columns. The expanded column must be of datetime type.

        For more details regarding name generation and
        other, refer to :py:func:`xframes.XArray.expand()`

        Parameters
        ----------
        expand_column : str
            Name of the unpacked column.

        column_name_prefix : str, optional
            If provided, expanded column names would start with the given prefix.
            If not provided, the default value is the name of the expanded column.

        limit : list[str], optional
            Limits the set of datetime elements to expand.
            Elements are 'year','month','day','hour','minute',
            and 'second'.

        tzone : bool, optional
            A boolean parameter that determines whether to show the timezone
            column or not. Defaults to False.

        Returns
        -------
        out : XFrame
            A new XFrame that contains rest of columns from original XFrame with
            the given column replaced with a collection of expanded columns.

        Examples
        --------

        >>> xf
        Columns:
            id   int
            submission  datetime
        Rows: 2
        Data:
            +----+-------------------------------------------------+
            | id |               submission                        |
            +----+-------------------------------------------------+
            | 1  | datetime(2011, 1, 21, 7, 17, 21, tzinfo=GMT(+1))|
            | 2  | datetime(2011, 1, 21, 5, 43, 21, tzinfo=GMT(+1))|
            +----+-------------------------------------------------+

        >>> xf.split_datetime('submission',limit=['hour','minute'])
        Columns:
            id  int
            submission.hour int
            submission.minute int
        Rows: 2
        Data:
        +----+-----------------+-------------------+
        | id | submission.hour | submission.minute |
        +----+-----------------+-------------------+
        | 1  |        7        |        17         |
        | 2  |        5        |        43         |
        +----+-----------------+-------------------+

        """
        if expand_column not in self.column_names():
            raise KeyError("Column '{}' does not exist in current XFrame.".format(expand_column))

        if column_name_prefix is None:
            column_name_prefix = expand_column

        new_xf = self[expand_column].split_datetime(column_name_prefix, limit, tzone)

        # construct return XFrame, check if there is conflict
        rest_columns = [name for name in self.column_names() if name != expand_column]
        new_names = new_xf.column_names()
        while set(new_names).intersection(rest_columns):
            new_names = [name + '.1' for name in new_names]
        new_xf.rename(dict(zip(new_xf.column_names(), new_names)))

        ret_xf = self.select_columns(rest_columns)
        ret_xf.add_columns(new_xf)
        return ret_xf

    # noinspection PyComparisonWithNone
    def filterby(self, values, column_name, exclude=False):
        """
        Filter an XFrame by values inside an iterable object. Result is an
        XFrame that only includes (or excludes) the rows that have a column
        with the given `column_name` which holds one of the values in the
        given `values` :class:`~xframes.XArray`. If `values` is not an
        XArray, we attempt to convert it to one before filtering.

        Parameters
        ----------
        values : XArray | list | numpy.ndarray | pandas.Series | str
            The values to use to filter the XFrame.  The resulting XFrame will
            only include rows that have one of these values in the given
            column.

        column_name : str
            The column of the XFrame to match with the given `values`.

        exclude : bool
            If True, the result XFrame will contain all rows EXCEPT those that
            have one of `values` in `column_name`.

        Returns
        -------
        out : XFrame
            The filtered XFrame.

        Examples
        --------
        >>> xf = xframes.XFrame({'id': [1, 2, 3, 4],
        ...                      'animal_type': ['dog', 'cat', 'cow', 'horse'],
        ...                      'name': ['bob', 'jim', 'jimbob', 'bobjim']})
        >>> household_pets = ['cat', 'hamster', 'dog', 'fish', 'bird', 'snake']
        >>> xf.filterby(household_pets, 'animal_type')
        +-------------+----+------+
        | animal_type | id | name |
        +-------------+----+------+
        |     dog     | 1  | bob  |
        |     cat     | 2  | jim  |
        +-------------+----+------+
        [2 rows x 3 columns]
        >>> xf.filterby(household_pets, 'animal_type', exclude=True)
        +-------------+----+--------+
        | animal_type | id |  name  |
        +-------------+----+--------+
        |    horse    | 4  | bobjim |
        |     cow     | 3  | jimbob |
        +-------------+----+--------+
        [2 rows x 3 columns]
        """
        if type(column_name) is not str:
            raise TypeError('Must pass a str as column_name.')

        existing_columns = self.column_names()
        if column_name not in existing_columns:
            raise KeyError("Column '{}' not in XFrame.".format(column_name))

        if type(values) is not XArray:
            # If we were given a single element, try to put in list and convert
            # to XArray
            # TODO filter directly, not through join
            if not hasattr(values, '__iter__'):
                values = [values]
            values = XArray(values)

        value_xf = XFrame()
        value_xf.add_column(values, column_name)

        # Make sure the values list has unique values, or else join will not
        # filter.
        value_xf = value_xf.groupby(column_name, {})

        existing_type = self.column_types()[existing_columns.index(column_name)]
        given_type = value_xf.column_types()[0]
        if given_type != existing_type:
            raise TypeError("Type of given values ('{}') does not match type of column '{}' ('{}') in XFrame."
                            .format(given_type, column_name, existing_type))

        if exclude:
            id_name = "id"
            # Make sure this name is unique so we know what to remove in
            # the result
            while id_name in existing_columns:
                id_name += '1'
            value_xf = value_xf.add_row_number(id_name)

            tmp = XFrame(impl=self.__impl__.join(value_xf.__impl__,
                                                 'left',
                                                 {column_name: column_name}))
            ret_xf = tmp[tmp[id_name] == None]       # this is an xarray operator -- do not change
            del ret_xf[id_name]
            return ret_xf
        else:
            return XFrame(impl=self.__impl__.join(value_xf.__impl__,
                                                  'inner',
                                                  {column_name: column_name}))

    # noinspection PyTypeChecker
    def pack_columns(self, columns=None, column_prefix=None, dtype=list,
                     fill_na=None, remove_prefix=True, new_column_name=None):
        """
        Pack two or more columns of the current XFrame into one single
        column.  The result is a new XFrame with the unaffected columns from the
        original XFrame plus the newly created column.

        The list of columns that are packed is chosen through either the
        `columns` or `column_prefix` parameter. Only one of the parameters
        is allowed to be provided: `columns` explicitly specifies the list of
        columns to pack, while `column_prefix` specifies that all columns that
        have the given prefix are to be packed.

        The type of the resulting column is decided by the `dtype` parameter.
        Allowed values for `dtype` are dict, array.array and list:

         - dict: pack to a dictionary XArray where column name becomes
           dictionary key and column value becomes dictionary value

         - array.array: pack all values from the packing columns into an array

         - list: pack all values from the packing columns into a list.

        Parameters
        ----------
        columns : list[str], optional
            A list of column names to be packed.  There needs to have at least
            two columns to pack.  If omitted and `column_prefix` is not
            specified, all columns from current XFrame are packed.  This
            parameter is mutually exclusive with the `column_prefix` parameter.

        column_prefix : str, optional
            Pack all columns with the given `column_prefix`.
            This parameter is mutually exclusive with the `columns` parameter.

        dtype : dict | array.array | list, optional
            The resulting packed column type. If not provided, dtype is list.

        fill_na : value, optional
            Value to fill into packed column if missing value is encountered.
            If packing to dictionary, `fill_na` is only applicable to dictionary
            values; missing keys are not replaced.

        remove_prefix : bool, optional
            If True and `column_prefix` is specified, the dictionary key will
            be constructed by removing the prefix from the column name.
            This option is only applicable when packing to dict type.

        new_column_name : str, optional
            Packed column name.  If not given and `column_prefix` is given,
            then the prefix will be used as the new column name, otherwise name
            is generated automatically.

        Returns
        -------
        out : XFrame
            An XFrame that contains columns that are not packed, plus the newly
            packed column.

        See Also
        --------
        xframes.XFrame.unpack

        Notes
        -----
        - There must be at least two columns to pack.

        - If packing to dictionary, a missing key is always dropped. Missing
          values are dropped if `fill_na` is not provided, otherwise, missing
          value is replaced by `fill_na`. If packing to list or array, missing
          values will be kept. If `fill_na` is provided, the missing value is
          replaced with `fill_na` value.

        Examples
        --------
        Suppose 'xf' is an an XFrame that maintains business category
        information:

        >>> xf = xframes.XFrame({'business': range(1, 5),
        ...                       'category.retail': [1, None, 1, None],
        ...                       'category.food': [1, 1, None, None],
        ...                       'category.service': [None, 1, 1, None],
        ...                       'category.shop': [1, 1, None, 1]})
        >>> xf
        +----------+-----------------+---------------+------------------+---------------+
        | business | category.retail | category.food | category.service | category.shop |
        +----------+-----------------+---------------+------------------+---------------+
        |    1     |        1        |       1       |       None       |       1       |
        |    2     |       None      |       1       |        1         |       1       |
        |    3     |        1        |      None     |        1         |      None     |
        |    4     |       None      |       1       |       None       |       1       |
        +----------+-----------------+---------------+------------------+---------------+
        [4 rows x 5 columns]

        To pack all category columns into a list:

        >>> xf.pack_columns(column_prefix='category')
        +----------+--------------------+
        | business |         X2         |
        +----------+--------------------+
        |    1     |  [1, 1, None, 1]   |
        |    2     |  [None, 1, 1, 1]   |
        |    3     | [1, None, 1, None] |
        |    4     | [None, 1, None, 1] |
        +----------+--------------------+
        [4 rows x 2 columns]

        To pack all category columns into a dictionary, with new column name:

        >>> xf.pack_columns(column_prefix='category', dtype=dict,
        ...                 new_column_name='category')
        +----------+--------------------------------+
        | business |            category            |
        +----------+--------------------------------+
        |    1     | {'food': 1, 'shop': 1, 're ... |
        |    2     | {'food': 1, 'shop': 1, 'se ... |
        |    3     |  {'retail': 1, 'service': 1}   |
        |    4     |     {'food': 1, 'shop': 1}     |
        +----------+--------------------------------+
        [4 rows x 2 columns]

        To keep column prefix in the resulting dict key:

        >>> xf.pack_columns(column_prefix='category', dtype=dict,
        ...                 remove_prefix=False)
        +----------+--------------------------------+
        | business |               X2               |
        +----------+--------------------------------+
        |    1     | {'category.retail': 1, 'ca ... |
        |    2     | {'category.food': 1, 'cate ... |
        |    3     | {'category.retail': 1, 'ca ... |
        |    4     | {'category.food': 1, 'cate ... |
        +----------+--------------------------------+
        [4 rows x 2 columns]

        To explicitly pack a set of columns:

        >>> xf.pack_columns(columns = ['business', 'category.retail',
        ...                            'category.food', 'category.service',
        ...                            'category.shop'])
        +-----------------------+
        |           X1          |
        +-----------------------+
        |   [1, 1, 1, None, 1]  |
        |   [2, None, 1, 1, 1]  |
        | [3, 1, None, 1, None] |
        | [4, None, 1, None, 1] |
        +-----------------------+
        [4 rows x 1 columns]

        To pack all columns with name starting with 'category' into an array
        type, and with missing value replaced with 0:

        >>> xf.pack_columns(column_prefix="category", dtype=array.array,
        ...                 fill_na=0)
        +----------+--------------------------------+
        | business |               X2               |
        +----------+--------------------------------+
        |    1     | array('d', [1.0, 1.0, 0.0, ... |
        |    2     | array('d', [0.0, 1.0, 1.0, ... |
        |    3     | array('d', [1.0, 0.0, 1.0, ... |
        |    4     | array('d', [0.0, 1.0, 0.0, ... |
        +----------+--------------------------------+
        [4 rows x 2 columns]
        """
        if columns is not None and column_prefix is not None:
            raise ValueError("'Columns' and 'column_prefix' parameter cannot be given at the same time.")

        if new_column_name is None and column_prefix is not None:
            new_column_name = column_prefix

        if column_prefix is not None:
            if type(column_prefix) != str:
                raise TypeError("'Column_prefix' must be a string.")
            columns = [name for name in self.column_names() if name.startswith(column_prefix)]
            if len(columns) == 0:
                raise ValueError("There are no column starts with prefix '{}.".format(column_prefix))
        elif columns is None:
            columns = self.column_names()
        else:
            if not hasattr(columns, '__iter__'):
                raise TypeError("Columns must be an iterable type.")

            column_names = set(self.column_names())
            for column in columns:
                if column not in column_names:
                    raise ValueError("Current XFrame has no column called '{}'.".format(column))

            # check duplicate names
            if len(set(columns)) != len(columns):
                raise ValueError('There are duplicate column names in columns parameter.')

        if len(columns) <= 1:
            raise ValueError('Please provide at least two columns to pack.')

        if dtype not in (dict, list, array.array):
            raise ValueError("Resulting dtype has to be one of 'dict', 'array.array', or 'list type.")

        # fill_na value for array needs to be numeric
        if dtype == array.array:
            if fill_na is not None and type(fill_na) not in (int, float):
                raise ValueError('Fill_na value for array needs to be numeric type.')
            # all columns have to be numeric type
            for column in columns:
                if self[column].dtype() not in (int, float):
                    raise TypeError("Column '{}' type is not numeric, cannot pack into array type.".format(column))

        # generate dict key names if pack to dictionary
        # we try to be smart here
        # if all column names are like: a.b, a.c, a.d,...
        # we then use "b", "c", "d", etc as the dictionary key during packing
        if dtype == dict and column_prefix is not None and remove_prefix:
            size_prefix = len(column_prefix)
            first_char = set([c[size_prefix:size_prefix + 1] for c in columns])
            if len(first_char) == 1 and first_char.pop() in ['.', '-', '_']:
                dict_keys = [name[size_prefix + 1:] for name in columns]
            else:
                dict_keys = [name[size_prefix:] for name in columns]

        else:
            dict_keys = columns
            
        rest_columns = [name for name in self.column_names() if name not in columns]
        if new_column_name is not None:
            if type(new_column_name) != str:
                raise TypeError("'New_column_name' has to be a string.")
            if new_column_name in rest_columns:
                raise KeyError('Current XFrame already contains a column name {}.'.format(new_column_name))
        else:
            new_column_name = ''

        ret_sa = XArray(impl=self.__impl__.pack_columns(columns, dict_keys, dtype, fill_na))

        new_xf = self.select_columns(rest_columns)
        new_xf.add_column(ret_sa, new_column_name)
        return new_xf

    def unpack(self, unpack_column, column_name_prefix=None, column_types=None,
               na_value=None, limit=None):
        """
        Expand one column of this XFrame to multiple columns with each value in
        a separate column. Returns a new XFrame with the unpacked column
        replaced with a list of new columns.  The column must be of
        list, array, or dict type.

        For more details regarding name generation, missing value handling and
        other, refer to the XArray version of
        :py:func:`~xframes.XArray.unpack()`.

        Parameters
        ----------
        unpack_column : str
            Name of the unpacked column

        column_name_prefix : str, optional
            If provided, unpacked column names would start with the given
            prefix. If not provided, default value is the name of the unpacked
            column.

        column_types : [type], optional
            Column types for the unpacked columns.
            If not provided, column types are automatically inferred from first
            100 rows. For array type, default column types are float.  If
            provided, column_types also restricts how many columns to unpack.

        na_value : flexible_type, optional
            If provided, convert all values that are equal to "na_value" to
            missing value (None).

        limit : list[str] | list[int], optional
            Control unpacking only a subset of list/array/dict value. For
            dictionary XArray, `limit` is a list of dictionary keys to restrict.
            For list/array XArray, `limit` is a list of integers that are
            indexes into the list/array value.

        Returns
        -------
        out : XFrame
            A new XFrame that contains rest of columns from original XFrame with
            the given column replaced with a collection of unpacked columns.

        See Also
        --------
        xframes.XFrame.pack_columns
            The opposite of unpack.

        Examples
        ---------
        >>> xf = xframes.XFrame({'id': [1,2,3],
        ...                      'wc': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        +----+------------------+
        | id |        wc        |
        +----+------------------+
        | 1  |     {'a': 1}     |
        | 2  |     {'b': 2}     |
        | 3  | {'a': 1, 'b': 2} |
        +----+------------------+
        [3 rows x 2 columns]

        >>> xf.unpack('wc')
        +----+------+------+
        | id | wc.a | wc.b |
        +----+------+------+
        | 1  |  1   | None |
        | 2  | None |  2   |
        | 3  |  1   |  2   |
        +----+------+------+
        [3 rows x 3 columns]

        To not have prefix in the generated column name:

        >>> xf.unpack('wc', column_name_prefix="")
        +----+------+------+
        | id |  a   |  b   |
        +----+------+------+
        | 1  |  1   | None |
        | 2  | None |  2   |
        | 3  |  1   |  2   |
        +----+------+------+
        [3 rows x 3 columns]

        To limit subset of keys to unpack:

        >>> xf.unpack('wc', limit=['b'])
        +----+------+
        | id | wc.b |
        +----+------+
        | 1  | None |
        | 2  |  2   |
        | 3  |  2   |
        +----+------+
        [3 rows x 3 columns]

        To unpack an array column:

        >>> xf = xframes.XFrame({'id': [1,2,3],
        ...                       'friends': [array.array('d', [1.0, 2.0, 3.0]),
        ...                                   array.array('d', [2.0, 3.0, 4.0]),
        ...                                   array.array('d', [3.0, 4.0, 5.0])]})
        >>> xf
        +----+-----------------------------+
        | id |            friends          |
        +----+-----------------------------+
        | 1  | array('d', [1.0, 2.0, 3.0]) |
        | 2  | array('d', [2.0, 3.0, 4.0]) |
        | 3  | array('d', [3.0, 4.0, 5.0]) |
        +----+-----------------------------+
        [3 rows x 2 columns]

        >>> xf.unpack('friends')
        +----+-----------+-----------+-----------+
        | id | friends.0 | friends.1 | friends.2 |
        +----+-----------+-----------+-----------+
        | 1  |    1.0    |    2.0    |    3.0    |
        | 2  |    2.0    |    3.0    |    4.0    |
        | 3  |    3.0    |    4.0    |    5.0    |
        +----+-----------+-----------+-----------+
        [3 rows x 4 columns]
        """
        if unpack_column not in self.column_names():
            raise KeyError("Column '{}' does not exist in current XFrame.".format(unpack_column))

        if column_name_prefix is None:
            column_name_prefix = unpack_column

        new_xf = self[unpack_column].unpack(column_name_prefix, column_types, na_value, limit)

        # construct return XFrame, check if there is conflict
        rest_columns = [name for name in self.column_names() if name != unpack_column]
        new_names = new_xf.column_names()
        while set(new_names).intersection(rest_columns):
            new_names = [name + '.1' for name in new_names]
        new_xf.rename(dict(zip(new_xf.column_names(), new_names)))

        ret_xf = self.select_columns(rest_columns)
        ret_xf.add_columns(new_xf)
        return ret_xf

    def stack(self, column_name, new_column_name=None, drop_na=False):
        """
        Convert a "wide" column of an XFrame to one or two "tall" columns by
        stacking all values.

        The stack works only for columns of dict, list, or array type.  If the
        column is dict type, two new columns are created as a result of
        stacking: one column holds the key and another column holds the value.
        The rest of the columns are repeated for each key/value pair.

        If the column is array or list type, one new column is created as a
        result of stacking. With each row holds one element of the array or list
        value, and the rest columns from the same original row repeated.

        The new XFrame includes the newly created column and all columns other
        than the one that is stacked.

        Parameters
        --------------
        column_name : str
            The column to stack. This column must be of dict/list/array type

        new_column_name : str | list of str, optional
            The new column name(s). If original column is list/array type,
            new_column_name must a string. If original column is dict type,
            new_column_name must be a list of two strings. If not given, column
            names are generated automatically.

        drop_na : boolean, optional
            If True, missing values and empty list/array/dict are all dropped
            from the resulting column(s). If False, missing values are
            maintained in stacked column(s).

        Returns
        -------
        out : XFrame
            A new XFrame that contains newly stacked column(s) plus columns in
            original XFrame other than the stacked column.

        See Also
        --------
        xframes.XFrame.unstack
            Undo the effect of stack.

        Examples
        ---------
        Suppose 'xf' is an XFrame that contains a column of dict type:

        >>> xf = xframes.XFrame({'topic':[1,2,3,4],
        ...                       'words': [{'a':3, 'cat':2},
        ...                                 {'a':1, 'the':2},
        ...                                 {'the':1, 'dog':3},
        ...                                 {}]
        ...                      })
        +-------+----------------------+
        | topic |        words         |
        +-------+----------------------+
        |   1   |  {'a': 3, 'cat': 2}  |
        |   2   |  {'a': 1, 'the': 2}  |
        |   3   | {'the': 1, 'dog': 3} |
        |   4   |          {}          |
        +-------+----------------------+
        [4 rows x 2 columns]

        Stack would stack all keys in one column and all values in another
        column:

        >>> xf.stack('words', new_column_name=['word', 'count'])
        +-------+------+-------+
        | topic | word | count |
        +-------+------+-------+
        |   1   |  a   |   3   |
        |   1   | cat  |   2   |
        |   2   |  a   |   1   |
        |   2   | the  |   2   |
        |   3   | the  |   1   |
        |   3   | dog  |   3   |
        |   4   | None |  None |
        +-------+------+-------+
        [7 rows x 3 columns]

        Observe that since topic 4 had no words, an empty row is inserted.
        To drop that row, set ``dropna=True`` in the parameters to stack.

        Suppose 'xf' is an XFrame that contains a user and his/her friends,
        where 'friends' columns is an array type. Stack on 'friends' column
        would create a user/friend list for each user/friend pair:

        >>> xf = xframes.XFrame({'topic':[1,2,3],
        ...                       'friends':[[2,3,4], [5,6],
        ...                                  [4,5,10,None]]
        ...                      })
        >>> xf
        +------+------------------+
        | user |     friends      |
        +------+------------------+
        |  1   |     [2, 3, 4]    |
        |  2   |      [5, 6]      |
        |  3   | [4, 5, 10, None] |
        +------+------------------+
        [3 rows x 2 columns]

        >>> xf.stack('friends', new_column_name='friend')
        +------+--------+
        | user | friend |
        +------+--------+
        |  1   |  2     |
        |  1   |  3     |
        |  1   |  4     |
        |  2   |  5     |
        |  2   |  6     |
        |  3   |  4     |
        |  3   |  5     |
        |  3   |  10    |
        |  3   |  None  |
        +------+--------+
        [9 rows x 2 columns]
        """
        # validate column_name
        column_name = str(column_name)
        if column_name not in self.column_names():
            raise ValueError("Cannot find column '{}' in the XFrame.".format(column_name))

        stack_column_type = self[column_name].dtype()
        if stack_column_type not in [dict, array.array, list]:
            raise TypeError("Stack is only supported for column of 'dict', 'list', or 'array' type.")

        if new_column_name is not None:
            if stack_column_type == dict:
                if type(new_column_name) is not list:
                    raise TypeError("'New_column_name' has to be a 'list' to stack 'dict' type.")
                elif len(new_column_name) != 2:
                    raise TypeError("'New_column_name' must have length of two.")
            else:
                if type(new_column_name) != str:
                    raise TypeError("'New_column_name' has to be a 'str'.")
                new_column_name = [new_column_name]

            # check if the new column name conflicts with existing ones
            for name in new_column_name:
                if name in self.column_names() and name != column_name:
                    raise ValueError("Column with name '{}' already exists, pick a new column name.".format(name))
        else:
            if stack_column_type == dict:
                new_column_name = ['', '']
            else:
                new_column_name = ['']

        # infer column types
        # TODO do this with head_as_list
        head_row = XArray(self[column_name].head(100)).dropna()
        if len(head_row) == 0:
            raise ValueError('Cannot infer column type because there are not enough rows to infer value.')
        if stack_column_type == dict:
            # infer key/value type
            keys = []
            values = []
            for row in head_row:
                for val in row:
                    keys.append(val)
                    if val is not None:
                        values.append(row[val])

            new_column_type = [
                infer_type_of_list(keys),
                infer_type_of_list(values)
            ]
        else:
            values = [v for v in itertools.chain.from_iterable(head_row)]
            new_column_type = [infer_type_of_list(values)]

        if stack_column_type == dict:
            return XFrame(impl=self.__impl__.stack_dict(column_name, new_column_name, new_column_type, drop_na))
        else:
            return XFrame(impl=self.__impl__.stack_list(column_name, new_column_name, new_column_type, drop_na))

    def unstack(self, column, new_column_name=None):
        """
        Concatenate values from one or two columns into one column, grouping by
        all other columns. The resulting column could be of type list, array or
        dictionary.  If `column` is a numeric column, the result will be of
        array.array type.  If `column` is a non-numeric column, the new column
        will be of list type. If `column` is a list of two columns, the new
        column will be of dict type where the keys are taken from the first
        column in the list.

        Parameters
        ----------
        column : str | [str, str]
            The column(s) that is(are) to be concatenated.
            If str, then collapsed column type is either array or list.
            If [str, str], then collapsed column type is dict

        new_column_name : str, optional
            New column name. If not given, a name is generated automatically.

        Returns
        -------
        out : XFrame
            A new XFrame containing the grouped columns as well as the new
            column.

        See Also
        --------
        xframes.XFrame.stack
            The inverse of unstack.

        xframes.XFrame.groupby : ``Unstack`` is a special version of ``groupby`` that uses the
          :mod:`~xframes.aggregate.CONCAT` aggregator

        Notes
        -----
        - There is no guarantee the resulting XFrame maintains the same order as
          the original XFrame.

        - Missing values are maintained during unstack.

        - When unstacking into a dictionary, if there is more than one instance
          of a given key for a particular group, an arbitrary value is selected.

        Examples
        --------
        >>> xf = xframes.XFrame({'count':[4, 2, 1, 1, 2, None],
        ...                       'topic':['cat', 'cat', 'dog', 'elephant', 'elephant', 'fish'],
        ...                       'word':['a', 'c', 'c', 'a', 'b', None]})
        >>> xf.unstack(column=['word', 'count'], new_column_name='words')
        +----------+------------------+
        |  topic   |      words       |
        +----------+------------------+
        | elephant | {'a': 1, 'b': 2} |
        |   dog    |     {'c': 1}     |
        |   cat    | {'a': 4, 'c': 2} |
        |   fish   |       None       |
        +----------+------------------+
        [4 rows x 2 columns]

        >>> xf = xframes.XFrame({'friend': [2, 3, 4, 5, 6, 4, 5, 2, 3],
        ...                      'user': [1, 1, 1, 2, 2, 2, 3, 4, 4]})
        >>> xf.unstack('friend', new_column_name='friends')
        +------+-----------------------------+
        | user |           friends           |
        +------+-----------------------------+
        |  3   |      array('d', [5.0])      |
        |  1   | array('d', [2.0, 4.0, 3.0]) |
        |  2   | array('d', [5.0, 6.0, 4.0]) |
        |  4   |    array('d', [2.0, 3.0])   |
        +------+-----------------------------+
        [4 rows x 2 columns]
        """
        if type(column) != str and len(column) != 2:
            raise TypeError("'Column' parameter has to be either a string or a list of two strings.")

        if new_column_name is None:
            new_column_name = 'unstack'
        if type(column) == str:
            key_columns = [i for i in self.column_names() if i != column]
            if new_column_name is not None:
                return self.groupby(key_columns, {new_column_name: xframes.aggregate.CONCAT(column)})
            else:
                return self.groupby(key_columns, xframes.aggregate.CONCAT(column))
        elif len(column) == 2:
            key_columns = [i for i in self.column_names() if i not in column]
            if new_column_name is not None:
                return self.groupby(key_columns, {new_column_name: xframes.aggregate.CONCAT(column[0], column[1])})
            else:
                return self.groupby(key_columns, xframes.aggregate.CONCAT(column[0], column[1]))

    def unique(self):
        """
        Remove duplicate rows of the XFrame. Will not necessarily preserve the
        order of the given XFrame in the new XFrame.

        Returns
        -------
        out : XFrame
            A new XFrame that contains the unique rows of the current XFrame.

        Raises
        ------
        TypeError
          If any column in the XFrame is a dictionary type.

        See Also
        --------
        xframes.XFrame.unique

        Examples
        --------
        >>> xf = xframes.XFrame({'id':[1,2,3,3,4], 'value':[1,2,3,3,4]})
        >>> xf
        +----+-------+
        | id | value |
        +----+-------+
        | 1  |   1   |
        | 2  |   2   |
        | 3  |   3   |
        | 3  |   3   |
        | 4  |   4   |
        +----+-------+
        [5 rows x 2 columns]

        >>> xf.unique()
        +----+-------+
        | id | value |
        +----+-------+
        | 2  |   2   |
        | 4  |   4   |
        | 3  |   3   |
        | 1  |   1   |
        +----+-------+
        [4 rows x 2 columns]
        """
        return XFrame(impl=self.__impl__.unique())

    def sort(self, sort_columns, ascending=True):
        """
        Sort current XFrame by the given columns, using the given sort order.
        Only columns that are type of str, int and float can be sorted.

        Parameters
        ----------
        sort_columns : str | list of str | list of (str, bool) pairs
            Names of columns to be sorted.  The result will be sorted first by
            first column, followed by second column, and so on. All columns will
            be sorted in the same order as governed by the `ascending`
            parameter. To control the sort ordering for each column
            individually, `sort_columns` must be a list of (str, bool) pairs.
            Given this case, the first value is the column name and the second
            value is a boolean indicating whether the sort order is ascending.

        ascending : bool, optional
            Sort all columns in the given order.

        Returns
        -------
        out : XFrame
            A new XFrame that is sorted according to given sort criteria

        See Also
        --------
        xframes.XFrame.topk

        Examples
        --------
        Suppose 'xf' is an xframe that has three columns 'a', 'b', 'c'.
        To sort by column 'a', ascending:

        >>> xf = xframes.XFrame({'a':[1,3,2,1],
        ...                       'b':['a','c','b','b'],
        ...                       'c':['x','y','z','y']})
        >>> xf
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | a | x |
        | 3 | c | y |
        | 2 | b | z |
        | 1 | b | y |
        +---+---+---+
        [4 rows x 3 columns]

        >>> xf.sort('a')
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | a | x |
        | 1 | b | y |
        | 2 | b | z |
        | 3 | c | y |
        +---+---+---+
        [4 rows x 3 columns]

        To sort by column 'a', descending:

        >>> xf.sort('a', ascending = False)
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 3 | c | y |
        | 2 | b | z |
        | 1 | a | x |
        | 1 | b | y |
        +---+---+---+
        [4 rows x 3 columns]

        To sort by column 'a' and 'b', all ascending:

        >>> xf.sort(['a', 'b'])
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | a | x |
        | 1 | b | y |
        | 2 | b | z |
        | 3 | c | y |
        +---+---+---+
        [4 rows x 3 columns]

        To sort by column 'a' ascending, and then by column 'c' descending:

        >>> xf.sort([('a', True), ('c', False)])
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | b | y |
        | 1 | a | x |
        | 2 | b | z |
        | 3 | c | y |
        +---+---+---+
        [4 rows x 3 columns]
        """
        sort_column_orders = []

        # validate sort_columns
        if type(sort_columns) == str:
            sort_column_names = [sort_columns]
        elif type(sort_columns) == list:
            if len(sort_columns) == 0:
                raise ValueError('Please provide at least one column to sort.')

            first_param_types = set([type(i) for i in sort_columns])
            if len(first_param_types) != 1:
                raise ValueError('Sort_columns element are not of the same type.')

            first_param_type = first_param_types.pop()
            if first_param_type == tuple:
                sort_column_names = [i[0] for i in sort_columns]
                sort_column_orders = [i[1] for i in sort_columns]
            elif first_param_type == str:
                sort_column_names = sort_columns
            else:
                raise TypeError('Sort_columns type is not supported.')
        else:
            raise TypeError('Sort_columns type is not correct. Supported types are ' +
                            "'str', 'list of str' or 'list of (str,bool)' pair.")

        # use the second parameter if the sort order is not given
        if len(sort_column_orders) == 0:
            sort_column_orders = [ascending for _ in sort_column_names]

        # make sure all column exists
        my_column_names = set(self.column_names())
        for column in sort_column_names:
            if type(column) != str:
                raise TypeError('Only string parameter can be passed in as column names.')
            if column not in my_column_names:
                raise ValueError("XFrame has no column named: '{}'.".format(column))
            else:
                sortable_types = (str, int, float, numpy.int32, datetime.datetime)
            if self[column].dtype() not in sortable_types:
                raise TypeError("Only columns of type ('str', 'int', 'float', 'numpy.int32') can be sorted: {}."
                                .format(self[column].dtype()))

        return XFrame(impl=self.__impl__.sort(sort_column_names, sort_column_orders))

    def dropna(self, columns=None, how='any'):
        """
        Remove missing values from an XFrame. A missing value is either None
        or NaN.  If `how` is 'any', a row will be removed if any of the
        columns in the `columns` parameter contains at least one missing
        value.  If `how` is 'all', a row will be removed if all of the columns
        in the `columns` parameter are missing values.

        If the `columns` parameter is not specified, the default is to
        consider all columns when searching for missing values.

        Parameters
        ----------
        columns : list or str, optional
            The columns to use when looking for missing values. By default, all
            columns are used.

        how : {'any', 'all'}, optional
            Specifies whether a row should be dropped if at least one column
            has missing values, or if all columns have missing values.  'any' is
            default.

        Returns
        -------
        out : XFrame
            XFrame with missing values removed (according to the given rules).

        See Also
        --------
        xframes.XFrame.dropna_split:  Drops missing rows from the XFrame and returns them.

        Examples
        --------
        Drop all missing values.

        >>> xf = xframes.XFrame({'a': [1, None, None], 'b': ['a', 'b', None]})
        >>> xf.dropna()
        +---+---+
        | a | b |
        +---+---+
        | 1 | a |
        +---+---+
        [1 rows x 2 columns]

        Drop rows where every value is missing.

        >>> xf.dropna(any="all")
        +------+---+
        |  a   | b |
        +------+---+
        |  1   | a |
        | None | b |
        +------+---+
        [2 rows x 2 columns]

        Drop rows where column 'a' has a missing value.

        >>> xf.dropna('a', any="all")
        +---+---+
        | a | b |
        +---+---+
        | 1 | a |
        +---+---+
        [1 rows x 2 columns]
        """

        # If the user gives me an empty list (the indicator to use all columns)
        # NA values being dropped would not be the expected behavior. This
        # is a NOOP, so let's not bother the server
        if type(columns) is list and len(columns) == 0:
            return XFrame(impl=self.__impl__)

        (columns, all_behavior) = self.__dropna_errchk(columns, how)

        return XFrame(impl=self.__impl__.drop_missing_values(columns, all_behavior, False))

    def dropna_split(self, columns=None, how='any'):
        """
        Split rows with missing values from this XFrame. This function has the
        same functionality as :py:func:`~xframes.XFrame.dropna`, but returns a
        tuple of two XFrames.  The first item is the expected output from
        :py:func:`~xframes.XFrame.dropna`, and the second item contains all the
        rows filtered out by the `dropna` algorithm.

        Parameters
        ----------
        columns : list or str, optional
            The columns to use when looking for missing values. By default, all
            columns are used.

        how : {'any', 'all'}, optional
            Specifies whether a row should be dropped if at least one column
            has missing values, or if all columns have missing values.  'any' is
            default.

        Returns
        -------
        out : (XFrame, XFrame)
            (XFrame with missing values removed,
             XFrame with the removed missing values)

        See Also
        --------
        xframes.XFrame.dropna

        Examples
        --------
        >>> xf = xframes.XFrame({'a': [1, None, None], 'b': ['a', 'b', None]})
        >>> good, bad = xf.dropna_split()
        >>> good
        +---+---+
        | a | b |
        +---+---+
        | 1 | a |
        +---+---+
        [1 rows x 2 columns]

        >>> bad
        +------+------+
        |  a   |  b   |
        +------+------+
        | None |  b   |
        | None | None |
        +------+------+
        [2 rows x 2 columns]
        """

        # If the user gives me an empty list (the indicator to use all columns)
        # NA values being dropped would not be the expected behavior. This
        # is a NOOP, so let's not bother the server
        if type(columns) is list and len(columns) == 0:
            return XFrame(impl=self.__impl__), XFrame()

        (columns, all_behavior) = self.__dropna_errchk(columns, how)

        xframe_tuple = self.__impl__.drop_missing_values(columns, all_behavior, True)

        if len(xframe_tuple) != 2:
            raise RuntimeError('Did not return two XFrames.')

        return XFrame(impl=xframe_tuple[0]), XFrame(impl=xframe_tuple[1])

    @staticmethod
    def __dropna_errchk(columns, how):
        if columns is None:
            # Default behavior is to consider every column, specified to
            # the server by an empty list (to avoid sending all the column
            # in this case, since it is the most common)
            columns = list()
        elif type(columns) is str:
            columns = [columns]
        elif type(columns) is not list:
            raise TypeError("Must give columns as a 'list', 'str', or 'None'.")
        else:
            # Verify that we are only passing strings in our list
            list_types = set([type(i) for i in columns])
            if str not in list_types or len(list_types) > 1:
                raise TypeError("All columns must be of 'str' type.")

        if how not in ['any', 'all']:
            raise ValueError("Must specify 'any' or 'all'.")

        if how == 'all':
            all_behavior = True
        else:
            all_behavior = False

        return columns, all_behavior

    def fillna(self, column, value):
        """
        Fill all missing values with a given value in a given column. If the
        `value` is not the same type as the values in `column`, this method
        attempts to convert the value to the original column's type. If this
        fails, an error is raised.

        Parameters
        ----------
        column : str
            The name of the column to modify.

        value : type convertible to XArray's type
            The value used to replace all missing values.

        Returns
        -------
        out : XFrame
            A new XFrame with the specified value in place of missing values.

        See Also
        --------
        xframes.XFrame.dropna

        Examples
        --------
        >>> xf = xframes.XFrame({'a':[1, None, None],
        ...                       'b':['13.1', '17.2', None]})
        >>> xf = xf.fillna('a', 0)
        >>> xf
        +---+------+
        | a |  b   |
        +---+------+
        | 1 | 13.1 |
        | 0 | 17.2 |
        | 0 | None |
        +---+------+
        [3 rows x 2 columns]
        """
        # Normal error checking
        if type(column) is not str:
            raise TypeError("Must give column name as a 'str'.")
        ret = self[self.column_names()]
        ret[column] = ret[column].fillna(value)
        return ret

    def add_row_number(self, column_name='id', start=0):
        """
        Returns a new XFrame with a new column that numbers each row
        sequentially. By default the count starts at 0, but this can be changed
        to a positive or negative number.  The new column will be named with
        the given column name.  An error will be raised if the given column
        name already exists in the XFrame.

        Parameters
        ----------
        column_name : str, optional
            The name of the new column that will hold the row numbers.

        start : int, optional
            The number used to start the row number count.

        Returns
        -------
        out : XFrame
            The new XFrame with a column name

        Notes
        -----
        The range of numbers is constrained by a signed 64-bit integer, so
        beware of overflow if you think the results in the row number column
        will be greater than 9 quintillion.

        Examples
        --------
        >>> xf = xframes.XFrame({'a': [1, None, None], 'b': ['a', 'b', None]})
        >>> xf.add_row_number()
        +----+------+------+
        | id |  a   |  b   |
        +----+------+------+
        | 0  |  1   |  a   |
        | 1  | None |  b   |
        | 2  | None | None |
        +----+------+------+
        [3 rows x 3 columns]
        """

        if type(column_name) is not str:
            raise TypeError("Must give column_name as 'str's.")

        if type(start) is not int:
            raise TypeError("Must give start as 'int'.")

        if column_name in self.column_names():
            raise RuntimeError("Column '{}' already exists in the current XFrame.".format(column_name))

        return XFrame(impl=self.__impl__.add_row_number(column_name, start))

    def sql(self, sql_statement, table_name='xframe'):
        """
        Executes the given sql statement over the data in the table.
        Returns a new XFrame with the results.

        Parameters
        ----------
        sql_statement : str
            The statement to execute.

            The statement is executed by the Spark Sql query processor.  
            See the SparkSql documentation for details.  
            XFrame column names and types are translated to Spark
            for query processing.

        table_name : str, optional
            The table name to create, referred to in the sql statement.
            Defaulst to 'xframe'.

        Returns
        -------
        out : XFrame
            The new XFrame with the results.


        Examples
        --------
        >>> xf = xframes.XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        >>> xf.sql("SELECT * FROM xframe WHERE id > 1"
        +----+--------+
        | id |  val   |
        +----+--------+
        | 2  |   'b'  |
        | 3  |   'c'  |
        +----+-----  -+
        [3 rows x 2 columns]
        """
        return XFrame(impl=self.__impl__.sql(sql_statement, table_name=table_name))

    @property
    def shape(self):
        """
        The shape of the XFrame, in a tuple. The first entry is the number of
        rows, the second is the number of columns.

        Examples
        --------
        >>> xf = xframes.XFrame({'id':[1,2,3], 'val':['A','B','C']})
        >>> xf.shape
        (3, 2)
        """
        return self.num_rows(), self.num_columns()

    def show(self):
        """
        Create an XPlot object from an XFrame.  

        This can be used to produce plots.

        See Also
        --------
        xframes.XPlot : Plot library
        """

        return XPlot(self)
