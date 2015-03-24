"""
This module defines the XArray class which provides the
ability to create, access and manipulate a remote scalable array object.

XArray acts similarly to pandas.Series but without indexing.
The data is immutable, homogeneous, and is stored in a Spark RDD.
"""
from xpatterns.xarray_impl import XArrayImpl, infer_type_of_list
from util import make_internal_url, split_path_elements
import xpatterns as xp

import inspect
import math
import numpy
import pandas
import time
import array
import datetime
import itertools
import warnings
import datetime

__all__ = ['XArray']

def _create_sequential_xarray(size, start=0, reverse=False):
    if type(size) is not int:
        raise TypeError("size must be int")

    if type(start) is not int:
        raise TypeError("size must be int")

    if type(reverse) is not bool:
        raise TypeError("reverse must me bool")

    return XArray(_impl=XArrayImpl.create_sequential_xarray(size, start, reverse))

class XArray(object):
    """
    An immutable, homogeneously typed array object backed by Spark RDD.

    XArray is able to hold data that are much larger than the machine's main
    memory. It fully supports missing values and random access (although random
    access is inefficient). The data backing an XArray is located on the cluster 
    hosting Spark.

    Parameters
    ----------
    data : list | numpy.ndarray | pandas.Series | string
        The input data. If this is a list, numpy.ndarray, or pandas.Series,
        the data in the list is converted and stored in an XArray.
        Alternatively if this is a string, it is interpreted as a path (or
        url) to a text file. Each line of the text file is loaded as a
        separate row. If ``data`` is a directory where an XArray was previously
        saved, this is loaded as an XArray read directly out of that
        directory.

    dtype : {None, int, float, str, list, array.array, dict, datetime.datetime}, optional
        The data type of the XArray. If not specified (None), we attempt to
        infer it from the input. If it is a numpy array or a Pandas series, the
        dtype of the array/series is used. If it is a list, the dtype is
        inferred from the inner list. If it is a URL or path to a text file, we
        default the dtype to str.

    ignore_cast_failure : bool, optional
        If True, ignores casting failures but warns when elements cannot be
        casted into the specified dtype.

    Notes
    -----
    - If ``data`` is pandas.Series, the index will be ignored.
    - The datetime is based on the Boost datetime format (see http://www.boost.org/doc/libs/1_48_0/doc/html/date_time/date_time_io.html
      for details)

    Examples
    --------
    >>> sa = XArray(data=[1,2,3,4,5], dtype=int)
    >>> sa = XArray('http://s3-us-west-2.amazonaws.com/testdatasets/a_to_z.txt.gz')
    >>> sa = XArray([[1,2,3], [3,4,5]])
    >>> sa = XArray(data=[{'a':1, 'b': 2}, {'b':2, 'c': 1}])
    >>> sa = XArray(data=[datetime.datetime(2011, 10, 20, 9, 30, 10)])

    Missing Functionality
    ---------------------
    - data numpy.ndarray
    - data pandar.Series
    - dtype array.array
    - dtype datetime.datetime
    """

    def __init__(self, data=[], dtype=None, ignore_cast_failure=False, _impl=None):
        """
        __init__(data=list(), dtype=None, ignore_cast_failure=False)

        Construct a new XArray. The source of data includes: list,
        numpy.ndarray, pandas.Series, and urls.
        """
        if dtype is not None and type(dtype) != type:
            raise TypeError('dtype must be a type, e.g. use int rather than \'int\'')

        if (_impl):
            self.__impl__ = _impl
        elif type(data) == XArray:
            self.__impl__ = data.__impl__
        else:
            self.__impl__ = XArrayImpl()
            # we need to perform type inference
            if dtype is None:
                if (isinstance(data, list)):
                    # if it is a list, Get the first type and make sure
                    # the remaining items are all of the same type
                    dtype = infer_type_of_list(data)
                elif isinstance(data, array.array):
                    dtype = infer_type_of_list(data)
                elif isinstance(data, pandas.Series):
                    # if it is a pandas series get the dtype of the series
                    dtype = pytype_from_dtype(data.dtype)
                    if dtype == object:
                        # we need to get a bit more fine grained than that
                        dtype = infer_type_of_list(data)

                elif isinstance(data, numpy.ndarray):
                    # if it is a numpy array, get the dtype of the array
                    dtype = pytype_from_dtype(data.dtype)
                    if dtype == object:
                        # we need to get a bit more fine grained than that
                        dtype = infer_type_of_list(data)
                    if len(data.shape) == 2:
                        # we need to make it an array or a list
                        if dtype == float or dtype == int:
                            dtype = array.array
                        else:
                            dtype = list
                    elif len(data.shape) > 2:
                        raise TypeError("Cannot convert Numpy arrays of greater than 2 dimensions")

                elif (isinstance(data, str)):
                    # if it is a file, we default to string
                    dtype = str

            if isinstance(data, pandas.Series):
                self.__impl__.load_from_iterable(data.values, dtype, ignore_cast_failure)
            elif (isinstance(data, numpy.ndarray)) or isinstance(data, list) or isinstance(data, array.array):
                self.__impl__.load_from_iterable(data, dtype, ignore_cast_failure)
            elif (isinstance(data, str)):
                internal_url = make_internal_url(data)
                self.__impl__.load_autodetect(internal_url, dtype)
            else:
                raise TypeError("Unexpected data source. " \
                                "Possible data source types are: list, " \
                                "numpy.ndarray, pandas.Series, and string(url)")

    @staticmethod
    def set_trace(entry_trace=None, exit_trace=None):
        XArrayImpl.set_trace(entry_trace, exit_trace)

    @classmethod
    def from_const(cls, value, size):
        """
        Constructs an XArray of size with a const value.

        Parameters
        ----------
        value : [int | float | str | array.array | list | dict]
          The value to fill the XArray
        size : int
          The size of the XArray

        Examples
        --------
        Construct an XArray consisting of 10 zeroes:

        >>> xpatterns.XArray.from_const(0, 10)
        """
        assert type(size) is int and size >= 0, "size must be a positive int"
        if (type(value) not in set([int, float, str, array.array, list, dict])):
            raise TypeError('Cannot create xarray of value type %s' % str(type(value)))
        impl = XArrayImpl()
        impl.load_from_const(value, size)
        return cls(_impl=impl)

    @classmethod
    def from_sequence(cls, *args):
        """
        Constructs an XArray by generating a sequence of consecutive numbers.

        Parameters
        ----------
        start : int, optional
            The start of the sequence. The sequence will contain this value.

        stop : int
          The end of the sequence. The sequence will not contain this value.

        Examples
        --------
        >>> from_sequence(1000)
        Construct an XArray of integer values from 0 to 999

        This is equivalent, but more efficient than:
        >>> XArray(range(1000))

        >>> from_sequence(10, 1000)
        Construct an XArray of integer values from 10 to 999

        This is equivalent, but more efficient than:
        >>> XArray(range(10, 1000))

        """
        start = None
        stop = None
        # fill with args. This checks for from_sequence(100), from_sequence(10,100)
        if len(args) == 1:
            stop = args[0]
        elif len(args) == 2:
            start = args[0]
            stop = args[1]

        if stop is None and start is None:
            raise TypeError("from_sequence expects at least 1 argument. got 0")
        elif start is None:
            return _create_sequential_xarray(stop)
        else:
            size = stop - start
            # this matches the behavior of range
            # i.e. range(100,10) just returns an empty array
            if (size < 0):
                size = 0
            return _create_sequential_xarray(size, start)

    def __get_content_identifier__(self):
        """
        Returns the unique identifier of the content that backs the XArray

        Notes
        -----
        Meant for internal use only.
        """
        return self.__impl__.get_content_identifier()

    def save(self, filename, format=None):
        """
        Saves the XArray to file.

        The saved XArray will be in a directory named with the `targetfile`
        parameter.

        Parameters
        ----------
        filename : string
            A local path or a remote URL.  If format is 'text', it will be
            saved as a text file. If format is 'binary', a directory will be
            created at the location which will contain the XArray.

        format : {'binary', 'text', 'csv'}, optional
            Format in which to save the XFrame. Binary saved XArrays can be
            loaded much faster and without any format conversion losses.
            'text' and 'csv' are synonymous: Each XArray row will be written
            as a single line in an output text file. If not
            given, will try to infer the format from filename given. If file
            name ends with 'csv', 'txt' or '.csv.gz', then save as 'csv' format,
            otherwise save as 'binary' format.
        """
        if format == None:
            if filename.endswith(('.csv', '.csv.gz', 'txt')):
                format = 'text'
            else:
                format = 'binary'
        if format == 'binary':
            self.__impl__.save(make_internal_url(filename))
        elif format == 'text':
            self.__impl__.save_as_text(make_internal_url(filename))
        elif format == 'csv':
            self.__impl__.save_as_csv(make_internal_url(filename))

    def to_rdd(self, number_of_partitions=4):
        """
        Convert the current XArray to the Spark RDD.

        ----------
        out: RDD

        """

        if type(number_of_partitions) is not int:
            raise ValueError("number_of_partitions parameter expects an integer type")
        if number_of_partitions == 0:
            raise ValueError("number_of_partitions can not be initialized to zero")

        return self.__impl__.to_rdd(number_of_partitions)

    @classmethod
    def from_rdd(cls, rdd, dtype):
        """
        Convert a Spark RDD into an XArray


        Parameters
        ----------
        rdd : pyspark.rdd.RDD

        dtype : type

        Returns
        -------
        out : XArray
        """
        return cls(_impl=XArrayImpl.from_rdd(rdd, dtype))


    def __repr__(self):
        """
        Returns a string description of the XArray.
        """
        ret = "dtype: " + str(self.dtype()) + "\n"
        ret = ret + "Rows: " + str(self.size()) + "\n"
        ret = ret + str(self)
        return ret

    def __str__(self):
        """
        Returns a string containing the first 100 elements of the array.
        """
        h = self.__impl__.head_as_list(100)
        headln = str(h)
        if (self.size() > 100):
            # cut the last close bracket
            # and replace it with ...
            headln = headln[0:-1] + ", ... ]"
        return headln

    def __nonzero__(self):
        """
        Returns true if the array is not empty.
        """
        return self.size() != 0

    def __len__(self):
        """
        Returns the length of the array
        """
        return self.size()

    def __iter__(self):
        """
        Provides an iterator to the contents of the array.
        """
        def generator():
            elems_at_a_time = 262144
            self.__impl__.begin_iterator()
            ret = self.__impl__.iterator_get_next(elems_at_a_time)
            while(True):
                for j in ret:
                    yield j

                if len(ret) == elems_at_a_time:
                    ret = self.__impl__.iterator_get_next(elems_at_a_time)
                else:
                    break

        return generator()

    def __add__(self, other):
        """
        If other is a scalar value, adds it to the current array, returning
        the new result. If other is an XArray, performs an element-wise
        addition of the two arrays.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '+'))
        else:
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '+'))

    def __sub__(self, other):
        """
        If other is a scalar value, subtracts it from the current array, returning
        the new result. If other is an XArray, performs an element-wise
        subtraction of the two arrays.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '-'))
        else:
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '-'))

    def __mul__(self, other):
        """
        If other is a scalar value, multiplies it to the current array, returning
        the new result. If other is an XArray, performs an element-wise
        multiplication of the two arrays.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '*'))
        else:
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '*'))

    def __div__(self, other):
        """
        If other is a scalar value, divides each element of the current array
        by the value, returning the result. If other is an XArray, performs
        an element-wise division of the two arrays.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '/'))
        else:
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '/'))

    def __pow__(self, other):
        """
        Oher must be a scalar value, raises to the current array to thet power, returning
        the new result.
        """
        if type(other) in (int, long, float):
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '**'))

    def __lt__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the result. If other is an XArray, performs
        an element-wise comparison of the two arrays.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '<'))
        else:
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '<'))

    def __gt__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the result. If other is an XArray, performs
        an element-wise comparison of the two arrays.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '>'))
        else:
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '>'))

    def __le__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the result. If other is an XArray, performs
        an element-wise comparison of the two arrays.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '<='))
        else:
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '<='))

    def __ge__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the result. If other is an XArray, performs
        an element-wise comparison of the two arrays.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '>='))
        else:
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '>='))

    def __radd__(self, other):
        """
        Adds a scalar value to the current array.
        Returned array has the same type as the array on the right hand side
        """
        return XArray(_impl = self.__impl__.right_scalar_operator(other, '+'))

    def __rsub__(self, other):
        """
        Subtracts a scalar value from the current array.
        Returned array has the same type as the array on the right hand side
        """
        return XArray(_impl = self.__impl__.right_scalar_operator(other, '-'))

    def __rmul__(self, other):
        """
        Multiplies a scalar value to the current array.
        Returned array has the same type as the array on the right hand side
        """
        return XArray(_impl = self.__impl__.right_scalar_operator(other, '*'))

    def __rdiv__(self, other):
        """
        Divides a scalar value by each element in the array
        Returned array has the same type as the array on the right hand side
        """
        return XArray(_impl = self.__impl__.right_scalar_operator(other, '/'))

    def __eq__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the new result. If other is an XArray, performs
        an element-wise comparison of the two arrays.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '=='))
        else:
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '=='))

    def __ne__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the new result. If other is an XArray, performs
        an element-wise comparison of the two arrays.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '!='))
        else:
            return XArray(_impl = self.__impl__.left_scalar_operator(other, '!='))

    def __and__(self, other):
        """
        Perform a logical element-wise 'and' against another XArray.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '&'))
        else:
            raise TypeError("XArray can only perform logical and against another XArray")


    def __or__(self, other):
        """
        Perform a logical element-wise 'or' against another XArray.
        """
        if type(other) is XArray:
            return XArray(_impl = self.__impl__.vector_operator(other.__impl__, '|'))
        else:
            raise TypeError("XArray can only perform logical or against another XArray")


    def __getitem__(self, other):
        """
        If the key is an XArray of identical length, this function performs a
        logical filter: i.e. it subselects all the elements in this array
        where the corresponding value in the other array evaluates to true.
        If the key is an integer this returns a single row of
        the XArray. If the key is a slice, this returns an XArray with the
        sliced rows. 
        """
        if type(other) is XArray:
            if len(other) != len(self):
                raise IndexError("Cannot perform logical indexing on arrays of different length.")
            return XArray(_impl = self.__impl__.logical_filter(other.__impl__))
        elif type(other) is int:
            if other < 0:
                other = len(self) + other
            if other >= len(self):
                raise IndexError("XArray index out of range")
            return list(XArray(_impl = self.__impl__.copy_range(other, 1, other+1)))[0]
        elif type(other) is slice:
            start = other.start
            stop = other.stop
            step = other.step
            if start is None:
                start = 0
            if stop is None:
                stop = len(self)
            if step is None:
                step = 1
            # handle negative indices
            if start < 0:
                start = len(self) + start
            if stop < 0:
                stop = len(self) + stop
            return XArray(_impl = self.__impl__.copy_range(start, step, stop))
        else:
            raise IndexError("Invalid type to use for indexing")

    def __materialize__(self):
        """
        For a XArray that is lazily evaluated, force persist this xarray
        to disk, committing all lazy evaluated operations.
        """
        self.__impl__.materialize()

    def __is_materialized__(self):
        """
        Returns whether or not the xarray has been materialized.
        """
        return self.__impl__.is_materialized()

    def size(self):
        """
        The size of the XArray.
        """
        return self.__impl__.size()

    def dtype(self):
        """
        The data type of the XArray.

        Returns
        -------
        out : type
            The type of the XArray.

        Examples
        --------
        >>> sa = XArray(["The quick brown fox jumps over the lazy dog."])
        >>> sa.dtype()
        str
        >>> sa = XArray(range(10))
        >>> sa.dtype()
        int
        """
        return self.__impl__.dtype()

    def head(self, n=10):
        """
        Returns an XArray which contains the first n rows of this XArray.

        Parameters
        ----------
        n : int
            The number of rows to fetch.

        Returns
        -------
        out : XArray
            A new XArray which contains the first n rows of the current XArray.

        Examples
        --------
        >>> XArray(range(10)).head(5)
        dtype: int
        Rows: 5
        [0, 1, 2, 3, 4]
        """
        return XArray(_impl=self.__impl__.head(n))

    def vector_slice(self, start, end=None):
        """
        If this XArray contains vectors or recursive types, this returns a new XArray
        containing each individual vector sliced, between start and end, exclusive.

        Parameters
        ----------
        start : int
            The start position of the slice.

        end : int, optional.
            The end position of the slice. Note that the end position
            is NOT included in the slice. Thus a g.vector_slice(1,3) will extract
            entries in position 1 and 2.

        Returns
        -------
        out : XArray
            Each individual vector sliced according to the arguments.

        Examples
        --------

        If g is a vector of floats:

        >>> g = XArray([[1,2,3],[2,3,4]])
        >>> g
        dtype: array
        Rows: 2
        [array('d', [1.0, 2.0, 3.0]), array('d', [2.0, 3.0, 4.0])]

        >>> g.vector_slice(0) # extracts the first element of each vector
        dtype: float
        Rows: 2
        [1.0, 2.0]

        >>> g.vector_slice(0, 2) # extracts the first two elements of each vector
        dtype: array.array
        Rows: 2
        [array('d', [1.0, 2.0]), array('d', [2.0, 3.0])]

        If a vector cannot be sliced, the result will be None:

        >>> g = XArray([[1],[1,2],[1,2,3]])
        >>> g
        dtype: array.array
        Rows: 3
        [array('d', [1.0]), array('d', [1.0, 2.0]), array('d', [1.0, 2.0, 3.0])]

        >>> g.vector_slice(2)
        dtype: float
        Rows: 3
        [None, None, 3.0]

        >>> g.vector_slice(0,2)
        dtype: list
        Rows: 3
        [None, array('d', [1.0, 2.0]), array('d', [1.0, 2.0])]

        If g is a vector of mixed types (float, int, str, array, list, etc.):

        >>> g = XArray([['a',1,1.0],['b',2,2.0]])
        >>> g
        dtype: list
        Rows: 2
        [['a', 1, 1.0], ['b', 2, 2.0]]

        >>> g.vector_slice(0) # extracts the first element of each vector
        dtype: list
        Rows: 2
        [['a'], ['b']]
        """
        if (self.dtype() != array.array) and (self.dtype() != list):
            raise RuntimeError("Only Vector type can be sliced")
        if end == None:
            end = start + 1

        return XArray(_impl=self.__impl__.vector_slice(start, end))

    def _count_words(self, to_lower=True):
        """
        For documentation, see text_analytics.count_ngrams().
        """
        if (self.dtype() != str):
            raise TypeError("Only XArray of string type is supported for counting bag of words")

        # construct options, will extend over time
        options = dict()
        options["to_lower"] = to_lower == True


        return XArray(_impl=self.__impl__.count_bag_of_words(options))

    def _count_ngrams(self, n=2, method="word", to_lower=True, ignore_space=True):
        """
        For documentation, see text_analytics.count_ngrams().
        """
        if (self.dtype() != str):
            raise TypeError("Only XArray of string type is supported for counting n-grams")

        if (type(n) != int):
            raise TypeError("Input 'n' must be of type int")

        if (n < 1):
            raise ValueError("Input 'n' must be greater than 0")

        if (n > 5):
            warnings.warn("It is unusual for n-grams to be of size larger than 5.")

        # construct options, will extend over time
        options = dict()
        options["to_lower"] = to_lower == True
        options["ignore_space"] = ignore_space == True


        if method == "word":
            return XArray(_impl=self.__impl__.count_ngrams(n, options ))
        elif method == "character" :
            return XArray(_impl=self.__impl__.count_character_ngrams(n, options ))
        else:
            raise ValueError("Invalid 'method' input  value. Please input either 'word' or 'character' ")

    def dict_trim_by_keys(self, keys, exclude=True):
        """
        Filter an XArray of dictionary type by the given keys. By default, all
        keys that are in the provided list in ``keys`` are *excluded* from the
        returned XArray.

        Parameters
        ----------
        keys : list
            A collection of keys to trim down the elements in the XArray.

        exclude : bool, optional
            If True, all keys that are in the input key list are removed. If
            False, only keys that are in the input key list are retained.

        Returns
        -------
        out : XArray
            A XArray of dictionary type, with each dictionary element trimmed
            according to the input criteria.

        See Also
        --------
        dict_trim_by_values

        Examples
        --------
        >>> sa = xpatterns.XArray([{"this":1, "is":1, "dog":2},
                                  {"this": 2, "are": 2, "cat": 1}])
        >>> sa.dict_trim_by_keys(["this", "is", "and", "are"], exclude=True)
        dtype: dict
        Rows: 2
        [{'dog': 2}, {'cat': 1}]
        """
        if isinstance(keys, str) or (not hasattr(keys, "__iter__")):
            keys = [keys]

        return XArray(_impl=self.__impl__.dict_trim_by_keys(keys, exclude))

    def dict_trim_by_values(self, lower=None, upper=None):
        """
        Filter dictionary values to a given range (inclusive). Trimming is only
        performed on values which can be compared to the bound values. Fails on
        XArrays whose data type is not ``dict``.

        Parameters
        ----------
        lower : int or long or float, optional
            The lowest dictionary value that would be retained in the result. If
            not given, lower bound is not applied.

        upper : int or long or float, optional
            The highest dictionary value that would be retained in the result.
            If not given, upper bound is not applied.

        Returns
        -------
        out : XArray
            An XArray of dictionary type, with each dict element trimmed
            according to the input criteria.

        See Also
        --------
        dict_trim_by_keys

        Examples
        --------
        >>> sa = xpatterns.XArray([{"this":1, "is":5, "dog":7},
                                  {"this": 2, "are": 1, "cat": 5}])
        >>> sa.dict_trim_by_values(2,5)
        dtype: dict
        Rows: 2
        [{'is': 5}, {'this': 2, 'cat': 5}]

        >>> sa.dict_trim_by_values(upper=5)
        dtype: dict
        Rows: 2
        [{'this': 1, 'is': 5}, {'this': 2, 'are': 1, 'cat': 5}]
        """

        if None != lower and (not is_numeric_type(type(lower))):
            raise TypeError("lower bound has to be a numeric value")

        if None != upper and (not is_numeric_type(type(upper))):
            raise TypeError("upper bound has to be a numeric value")

        return XArray(_impl=self.__impl__.dict_trim_by_values(lower, upper))

    def dict_keys(self):
        """
        Create an XArray that contains all the keys from each dictionary
        element as a list. Fails on XArrays whose data type is not ``dict``.

        Returns
        -------
        out : XArray
            A XArray of list type, where each element is a list of keys
            from the input XArray element.

        See Also
        --------
        dict_values

        Examples
        ---------
        >>> sa = xpatterns.XArray([{"this":1, "is":5, "dog":7},
                                  {"this": 2, "are": 1, "cat": 5}])
        >>> sa.dict_keys()
        dtype: list
        Rows: 2
        [['this', 'is', 'dog'], ['this', 'are', 'cat']]
        """
        return XArray(_impl=self.__impl__.dict_keys())

    def dict_values(self):
        """
        Create an XArray that contains all the values from each dictionary
        element as a list. Fails on XArrays whose data type is not ``dict``.

        Returns
        -------
        out : XArray
            A XArray of list type, where each element is a list of values
            from the input XArray element.

        See Also
        --------
        dict_keys

        Examples
        --------
        >>> sa = xpatterns.XArray([{"this":1, "is":5, "dog":7},
                                 {"this": 2, "are": 1, "cat": 5}])
        >>> sa.dict_values()
        dtype: list
        Rows: 2
        [[1, 5, 7], [2, 1, 5]]

        """
        return XArray(_impl=self.__impl__.dict_values())

    def dict_has_any_keys(self, keys):
        """
        Create a boolean XArray by checking the keys of an XArray of
        dictionaries. An element of the output XArray is True if the
        corresponding input element's dictionary has any of the given keys.
        Fails on XArrays whose data type is not ``dict``.

        Parameters
        ----------
        keys : list
            A list of key values to check each dictionary against.

        Returns
        -------
        out : XArray
            A XArray of int type, where each element indicates whether the
            input XArray element contains any key in the input list.

        See Also
        --------
        dict_has_all_keys

        Examples
        --------
        >>> sa = xpatterns.XArray([{"this":1, "is":5, "dog":7}, {"animal":1},
                                 {"this": 2, "are": 1, "cat": 5}])
        >>> sa.dict_has_any_keys(["is", "this", "are"])
        dtype: int
        Rows: 3
        [1, 1, 0]
        """
        if isinstance(keys, str) or (not hasattr(keys, "__iter__")):
            keys = [keys]

        return XArray(_impl=self.__impl__.dict_has_any_keys(keys))

    def dict_has_all_keys(self, keys):
        """
        Create a boolean XArray by checking the keys of an XArray of
        dictionaries. An element of the output XArray is True if the
        corresponding input element's dictionary has all of the given keys.
        Fails on XArrays whose data type is not ``dict``.

        Parameters
        ----------
        keys : list
            A list of key values to check each dictionary against.

        Returns
        -------
        out : XArray
            A XArray of int type, where each element indicates whether the
            input XArray element contains all keys in the input list.

        See Also
        --------
        dict_has_any_keys

        Examples
        --------
        >>> sa = xpatterns.XArray([{"this":1, "is":5, "dog":7},
                                 {"this": 2, "are": 1, "cat": 5}])
        >>> sa.dict_has_all_keys(["is", "this"])
        dtype: int
        Rows: 2
        [1, 0]
        """
        if isinstance(keys, str) or (not hasattr(keys, "__iter__")):
            keys = [keys]

        return XArray(_impl=self.__impl__.dict_has_all_keys(keys))

    def apply(self, fn, dtype=None, skip_undefined=True, seed=None):
        """
        apply(fn, dtype=None, skip_undefined=True, seed=None)

        Transform each element of the XArray by a given function. The result
        XArray is of type ``dtype``. ``fn`` should be a function that returns
        exactly one value which can be cast into the type specified by
        ``dtype``. If ``dtype`` is not specified, the first 100 elements of the
        XArray are used to make a guess about the data type.

        Parameters
        ----------
        fn : function
            The function to transform each element. Must return exactly one
            value which can be cast into the type specified by ``dtype``.

        dtype : {None, int, float, str, list, array.array, dict}, optional
            The data type of the new XArray. If ``None``, the first 100 elements
            of the array are used to guess the target data type.

        skip_undefined : bool, optional
            If True, will not apply ``fn`` to any undefined values.

        seed : int, optional
            Used as the seed if a random number generator is included in ``fn``.

        Returns
        -------
        out : XArray
            The XArray transformed by ``fn``. Each element of the XArray is of
            type ``dtype``.

        See Also
        --------
        XFrame.apply

        Examples
        --------
        >>> sa = xpatterns.XArray([1,2,3])
        >>> sa.apply(lambda x: x*2)
        dtype: int
        Rows: 3
        [2, 4, 6]
        """
        assert inspect.isfunction(fn), "Input must be a function"

        if dtype == None:
            h = self.__impl__.head_as_list(100)
            dryrun = [fn(i) for i in h if i is not None]
            dtype = infer_type_of_list(dryrun)
        if not seed:
            seed = time.time()

        return XArray(_impl=self.__impl__.transform(fn, dtype, skip_undefined, seed))


    def filter(self, fn, skip_undefined=True, seed=None):
        """
        Filter this XArray by a function.

        Returns a new XArray filtered by this XArray.  If `fn` evaluates an
        element to true, this element is copied to the new XArray. If not, it
        isn't. Throws an exception if the return type of `fn` is not castable
        to a boolean value.

        Parameters
        ----------
        fn : function
            Function that filters the XArray. Must evaluate to bool or int.

        skip_undefined : bool, optional
            If True, will not apply fn to any undefined values.

        seed : int, optional
            Used as the seed if a random number generator is included in fn.

        Returns
        -------
        out : XArray
            The XArray filtered by fn. Each element of the XArray is of
            type int.

        Examples
        --------
        >>> sa = xpatterns.XArray([1,2,3])
        >>> sa.filter(lambda x: x < 3)
        dtype: int
        Rows: 2
        [1, 2]
        """
        assert inspect.isfunction(fn), "Input must be a function"
        if not seed:
            seed = time.time()

        return XArray(_impl=self.__impl__.filter(fn, skip_undefined, seed))


    def sample(self, fraction, seed=None):
        """
        Create an XArray which contains a subsample of the current XArray.

        Parameters
        ----------
        fraction : float
            The fraction of the rows to fetch. Must be between 0 and 1.

        seed : int
            The random seed for the random number generator.

        Returns
        -------
        out : XArray
            The new XArray which contains the subsampled rows.

        Examples
        --------
        >>> sa = xpatterns.XArray(range(10))
        >>> sa.sample(.3)
        dtype: int
        Rows: 3
        [2, 6, 9]
        """
        if (fraction > 1 or fraction < 0):
            raise ValueError('Invalid sampling rate: ' + str(fraction))
        if (self.size() == 0):
            return XArray()
        if not seed:
            seed = int(time.time())

        return XArray(_impl=self.__impl__.sample(fraction, seed))

    def _save_as_text(self, url):
        """
        Save the XArray to disk as text file.
        """
        raise NotImplementedError


    def all(self):
        """
        Return True if every element of the XArray evaluates to True. For
        numeric XArrays zeros and missing values (``None``) evaluate to False,
        while all non-zero, non-missing values evaluate to True. For string,
        list, and dictionary XArrays, empty values (zero length strings, lists
        or dictionaries) or missing values (``None``) evaluate to False. All
        other values evaluate to True.

        Returns True on an empty XArray.

        Returns
        -------
        out : bool

        See Also
        --------
        any

        Examples
        --------
        >>> xpatterns.XArray([1, None]).all()
        False
        >>> xpatterns.XArray([1, 0]).all()
        False
        >>> xpatterns.XArray([1, 2]).all()
        True
        >>> xpatterns.XArray(["hello", "world"]).all()
        True
        >>> xpatterns.XArray(["hello", ""]).all()
        False
        >>> xpatterns.XArray([]).all()
        True
        """
        return self.__impl__.all()


    def any(self):
        """
        Return True if any element of the XArray evaluates to True. For numeric
        XArrays any non-zero value evaluates to True. For string, list, and
        dictionary XArrays, any element of non-zero length evaluates to True.

        Returns False on an empty XArray.

        Returns
        -------
        out : bool

        See Also
        --------
        all

        Examples
        --------
        >>> xpatterns.XArray([1, None]).any()
        True
        >>> xpatterns.XArray([1, 0]).any()
        True
        >>> xpatterns.XArray([0, 0]).any()
        False
        >>> xpatterns.XArray(["hello", "world"]).any()
        True
        >>> xpatterns.XArray(["hello", ""]).any()
        True
        >>> xpatterns.XArray(["", ""]).any()
        False
        >>> xpatterns.XArray([]).any()
        False
        """
        return self.__impl__.any()


    def max(self):
        """
        Get maximum numeric value in XArray.

        Returns None on an empty XArray. Raises an exception if called on an
        XArray with non-numeric type.

        Returns
        -------
        out : type of XArray
            Maximum value of XArray

        See Also
        --------
        min

        Examples
        --------
        >>> xpatterns.XArray([14, 62, 83, 72, 77, 96, 5, 25, 69, 66]).max()
        96
        """
        return self.__impl__.max()


    def min(self):
        """
        Get minimum numeric value in XArray.

        Returns None on an empty XArray. Raises an exception if called on an
        XArray with non-numeric type.

        Returns
        -------
        out : type of XArray
            Minimum value of XArray

        See Also
        --------
        max

        Examples
        --------
        >>> xpatterns.XArray([14, 62, 83, 72, 77, 96, 5, 25, 69, 66]).min()

        """
        return self.__impl__.min()


    def sum(self):
        """
        Sum of all values in this XArray.

        Raises an exception if called on an XArray of strings, lists, or
        dictionaries. If the XArray contains numeric arrays (array.array) and
        all the arrays are the same length, the sum over all the arrays will be
        returned. Returns None on an empty XArray. For large values, this may
        overflow without warning.

        Returns
        -------
        out : type of XArray
            Sum of all values in XArray
        """
        return self.__impl__.sum()

    def mean(self):
        """
        Mean of all the values in the XArray.

        Returns None on an empty XArray. Raises an exception if called on an
        XArray with non-numeric type.

        Returns
        -------
        out : float
            Mean of all values in XArray.
        """
        return self.__impl__.mean()


    def std(self, ddof=0):
        """
        Standard deviation of all the values in the XArray.

        Returns None on an empty XArray. Raises an exception if called on an
        XArray with non-numeric type or if `ddof` >= length of XArray.

        Parameters
        ----------
        ddof : int, optional
            "delta degrees of freedom" in the variance calculation.

        Returns
        -------
        out : float
            The standard deviation of all the values.
        """
        return self.__impl__.std(ddof)


    def var(self, ddof=0):
        """
        Variance of all the values in the XArray.

        Returns None on an empty XArray. Raises an exception if called on an
        XArray with non-numeric type or if `ddof` >= length of XArray.

        Parameters
        ----------
        ddof : int, optional
            "delta degrees of freedom" in the variance calculation.

        Returns
        -------
        out : float
            Variance of all values in XArray.
        """
        return self.__impl__.var(ddof)

    def num_missing(self):
        """
        Number of missing elements in the XArray.

        Returns
        -------
        out : int
            Number of missing values.
        """
        return self.__impl__.num_missing()

    def nnz(self):
        """
        Number of non-zero elements in the XArray.

        Returns
        -------
        out : int
            Number of non-zero elements.
        """
        return self.__impl__.nnz()

    def datetime_to_str(self,str_format="%Y-%m-%dT%H:%M:%S%ZP"):
        """
        Create a new XArray with all the values cast to str. The string format is
        specified by the 'str_format' parameter.

        Parameters
        ----------
        str_format : str
            The format to output the string. Default format is "%Y-%m-%dT%H:%M:%S%ZP".

        Returns
        -------
        out : XArray[str]
            The XArray converted to the type 'str'.

        Examples
        --------
        >>> dt = datetime.datetime(2011, 10, 20, 9, 30, 10, tzinfo=GMT(-5))
        >>> sa = xpatterns.XArray([dt])
        >>> sa.datetime_to_str("%e %b %Y %T %ZP")
        dtype: str
        Rows: 1
        [20 Oct 2011 09:30:10 GMT-05:00]

        See Also
        ----------
        str_to_datetime

        References
        ----------
        [1] Boost date time from string conversion guide (http://www.boost.org/doc/libs/1_48_0/doc/html/date_time/date_time_io.html)

        """
        if(self.dtype() != datetime.datetime):
            raise TypeError("datetime_to_str expects XArray of datetime as input XArray")

        return XArray(_impl=self.__impl__.datetime_to_str(str_format))

    def str_to_datetime(self,str_format="%Y-%m-%dT%H:%M:%S%ZP"):
        """
        Create a new XArray with all the values cast to datetime. The string format is
        specified by the 'str_format' parameter.

        Parameters
        ----------
        str_format : str
            The string format of the input XArray. Default format is "%Y-%m-%dT%H:%M:%S%ZP".

        Returns
        -------
        out : XArray[datetime.datetime]
            The XArray converted to the type 'datetime'.

        Examples
        --------
        >>> sa = xpatterns.XArray(["20-Oct-2011 09:30:10 GMT-05:30"])
        >>> sa.str_to_datetime("%d-%b-%Y %H:%M:%S %ZP")
        dtype: datetime
        Rows: 1
        datetime.datetime(2011, 10, 20, 9, 30, 10, tzinfo=GMT(-5.5))

        See Also
        ----------
        datetime_to_str

        References
        ----------
        [1] boost date time to string conversion guide (http://www.boost.org/doc/libs/1_48_0/doc/html/date_time/date_time_io.html)

        """
        if(self.dtype() != str):
            raise TypeError("str_to_datetime expects XArray of str as input XArray")

        return XArray(_impl=self.__impl__.str_to_datetime(str_format))

    def astype(self, dtype, undefined_on_failure=False):
        """
        Create a new XArray with all values cast to the given type. Throws an
        exception if the types are not castable to the given type.

        Parameters
        ----------
        dtype : {int, float, str, list, array.array, dict, datetime.datetime}
            The type to cast the elements to in XArray

        undefined_on_failure: bool, optional
            If set to True, runtime cast failures will be emitted as missing
            values rather than failing.

        Returns
        -------
        out : XArray [dtype]
            The XArray converted to the type ``dtype``.

        Notes
        -----
        - The string parsing techniques used to handle conversion to dictionary
          and list types are quite generic and permit a variety of interesting
          formats to be interpreted. For instance, a JSON string can usually be
          interpreted as a list or a dictionary type. See the examples below.
        - For datetime-to-string  and string-to-datetime conversions,
          use sa.datetime_to_str() and sa.str_to_datetime() functions.

        Examples
        --------
        >>> sa = xpatterns.XArray(['1','2','3','4'])
        >>> sa.astype(int)
        dtype: int
        Rows: 4
        [1, 2, 3, 4]

        Given an XArray of strings that look like dicts, convert to a dictionary
        type:

        >>> sa = xpatterns.XArray(['{1:2 3:4}', '{a:b c:d}'])
        >>> sa.astype(dict)
        dtype: dict
        Rows: 2
        [{1: 2, 3: 4}, {'a': 'b', 'c': 'd'}]
        """

        return XArray(_impl=self.__impl__.astype(dtype, undefined_on_failure))

    def clip(self, lower=float('nan'), upper=float('nan')):
        """
        Create a new XArray with each value clipped to be within the given
        bounds.

        In this case, "clipped" means that values below the lower bound will be
        set to the lower bound value. Values above the upper bound will be set
        to the upper bound value. This function can operate on XArrays of
        numeric type as well as array type, in which case each individual
        element in each array is clipped. By default ``lower`` and ``upper`` are
        set to ``float('nan')`` which indicates the respective bound should be
        ignored. The method fails if invoked on an XArray of non-numeric type.

        Parameters
        ----------
        lower : int, optional
            The lower bound used to clip. Ignored if equal to ``float('nan')``
            (the default).

        upper : int, optional
            The upper bound used to clip. Ignored if equal to ``float('nan')``
            (the default).

        Returns
        -------
        out : XArray

        See Also
        --------
        clip_lower, clip_upper

        Examples
        --------
        >>> sa = xpatterns.XArray([1,2,3])
        >>> sa.clip(2,2)
        dtype: int
        Rows: 3
        [2, 2, 2]
        """
        return XArray(_impl=self.__impl__.clip(lower, upper))

    def clip_lower(self, threshold):
        """
        Create new XArray with all values clipped to the given lower bound. This
        function can operate on numeric arrays, as well as vector arrays, in
        which case each individual element in each vector is clipped. Throws an
        exception if the XArray is empty or the types are non-numeric.

        Parameters
        ----------
        threshold : float
            The lower bound used to clip values.

        Returns
        -------
        out : XArray

        See Also
        --------
        clip, clip_upper

        Examples
        --------
        >>> sa = xpatterns.XArray([1,2,3])
        >>> sa.clip_lower(2)
        dtype: int
        Rows: 3
        [2, 2, 3]
        """
        return XArray(_impl=self.__impl__.clip(threshold, float('nan')))


    def clip_upper(self, threshold):
        """
        Create new XArray with all values clipped to the given upper bound. This
        function can operate on numeric arrays, as well as vector arrays, in
        which case each individual element in each vector is clipped.

        Parameters
        ----------
        threshold : float
            The upper bound used to clip values.

        Returns
        -------
        out : XArray

        See Also
        --------
        clip, clip_lower

        Examples
        --------
        >>> sa = xpatterns.XArray([1,2,3])
        >>> sa.clip_upper(2)
        dtype: int
        Rows: 3
        [1, 2, 2]
        """
        return XArray(_impl=self.__impl__.clip(float('nan'), threshold))

    def tail(self, n=10):
        """
        Get an XArray that contains the last n elements in the XArray.

        Parameters
        ----------
        n : int
            The number of elements to fetch

        Returns
        -------
        out : XArray
            A new XArray which contains the last n rows of the current XArray.
        """
        return XArray(_impl=self.__impl__.tail(n))


    def dropna(self):
        """
        Create new XArray containing only the non-missing values of the
        XArray.

        A missing value shows up in an XArray as 'None'.  This will also drop
        float('nan').

        Returns
        -------
        out : XArray
            The new XArray with missing values removed.
        """

        return XArray(_impl = self.__impl__.drop_missing_values())

    def fillna(self, value):
        """
        Create new XArray with all missing values (None or NaN) filled in
        with the given value.

        The size of the new XArray will be the same as the original XArray. If
        the given value is not the same type as the values in the XArray,
        `fillna` will attempt to convert the value to the original XArray's
        type. If this fails, an error will be raised.

        Parameters
        ----------
        value : type convertible to XArray's type
            The value used to replace all missing values

        Returns
        -------
        out : XArray
            A new XArray with all missing values filled
        """
        return XArray(_impl = self.__impl__.fill_missing_values(value))

    def topk_index(self, topk=10, reverse=False):
        """
        Create an XArray indicating which elements are in the top k.

        Entries are '1' if the corresponding element in the current XArray is a
        part of the top k elements, and '0' if that corresponding element is
        not. Order is descending by default.

        Parameters
        ----------
        topk : int
            The number of elements to determine if 'top'

        reverse: bool
            If True, return the topk elements in ascending order

        Returns
        -------
        out : XArray (of type int)

        Notes
        -----
        This is used internally by XFrame's topk function.
        """
        return XArray(_impl = self.__impl__.topk_index(topk, reverse))

    def sketch_summary(self, sub_sketch_keys=None):
        """
        Summary statistics that can be calculated with one pass over the XArray.

        Returns a xpatterns.Sketch object which can be further queried for many
        descriptive statistics over this XArray. Many of the statistics are
        approximate. See the :class:`~xpatterns.Sketch` documentation for more
        detail.

        Parameters
        ----------
        sub_sketch_keys: int | str | list of int | list of str, optional
            FOR30 XArray of dict type, also constructs sketches for a given set of keys,
            For XArray of array type, also constructs sketches for the given indexes.
            The sub sketches may be queried using:
                 :py:func:`~xpatterns.Sketch.element_sub_sketch()`
            Defaults to None in which case no subsketches will be constructed.

        Returns
        -------
        out : Sketch
            Sketch object that contains descriptive statistics for this XArray.
            Many of the statistics are approximate.
        """
        from xpatterns.sketch import Sketch
        if (sub_sketch_keys != None):
            if (self.dtype() != dict and self.dtype() != array.array):
                raise TypeError("sub_sketch_keys is only supported for XArray of dictionary or array type")
            if not hasattr(sub_sketch_keys, "__iter__"):
                sub_sketch_keys = [sub_sketch_keys]
            value_types = set([type(i) for i in sub_sketch_keys])
            if (len(value_types) != 1):
                raise ValueError("sub_sketch_keys member values need to have the same type.")
            value_type = value_types.pop();
            if (self.dtype() == dict and value_type != str):
                raise TypeError("Only string value(s) can be passed to sub_sketch_keys for XArray of dictionary type. "+
                    "For dictionary types, sketch summary is computed by casting keys to string values.")
            if (self.dtype() == array.array and value_type != int):
                raise TypeError("Only int value(s) can be passed to sub_sketch_keys for XArray of array type")

        return Sketch(self, sub_sketch_keys=sub_sketch_keys)

    def append(self, other):
        """
        Append an XArray to the current XArray. Creates a new XArray with the
        rows from both XArrays. Both XArrays must be of the same type.

        Parameters
        ----------
        other : XArray
            Another XArray whose rows are appended to current XArray.

        Returns
        -------
        out : XArray
            A new XArray that contains rows from both XArrays, with rows from
            the ``other`` XArray coming after all rows from the current XArray.

        See Also
        --------
        XFrame.append

        Examples
        --------
        >>> sa = xpatterns.XArray([1, 2, 3])
        >>> sa2 = xpatterns.XArray([4, 5, 6])
        >>> sa.append(sa2)
        dtype: int
        Rows: 6
        [1, 2, 3, 4, 5, 6]
        """
        if type(other) is not XArray:
            raise RuntimeError("XArray append can only work with XArray")

        if self.dtype() != other.dtype():
            raise RuntimeError("Data types in both XArrays have to be the same")

        return XArray(_impl = self.__impl__.append(other.__impl__))

    def unique(self):
        """
        Get all unique values in the current XArray.

        Raises a TypeError if the XArray is of dictionary type. Will not
        necessarily preserve the order of the given XArray in the new XArray.


        Returns
        -------
        out : XArray
            A new XArray that contains the unique values of the current XArray.

        See Also
        --------
        XFrame.unique
        """
        return XArray(_impl = self.__impl__.unique())

    def item_length(self):
        """
        Length of each element in the current XArray.

        Only works on XArrays of dict, array, or list type. If a given element
        is a missing value, then the output elements is also a missing value.
        This function is equivalent to the following but more performant:

            sa_item_len =  sa.apply(lambda x: len(x) if x is not None else None)

        Returns
        -------
        out_xf : XArray
            A new XArray, each element in the XArray is the len of the corresponding
            items in original XArray.

        Examples
        --------
        >>> sa = XArray([
        ...  {"is_restaurant": 1, "is_electronics": 0},
        ...  {"is_restaurant": 1, "is_retail": 1, "is_electronics": 0},
        ...  {"is_restaurant": 0, "is_retail": 1, "is_electronics": 0},
        ...  {"is_restaurant": 0},
        ...  {"is_restaurant": 1, "is_electronics": 1},
        ...  None])
        >>> sa.item_length()
        dtype: int
        Rows: 6
        [2, 3, 3, 1, 2, None]
        """
        if (self.dtype() not in [list, dict, array.array]):
            raise TypeError("item_length() is only applicable for XArray of type list, dict and array.")

        return XArray(_impl = self.__impl__.item_length())

    def split_datetime(self, column_name_prefix = "X", limit=None, tzone=False):
        """
        Splits an XArray of datetime type to multiple columns, return a
        new XFrame that contains expanded columns. A XArray of datetime will be
        split by default into an XFrame of 6 columns, one for each
        year/month/day/hour/minute/second element.

        column naming:
        When splitting a XArray of datetime type, new columns are named:
        prefix.year, prefix.month, etc. The prefix is set by the parameter
        "column_name_prefix" and defaults to 'X'. If column_name_prefix is
        None or empty, then no prefix is used.

        Timezone column:
        If tzone parameter is True, then timezone information is represented
        as one additional column which is a float shows the offset from
        GMT(0.0) or from UTC.

        Parameters
        ----------
        column_name_prefix: str, optional
            If provided, expanded column names would start with the given prefix.
            Defaults to "X".

        limit: list[str], optional
            Limits the set of datetime elements to expand.
            Elements are 'year','month','day','hour','minute',
            and 'second'.

        tzone: bool, optional
            A boolean parameter that determines whether to show timezone column or not.
            Defaults to False.

        Returns
        -------
        out : XFrame
            A new XFrame that contains all expanded columns

        Examples
        --------
        To expand only day and year elements of a datetime XArray

         >>> sa = XArray(
            [datetime(2011, 1, 21, 7, 7, 21, tzinfo=GMT(0)),
             datetime(2010, 2, 5, 7, 8, 21, tzinfo=GMT(4.5)])

         >>> sa.expand(column_name_prefix=None,limit=['day','year'])
            Columns:
                day   int
                year  int
            Rows: 2
            Data:
            +-------+--------+
            |  day  |  year  |
            +-------+--------+
            |   21  |  2011  |
            |   5   |  2010  |
            +-------+--------+
            [2 rows x 2 columns]


        To expand only year and tzone elements of a datetime XArray
        with tzone column represented as a string. Columns are named with prefix:
        'Y.column_name'.

        >>> sa.split_datetime(column_name_prefix="Y",limit=['year'],tzone=True)
            Columns:
                Y.year  int
                Y.tzone float
            Rows: 2
            Data:
            +----------+---------+
            |  Y.year  | Y.tzone |
            +----------+---------+
            |    2011  |  0.0    |
            |    2010  |  4.5    |
            +----------+---------+
            [2 rows x 2 columns]

        """
        if self.dtype() != datetime.datetime:
            raise TypeError("Only column of datetime type is supported.")

        if column_name_prefix == None:
            column_name_prefix = ""
        if type(column_name_prefix) != str:
            raise TypeError("'column_name_prefix' must be a string")

        # convert limit to column_keys
        if limit != None:
            if (not hasattr(limit, '__iter__')):
                raise TypeError("'limit' must be a list");

            name_types = set([type(i) for i in limit])
            if (len(name_types) != 1):
                raise TypeError("'limit' contains values that are different types")

            if (name_types.pop() != str):
                raise TypeError("'limit' must contain string values.")

            if len(set(limit)) != len(limit):
                raise ValueError("'limit' contains duplicate values")

        column_types = []

        if(limit != None):
            column_types = list()
            for i in limit:
                column_types.append(int);
        else:
            limit = ['year','month','day','hour','minute','second']
            column_types = [int, int, int, int, int, int]

        if(tzone == True):
            limit += ['tzone']
            column_types += [float]

        return xp.XFrame(_impl=self.__impl__.expand(column_name_prefix, limit, column_types))

    def unpack(self, column_name_prefix = "X", column_types=None, na_value=None, limit=None):
        """
        Convert an XArray of list, array, or dict type to an XFrame with
        multiple columns.

        `unpack` expands an XArray using the values of each list/array/dict as
        elements in a new XFrame of multiple columns. For example, an XArray of
        lists each of length 4 will be expanded into an XFrame of 4 columns,
        one for each list element. An XArray of lists/arrays of varying size
        will be expand to a number of columns equal to the longest list/array.
        An XArray of dictionaries will be expanded into as many columns as
        there are keys.

        When unpacking an XArray of list or array type, new columns are named:
        `column_name_prefix`.0, `column_name_prefix`.1, etc. If unpacking a
        column of dict type, unpacked columns are named
        `column_name_prefix`.key1, `column_name_prefix`.key2, etc.

        When unpacking an XArray of list or dictionary types, missing values in
        the original element remain as missing values in the resultant columns.
        If the `na_value` parameter is specified, all values equal to this
        given value are also replaced with missing values. In an XArray of
        array.array type, NaN is interpreted as a missing value.

        :py:func:`xpatterns.XFrame.pack_columns()` is the reverse effect of unpack

        Parameters
        ----------
        column_name_prefix: str, optional
            If provided, unpacked column names would start with the given prefix.

        column_types: list[type], optional
            Column types for the unpacked columns. If not provided, column
            types are automatically inferred from first 100 rows. Defaults to
            None.

        na_value: optional
            Convert all values that are equal to `na_value` to
            missing value if specified.

        limit: list, optional
            Limits the set of list/array/dict keys to unpack.
            For list/array XArrays, 'limit' must contain integer indices.
            For dict XArray, 'limit' must contain dictionary keys.

        Returns
        -------
        out : XFrame
            A new XFrame that contains all unpacked columns

        Examples
        --------
        To unpack a dict XArray

        >>> sa = XArray([{ 'word': 'a',     'count': 1},
        ...              { 'word': 'cat',   'count': 2},
        ...              { 'word': 'is',    'count': 3},
        ...              { 'word': 'coming','count': 4}])

        Normal case of unpacking XArray of type dict:

        >>> sa.unpack(column_name_prefix=None)
        Columns:
            count   int
            word    str
        <BLANKLINE>
        Rows: 4
        <BLANKLINE>
        Data:
        +-------+--------+
        | count |  word  |
        +-------+--------+
        |   1   |   a    |
        |   2   |  cat   |
        |   3   |   is   |
        |   4   | coming |
        +-------+--------+
        [4 rows x 2 columns]
        <BLANKLINE>

        Unpack only keys with 'word':

        >>> sa.unpack(limit=['word'])
        Columns:
            X.word  str
        <BLANKLINE>
        Rows: 4
        <BLANKLINE>
        Data:
        +--------+
        | X.word |
        +--------+
        |   a    |
        |  cat   |
        |   is   |
        | coming |
        +--------+
        [4 rows x 1 columns]
        <BLANKLINE>

        >>> sa2 = XArray([
        ...               [1, 0, 1],
        ...               [1, 1, 1],
        ...               [0, 1]])

        Convert all zeros to missing values:

        >>> sa2.unpack(column_types=[int, int, int], na_value=0)
        Columns:
            X.0     int
            X.1     int
            X.2     int
        <BLANKLINE>
        Rows: 3
        <BLANKLINE>
        Data:
        +------+------+------+
        | X.0  | X.1  | X.2  |
        +------+------+------+
        |  1   | None |  1   |
        |  1   |  1   |  1   |
        | None |  1   | None |
        +------+------+------+
        [3 rows x 3 columns]
        <BLANKLINE>
        """
        def is_missing(val):
            if val is None: return True
            if type(val) == float and math.isnan(val): return True
            return False
        def make_column_types(head_rows, keys):
            col_types = {}
            for row in head_rows:
                for key in row.keys():
                    val = row[key]
                    if not key in col_types and not is_missing(val):
                        col_types[key] = type(val)
                        
            column_types = []
            for key in keys:
                column_types.append(col_types[key])
            return column_types

        if self.dtype() not in [dict, array.array, list]:
            raise TypeError("Only XArray of dict/list/array type supports unpack")

        if column_name_prefix == None:
            column_name_prefix = ""
        if type(column_name_prefix) != str:
            raise TypeError("'column_name_prefix' must be a string")

        # validdate 'limit'
        if limit != None:
            if (not hasattr(limit, '__iter__')):
                raise TypeError("'limit' must be a list");

            name_types = set([type(i) for i in limit])
            if (len(name_types) != 1):
                raise TypeError("'limit' contains values that are different types")

            # limit value should be numeric if unpacking xarray.array value
            if (self.dtype() != dict) and (name_types.pop() != int):
                raise TypeError("'limit' must contain integer values.")

            if len(set(limit)) != len(limit):
                raise ValueError("'limit' contains duplicate values")

        if (column_types != None):
            if not hasattr(column_types, '__iter__'):
                raise TypeError("column_types must be a list");

            for column_type in column_types:
                if (column_type not in (int, float, str, list, dict, array.array)):
                    raise TypeError("column_types contains unsupported types. Supported types are ['float', 'int', 'list', 'dict', 'str', 'array.array']")

            if limit != None:
                if len(limit) != len(column_types):
                    raise ValueError("limit and column_types do not have the same length")
            elif self.dtype() == dict:
                raise ValueError("if 'column_types' is given, 'limit' has to be provided to unpack dict type.")
            else:
                limit = range(len(column_types))

        else:
            head_rows = self.head(100).dropna()
            lengths = [len(i) for i in head_rows]
            if len(lengths) == 0 or max(lengths) == 0:
                raise RuntimeError("Cannot infer number of items from the XArray, XArray may be empty. please explicitly provide column types")

            # infer column types for dict type at server side, for list and array, infer from client side
            if self.dtype() != dict:
                length = max(lengths)
                if limit == None:
                    limit = range(length)
                else:
                    # adjust the length
                    length = len(limit)

                if self.dtype() == array.array:
                    column_types = [float for i in range(length)]
                else:
                    column_types = list()
                    for i in limit:
                        t = [(x[i] if ((x is not None) and len(x) > i) else None) for x in head_rows]
                        column_types.append(infer_type_of_list(t))

            else:                      # self.dtype() == dict
                if limit is None:  
                    keys = set()
                    for row in head_rows:
                        keys |= set(row.keys())
                    # translate to indexes
                    limit = list(keys)
                if column_types is None:
                    column_types = make_column_types(head_rows, limit)

        return xp.XFrame(_impl=self.__impl__.unpack(column_name_prefix, limit, column_types, na_value))

    def sort(self, ascending=True):
        """
        Sort all values in this XArray.

        Sort only works for xarray of type str, int and float, otherwise TypeError
        will be raised. Creates a new, sorted XArray.

        Parameters
        ----------
        ascending: boolean, optional
           If true, the xarray values are sorted in ascending order, otherwise,
           descending order.

        Returns
        -------
        out: XArray

        Examples
        --------
        >>> sa = XArray([3,2,1])
        >>> sa.sort()
        dtype: int
        Rows: 3
        [1, 2, 3]

        """
        if self.dtype() not in (int, float, str, datetime.datetime):
            raise TypeError("Only xarray with type (int, float, str, datetime.datetime) can be sorted")
        return XArray(_impl = self.__impl__.sort(ascending))
