"""
This module defines the Sketch class which provides
summary information about columns.
"""

"""
Copyright (c) 2015, Dato, Inc.
All rights reserved.

Copyright (c) 2015, Atigeo, Inc.
All rights reserved.
"""

import operator
from math import sqrt

from xframes.xarray import XArray
from xframes.xframe import XFrame
from xframes.sketch_impl import SketchImpl

__all__ = ['Sketch']


class Sketch(object):
    """
    The Sketch object contains a sketch of a single XArray (a column of an
    SFrame). Using a sketch representation of an XArray, many approximate and
    exact statistics can be computed very quickly.

    To construct a Sketch object, the following methods are equivalent:

    >>> my_xarray = xframes.XArray([1,2,3,4,5])
    >>> sketch = xframes.Sketch(my_xarray)
    >>> sketch = my_xarray.sketch_summary()

    Typically, the XArray is a column of an XFrame:

    >>> my_sframe =  xframes.XFrame({'column1': [1,2,3]})
    >>> sketch = xframes.Sketch(my_sframe['column1'])
    >>> sketch = my_sframe['column1'].sketch_summary()

    The sketch computation is fast, with complexity approximately linear in the
    length of the XArray. After the Sketch is computed, all queryable functions
    are performed nearly instantly.

    A sketch can compute the following information depending on the dtype of the
    XArray:

    For numeric columns, the following information is provided exactly:
     - length (:func:`~xframes.Sketch.size`)
     - number of missing Values (:func:`~xframes.Sketch.num_undefined`)
     - minimum  value (:func:`~xframes.Sketch.min`)
     - maximum value (:func:`~xframes.Sketch.max`)
     - mean (:func:`~xframes.Sketch.mean`)
     - variance (:func:`~xframes.Sketch.var`)
     - standard deviation (:func:`~xframes.Sketch.std`)

    And the following information is provided approximately:
     - number of unique values (:func:`~xframes.Sketch.num_unique`)
     - quantiles (:func:`~xframes.Sketch.quantile`)
     - frequent items (:func:`~xframes.Sketch.frequent_items`)
     - frequency count for any value (:func:`~xframes.Sketch.frequency_count`)

    For non-numeric columns(str), the following information is provided exactly:
     - length (:func:`~xframes.Sketch.size`)
     - number of missing values (:func:`~xframes.Sketch.num_undefined`)

    And the following information is provided approximately:
     - number of unique Values (:func:`~xframes.Sketch.num_unique`)
     - frequent items (:func:`~xframes.Sketch.frequent_items`)
     - frequency count of any value (:func:`~xframes.Sketch.frequency_count`)

    For XArray of type list or array, there is a sub sketch for all sub elements.
    The sub sketch flattens all list/array values and then computes sketch summary over flattened values.

    Element sub sketch may be retrieved through:
     - element_summary(:func:`~xframes.Sketch.element_summary`)

    For XArray of type dict, there are sub sketches for both dict key and value.

    The sub sketch may be retrieved through:
     - dict_key_summary(:func:`~xframes.Sketch.dict_key_summary`)
     - dict_value_summary(:func:`~xframes.Sketch.dict_value_summary`)

    For XArray of type dict, user can also pass in a list of dictionary keys to
    sketch_summary function, this would generate one sub sketch for each key.
    For example:

         >>> sa = xframes.XArray([{'a':1, 'b':2}, {'a':3}])
         >>> sketch = sa.sketch_summary(sub_sketch_keys=["a", "b"])

    Then the sub summary may be retrieved by:

         >>> sketch.element_sub_sketch()

    or to get subset keys:

         >>> sketch.element_sub_sketch(["a"])

    Similarly, for XArray of type vector(array), user can also pass in a list of
    integers which is the index into the vector to get sub sketch
    For example:

         >>> sa = xframes.XArray([[100,200,300,400,500], [100,200,300], [400,500]])
         >>> sketch = sa.sketch_summary(sub_sketch_keys=[1,3,5])

    Then the sub summary may be retrieved by:

         >>> sketch.element_sub_sketch()

    Or:

         >>> sketch.element_sub_sketch([1,3])

    for subset of keys.

    Please see the individual function documentation for detail about each of
    these statistics.

    Parameters
    ----------
    array : XArray
        Array to generate sketch summary.

    References
    ----------
    - Wikipedia. `Streaming algorithms. <http://en.wikipedia.org/wiki/Streaming_algorithm>`_
    - Charikar, et al. (2002) `Finding frequent items in data streams.
      <https://www.cs.rutgers.edu/~farach/pubs/FrequentStream.pdf>`_
    - Cormode, G. and Muthukrishnan, S. (2004) `An Improved Data Stream Summary:
      The Count-Min Sketch and its Applications.
      <http://dimacs.rutgers.edu/~graham/pubs/papers/cm-latin.pdf>`_
    """

    def __init__(self, array=None, sub_sketch_keys=[], impl=None):
        """__init__(array)
        Construct a new Sketch from an XArray.

        Parameters
        ----------
        array : XArray
            Array to sketch.

        sub_sketch_keys : list
            The list of sub sketch to calculate, for XArray of dictionary type.
            key needs to be a string, for XArray of vector(array) type, the key
            needs to be positive integer
        """
        if impl:
            self.__impl__ = impl
        else:
            self.__impl__ = SketchImpl()
            if not isinstance(array, XArray):
                raise TypeError("Sketch object can only be constructed from XArrays")

            self.__impl__.construct_from_xarray(array.__impl__, sub_sketch_keys)

    def __repr__(self):
        """
        Emits a brief summary of all the statistics as a string.
        """
        return "<sketch>"     # TODO remove

        fields = [
            ['size',           'Length' ,       'Yes'],
            ['min',            'Min' ,          'Yes'],
            ['max',            'Max' ,          'Yes'],
            ['mean',           'Mean' ,         'Yes'],
            ['sum',            'Sum' ,          'Yes'],
            ['var',            'Variance' ,     'Yes'],
            ['std',            'Standard Deviation' , 'Yes'],
            ['num_undefined', '# Missing Values' , 'Yes',],
            ['num_unique',     '# unique values',  'No' ]
        ]

        s = '\n'
        result = []
        for field in fields:
            try:
                method_to_call = getattr(self, field[0])
                result.append([field[1], str(method_to_call()), field[2]])
            except:
                pass
        sf = XArray(result).unpack(column_name_prefix = "")
        sf.rename({'0': 'item', '1':'value', '2': 'is exact'})
        s += sf.__str__(footer=False)
        s += "\n"

        s += "\nMost frequent items:\n"
        frequent = self.frequent_items()
        sorted_freq = sorted(frequent.iteritems(), key=operator.itemgetter(1), reverse=True)
        if len(sorted_freq) == 0:
            s += " -- All elements appear with less than 0.01% frequency -- \n"
        else:
            sorted_freq = sorted_freq[:10]
            sf = XFrame()
            sf.add_column(XArray(['count']), 'value')
            for elem in sorted_freq:
                sf.add_column(XArray([elem[1]]), str(elem[0]))
            s += sf.__str__(footer=False) + "\n"
        s += "\n"

        try:
            # print quantiles
            t = self.quantile(0)
            s += "Quantiles: \n"
            sf = XFrame()
            for q in [0.0,0.01,0.05,0.25,0.5,0.75,0.95,0.99,1.00]:
                sf.add_column(XArray([self.quantile(q)]), str(int(q * 100)) + '%')
            s += sf.__str__(footer=False) + "\n"
        except:
            pass

        try:
            t_k = self.dict_key_summary()
            t_v = self.dict_value_summary()
            s += "\n******** Dictionary Element Key Summary ********\n"
            s += t_k.__repr__()
            s += "\n******** Dictionary Element Value Summary ********\n"
            s += t_v.__repr__() + '\n'
        except:
            pass

        try:
            t_k = self.element_summary()
            s += "\n******** Element Summary ********\n"
            s += t_k.__repr__() + '\n'
        except:
            pass

        return s.expandtabs(8)

    def __str__(self):
        """
        Emits a brief summary of all the statistics as a string.
        """
        return self.__repr__()

    def size(self):
        """
        Returns the size of the input XArray.

        Returns
        -------
        out : int
            The number of elements of the input XArray.
        """
        return int(self.__impl__.size())

    def max(self):
        """
        Returns the maximum value in the XArray. Returns *nan* on an empty
        array. Throws an exception if called on an XArray with non-numeric type.

        Raises
        ------
        RuntimeError
            Throws an exception if the XArray is a non-numeric type.

        Returns
        -------
        out : type of XArray
            Maximum value of XArray. Returns *nan* if the XArray is empty.
        """
        return self.__impl__.max()

    def min(self):
        """
        Returns the minimum value in the XArray. Returns *nan* on an empty
        array. Throws an exception if called on an XArray with non-numeric type.

        Raises
        ------
        RuntimeError
            If the xarray is a non-numeric type.

        Returns
        -------
        out : type of XArray
            Minimum value of XArray. Returns *nan* if the xarray is empty.
        """
        return self.__impl__.min()

    def sum(self):
        """
        Returns the sum of all the values in the XArray.  Returns 0 on an empty
        array. Throws an exception if called on an xarray with non-numeric type.
        Will overflow without warning.

        Raises
        ------
        RuntimeError
            If the xarray is a non-numeric type.

        Returns
        -------
        out : type of XArray
            Sum of all values in XArray. Returns 0 if the XArray is empty.
        """
        return self.__impl__.sum()

    def mean(self):
        """
        Returns the mean of the values in the XArray. Returns 0 on an empty
        array. Throws an exception if called on an XArray with non-numeric type.

        Raises
        ------
        RuntimeError
            If the xarray is a non-numeric type.

        Returns
        -------
        out : float
            Mean of all values in XArray. Returns 0 if the xarray is empty.
        """
        return self.__impl__.mean()

    def std(self):
        """
        Returns the standard deviation of the values in the XArray. Returns 0 on
        an empty array. Throws an exception if called on an XArray with
        non-numeric type.

        Returns
        -------
        out : float
            The standard deviation of all the values. Returns 0 if the xarray is
            empty.

        Raises
        ------
        RuntimeError
            If the xarray is a non-numeric type.
        """
        return sqrt(self.var())

    def var(self):
        """
        Returns the variance of the values in the xarray. Returns 0 on an empty
        array. Throws an exception if called on an XArray with non-numeric type.

        Raises
        ------
        RuntimeError
            If the xarray is a non-numeric type.

        Returns
        -------
        out : float
            The variance of all the values. Returns 0 if the XArray is empty.
        """
        return self.__impl__.var()

    def num_undefined(self):
        """
        Returns the the number of undefined elements in the XArray. Return 0
        on an empty XArray.

        Returns
        -------
        out : int
            The number of missing values in the XArray.
        """
        return int(self.__impl__.num_undefined())

    def num_unique(self):
        """
        Returns a sketched estimate of the number of unique values in the
        XArray based on the Hyperloglog sketch.

        Returns
        -------
        out : float
            An estimate of the number of unique values in the XArray.
        """
        return int(self.__impl__.num_unique())

    def frequent_items(self):
        """
        Returns a sketched estimate of the most frequent elements in the XArray
        based on the SpaceSaving sketch. It is only guaranteed that all
        elements which appear in more than 0.01% rows of the array will
        appear in the set of returned elements. However, other elements may
        also appear in the result. The item counts are estimated using
        the CountSketch.

        Missing values are not taken into account when copmuting frequent items.

        If this function returns no elements, it means that all elements appear
        with less than 0.01% occurrence.

        Returns
        -------
        out : dict
            A dictionary mapping items and their estimated occurrence frequencies.
        """
        return self.__impl__.frequent_items()

    def quantile(self, quantile_val):
        """
        Returns a sketched estimate of the value at a particular quantile
        between 0.0 and 1.0. The quantile is guaranteed to be accurate within
        1%: meaning that if you ask for the 0.55 quantile, the returned value is
        guaranteed to be between the true 0.54 quantile and the true 0.56
        quantile. The quantiles are only defined for numeric arrays and this
        function will throw an exception if called on a sketch constructed for a
        non-numeric column.

        Parameters
        ----------
        quantile_val : float
            A value between 0.0 and 1.0 inclusive. Values below 0.0 will be
            interpreted as 0.0. Values above 1.0 will be interpreted as 1.0.

        Raises
        ------
        RuntimeError
            If the xarray is a non-numeric type.

        Returns
        -------
        out : float | str
            An estimate of the value at a quantile.
        """
        return self.__impl__.get_quantile(quantile_val)

    def frequency_count(self, element):
        """
        Returns a sketched estimate of the number of occurrences of a given
        element. This estimate is based on the count sketch. The element type
        must be of the same type as the input XArray. Throws an exception if
        element is of the incorrect type.

        Parameters
        ----------
        element : val
            An element of the same type as the XArray.

        Raises
        ------
        RuntimeError
            Throws an exception if element is of the incorrect type.

        Returns
        -------
        out : int
            An estimate of the number of occurrences of the element.
        """
        return int(self.__impl__.frequency_count(element))

    def element_length_summary(self):
        """
        Returns the sketch summary for the element length. This is only valid for
        a sketch constructed XArray of type list/array/dict, raises Runtime
        exception otherwise.

        Examples
        --------
        >>> sa = xframes.XArray([[j for j in range(i)] for i in range(1,1000)])
        >>> sa.sketch_summary().element_length_summary()
        +--------------------+---------------+----------+
        |        item        |     value     | is exact |
        +--------------------+---------------+----------+
        |       Length       |      999      |   Yes    |
        |        Min         |      1.0      |   Yes    |
        |        Max         |     999.0     |   Yes    |
        |        Mean        |     500.0     |   Yes    |
        |        Sum         |    499500.0   |   Yes    |
        |      Variance      | 83166.6666667 |   Yes    |
        | Standard Deviation | 288.386314978 |   Yes    |
        |  # Missing Values  |       0       |   Yes    |
        |  # unique values   |      992      |    No    |
        +--------------------+---------------+----------+
        Most frequent items:
        +-------+---+---+---+---+---+---+---+---+---+----+
        | value | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
        +-------+---+---+---+---+---+---+---+---+---+----+
        | count | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1  |
        +-------+---+---+---+---+---+---+---+---+---+----+
        Quantiles:
        +-----+------+------+-------+-------+-------+-------+-------+-------+
        |  0% |  1%  |  5%  |  25%  |  50%  |  75%  |  95%  |  99%  |  100% |
        +-----+------+------+-------+-------+-------+-------+-------+-------+
        | 1.0 | 10.0 | 50.0 | 250.0 | 500.0 | 750.0 | 950.0 | 990.0 | 999.0 |
        +-----+------+------+-------+-------+-------+-------+-------+-------+

        Returns
        -------
        out : Sketch
          An new sketch object regarding the element length of the current XArray
        """
        return Sketch(impl = self.__impl__.element_length_summary())

    def dict_key_summary(self):
        """
        Returns the sketch summary for all dictionary keys. This is only valid
        for sketch object from an XArray of dict type. Dictionary keys are
        converted to strings and then do the sketch summary.

        Examples
        --------
        >>> sa = xframes.XArray([{'I':1, 'love': 2}, {'nature':3, 'beauty':4}])
        >>> sa.sketch_summary().dict_key_summary()
        +------------------+-------+----------+
        |       item       | value | is exact |
        +------------------+-------+----------+
        |      Length      |   4   |   Yes    |
        | # Missing Values |   0   |   Yes    |
        | # unique values  |   4   |    No    |
        +------------------+-------+----------+
        Most frequent items:
        +-------+---+------+--------+--------+
        | value | I | love | beauty | nature |
        +-------+---+------+--------+--------+
        | count | 1 |  1   |   1    |   1    |
        +-------+---+------+--------+--------+

        """
        return Sketch(impl = self.__impl__.dict_key_summary())

    def dict_value_summary(self):
        """
        Returns the sketch summary for all dictionary values. This is only valid
        for sketch object from an XArray of dict type.

        Type of value summary is inferred from first set of values.

        Examples
        --------

        >>> sa = xframes.XArray([{'I':1, 'love': 2}, {'nature':3, 'beauty':4}])
        >>> sa.sketch_summary().dict_value_summary()
        +--------------------+---------------+----------+
        |        item        |     value     | is exact |
        +--------------------+---------------+----------+
        |       Length       |       4       |   Yes    |
        |        Min         |      1.0      |   Yes    |
        |        Max         |      4.0      |   Yes    |
        |        Mean        |      2.5      |   Yes    |
        |        Sum         |      10.0     |   Yes    |
        |      Variance      |      1.25     |   Yes    |
        | Standard Deviation | 1.11803398875 |   Yes    |
        |  # Missing Values  |       0       |   Yes    |
        |  # unique values   |       4       |    No    |
        +--------------------+---------------+----------+
        Most frequent items:
        +-------+-----+-----+-----+-----+
        | value | 1.0 | 2.0 | 3.0 | 4.0 |
        +-------+-----+-----+-----+-----+
        | count |  1  |  1  |  1  |  1  |
        +-------+-----+-----+-----+-----+
        Quantiles:
        +-----+-----+-----+-----+-----+-----+-----+-----+------+
        |  0% |  1% |  5% | 25% | 50% | 75% | 95% | 99% | 100% |
        +-----+-----+-----+-----+-----+-----+-----+-----+------+
        | 1.0 | 1.0 | 1.0 | 2.0 | 3.0 | 4.0 | 4.0 | 4.0 | 4.0  |
        +-----+-----+-----+-----+-----+-----+-----+-----+------+

        """
        return Sketch(impl = self.__impl__.dict_value_summary())

    def element_summary(self):
        """
        Returns the sketch summary for all element values. This is only valid for
        sketch object created from XArray of list or vector(array) type.
        For XArray of list type, all list values are treated as string for
        sketch summary.
        For XArray of vector type, the sketch summary is on FLOAT type.

        Examples
        --------
        >>> sa = xframes.XArray([[1,2,3], [4,5]])
        >>> sa.sketch_summary().element_summary()
        +--------------------+---------------+----------+
        |        item        |     value     | is exact |
        +--------------------+---------------+----------+
        |       Length       |       5       |   Yes    |
        |        Min         |      1.0      |   Yes    |
        |        Max         |      5.0      |   Yes    |
        |        Mean        |      3.0      |   Yes    |
        |        Sum         |      15.0     |   Yes    |
        |      Variance      |      2.0      |   Yes    |
        | Standard Deviation | 1.41421356237 |   Yes    |
        |  # Missing Values  |       0       |   Yes    |
        |  # unique values   |       5       |    No    |
        +--------------------+---------------+----------+
        Most frequent items:
        +-------+-----+-----+-----+-----+-----+
        | value | 1.0 | 2.0 | 3.0 | 4.0 | 5.0 |
        +-------+-----+-----+-----+-----+-----+
        | count |  1  |  1  |  1  |  1  |  1  |
        +-------+-----+-----+-----+-----+-----+
        Quantiles:
        +-----+-----+-----+-----+-----+-----+-----+-----+------+
        |  0% |  1% |  5% | 25% | 50% | 75% | 95% | 99% | 100% |
        +-----+-----+-----+-----+-----+-----+-----+-----+------+
        | 1.0 | 1.0 | 1.0 | 2.0 | 3.0 | 4.0 | 5.0 | 5.0 | 5.0  |
        +-----+-----+-----+-----+-----+-----+-----+-----+------+
        """
        return Sketch(impl = self.__impl__.element_summary())

    def element_sub_sketch(self, keys = None):
        """
        Returns the sketch summary for the given set of keys. This is only
        applicable for sketch summary created from XArray of xarray or dict type.
        For dict XArray, the keys are the keys in dict value.
        For array Xarray, the keys are indexes into the array value.

        The keys must be passed into original sketch_summary() call in order to
        be able to be retrieved later

        Parameters
        -----------
        keys : list of str | str | list of int | int
            The list of dictionary keys or array index to get sub sketch from.
            if not given, then retrieve all sub sketches that are available

        Returns
        -------
        A dictionary that maps from the key(index) to the actual sketch summary
        for that key(index)

        Examples
        --------
        >>> sa = xframes.XArray([{'a':1, 'b':2}, {'a':4, 'd':1}])
        >>> s = sa.sketch_summary(sub_sketch_keys=['a','b'])
        >>> s.element_sub_sketch(['a'])
        {'a':
         +--------------------+-------+----------+
         |        item        | value | is exact |
         +--------------------+-------+----------+
         |       Length       |   2   |   Yes    |
         |        Min         |  1.0  |   Yes    |
         |        Max         |  4.0  |   Yes    |
         |        Mean        |  2.5  |   Yes    |
         |        Sum         |  5.0  |   Yes    |
         |      Variance      |  2.25 |   Yes    |
         | Standard Deviation |  1.5  |   Yes    |
         |  # Missing Values  |   0   |   Yes    |
         |  # unique values   |   2   |    No    |
         +--------------------+-------+----------+
         Most frequent items:
         +-------+-----+-----+
         | value | 1.0 | 4.0 |
         +-------+-----+-----+
         | count |  1  |  1  |
         +-------+-----+-----+
         Quantiles:
         +-----+-----+-----+-----+-----+-----+-----+-----+------+
         |  0% |  1% |  5% | 25% | 50% | 75% | 95% | 99% | 100% |
         +-----+-----+-----+-----+-----+-----+-----+-----+------+
         | 1.0 | 1.0 | 1.0 | 1.0 | 4.0 | 4.0 | 4.0 | 4.0 | 4.0  |
         +-----+-----+-----+-----+-----+-----+-----+-----+------+}
        """
        single_val = False
        if keys is None:
            keys = []
        else:
            if not hasattr(keys, "__iter__"):
                single_val = True
                keys = [keys]
            value_types = set([type(i) for i in keys])
            if len(value_types) > 1:
                raise ValueError("All keys should have the same type.")

        ret_sketches = self.__impl__.element_sub_sketch(keys)
        ret = {}

        # check return key matches input key
        for key in keys:
          if key not in ret_sketches:
            raise KeyError("Cannot retrieve element sub sketch for key '" + 
                           str(key) + 
                           "'. Element sub sketch can only be retrieved when the sketch_summary " + 
                           "object was created using the 'sub_sketch_keys' option.")
        for key in ret_sketches:
            ret[key] = Sketch(impl = ret_sketches[key])

        if single_val:
            return ret[keys[0]]
        else:
            return ret
