import unittest
import os
import math
import copy
from datetime import datetime
import array
import pickle

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

import pandas

# python testxframe.py
# python -m unittest testxframe
# python -m unittest testxframe.TestXFrameVersion
# python -m unittest testxframe.TestXFrameVersion.test_version

from xframes import XArray
from xframes import XFrame
from xframes.aggregate import SUM, ARGMAX, ARGMIN, MAX, MIN, COUNT, MEAN, \
    VARIANCE, STDV, SELECT_ONE, CONCAT


class XFrameUnitTestCase(unittest.TestCase):

    def assertEqualLen(self, expect, obj):
        return self.assertEqual(expect, len(obj))

    def assertColumnEqual(self, expect, obj):
        return self.assertListEqual(expect, list(obj))


class TestXFrameVersion(XFrameUnitTestCase):
    """
    Tests XFrame version
    """

    def test_version(self):
        ver = XFrame.version()
        self.assertIs(str, type(ver))


class TestXFrameConstructor(XFrameUnitTestCase):
    """
    Tests XFrame constructors that create data from local sources.
    """

    def test_construct_auto_dataframe(self):
        path = 'files/test-frame-auto.csv'
        res = XFrame(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['val_int', 'val_int_signed', 'val_float', 'val_float_signed',
                              'val_str', 'val_list', 'val_dict'], res.column_names())
        self.assertListEqual([int, int, float, float, str, list, dict], res.column_types())
        self.assertDictEqual({'val_int': 1, 'val_int_signed': -1, 'val_float': 1.0, 'val_float_signed': -1.0,
                              'val_str': 'a', 'val_list': ['a'], 'val_dict': {1: 'a'}}, res[0])
        self.assertDictEqual({'val_int': 2, 'val_int_signed': -2, 'val_float': 2.0, 'val_float_signed': -2.0,
                              'val_str': 'b', 'val_list': ['b'], 'val_dict': {2: 'b'}}, res[1])
        self.assertDictEqual({'val_int': 3, 'val_int_signed': -3, 'val_float': 3.0, 'val_float_signed': -3.0,
                              'val_str': 'c', 'val_list': ['c'], 'val_dict': {3: 'c'}}, res[2])

    def test_construct_auto_str_csv(self):
        path = 'files/test-frame.csv'
        res = XFrame(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_str_tsv(self):
        path = 'files/test-frame.tsv'
        res = XFrame(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_str_psv(self):
        path = 'files/test-frame.psv'
        res = XFrame(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_str_txt(self):
        # construct and XFrame given a text file
        # interpret as csv
        path = 'files/test-frame.txt'
        res = XFrame(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_str_noext(self):
        # construct and XFrame given a text file
        # interpret as csv
        path = 'files/test-frame'
        res = XFrame(path)
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_pandas_dataframe(self):
        df = pandas.DataFrame({'id': [1, 2, 3], 'val': [10.0, 20.0, 30.0]})
        res = XFrame(df)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 10.0}, res[0])
        self.assertDictEqual({'id': 2, 'val': 20.0}, res[1])
        self.assertDictEqual({'id': 3, 'val': 30.0}, res[2])

    def test_construct_auto_str_xframe(self):
        # construct an XFrame given a file with unrecognized file extension
        path = 'files/test-frame'
        res = XFrame(path)
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_xarray(self):
        # construct and XFrame given an XArray
        xa = XArray([1, 2, 3])
        t = XFrame(xa)
        self.assertEqualLen(3, t)
        self.assertListEqual(['X.0'], t.column_names())
        self.assertListEqual([int], t.column_types())
        self.assertDictEqual({'X.0': 1}, t[0])
        self.assertDictEqual({'X.0': 2}, t[1])
        self.assertDictEqual({'X.0': 3}, t[2])

    def test_construct_xframe(self):
        # construct an XFrame given another XFrame
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = XFrame(t)
        self.assertEqualLen(3, res)
        res = res.sort('id')
        self.assertColumnEqual([1, 2, 3], res['id'])
        self.assertListEqual([int, str], res.column_types())
        self.assertListEqual(['id', 'val'], res.column_names())

    def test_construct_iteritems(self):
        # construct an XFrame from an object that has iteritems
        class MyIterItem(object):
            def iteritems(self):
                return iter([('id', [1, 2, 3]), ('val', ['a', 'b', 'c'])])

        t = XFrame(MyIterItem())
        self.assertEqualLen(3, t)
        self.assertListEqual(['id', 'val'], t.column_names())
        self.assertListEqual([int, str], t.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, t[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, t[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, t[2])

    def test_construct_iteritems_bad(self):
        # construct an XFrame from an object that has iteritems
        class MyIterItem(object):
            def iteritems(self):
                return iter([('id', 1), ('val', 'a')])

        with self.assertRaises(TypeError):
            _ = XFrame(MyIterItem())

    def test_construct_iter(self):
        # construct an XFrame from an object that has __iter__
        class MyIter(object):
            def __iter__(self):
                return iter([1, 2, 3])

        t = XFrame(MyIter())
        self.assertEqualLen(3, t)
        self.assertListEqual(['X.0'], t.column_names())
        self.assertListEqual([int], t.column_types())
        self.assertDictEqual({'X.0': 1}, t[0])
        self.assertDictEqual({'X.0': 2}, t[1])
        self.assertDictEqual({'X.0': 3}, t[2])

    def test_construct_iter_bad(self):
        # construct an XFrame from an object that has __iter__
        class MyIter(object):
            def __iter__(self):
                return iter([])

        with self.assertRaises(TypeError):
            _ = XFrame(MyIter())

    def test_construct_none(self):
        # construct an empty XFrame
        t = XFrame()
        self.assertEqualLen(0, t)

    def test_construct_str_csv(self):
        # construct and XFrame given a text file
        # interpret as csv
        path = 'files/test-frame.txt'
        res = XFrame(path, format='csv')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_str_xframe(self):
        # construct and XFrame given a saved xframe
        path = 'files/test-frame'
        res = XFrame(path, format='xframe')
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_array(self):
        # construct an XFrame from an array
        t = XFrame([1, 2, 3], format='array')
        self.assertEqualLen(3, t)
        self.assertColumnEqual([1, 2, 3], t['X.0'])

    def test_construct_array_mixed_xarray(self):
        # construct an XFrame from an xarray and values
        xa = XArray([1, 2, 3])
        with self.assertRaises(ValueError):
            _ = XFrame([1, 2, xa], format='array')

    def test_construct_array_mixed_types(self):
        # construct an XFrame from
        # an array of mixed types
        with self.assertRaises(TypeError):
            _ = XFrame([1, 2, 'a'], format='array')

    def test_construct_unknown_format(self):
        # test unknown format
        with self.assertRaises(ValueError):
            _ = XFrame([1, 2, 'a'], format='bad-format')

    def test_construct_array_empty(self):
        # construct an XFrame from an empty array
        t = XFrame([], format='array')
        self.assertEqualLen(0, t)

    def test_construct_array_xarray(self):
        # construct an XFrame from an xarray
        xa1 = XArray([1, 2, 3])
        xa2 = XArray(['a', 'b', 'c'])
        t = XFrame([xa1, xa2], format='array')
        self.assertEqualLen(3, t)
        self.assertListEqual(['X.0', 'X.1'], t.column_names())
        self.assertListEqual([int, str], t.column_types())
        self.assertDictEqual({'X.0': 1, 'X.1': 'a'}, t[0])
        self.assertDictEqual({'X.0': 2, 'X.1': 'b'}, t[1])
        self.assertDictEqual({'X.0': 3, 'X.1': 'c'}, t[2])

    def test_construct_dict_int(self):
        # construct an XFrame from a dict of int
        t = XFrame({'id': [1, 2, 3], 'val': [10, 20, 30]}, format='dict')
        res = XFrame(t)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], t.column_names())
        self.assertListEqual([int, int], t.column_types())
        self.assertDictEqual({'id': 1, 'val': 10}, t[0])
        self.assertDictEqual({'id': 2, 'val': 20}, t[1])
        self.assertDictEqual({'id': 3, 'val': 30}, t[2])

    def test_construct_dict_float(self):
        # construct an XFrame from a dict of float
        t = XFrame({'id': [1.0, 2.0, 3.0], 'val': [10.0, 20.0, 30.0]}, format='dict')
        res = XFrame(t)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], t.column_names())
        self.assertListEqual([float, float], t.column_types())
        self.assertDictEqual({'id': 1.0, 'val': 10.0}, t[0])
        self.assertDictEqual({'id': 2.0, 'val': 20.0}, t[1])
        self.assertDictEqual({'id': 3.0, 'val': 30.0}, t[2])

    def test_construct_dict_str(self):
        # construct an XFrame from a dict of str
        t = XFrame({'id': ['a', 'b', 'c'], 'val': ['A', 'B', 'C']}, format='dict')
        res = XFrame(t)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], t.column_names())
        self.assertListEqual([str, str], t.column_types())
        self.assertDictEqual({'id': 'a', 'val': 'A'}, t[0])
        self.assertDictEqual({'id': 'b', 'val': 'B'}, t[1])
        self.assertDictEqual({'id': 'c', 'val': 'C'}, t[2])

    def test_construct_dict_int_str(self):
        # construct an XFrame from a dict of int and str
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']}, format='dict')
        self.assertEqualLen(3, t)
        t = t.sort('id')
        self.assertColumnEqual([1, 2, 3], t['id'])
        self.assertListEqual([int, str], t.column_types())
        self.assertListEqual(['id', 'val'], t.column_names())

    def test_construct_binary(self):
        # make binary file
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame'
        t.save(path, format='binary')  # File does not necessarily save in order
        res = XFrame(path).sort('id')  # so let's sort after we read it back
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])

    def test_construct_rdd(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame(rdd)
        self.assertEqualLen(3, res)
        self.assertDictEqual({'X.0': 1, 'X.1': 'a'}, res[0])
        self.assertDictEqual({'X.0': 2, 'X.1': 'b'}, res[1])

    def test_construct_spark_dataframe(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        fields = [StructField('id', IntegerType(), True), StructField('val', StringType(), True)]
        schema = StructType(fields)
        sqlc = XFrame.spark_sql_context()
        s_rdd = sqlc.createDataFrame(rdd, schema)
        res = XFrame(s_rdd)
        self.assertEqualLen(3, res)
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])


class TestXFrameReadCsvWithErrors(XFrameUnitTestCase):
    """
    Tests XFrame read_csv_with_errors
    """

    def test_read_csv_no_errors(self):
        path = 'files/test-frame.csv'
        res, errs = XFrame.read_csv_with_errors(path)
        self.assertEqualLen(3, res)
        self.assertDictEqual({}, errs)

    def test_read_csv_width_error(self):
        path = 'files/test-frame-width-err.csv'
        res, errs = XFrame.read_csv_with_errors(path)
        self.assertIn('width', errs)
        width_errs = errs['width']
        self.assertIsInstance(width_errs, XArray)
        self.assertEqualLen(2, width_errs)
        self.assertEqual('1', width_errs[0])
        self.assertEqual('2,x,y', width_errs[1])
        self.assertEqualLen(2, res)

    def test_read_csv_null_error(self):
        path = 'files/test-frame-null.csv'
        res, errs = XFrame.read_csv_with_errors(path)
        self.assertIn('csv', errs)
        csv_errs = errs['csv']
        self.assertIsInstance(csv_errs, XArray)
        self.assertEqualLen(1, csv_errs)
        self.assertEqual('2,\x00b', csv_errs[0])
        self.assertEqualLen(1, res)

    def test_read_csv_null_header_error(self):
        path = 'files/test-frame-null-header.csv'
        res, errs = XFrame.read_csv_with_errors(path)
        self.assertIn('header', errs)
        csv_errs = errs['header']
        self.assertIsInstance(csv_errs, XArray)
        self.assertEqualLen(1, csv_errs)
        self.assertEqual('id,\x00val', csv_errs[0])
        self.assertEqualLen(0, res)

    # Cannot figure out how to cause SystemError in csv reader.
    # But it happened one time


class TestXFrameReadCsv(XFrameUnitTestCase):
    """
    Tests XFrame read_csv
    """

    def test_read_csv(self):
        path = 'files/test-frame.csv'
        res = XFrame.read_csv(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_verbose(self):
        path = 'files/test-frame.csv'
        res = XFrame.read_csv(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_delim(self):
        path = 'files/test-frame.psv'
        res = XFrame.read_csv(path, delimiter='|')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_no_header(self):
        path = 'files/test-frame-no-header.csv'
        res = XFrame.read_csv(path, header=False)
        self.assertEqualLen(3, res)
        self.assertListEqual(['X.0', 'X.1'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'X.0': 1, 'X.1': 'a'}, res[0])
        self.assertDictEqual({'X.0': 2, 'X.1': 'b'}, res[1])
        self.assertDictEqual({'X.0': 3, 'X.1': 'c'}, res[2])

    def test_read_csv_comment(self):
        path = 'files/test-frame-comment.csv'
        res = XFrame.read_csv(path, comment_char='#')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_escape(self):
        path = 'files/test-frame-escape.csv'
        res = XFrame.read_csv(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a,a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b,b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c,c'}, res[2])

    def test_read_csv_escape_custom(self):
        path = 'files/test-frame-escape-custom.csv'
        res = XFrame.read_csv(path, escape_char='$')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a,a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b,b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c,c'}, res[2])

    def test_read_csv_initial_space(self):
        path = 'files/test-frame-initial_space.csv'
        res = XFrame.read_csv(path, skip_initial_space=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_hints_type(self):
        path = 'files/test-frame.csv'
        res = XFrame.read_csv(path, column_type_hints=str)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([str, str], res.column_types())
        self.assertDictEqual({'id': '1', 'val': 'a'}, res[0])
        self.assertDictEqual({'id': '2', 'val': 'b'}, res[1])
        self.assertDictEqual({'id': '3', 'val': 'c'}, res[2])

    def test_read_csv_hints_list(self):
        path = 'files/test-frame-extra.csv'
        res = XFrame.read_csv(path, column_type_hints=[str, str, int])
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val1', 'val2'], res.column_names())
        self.assertListEqual([str, str, int], res.column_types())
        self.assertDictEqual({'id': '1', 'val1': 'a', 'val2': 10}, res[0])
        self.assertDictEqual({'id': '2', 'val1': 'b', 'val2': 20}, res[1])
        self.assertDictEqual({'id': '3', 'val1': 'c', 'val2': 30}, res[2])

    def test_read_csv_hints_dict(self):
        path = 'files/test-frame-extra.csv'
        res = XFrame.read_csv(path, column_type_hints={'val2': int})
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val1', 'val2'], res.column_names())
        self.assertListEqual([str, str, int], res.column_types())
        self.assertDictEqual({'id': '1', 'val1': 'a', 'val2': 10}, res[0])
        self.assertDictEqual({'id': '2', 'val1': 'b', 'val2': 20}, res[1])
        self.assertDictEqual({'id': '3', 'val1': 'c', 'val2': 30}, res[2])

    def test_read_csv_na(self):
        path = 'files/test-frame-na.csv'
        res = XFrame.read_csv(path, na_values='None')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'NA'}, res[0])
        self.assertDictEqual({'id': None, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_na_mult(self):
        path = 'files/test-frame-na.csv'
        res = XFrame.read_csv(path, na_values=['NA', 'None'])
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': None}, res[0])
        self.assertDictEqual({'id': None, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])


class TestXFrameReadText(XFrameUnitTestCase):
    """
    Tests XFrame read_text
    """

    def test_read_text(self):
        path = 'files/test-frame-text.txt'
        res = XFrame.read_text(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['text', ], res.column_names())
        self.assertListEqual([str], res.column_types())
        self.assertDictEqual({'text': 'This is a test'}, res[0])
        self.assertDictEqual({'text': 'of read_text.'}, res[1])
        self.assertDictEqual({'text': 'Here is another sentence.'}, res[2])

    def test_read_text_delimited(self):
        path = 'files/test-frame-text.txt'
        res = XFrame.read_text(path, delimiter='.')
        self.assertEqualLen(3, res)
        self.assertListEqual(['text', ], res.column_names())
        self.assertListEqual([str], res.column_types())
        self.assertDictEqual({'text': 'This is a test of read_text'}, res[0])
        self.assertDictEqual({'text': 'Here is another sentence'}, res[1])
        self.assertDictEqual({'text': ''}, res[2])


class TestXFrameReadParquet(XFrameUnitTestCase):
    """
    Tests XFrame read_parquet
    """

    def test_read_parquet_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        # results may not come back in the same order
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_parquet_bool(self):
        t = XFrame({'id': [1, 2, 3], 'val': [True, False, True]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, bool], res.column_types())
        self.assertDictEqual({'id': 1, 'val': True}, res[0])
        self.assertDictEqual({'id': 2, 'val': False}, res[1])
        self.assertDictEqual({'id': 3, 'val': True}, res[2])

    def test_read_parquet_int(self):
        t = XFrame({'id': [1, 2, 3], 'val': [10, 20, 30]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 10}, res[0])
        self.assertDictEqual({'id': 2, 'val': 20}, res[1])
        self.assertDictEqual({'id': 3, 'val': 30}, res[2])

    def test_read_parquet_float(self):
        t = XFrame({'id': [1, 2, 3], 'val': [1.0, 2.0, 3.0]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 1.0}, res[0])
        self.assertDictEqual({'id': 2, 'val': 2.0}, res[1])
        self.assertDictEqual({'id': 3, 'val': 3.0}, res[2])

    def test_read_parquet_long(self):
        t = XFrame({'id': [1, 2**70, 3], 'val': [10, 20, 30]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('val')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([str, int], res.column_types())
        self.assertDictEqual({'id': '1', 'val': 10}, res[0])
        self.assertDictEqual({'id': '{}'.format(2**70), 'val': 20}, res[1])
        self.assertDictEqual({'id': '3', 'val': 30}, res[2])

    def test_read_parquet_list(self):
        t = XFrame({'id': [1, 2, 3], 'val': [[1, 1], [2, 2], [3, 3]]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, list], res.column_types())
        self.assertDictEqual({'id': 1, 'val': [1, 1]}, res[0])
        self.assertDictEqual({'id': 2, 'val': [2, 2]}, res[1])
        self.assertDictEqual({'id': 3, 'val': [3, 3]}, res[2])

    def test_read_parquet_dict(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{1: 1}, {2: 2}, {3: 3}]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, dict], res.column_types())
        self.assertDictEqual({'id': 1, 'val': {1: 1}}, res[0])
        self.assertDictEqual({'id': 2, 'val': {2: 2}}, res[1])
        self.assertDictEqual({'id': 3, 'val': {3: 3}}, res[2])


class TestXFrameFromXArray(XFrameUnitTestCase):
    """
    Tests XFrame from_xarray
    """

    def test_from_xarray(self):
        a = XArray([1, 2, 3])
        res = XFrame.from_xarray(a, 'id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id'], res.column_names())
        self.assertListEqual([int], res.column_types())
        self.assertDictEqual({'id': 1}, res[0])
        self.assertDictEqual({'id': 2}, res[1])
        self.assertDictEqual({'id': 3}, res[2])


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class TestXFrameToSparkDataFrame(XFrameUnitTestCase):
    """
    Tests XFrame to_spark_dataframe
    """

    def test_to_spark_dataframe_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertEqual('a', row.val)

    def test_to_spark_dataframe_bool(self):
        t = XFrame({'id': [1, 2, 3], 'val': [True, False, True]})
        t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertEqual(True, row.val)

    def test_to_spark_dataframe_float(self):
        t = XFrame({'id': [1, 2, 3], 'val': [1.0, 2.0, 3.0]})
        t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertEqual(1.0, row.val)

    def test_to_spark_dataframe_int(self):
        t = XFrame({'id': [1, 2, 3], 'val': [1, 2, 3]})
        t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertEqual(1, row.val)

    def test_to_spark_dataframe_list(self):
        t = XFrame({'id': [1, 2, 3], 'val': [[1, 1], [2, 2], [3, 3]]})
        t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertListEqual([1, 1], row.val)

    def test_to_spark_dataframe_list_bad(self):
        t = XFrame({'id': [1, 2, 3], 'val': [[[1], 1], [[2], 2], [[3], 3]]})
        with self.assertRaises(ValueError):
            t.to_spark_dataframe('tmp_tbl')

    def test_to_spark_dataframe_map(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'x': 1}, {'y': 2}, {'z': 3}]})
        t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertDictEqual({'x': 1}, row.val)

    def test_to_spark_dataframe_map_bad(self):
        t = XFrame({'id': [1, 2, 3], 'val': [None, {'y': 2}, {'z': 3}]})
        with self.assertRaises(ValueError):
            t.to_spark_dataframe('tmp_tbl')


class TestXFrameToRdd(XFrameUnitTestCase):
    """
    Tests XFrame to_rdd
    """

    def test_to_rdd(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        rdd_val = t.to_rdd().collect()
        self.assertTupleEqual((1, 'a'), rdd_val[0])
        self.assertTupleEqual((2, 'b'), rdd_val[1])
        self.assertTupleEqual((3, 'c'), rdd_val[2])


class TestXFrameFromRdd(XFrameUnitTestCase):
    """
    Tests XFrame from_rdd with regular rdd
    """

    def test_from_rdd(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame.from_rdd(rdd)
        self.assertEqualLen(3, res)
        self.assertDictEqual({'X.0': 1, 'X.1': 'a'}, res[0])
        self.assertDictEqual({'X.0': 2, 'X.1': 'b'}, res[1])

    def test_from_rdd_names(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame.from_rdd(rdd, column_names=('id', 'val'))
        self.assertEqualLen(3, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])

    def test_from_rdd_types(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(None, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame.from_rdd(rdd, column_types=(int, str))
        self.assertEqualLen(3, res)
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'X.0': None, 'X.1': 'a'}, res[0])
        self.assertDictEqual({'X.0': 2, 'X.1': 'b'}, res[1])

    def test_from_rdd_names_types(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(None, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame.from_rdd(rdd, column_names=('id', 'val'), column_types=(int, str))
        self.assertEqualLen(3, res)
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': None, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])

    def test_from_rdd_names_bad(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        with self.assertRaises(ValueError):
            XFrame.from_rdd(rdd, column_names=('id',))

    def test_from_rdd_types_bad(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(None, 'a'), (2, 'b'), (3, 'c')])
        with self.assertRaises(ValueError):
            XFrame.from_rdd(rdd, column_types=(int,))


class TestXFrameFromSparkDataFrame(XFrameUnitTestCase):
    """
    Tests XFrame from_rdd with spark dataframe
    """

    def test_from_rdd(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        fields = [StructField('id', IntegerType(), True), StructField('val', StringType(), True)]
        schema = StructType(fields)
        sqlc = XFrame.spark_sql_context()
        s_rdd = sqlc.createDataFrame(rdd, schema)

        res = XFrame.from_rdd(s_rdd)
        self.assertEqualLen(3, res)
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])


class TestXFramePrintRows(XFrameUnitTestCase):
    """
    Tests XFrame print_rows
    """

    def test_print_rows(self):
        pass


class TestXFrameToStr(XFrameUnitTestCase):
    """
    Tests XFrame __str__
    """

    def test_to_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        s = str(t).split('\n')
        self.assertEqual('+----+-----+', s[0])
        self.assertEqual('| id | val |', s[1])
        self.assertEqual('+----+-----+', s[2])
        self.assertEqual('| 1  |  a  |', s[3])
        self.assertEqual('| 2  |  b  |', s[4])
        self.assertEqual('| 3  |  c  |', s[5])
        self.assertEqual('+----+-----+', s[6])


class TestXFrameNonzero(XFrameUnitTestCase):
    """
    Tests XFrame __nonzero__
    """

    def test_nonzero_true(self):
        # not empty, nonzero data
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertTrue(t)

    def test_nonzero_false(self):
        # empty
        t = XFrame()
        self.assertFalse(t)

    def test_empty_false(self):
        # empty, but previously not empty
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t = t.filterby([99], 'id')
        self.assertFalse(t)


class TestXFrameLen(XFrameUnitTestCase):
    """
    Tests XFrame __len__
    """

    def test_len_nonzero(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertEqualLen(3, t)

    def test_len_zero(self):
        t = XFrame()
        self.assertEqualLen(0, t)

        # TODO make an XFrame and then somehow delete all its rows, so the RDD
        # exists but is empty


class TestXFrameCopy(XFrameUnitTestCase):
    """
    Tests XFrame __copy__
    """

    def test_copy(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        x = copy.copy(t)
        self.assertEqualLen(3, x)
        self.assertColumnEqual([1, 2, 3], x['id'])
        self.assertListEqual([int, str], x.column_types())
        self.assertListEqual(['id', 'val'], x.column_names())


class TestXFrameDtype(XFrameUnitTestCase):
    """
    Tests XFrame dtype
    """

    def test_dtype(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        dt = t.dtype()
        self.assertIs(int, dt[0])
        self.assertIs(str, dt[1])


class TestXFrameLineage(XFrameUnitTestCase):
    """
    Tests XFrame lineage
    """

    def test_lineage(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        lineage = t.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = list(lineage)[0]
        self.assertEqual('PROGRAM', item)

    def test_lineage_csv(self):
        path = 'files/test-frame-auto.csv'
        res = XFrame(path)
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = list(lineage)[0]
        filename = os.path.basename(item)
        self.assertEqual('test-frame-auto.csv', filename)

    def test_lineage_transform(self):
        path = 'files/test-frame-auto.csv'
        res = XFrame(path).transform_col('val_int', lambda row: row['val_int'] * 2)
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        filename = os.path.basename(list(lineage)[0])
        self.assertEqual('test-frame-auto.csv', filename)

    def test_lineage_rdd(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame.from_rdd(rdd)
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = list(lineage)[0]
        self.assertEqual('RDD', item)

    def test_lineage_hive(self):
        pass

    def test_lineage_pandas_dataframe(self):
        df = pandas.DataFrame({'id': [1, 2, 3], 'val': [10.0, 20.0, 30.0]})
        res = XFrame(df)
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = list(lineage)[0]
        self.assertEqual('PANDAS', item)

    def test_lineage_spark_dataframe(self):
        pass

    def test_lineage_program_data(self):
        res = XFrame({'id': [1, 2, 3], 'val': [10.0, 20.0, 30.0]})
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = list(lineage)[0]
        self.assertEqual('PROGRAM', item)

    def test_lineage_append(self):
        res1 = XFrame('files/test-frame.csv')
        res2 = XFrame('files/test-frame.psv')
        res = res1.append(res2)
        lineage = res.lineage()['table']
        self.assertEqualLen(2, lineage)
        basenames = set([os.path.basename(item) for item in lineage])
        self.assertTrue('test-frame.csv' in basenames)
        self.assertTrue('test-frame.psv' in basenames)

    def test_lineage_join(self):
        res1 = XFrame('files/test-frame.csv')
        res2 = XFrame('files/test-frame.psv').transform_col('val', lambda row: row['val'] + 'xxx')
        res = res1.join(res2, on='id').sort('id').head()
        lineage = res.lineage()['table']
        self.assertEqualLen(2, lineage)
        basenames = set([os.path.basename(item) for item in lineage])
        self.assertTrue('test-frame.csv' in basenames)
        self.assertTrue('test-frame.psv' in basenames)

    def test_lineage_add_column(self):
        res1 = XFrame('files/test-frame.csv')
        res2 = XArray('files/test-array-int')
        res = res1.add_column(res2, 'new-col')
        lineage = res.lineage()['table']
        self.assertEqualLen(2, lineage)
        basenames = set([os.path.basename(item) for item in lineage])
        self.assertTrue('test-frame.csv' in basenames)
        self.assertTrue('test-array-int' in basenames)

    def test_lineage_save(self):
        res = XFrame('files/test-frame.csv')
        path = 'tmp/frame'
        res.save(path, format='binary')
        with open(os.path.join(path, '_metadata')) as f:
            metadata = pickle.load(f)
        self.assertListEqual([['id', 'val'], [int, str]], metadata)
        with open(os.path.join(path, '_lineage')) as f:
            lineage = pickle.load(f)
            table_lineage = lineage['table']
            self.assertEqualLen(1, table_lineage)
            basenames = set([os.path.basename(item) for item in table_lineage])
            self.assertTrue('test-frame.csv' in basenames)

    def test_lineage_load(self):
        res = XFrame('files/test-frame.csv')
        path = 'tmp/frame'
        res.save(path, format='binary')
        res = XFrame(path)
        lineage = res.lineage()['table']
        self.assertEqualLen(2, lineage)
        basenames = set([os.path.basename(item) for item in lineage])
        self.assertTrue('test-frame.csv' in basenames)
        self.assertTrue('frame' in basenames)


class TestXFrameNumRows(XFrameUnitTestCase):
    """
    Tests XFrame num_rows
    """

    def test_num_rows(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertEqual(3, t.num_rows())


class TestXFrameNumColumns(XFrameUnitTestCase):
    """
    Tests XFrame num_columns
    """

    def test_num_columns(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertEqual(2, t.num_columns())


class TestXFrameColumnNames(XFrameUnitTestCase):
    """
    Tests XFrame column_names
    """

    def test_column_names(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        names = t.column_names()
        self.assertListEqual(['id', 'val'], names)


class TestXFrameColumnTypes(XFrameUnitTestCase):
    """
    Tests XFrame column_types
    """

    def test_column_types(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        types = t.column_types()
        self.assertListEqual([int, str], types)


class TestXFrameSelectRows(XFrameUnitTestCase):
    """
    Tests XFrame select_rows
    """

    def test_select_rowsr(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        a = XArray([1, 0, 1])
        res = t.select_rows(a)
        self.assertEqualLen(2, res)
        self.assertColumnEqual([1, 3], res['id'])
        self.assertColumnEqual(['a', 'c'], res['val'])


class TestXFrameHead(XFrameUnitTestCase):
    """
    Tests XFrame head
    """

    def test_head(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        hd = t.head(2)
        self.assertEqualLen(2, hd)
        self.assertColumnEqual([1, 2], hd['id'])
        self.assertColumnEqual(['a', 'b'], hd['val'])


class TestXFrameTail(XFrameUnitTestCase):
    """
    Tests XFrame tail
    """

    def test_tail(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        tl = t.tail(2)
        self.assertEqualLen(2, tl)
        self.assertColumnEqual([2, 3], tl['id'])
        self.assertColumnEqual(['b', 'c'], tl['val'])


class TestXFrameToPandasDataframe(XFrameUnitTestCase):
    """
    Tests XFrame to_pandas_dataframe
    """

    def test_to_pandas_dataframe_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        df = t.to_pandas_dataframe()
        self.assertEqualLen(3, df)
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual('a', df['val'][0])

    def test_to_pandas_dataframe_bool(self):
        t = XFrame({'id': [1, 2, 3], 'val': [True, False, True]})
        df = t.to_pandas_dataframe()
        self.assertEqualLen(3, df)
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual(True, df['val'][0])
        self.assertEqual(False, df['val'][1])

    def test_to_pandas_dataframe_float(self):
        t = XFrame({'id': [1, 2, 3], 'val': [1.0, 2.0, 3.0]})
        df = t.to_pandas_dataframe()
        self.assertEqualLen(3, df)
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual(1.0, df['val'][0])
        self.assertEqual(2.0, df['val'][1])

    def test_to_pandas_dataframe_int(self):
        t = XFrame({'id': [1, 2, 3], 'val': [1, 2, 3]})
        df = t.to_pandas_dataframe()
        self.assertEqualLen(3, df)
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual(1, df['val'][0])
        self.assertEqual(2, df['val'][1])

    def test_to_pandas_dataframe_list(self):
        t = XFrame({'id': [1, 2, 3], 'val': [[1, 1], [2, 2], [3, 3]]})
        df = t.to_pandas_dataframe()
        self.assertEqualLen(3, df)
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertListEqual([1, 1], df['val'][0])
        self.assertListEqual([2, 2], df['val'][1])

    def test_to_pandas_dataframe_map(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'x': 1}, {'y': 2}, {'z': 3}]})
        df = t.to_pandas_dataframe()
        self.assertEqualLen(3, df)
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertDictEqual({'x': 1}, df['val'][0])
        self.assertDictEqual({'y': 2}, df['val'][1])


class TestXFrameToDataframeplus(XFrameUnitTestCase):
    """
    Tests XFrame to_dataframeplus
    """

    @unittest.skip('depends on dataframe_plus')
    def test_to_dataframeplus_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        df = t.to_dataframeplus()
        self.assertEqualLen(3, df)
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual('a', df['val'][0])


class TestXFrameApply(XFrameUnitTestCase):
    """
    Tests XFrame apply
    """

    def test_apply(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.apply(lambda row: row['id'] * 2)
        self.assertEqualLen(3, res)
        self.assertIs(int, res.dtype())
        self.assertColumnEqual([2, 4, 6], res)

    def test_apply_float(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.apply(lambda row: row['id'] * 2, dtype=float)
        self.assertEqualLen(3, res)
        self.assertIs(float, res.dtype())
        self.assertColumnEqual([2.0, 4.0, 6.0], res)

    def test_apply_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.apply(lambda row: row['id'] * 2, dtype=str)
        self.assertEqualLen(3, res)
        self.assertIs(str, res.dtype())
        self.assertColumnEqual(['2', '4', '6'], res)


class TestXFrameTransformCol(XFrameUnitTestCase):
    """
    Tests XFrame transform_col
    """

    def test_transform_col_identity(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.transform_col('id')
        self.assertEqualLen(3, res)
        self.assertListEqual([int, str], res.dtype())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])

    def test_transform_col_lambda(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.transform_col('id', lambda row: row['id'] * 2)
        self.assertEqualLen(3, res)
        self.assertListEqual([int, str], res.dtype())
        self.assertDictEqual({'id': 2, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 4, 'val': 'b'}, res[1])

    def test_transform_col_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.transform_col('id', lambda row: 'x' * row['id'])
        self.assertEqualLen(3, res)
        self.assertListEqual([str, str], res.dtype())
        self.assertDictEqual({'id': 'x', 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 'xx', 'val': 'b'}, res[1])

    def test_transform_col_cast(self):
        t = XFrame({'id': ['1', '2', '3'], 'val': ['a', 'b', 'c']})
        res = t.transform_col('id', dtype=int)
        self.assertEqualLen(3, res)
        self.assertListEqual([int, str], res.dtype())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])


class TestXFrameTransformCols(XFrameUnitTestCase):
    """
    Tests XFrame transform_cols
    """

    def test_transform_cols_identity(self):
        t = XFrame({'other': ['x', 'y', 'z'], 'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.transform_cols(['id', 'val'])
        self.assertEqualLen(3, res)
        self.assertListEqual([int, str, str], res.dtype())
        self.assertDictEqual({'other': 'x', 'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'other': 'y', 'id': 2, 'val': 'b'}, res[1])

    def test_transform_cols_lambda(self):
        t = XFrame({'other': ['x', 'y', 'z'], 'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.transform_cols(['id', 'val'], lambda row: [row['id'] * 2, row['val'] + 'x'])
        self.assertEqualLen(3, res)
        self.assertListEqual([int, str, str], res.dtype())
        self.assertDictEqual({'other': 'x', 'id': 2, 'val': 'ax'}, res[0])
        self.assertDictEqual({'other': 'y', 'id': 4, 'val': 'bx'}, res[1])

    def test_transform_cols_type(self):
        t = XFrame({'other': ['x', 'y', 'z'], 'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.transform_cols(['id', 'val'], lambda row: ['x' * row['id'], ord(row['val'][0])])
        self.assertEqualLen(3, res)
        self.assertListEqual([str, str, int], res.dtype())
        self.assertDictEqual({'other': 'x', 'id': 'x', 'val': 97}, res[0])
        self.assertDictEqual({'other': 'y', 'id': 'xx', 'val': 98}, res[1])

    def test_transform_cols_cast(self):
        t = XFrame({'other': ['x', 'y', 'z'], 'id': ['1', '2', '3'], 'val': [10, 20, 30]})
        res = t.transform_cols(['id', 'val'], dtypes=[int, str])
        self.assertEqualLen(3, res)
        self.assertListEqual([int, str, str], res.dtype())
        self.assertDictEqual({'other': 'x', 'id': 1, 'val': '10'}, res[0])
        self.assertDictEqual({'other': 'y', 'id': 2, 'val': '20'}, res[1])


class TestXFrameFlatMap(XFrameUnitTestCase):
    """
    Tests XFrame flat_map
    """

    def test_flat_map(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.flat_map(['number', 'letter'],
                         lambda row: [list(row.itervalues()) for _ in range(0, row['id'])],
                         column_types=[int, str])
        self.assertListEqual(['number', 'letter'], res.column_names())
        self.assertListEqual([int, str], res.dtype())
        self.assertDictEqual({'number': 1, 'letter': 'a'}, res[0])
        self.assertDictEqual({'number': 2, 'letter': 'b'}, res[1])
        self.assertDictEqual({'number': 2, 'letter': 'b'}, res[2])
        self.assertDictEqual({'number': 3, 'letter': 'c'}, res[3])
        self.assertDictEqual({'number': 3, 'letter': 'c'}, res[4])
        self.assertDictEqual({'number': 3, 'letter': 'c'}, res[5])

    def test_flat_map_identity(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.flat_map(['number', 'letter'],
                         lambda row: [[row['id'], row['val']]],
                         column_types=[int, str])
        self.assertListEqual(['number', 'letter'], res.column_names())
        self.assertListEqual([int, str], res.dtype())
        self.assertDictEqual({'number': 1, 'letter': 'a'}, res[0])
        self.assertDictEqual({'number': 2, 'letter': 'b'}, res[1])
        self.assertDictEqual({'number': 3, 'letter': 'c'}, res[2])

    def test_flat_map_mapped(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.flat_map(['number', 'letter'],
                         lambda row: [[row['id'] * 2, row['val'] + 'x']],
                         column_types=[int, str])
        self.assertListEqual(['number', 'letter'], res.column_names())
        self.assertListEqual([int, str], res.dtype())
        self.assertDictEqual({'number': 2, 'letter': 'ax'}, res[0])
        self.assertDictEqual({'number': 4, 'letter': 'bx'}, res[1])
        self.assertDictEqual({'number': 6, 'letter': 'cx'}, res[2])

    def test_flat_map_auto(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.flat_map(['number', 'letter'],
                         lambda row: [[row['id'] * 2, row['val'] + 'x']])
        self.assertListEqual(['number', 'letter'], res.column_names())
        self.assertListEqual([int, str], res.dtype())
        self.assertDictEqual({'number': 2, 'letter': 'ax'}, res[0])
        self.assertDictEqual({'number': 4, 'letter': 'bx'}, res[1])
        self.assertDictEqual({'number': 6, 'letter': 'cx'}, res[2])

        # TODO: test auto error cases


class TestXFrameSample(XFrameUnitTestCase):
    """
    Tests XFrame sample
    """

    @unittest.skip('depends on number of partitions')
    def test_sample_02(self):
        t = XFrame({'id': [1, 2, 3, 4, 5], 'val': ['a', 'b', 'c', 'd', 'e']})
        res = t.sample(0.2, 2)
        self.assertEqualLen(1, res)
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[0])

    @unittest.skip('depends on number of partitions')
    def test_sample_08(self):
        t = XFrame({'id': [1, 2, 3, 4, 5], 'val': ['a', 'b', 'c', 'd', 'e']})
        res = t.sample(0.8, 3)
        self.assertEqualLen(3, res)
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[0])
        self.assertDictEqual({'id': 4, 'val': 'd'}, res[1])
        self.assertDictEqual({'id': 5, 'val': 'e'}, res[2])


class TestXFrameRandomSplit(XFrameUnitTestCase):
    """
    Tests XFrame random_split
    """

    @unittest.skip('depends on number of partitions')
    def test_random_split(self):
        t = XFrame({'id': [1, 2, 3, 4, 5], 'val': ['a', 'b', 'c', 'd', 'e']})
        res1, res2 = t.random_split(0.5, 1)
        self.assertEqualLen(3, res1)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res1[0])
        self.assertDictEqual({'id': 4, 'val': 'd'}, res1[1])
        self.assertDictEqual({'id': 5, 'val': 'e'}, res1[2])
        self.assertEqualLen(2, res2)
        self.assertDictEqual({'id': 2, 'val': 'b'}, res2[0])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res2[1])


class TestXFrameTopk(XFrameUnitTestCase):
    """
    Tests XFrame topk
    """

    def test_topk_int(self):
        t = XFrame({'id': [10, 20, 30], 'val': ['a', 'b', 'c']})
        res = t.topk('id', 2)
        self.assertEqualLen(2, res)
        # noinspection PyUnresolvedReferences
        self.assertTrue((XArray([30, 20]) == res['id']).all())
        self.assertColumnEqual(['c', 'b'], res['val'])
        self.assertListEqual([int, str], res.column_types())
        self.assertListEqual(['id', 'val'], res.column_names())

    def test_topk_int_reverse(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['c', 'b', 'a']})
        res = t.topk('id', 2, reverse=True)
        self.assertEqualLen(2, res)
        self.assertColumnEqual([10, 20], res['id'])
        self.assertColumnEqual(['a', 'b'], res['val'])

    # noinspection PyUnresolvedReferences
    def test_topk_float(self):
        t = XFrame({'id': [10.0, 20.0, 30.0], 'val': ['a', 'b', 'c']})
        res = t.topk('id', 2)
        self.assertEqualLen(2, res)
        self.assertTrue((XArray([30.0, 20.0]) == res['id']).all())
        self.assertColumnEqual(['c', 'b'], res['val'])
        self.assertListEqual([float, str], res.column_types())
        self.assertListEqual(['id', 'val'], res.column_names())

    def test_topk_float_reverse(self):
        t = XFrame({'id': [30.0, 20.0, 10.0], 'val': ['c', 'b', 'a']})
        res = t.topk('id', 2, reverse=True)
        self.assertEqualLen(2, res)
        self.assertColumnEqual([10.0, 20.0], res['id'])
        self.assertColumnEqual(['a', 'b'], res['val'])

    def test_topk_str(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['a', 'b', 'c']})
        res = t.topk('val', 2)
        self.assertEqualLen(2, res)
        self.assertColumnEqual([10, 20], res['id'])
        self.assertColumnEqual(['c', 'b'], res['val'])
        self.assertListEqual([int, str], res.column_types())
        self.assertListEqual(['id', 'val'], res.column_names())

    def test_topk_str_reverse(self):
        t = XFrame({'id': [10, 20, 30], 'val': ['c', 'b', 'a']})
        res = t.topk('val', 2, reverse=True)
        self.assertEqualLen(2, res)
        self.assertColumnEqual([30, 20], res['id'])
        self.assertColumnEqual(['a', 'b'], res['val'])


class TestXFrameSaveBinary(XFrameUnitTestCase):
    """
    Tests XFrame save binary format
    """

    def test_save(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame'
        t.save(path, format='binary')
        with open(os.path.join(path, '_metadata')) as f:
            metadata = pickle.load(f)
        self.assertListEqual([['id', 'val'], [int, str]], metadata)
        # TODO find some way to check the data


class TestXFrameSaveCsv(XFrameUnitTestCase):
    """
    Tests XFrame save csv format
    """

    def test_save(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame-csv'
        t.save(path, format='csv')

        with open(path + '.csv') as f:
            heading = f.readline().rstrip()
            self.assertEqual('id,val', heading)
            self.assertEqual('30,a', f.readline().rstrip())
            self.assertEqual('20,b', f.readline().rstrip())
            self.assertEqual('10,c', f.readline().rstrip())


class TestXFrameSaveParquet(XFrameUnitTestCase):
    """
    Tests XFrame save for parquet files
    """

    def test_save(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')
        res = XFrame(path + '.parquet')
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_save_rename(self):
        t = XFrame({'id col': [1, 2, 3], 'val,col': ['a', 'b', 'c']})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')
        res = XFrame(path + '.parquet')
        self.assertListEqual(['id_col', 'val_col'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id_col': 1, 'val_col': 'a'}, res[0])
        self.assertDictEqual({'id_col': 2, 'val_col': 'b'}, res[1])
        self.assertDictEqual({'id_col': 3, 'val_col': 'c'}, res[2])

    def test_save_large_int(self):
        t = XFrame({'id': [1, 2**34, 3], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')
        res = XFrame(path + '.parquet')
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([long, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2**34, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_save_xlarge_int(self):
        t = XFrame({'id': [1, 2**70, 3], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')
        res = XFrame(path + '.parquet')
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([long, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2**70, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])


class TestXFrameSelectColumn(XFrameUnitTestCase):
    """
    Tests XFrame select_column
    """

    def test_select_column_id(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.select_column('id')
        self.assertColumnEqual([1, 2, 3], res)

    def test_select_column_val(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.select_column('val')
        self.assertColumnEqual(['a', 'b', 'c'], res)

    def test_select_column_bad_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(ValueError):
            t.select_column('xx')

    def test_select_column_bad_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            t.select_column(1)


class TestXFrameSelectColumns(XFrameUnitTestCase):
    """
    Tests XFrame select_columns
    """

    def test_select_columns_id_val(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        res = t.select_columns(['id', 'val'])
        self.assertTrue([1, 'a'], res[0])

    def test_select_columns_id(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        res = t.select_columns(['id'])
        self.assertTrue([1], res[0])

    def test_select_columns_not_iterable(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(TypeError):
            t.select_columns(1)

    def test_select_columns_bad_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(TypeError):
            t.select_columns(['id', 2])

    def test_select_columns_bad_dup(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(ValueError):
            t.select_columns(['id', 'id'])


class TestXFrameAddColumn(XFrameUnitTestCase):
    """
    Tests XFrame add_column
    """

    def test_add_column_named(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta = XArray([3.0, 2.0, 1.0])
        res = tf.add_column(ta, name='another')
        self.assertListEqual(['id', 'val', 'another'], res.column_names())
        self.assertDictEqual({'id': 1, 'val': 'a', 'another': 3.0}, res[0])

    def test_add_column_name_default(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta = XArray([3.0, 2.0, 1.0])
        res = tf.add_column(ta)
        self.assertListEqual(['id', 'val', 'X.2'], res.column_names())
        self.assertDictEqual({'id': 1, 'val': 'a', 'X.2': 3.0}, res[0])

    def test_add_column_name_dup(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta = XArray([3.0, 2.0, 1.0])
        res = tf.add_column(ta, name='id')
        self.assertListEqual(['id', 'val', 'id.2'], res.column_names())
        self.assertDictEqual({'id': 1, 'val': 'a', 'id.2': 3.0}, res[0])


class TestXFrameAddColumnsArray(XFrameUnitTestCase):
    """
    Tests XFrame add_columns where data is array of XArray
    """

    def test_add_columns_one(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta = XArray([3.0, 2.0, 1.0])
        res = tf.add_columns([ta], namelist=['new1'])
        self.assertListEqual(['id', 'val', 'new1'], res.column_names())
        self.assertListEqual([int, str, float], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'new1': 3.0}, res[0])

    def test_add_columns_two(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta1 = XArray([3.0, 2.0, 1.0])
        ta2 = XArray([30.0, 20.0, 10.0])
        res = tf.add_columns([ta1, ta2], namelist=['new1', 'new2'])
        self.assertListEqual(['id', 'val', 'new1', 'new2'], res.column_names())
        self.assertListEqual([int, str, float, float], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'new1': 3.0, 'new2': 30.0}, res[0])

    def test_add_columns_namelist_missing(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta1 = XArray([3.0, 2.0, 1.0])
        ta2 = XArray([30.0, 20.0, 10.0])
        with self.assertRaises(TypeError):
            tf.add_columns([ta1, ta2])

    def test_add_columns_data_not_iterable(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            tf.add_columns(1, namelist=[])

    def test_add_columns_namelist_not_iterable(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta1 = XArray([3.0, 2.0, 1.0])
        ta2 = XArray([30.0, 20.0, 10.0])
        with self.assertRaises(TypeError):
            tf.add_columns([ta1, ta2], namelist=1)

    def test_add_columns_not_xarray(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta1 = XArray([3.0, 2.0, 1.0])
        ta2 = [30.0, 20.0, 10.0]
        with self.assertRaises(TypeError):
            tf.add_columns([ta1, ta2], namelist=['new1', 'new2'])

    def test_add_columns_name_not_str(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta1 = XArray([3.0, 2.0, 1.0])
        ta2 = XArray([30.0, 20.0, 10.0])
        with self.assertRaises(TypeError):
            tf.add_columns([ta1, ta2], namelist=['new1', 1])


class TestXFrameAddColumnsFrame(XFrameUnitTestCase):
    """
    Tests XFrame add_columns where data is XFrame
    """

    def test_add_columns(self):
        tf1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        tf2 = XFrame({'new1': [3.0, 2.0, 1.0], 'new2': [30.0, 20.0, 10.0]})
        res = tf1.add_columns(tf2)
        self.assertListEqual(['id', 'val', 'new1', 'new2'], res.column_names())
        self.assertListEqual([int, str, float, float], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'new1': 3.0, 'new2': 30.0}, res[0])

    def test_add_columns_dup_names(self):
        tf1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        tf2 = XFrame({'new1': [3.0, 2.0, 1.0], 'val': [30.0, 20.0, 10.0]})
        res = tf1.add_columns(tf2)
        self.assertListEqual(['id', 'val', 'new1', 'val.1'], res.column_names())
        self.assertListEqual([int, str, float, float], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'new1': 3.0, 'val.1': 30.0}, res[0])


class TestXFrameReplaceColumn(XFrameUnitTestCase):
    """
    Tests XFrame replace_column
    """

    def test_replace_column(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        a = XArray(['x', 'y', 'z'])
        res = t.replace_column('val', a)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertDictEqual({'id': 1, 'val': 'x'}, res[0])

    def test_replace_column_bad_col_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            _ = t.replace_column('val', ['x', 'y', 'z'])

    def test_replace_column_bad_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        a = XArray(['x', 'y', 'z'])
        with self.assertRaises(ValueError):
            _ = t.replace_column('xx', a)

    def test_replace_column_bad_name_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        a = XArray(['x', 'y', 'z'])
        with self.assertRaises(TypeError):
            _ = t.replace_column(2, a)


class TestXFrameRemoveColumn(XFrameUnitTestCase):
    """
    Tests XFrame remove_column
    """

    def test_remove_column(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        res = t.remove_column('another')
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqualLen(3, t.column_names())
        self.assertEqualLen(2, res.column_names())

    def test_remove_column_not_found(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            t.remove_column('xx')


class TestXFrameRemoveColumns(XFrameUnitTestCase):
    """
    Tests XFrame remove_columns
    """

    def test_remove_columns(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'new1': [3.0, 2.0, 1.0], 'new2': [30.0, 20.0, 10.0]})
        res = t.remove_columns(['new1', 'new2'])
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqualLen(2, res.column_names())
        self.assertEqualLen(4, t.column_names())

    def test_remove_column_not_iterable(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(TypeError):
            t.remove_columns('xx')

    def test_remove_column_not_found(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            t.remove_columns(['xx'])


class TestXFrameSwapColumns(XFrameUnitTestCase):
    """
    Tests XFrame swap_columns
    """

    def test_swap_columns(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'x': [3.0, 2.0, 1.0]})
        res = t.swap_columns('val', 'x')
        self.assertListEqual(['id', 'x', 'val'], res.column_names())
        self.assertListEqual(['id', 'val', 'x'], t.column_names())
        self.assertDictEqual({'id': 1, 'x': 3.0, 'val': 'a'}, res[0])

    def test_swap_columns_bad_col_1(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            t.swap_columns('xx', 'another')

    def test_swap_columns_bad_col_2(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            t.swap_columns('val', 'xx')


class TestXFrameReorderColumns(XFrameUnitTestCase):
    """
    Tests XFrame reorder_columns
    """

    def test_reorder_columns(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'x': [3.0, 2.0, 1.0]})
        res = t.reorder_columns(['val', 'x', 'id'])
        self.assertListEqual(['val', 'x', 'id'], res.column_names())
        self.assertListEqual(['id', 'val', 'x'], t.column_names())
        self.assertDictEqual({'id': 1, 'x': 3.0, 'val': 'a'}, res[0])

    def test_reorder_columns_list_not_iterable(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'x': [3.0, 2.0, 1.0]})
        with self.assertRaises(TypeError):
            t.reorder_columns('val')

    def test_reorder_columns_bad_col(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'x': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            t.reorder_columns(['val', 'y', 'id'])

    def test_reorder_columns_incomplete(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'x': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            t.reorder_columns(['val', 'id'])


class TestXFrameRename(XFrameUnitTestCase):
    """
    Tests XFrame rename
    """

    def test_rename(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.rename({'id': 'new_id'})
        self.assertListEqual(['new_id', 'val'], res.column_names())
        self.assertListEqual(['id', 'val'], t.column_names())
        self.assertDictEqual({'new_id': 1, 'val': 'a'}, res[0])

    def test_rename_arg_not_dict(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            t.rename('id')

    def test_rename_col_not_found(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(ValueError):
            t.rename({'xx': 'new_id'})

    def test_rename_bad_length(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(ValueError):
            t.rename(['id'])

    def test_rename_list(self):
        t = XFrame({'X0': [1, 2, 3], 'X1': ['a', 'b', 'c']})
        res = t.rename(['id', 'val'])
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual(['X0', 'X1'], t.column_names())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])


class TestXFrameGetitem(XFrameUnitTestCase):
    """
    Tests XFrame __getitem__
    """

    def test_getitem_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t['id']
        self.assertColumnEqual([1, 2, 3], res)

    def test_getitem_int_pos(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t[1]
        self.assertDictEqual({'id': 2, 'val': 'b'}, res)

    def test_getitem_int_neg(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t[-2]
        self.assertDictEqual({'id': 2, 'val': 'b'}, res)

    def test_getitem_int_too_low(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(IndexError):
            _ = t[-100]

    def test_getitem_int_too_high(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(IndexError):
            _ = t[100]

    def test_getitem_slice(self):
        # TODO we could test more variations of slice
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t[:2]
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])

    def test_getitem_list(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'x': [1.0, 2.0, 3.0]})
        res = t[['id', 'x']]
        self.assertDictEqual({'id': 2, 'x': 2.0}, res[1])

    def test_getitem_bad_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            _ = t[{'a': 1}]

    # TODO: need to implement
    def test_getitem_xarray(self):
        pass


class TestXFrameSetitem(XFrameUnitTestCase):
    """
    Tests XFrame __setitem__
    """

    def test_setitem_float_const(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t['x'] = 5.0
        self.assertListEqual(['id', 'val', 'x'], t.column_names())
        self.assertDictEqual({'id': 2, 'val': 'b', 'x': 5.0}, t[1])

    def test_setitem_str_const_replace(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t['val'] = 'x'
        self.assertListEqual(['id', 'val'], t.column_names())
        self.assertDictEqual({'id': 2, 'val': 'x'}, t[1])

    def test_setitem_list(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta1 = XArray([3.0, 2.0, 1.0])
        ta2 = XArray([30.0, 20.0, 10.0])
        tf[['new1', 'new2']] = [ta1, ta2]
        self.assertListEqual(['id', 'val', 'new1', 'new2'], tf.column_names())
        self.assertListEqual([int, str, float, float], tf.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'new1': 3.0, 'new2': 30.0}, tf[0])

    def test_setitem_str_iter(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t['x'] = [1.0, 2.0, 3.0]
        self.assertListEqual(['id', 'val', 'x'], t.column_names())
        self.assertDictEqual({'id': 2, 'val': 'b', 'x': 2.0}, t[1])

    def test_setitem_str_xarray(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta = XArray([3.0, 2.0, 1.0])
        tf['new'] = ta
        self.assertListEqual(['id', 'val', 'new'], tf.column_names())
        self.assertListEqual([int, str, float], tf.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'new': 3.0}, tf[0])

    def test_setitem_str_iter_replace(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t['val'] = [1.0, 2.0, 3.0]
        self.assertListEqual(['id', 'val'], t.column_names())
        self.assertDictEqual({'id': 2, 'val': 2.0}, t[1])

    def test_setitem_bad_key(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            t[{'a': 1}] = [1.0, 2.0, 3.0]

    def test_setitem_str_iter_replace_one_col(self):
        t = XFrame({'val': ['a', 'b', 'c']})
        t['val'] = [1.0, 2.0, 3.0, 4.0]
        self.assertListEqual(['val'], t.column_names())
        self.assertEqualLen(4, t)
        self.assertDictEqual({'val': 2.0}, t[1])


class TestXFrameDelitem(XFrameUnitTestCase):
    """
    Tests XFrame __delitem__
    """

    def test_delitem(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        del t['another']
        self.assertDictEqual({'id': 1, 'val': 'a'}, t[0])

    def test_delitem_not_found(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            del t['xx']


class TestXFrameIsMaterialized(XFrameUnitTestCase):
    """
    Tests XFrame _is_materialized
    """

    def test_is_materialized_false(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertFalse(t._is_materialized())

    def test_is_materialized(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        len(t)
        self.assertTrue(t._is_materialized())


class TestXFrameIter(XFrameUnitTestCase):
    """
    Tests XFrame __iter__
    """

    def test_iter(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        expect_id = [1, 2, 3]
        expect_val = ['a', 'b', 'c']
        for item in zip(t, expect_id, expect_val):
            self.assertEqual(item[1], item[0]['id'])
            self.assertEqual(item[2], item[0]['val'])


class TestXFrameRange(XFrameUnitTestCase):
    """
    Tests XFrame range
    """

    def test_range_int_pos(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.range(1)
        self.assertDictEqual({'id': 2, 'val': 'b'}, res)

    def test_range_int_neg(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.range(-2)
        self.assertDictEqual({'id': 2, 'val': 'b'}, res)

    def test_range_int_too_low(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(IndexError):
            _ = t.range(-100)

    def test_range_int_too_high(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(IndexError):
            _ = t.range(100)

    def test_range_slice(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.range(slice(0, 2))
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])

    def test_range_bad_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            _ = t.range({'a': 1})


class TestXFrameAppend(XFrameUnitTestCase):
    """
    Tests XFrame append
    """

    def test_append(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20, 30], 'val': ['aa', 'bb', 'cc']})
        res = t1.append(t2)
        self.assertEqualLen(6, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 10, 'val': 'aa'}, res[3])

    def test_append_bad_type(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(RuntimeError):
            t1.append(1)

    def test_append_both_empty(self):
        t1 = XFrame()
        t2 = XFrame()
        res = t1.append(t2)
        self.assertEqualLen(0, res)

    def test_append_first_empty(self):
        t1 = XFrame()
        t2 = XFrame({'id': [10, 20, 30], 'val': ['aa', 'bb', 'cc']})
        res = t1.append(t2)
        self.assertEqualLen(3, res)
        self.assertDictEqual({'id': 10, 'val': 'aa'}, res[0])

    def test_append_second_empty(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame()
        res = t1.append(t2)
        self.assertEqualLen(3, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])

    def test_append_unequal_col_length(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20, 30], 'val': ['aa', 'bb', 'cc'], 'another': [1.0, 2.0, 3.0]})
        with self.assertRaises(RuntimeError):
            t1.append(t2)

    def test_append_col_name_mismatch(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20, 30], 'xx': ['a', 'b', 'c']})
        with self.assertRaises(RuntimeError):
            t1.append(t2)

    def test_append_col_type_mismatch(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20], 'val': [1.0, 2.0]})
        with self.assertRaises(RuntimeError):
            t1.append(t2)


class TestXFrameGroupby(XFrameUnitTestCase):
    """
    Tests XFrame groupby
    """

    def test_groupby(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id'], res.column_names())
        self.assertListEqual([int], res.column_types())
        self.assertDictEqual({'id': 1}, res[0])
        self.assertDictEqual({'id': 2}, res[1])
        self.assertDictEqual({'id': 3}, res[2])

    def test_groupby_nooperation(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id')
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id'], res.column_names())
        self.assertListEqual([int], res.column_types())
        self.assertDictEqual({'id': 1}, res[0])
        self.assertDictEqual({'id': 2}, res[1])
        self.assertDictEqual({'id': 3}, res[2])

    def test_groupby_bad_col_name_type(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        with self.assertRaises(TypeError):
            t.groupby(1, {})

    def test_groupby_bad_col_name_list_type(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        with self.assertRaises(TypeError):
            t.groupby([1], {})

    def test_groupby_bad_col_group_name(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        with self.assertRaises(KeyError):
            t.groupby('xx', {})

    def test_groupby_bad_group_type(self):
        t = XFrame({'id': [{1: 'a', 2: 'b'}],
                    'val': ['a', 'b']})
        with self.assertRaises(TypeError):
            t.groupby('id', {})

    def test_groupby_bad_agg_group_name(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        with self.assertRaises(KeyError):
            t.groupby('id', SUM('xx'))

    def test_groupby_bad_agg_group_type(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        with self.assertRaises(TypeError):
            t.groupby('id', SUM(1))


class TestXFrameGroupbyAggregators(XFrameUnitTestCase):
    """
    Tests XFrame groupby aggregators
    """

    def test_groupby_count(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'count': COUNT})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'count'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'count': 3}, res[0])
        self.assertDictEqual({'id': 2, 'count': 2}, res[1])
        self.assertDictEqual({'id': 3, 'count': 1}, res[2])

    def test_groupby_sum(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'sum': SUM('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'sum'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'sum': 110}, res[0])
        self.assertDictEqual({'id': 2, 'sum': 70}, res[1])
        self.assertDictEqual({'id': 3, 'sum': 30}, res[2])

    def test_groupby_sum_def(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', SUM('another'))
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'sum'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'sum': 110}, res[0])
        self.assertDictEqual({'id': 2, 'sum': 70}, res[1])
        self.assertDictEqual({'id': 3, 'sum': 30}, res[2])

    def test_groupby_sum_sum_def(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', [SUM('another'), SUM('another')])
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'sum', 'sum.1'], res.column_names())
        self.assertListEqual([int, int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'sum': 110, 'sum.1': 110}, res[0])
        self.assertDictEqual({'id': 2, 'sum': 70, 'sum.1': 70}, res[1])
        self.assertDictEqual({'id': 3, 'sum': 30, 'sum.1': 30}, res[2])

    def test_groupby_sum_rename(self):
        t = XFrame({'sum': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('sum', SUM('another'))
        res = res.topk('sum', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['sum', 'sum.1'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'sum': 1, 'sum.1': 110}, res[0])
        self.assertDictEqual({'sum': 2, 'sum.1': 70}, res[1])
        self.assertDictEqual({'sum': 3, 'sum.1': 30}, res[2])

    def test_groupby_count_sum(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'count': COUNT, 'sum': SUM('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'count', 'sum'], res.column_names())
        self.assertListEqual([int, int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'count': 3, 'sum': 110}, res[0])
        self.assertDictEqual({'id': 2, 'count': 2, 'sum': 70}, res[1])
        self.assertDictEqual({'id': 3, 'count': 1, 'sum': 30}, res[2])

    def test_groupby_count_sum_def(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', [COUNT, SUM('another')])
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'count', 'sum'], res.column_names())
        self.assertListEqual([int, int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'count': 3, 'sum': 110}, res[0])
        self.assertDictEqual({'id': 2, 'count': 2, 'sum': 70}, res[1])
        self.assertDictEqual({'id': 3, 'count': 1, 'sum': 30}, res[2])

    def test_groupby_argmax(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'argmax': ARGMAX('another', 'val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'argmax'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'argmax': 'f'}, res[0])
        self.assertDictEqual({'id': 2, 'argmax': 'e'}, res[1])
        self.assertDictEqual({'id': 3, 'argmax': 'c'}, res[2])

    def test_groupby_argmin(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'argmin': ARGMIN('another', 'val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'argmin'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'argmin': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'argmin': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'argmin': 'c'}, res[2])

    def test_groupby_max(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'max': MAX('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'max'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'max': 60}, res[0])
        self.assertDictEqual({'id': 2, 'max': 50}, res[1])
        self.assertDictEqual({'id': 3, 'max': 30}, res[2])

    def test_groupby_max_float(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'max': MAX('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'max'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'max': 60.0}, res[0])
        self.assertDictEqual({'id': 2, 'max': 50.0}, res[1])
        self.assertDictEqual({'id': 3, 'max': 30.0}, res[2])

    def test_groupby_max_str(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'max': MAX('val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'max'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'max': 'f'}, res[0])
        self.assertDictEqual({'id': 2, 'max': 'e'}, res[1])
        self.assertDictEqual({'id': 3, 'max': 'c'}, res[2])

    def test_groupby_min(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'min': MIN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'min'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'min': 10}, res[0])
        self.assertDictEqual({'id': 2, 'min': 20}, res[1])
        self.assertDictEqual({'id': 3, 'min': 30}, res[2])

    def test_groupby_min_float(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'min': MIN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'min'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'min': 10.0}, res[0])
        self.assertDictEqual({'id': 2, 'min': 20.0}, res[1])
        self.assertDictEqual({'id': 3, 'min': 30.0}, res[2])

    def test_groupby_min_str(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'min': MIN('val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'min'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'min': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'min': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'min': 'c'}, res[2])

    def test_groupby_mean(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'mean': MEAN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'mean'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'mean': 110.0 / 3.0}, res[0])
        self.assertDictEqual({'id': 2, 'mean': 70.0 / 2.0}, res[1])
        self.assertDictEqual({'id': 3, 'mean': 30.0}, res[2])

    def test_groupby_variance(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'variance': VARIANCE('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'variance'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertAlmostEqual(3800.0 / 9.0, res[0]['variance'])
        self.assertAlmostEqual(225.0, res[1]['variance'])
        self.assertDictEqual({'id': 3, 'variance': 0.0}, res[2])

    def test_groupby_stdv(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'stdv': STDV('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'stdv'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertAlmostEqual(math.sqrt(3800.0 / 9.0), res[0]['stdv'])
        self.assertAlmostEqual(math.sqrt(225.0), res[1]['stdv'])
        self.assertDictEqual({'id': 3, 'stdv': 0.0}, res[2])

    def test_groupby_select_one(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'select_one': SELECT_ONE('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'select_one'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'select_one': 60}, res[0])
        self.assertDictEqual({'id': 2, 'select_one': 50}, res[1])
        self.assertDictEqual({'id': 3, 'select_one': 30}, res[2])

    def test_groupby_select_one_float(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'select_one': SELECT_ONE('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'select_one'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'select_one': 60.0}, res[0])
        self.assertDictEqual({'id': 2, 'select_one': 50.0}, res[1])
        self.assertDictEqual({'id': 3, 'select_one': 30.0}, res[2])

    def test_groupby_select_one_str(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'select_one': SELECT_ONE('val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'select_one'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'select_one': 'f'}, res[0])
        self.assertDictEqual({'id': 2, 'select_one': 'e'}, res[1])
        self.assertDictEqual({'id': 3, 'select_one': 'c'}, res[2])

    def test_groupby_concat_list(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'concat': CONCAT('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'concat'], res.column_names())
        self.assertListEqual([int, list], res.column_types())
        self.assertDictEqual({'id': 1, 'concat': [10, 40, 60]}, res[0])
        self.assertDictEqual({'id': 2, 'concat': [20, 50]}, res[1])
        self.assertDictEqual({'id': 3, 'concat': [30]}, res[2])

    def test_groupby_concat_dict(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'concat': CONCAT('val', 'another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'concat'], res.column_names())
        self.assertListEqual([int, dict], res.column_types())
        self.assertDictEqual({'id': 1, 'concat': {'a': 10, 'd': 40, 'f': 60}}, res[0])
        self.assertDictEqual({'id': 2, 'concat': {'b': 20, 'e': 50}}, res[1])
        self.assertDictEqual({'id': 3, 'concat': {'c': 30}}, res[2])

    def test_groupby_quantile(self):
        # not implemented
        pass


class TestXFrameGroupbyAggregatorsWithMissingValues(XFrameUnitTestCase):
    """
    Tests XFrame groupby aggregators with missing values
    """

    def test_groupby_count(self):
        t = XFrame({'id': [1, 2, 3, None, None, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', COUNT)
        res = res.topk('id', reverse=True)
        self.assertEqualLen(4, res)
        self.assertListEqual(['id', 'count'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': None, 'count': 2}, res[0])
        self.assertDictEqual({'id': 1, 'count': 2}, res[1])
        self.assertDictEqual({'id': 2, 'count': 1}, res[2])
        self.assertDictEqual({'id': 3, 'count': 1}, res[3])

    def test_groupby_sum(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, None, None, 60]})
        res = t.groupby('id', SUM('another'))
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'sum'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'sum': 70}, res[0])
        self.assertDictEqual({'id': 2, 'sum': 20}, res[1])
        self.assertDictEqual({'id': 3, 'sum': 30}, res[2])

    def test_groupby_argmax(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, None, None]})
        res = t.groupby('id', {'argmax': ARGMAX('another', 'val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'argmax'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'argmax': 'd'}, res[0])
        self.assertDictEqual({'id': 2, 'argmax': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'argmax': 'c'}, res[2])

    def test_groupby_argmin(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, None, 30, 40, 50, 60]})
        res = t.groupby('id', {'argmin': ARGMIN('another', 'val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'argmin'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'argmin': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'argmin': 'e'}, res[1])
        self.assertDictEqual({'id': 3, 'argmin': 'c'}, res[2])

    def test_groupby_max(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, None, None, 60]})
        res = t.groupby('id', {'max': MAX('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'max'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'max': 60}, res[0])
        self.assertDictEqual({'id': 2, 'max': 20}, res[1])
        self.assertDictEqual({'id': 3, 'max': 30}, res[2])

    def test_groupby_max_float(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10.0, 20.0, 30.0, float('nan'), float('nan'), 60.0]})
        res = t.groupby('id', {'max': MAX('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'max'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'max': 60.0}, res[0])
        self.assertDictEqual({'id': 2, 'max': 20.0}, res[1])
        self.assertDictEqual({'id': 3, 'max': 30.0}, res[2])

    def test_groupby_max_str(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', None, None, 'f'],
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'max': MAX('val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'max'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'max': 'f'}, res[0])
        self.assertDictEqual({'id': 2, 'max': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'max': 'c'}, res[2])

    def test_groupby_min(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [None, None, 30, 40, 50, 60]})
        res = t.groupby('id', {'min': MIN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'min'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'min': 40}, res[0])
        self.assertDictEqual({'id': 2, 'min': 50}, res[1])
        self.assertDictEqual({'id': 3, 'min': 30}, res[2])

    def test_groupby_min_float(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [None, None, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'min': MIN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'min'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'min': 40.0}, res[0])
        self.assertDictEqual({'id': 2, 'min': 50.0}, res[1])
        self.assertDictEqual({'id': 3, 'min': 30.0}, res[2])

    def test_groupby_min_str(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': [None, None, 'c', 'd', 'e', 'f'],
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'min': MIN('val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'min'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'min': 'd'}, res[0])
        self.assertDictEqual({'id': 2, 'min': 'e'}, res[1])
        self.assertDictEqual({'id': 3, 'min': 'c'}, res[2])

    def test_groupby_mean(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, None, None, 60]})
        res = t.groupby('id', {'mean': MEAN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'mean'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'mean': 70.0 / 2.0}, res[0])
        self.assertDictEqual({'id': 2, 'mean': 20.0}, res[1])
        self.assertDictEqual({'id': 3, 'mean': 30.0}, res[2])

    def test_groupby_variance(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, None, None, 60]})
        res = t.groupby('id', {'variance': VARIANCE('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'variance'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertAlmostEqual(2500.0 / 4.0, res[0]['variance'])
        self.assertAlmostEqual(0.0, res[1]['variance'])
        self.assertDictEqual({'id': 3, 'variance': 0.0}, res[2])

    def test_groupby_stdv(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, None, None, 60]})
        res = t.groupby('id', {'stdv': STDV('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'stdv'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertAlmostEqual(math.sqrt(2500.0 / 4.0), res[0]['stdv'])
        self.assertAlmostEqual(math.sqrt(0.0), res[1]['stdv'])
        self.assertDictEqual({'id': 3, 'stdv': 0.0}, res[2])

    def test_groupby_select_one(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, None, None, 60]})
        res = t.groupby('id', {'select_one': SELECT_ONE('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'select_one'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'select_one': 60}, res[0])
        self.assertDictEqual({'id': 2, 'select_one': 20}, res[1])
        self.assertDictEqual({'id': 3, 'select_one': 30}, res[2])

    def test_groupby_concat_list(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, None, None, 60]})
        res = t.groupby('id', {'concat': CONCAT('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'concat'], res.column_names())
        self.assertListEqual([int, list], res.column_types())
        self.assertDictEqual({'id': 1, 'concat': [10, 60]}, res[0])
        self.assertDictEqual({'id': 2, 'concat': [20]}, res[1])
        self.assertDictEqual({'id': 3, 'concat': [30]}, res[2])

    def test_groupby_concat_dict(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', None, None, 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'concat': CONCAT('val', 'another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'concat'], res.column_names())
        self.assertListEqual([int, dict], res.column_types())
        self.assertDictEqual({'id': 1, 'concat': {'a': 10, 'f': 60}}, res[0])
        self.assertDictEqual({'id': 2, 'concat': {'b': 20}}, res[1])
        self.assertDictEqual({'id': 3, 'concat': {'c': 30}}, res[2])


class TestXFrameGroupbyAggregatorsEmpty(XFrameUnitTestCase):
    """
    Tests XFrame groupby aggregators with missing values
    """

    def test_groupby_count(self):
        t = XFrame({'id': [1, 2, None, None, None, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', COUNT)
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'count'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': None, 'count': 3}, res[0])
        self.assertDictEqual({'id': 1, 'count': 2}, res[1])
        self.assertDictEqual({'id': 2, 'count': 1}, res[2])

    def test_groupby_sum(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, None, None, None, 60]})
        res = t.groupby('id', SUM('another'))
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'sum'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'sum': 70}, res[0])
        self.assertDictEqual({'id': 2, 'sum': 20}, res[1])
        self.assertDictEqual({'id': 3, 'sum': None}, res[2])

    def test_groupby_argmax(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, None, 40, None, None]})
        res = t.groupby('id', {'argmax': ARGMAX('another', 'val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'argmax'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'argmax': 'd'}, res[0])
        self.assertDictEqual({'id': 2, 'argmax': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'argmax': None}, res[2])

    def test_groupby_argmin(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, None, None, 40, 50, 60]})
        res = t.groupby('id', {'argmin': ARGMIN('another', 'val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'argmin'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'argmin': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'argmin': 'e'}, res[1])
        self.assertDictEqual({'id': 3, 'argmin': None}, res[2])

    def test_groupby_max(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, None, None, None, 60]})
        res = t.groupby('id', {'max': MAX('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'max'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'max': 60}, res[0])
        self.assertDictEqual({'id': 2, 'max': 20}, res[1])
        self.assertDictEqual({'id': 3, 'max': None}, res[2])

    def test_groupby_max_float(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10.0, 20.0, None, float('nan'), float('nan'), 60.0]})
        res = t.groupby('id', {'max': MAX('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'max'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'max': 60.0}, res[0])
        self.assertDictEqual({'id': 2, 'max': 20.0}, res[1])
        self.assertDictEqual({'id': 3, 'max': None}, res[2])

    def test_groupby_max_str(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', None, None, None, 'f'],
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'max': MAX('val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'max'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'max': 'f'}, res[0])
        self.assertDictEqual({'id': 2, 'max': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'max': None}, res[2])

    def test_groupby_min(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [None, None, None, 40, 50, 60]})
        res = t.groupby('id', {'min': MIN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'min'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'min': 40}, res[0])
        self.assertDictEqual({'id': 2, 'min': 50}, res[1])
        self.assertDictEqual({'id': 3, 'min': None}, res[2])

    def test_groupby_min_float(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [None, None, None, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'min': MIN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'min'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'min': 40.0}, res[0])
        self.assertDictEqual({'id': 2, 'min': 50.0}, res[1])
        self.assertDictEqual({'id': 3, 'min': None}, res[2])

    def test_groupby_min_str(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': [None, None, None, 'd', 'e', 'f'],
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'min': MIN('val')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'min'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'min': 'd'}, res[0])
        self.assertDictEqual({'id': 2, 'min': 'e'}, res[1])
        self.assertDictEqual({'id': 3, 'min': None}, res[2])

    def test_groupby_mean(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, None, None, None, 60]})
        res = t.groupby('id', {'mean': MEAN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'mean'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertDictEqual({'id': 1, 'mean': 70.0 / 2.0}, res[0])
        self.assertDictEqual({'id': 2, 'mean': 20.0}, res[1])
        self.assertDictEqual({'id': 3, 'mean': None}, res[2])

    def test_groupby_variance(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, None, None, None, 60]})
        res = t.groupby('id', {'variance': VARIANCE('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'variance'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertAlmostEqual(2500.0 / 4.0, res[0]['variance'])
        self.assertAlmostEqual(0.0, res[1]['variance'])
        self.assertDictEqual({'id': 3, 'variance': None}, res[2])

    def test_groupby_stdv(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, None, None, None, 60]})
        res = t.groupby('id', {'stdv': STDV('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'stdv'], res.column_names())
        self.assertListEqual([int, float], res.column_types())
        self.assertAlmostEqual(math.sqrt(2500.0 / 4.0), res[0]['stdv'])
        self.assertAlmostEqual(math.sqrt(0.0), res[1]['stdv'])
        self.assertDictEqual({'id': 3, 'stdv': None}, res[2])

    def test_groupby_select_one(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, None, None, None, 60]})
        res = t.groupby('id', {'select_one': SELECT_ONE('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'select_one'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'select_one': 60}, res[0])
        self.assertDictEqual({'id': 2, 'select_one': 20}, res[1])
        self.assertDictEqual({'id': 3, 'select_one': None}, res[2])

    def test_groupby_concat_list(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'],
                    'another': [10, 20, None, None, None, 60]})
        res = t.groupby('id', {'concat': CONCAT('another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'concat'], res.column_names())
        self.assertListEqual([int, list], res.column_types())
        self.assertDictEqual({'id': 1, 'concat': [10, 60]}, res[0])
        self.assertDictEqual({'id': 2, 'concat': [20]}, res[1])
        self.assertDictEqual({'id': 3, 'concat': []}, res[2])

    def test_groupby_concat_dict(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1],
                    'val': ['a', 'b', None, None, None, 'f'],
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'concat': CONCAT('val', 'another')})
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'concat'], res.column_names())
        self.assertListEqual([int, dict], res.column_types())
        self.assertDictEqual({'id': 1, 'concat': {'a': 10, 'f': 60}}, res[0])
        self.assertDictEqual({'id': 2, 'concat': {'b': 20}}, res[1])
        self.assertDictEqual({'id': 3, 'concat': None}, res[2])


class TestXFrameJoin(XFrameUnitTestCase):
    """
    Tests XFrame join
    """

    def test_join(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2).sort('id').head()
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'doubled': 'aa'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b', 'doubled': 'bb'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c', 'doubled': 'cc'}, res[2])

    def test_join_rename(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'val': ['aa', 'bb', 'cc']})
        res = t1.join(t2, on='id').sort('id').head()
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val', 'val.1'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'val.1': 'aa'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b', 'val.1': 'bb'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c', 'val.1': 'cc'}, res[2])

    def test_join_compound_key(self):
        t1 = XFrame({'id1': [1, 2, 3], 'id2': [10, 20, 30], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id1': [1, 2, 3], 'id2': [10, 20, 30], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2).sort('id1').head()
        self.assertEqualLen(3, res)
        self.assertListEqual(['id1', 'id2', 'val', 'doubled'], res.column_names())
        self.assertListEqual([int, int, str, str], res.column_types())
        self.assertDictEqual({'id1': 1, 'id2': 10, 'val': 'a', 'doubled': 'aa'}, res[0])
        self.assertDictEqual({'id1': 2, 'id2': 20, 'val': 'b', 'doubled': 'bb'}, res[1])
        self.assertDictEqual({'id1': 3, 'id2': 30, 'val': 'c', 'doubled': 'cc'}, res[2])

    def test_join_dict_key(self):
        t1 = XFrame({'id1': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id2': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2, on={'id1': 'id2'}).sort('id1').head()
        self.assertEqualLen(3, res)
        self.assertListEqual(['id1', 'val', 'doubled'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())
        self.assertDictEqual({'id1': 1, 'val': 'a', 'doubled': 'aa'}, res[0])
        self.assertDictEqual({'id1': 2, 'val': 'b', 'doubled': 'bb'}, res[1])
        self.assertDictEqual({'id1': 3, 'val': 'c', 'doubled': 'cc'}, res[2])

    def test_join_partial(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 4], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2).sort('id').head()
        self.assertEqualLen(2, res)
        self.assertListEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'doubled': 'aa'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b', 'doubled': 'bb'}, res[1])

    def test_join_empty(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [4, 5, 6], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2).head()
        self.assertEqualLen(0, res)
        self.assertListEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())

    def test_join_on_val(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20, 30], 'val': ['a', 'b', 'c']})
        res = t1.join(t2, on='val').sort('id').head()
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val', 'id.1'], res.column_names())
        self.assertListEqual([int, str, int], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'id.1': 10}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b', 'id.1': 20}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c', 'id.1': 30}, res[2])

    def test_join_inner(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 4], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2, how='inner').sort('id').head()
        self.assertEqualLen(2, res)
        self.assertListEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'doubled': 'aa'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b', 'doubled': 'bb'}, res[1])

    def test_join_left(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 4], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2, how='left').sort('id').head()
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'doubled': 'aa'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b', 'doubled': 'bb'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c', 'doubled': None}, res[2])

    def test_join_right(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 4], 'doubled': ['aa', 'bb', 'dd']})
        res = t1.join(t2, how='right').sort('id').head()
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'doubled': 'aa'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b', 'doubled': 'bb'}, res[1])
        self.assertDictEqual({'id': 4, 'val': None, 'doubled': 'dd'}, res[2])

    def test_join_full(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 4], 'doubled': ['aa', 'bb', 'dd']})
        res = t1.join(t2, how='full').sort('id').head()
        self.assertEqualLen(4, res)
        self.assertListEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'doubled': 'aa'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b', 'doubled': 'bb'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c', 'doubled': None}, res[2])
        self.assertDictEqual({'id': 4, 'val': None, 'doubled': 'dd'}, res[3])

    def test_join_cartesian(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20, 30], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2, how='cartesian').sort(['id', 'id.1']).head()
        self.assertEqualLen(9, res)
        self.assertListEqual(['id', 'val', 'doubled', 'id.1'], res.column_names())
        self.assertListEqual([int, str, str, int], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a', 'doubled': 'aa', 'id.1': 10}, res[0])
        self.assertDictEqual({'id': 1, 'val': 'a', 'doubled': 'bb', 'id.1': 20}, res[1])
        self.assertDictEqual({'id': 2, 'val': 'b', 'doubled': 'aa', 'id.1': 10}, res[3])
        self.assertDictEqual({'id': 3, 'val': 'c', 'doubled': 'cc', 'id.1': 30}, res[8])

    def test_join_bad_how(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        with self.assertRaises(ValueError):
            t1.join(t2, how='xx')

    def test_join_bad_right(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            t1.join([1, 2, 3])

    def test_join_bad_on_list(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        with self.assertRaises(TypeError):
            t1.join(t2, on=['id', 1])

    def test_join_bad_on_type(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        with self.assertRaises(TypeError):
            t1.join(t2, on=1)

    def test_join_bad_on_col_name(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        with self.assertRaises(ValueError):
            t1.join(t2, on='xx')


class TestXFrameSplitDatetime(XFrameUnitTestCase):
    """
    Tests XFrame split_datetime
    """

    def test_split_datetime(self):
        t = XFrame({'id': [1, 2, 3], 'val': [datetime(2011, 1, 1),
                                             datetime(2012, 2, 2),
                                             datetime(2013, 3, 3)]})
        res = t.split_datetime('val')
        self.assertListEqual(['id',
                          'val.year', 'val.month', 'val.day',
                          'val.hour', 'val.minute', 'val.second'], res.column_names())
        self.assertListEqual([int, int, int, int, int, int, int], res.column_types())
        self.assertEqualLen(3, res)
        self.assertColumnEqual([1, 2, 3], res['id'])
        self.assertColumnEqual([2011, 2012, 2013], res['val.year'])
        self.assertColumnEqual([1, 2, 3], res['val.month'])
        self.assertColumnEqual([1, 2, 3], res['val.day'])
        self.assertColumnEqual([0, 0, 0], res['val.hour'])
        self.assertColumnEqual([0, 0, 0], res['val.minute'])
        self.assertColumnEqual([0, 0, 0], res['val.second'])

    def test_split_datetime_col_conflict(self):
        t = XFrame({'id': [1, 2, 3],
                    'val.year': ['x', 'y', 'z'],
                    'val': [datetime(2011, 1, 1),
                            datetime(2012, 2, 2),
                            datetime(2013, 3, 3)]})
        res = t.split_datetime('val', limit='year')
        self.assertListEqual(['id', 'val.year', 'val.year.1'], res.column_names())
        self.assertListEqual([int, str, int], res.column_types())
        self.assertEqualLen(3, res)
        self.assertColumnEqual([1, 2, 3], res['id'])
        self.assertColumnEqual(['x', 'y', 'z'], res['val.year'])
        self.assertColumnEqual([2011, 2012, 2013], res['val.year.1'])

    def test_split_datetime_bad_col(self):
        t = XFrame({'id': [1, 2, 3], 'val': [datetime(2011, 1, 1),
                                             datetime(2011, 2, 2),
                                             datetime(2011, 3, 3)]})
        with self.assertRaises(KeyError):
            t.split_datetime('xx')


class TestXFrameFilterby(XFrameUnitTestCase):
    """
    Tests XFrame filterby
    """

    def test_filterby_int_id(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.filterby(1, 'id').sort('id')
        self.assertEqualLen(1, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])

    def test_filterby_str_id(self):
        t = XFrame({'id': ['qaz', 'wsx', 'edc', 'rfv'], 'val': ['a', 'b', 'c', 'd']})
        res = t.filterby('qaz', 'id').sort('id')
        self.assertEqualLen(1, res)
        self.assertDictEqual({'id': 'qaz', 'val': 'a'}, res[0])

    def test_filterby_object_id(self):
        t = XFrame({'id': [datetime(2016, 2, 1, 0, 0),
                           datetime(2016, 2, 2, 0, 0),
                           datetime(2016, 2, 3, 0, 0),
                           datetime(2016, 2, 4, 0, 0)],
                    'val': ['a', 'b', 'c', 'd']})
        res = t.filterby(datetime(2016, 2, 1, 0, 0), 'id').sort('id')
        self.assertEqualLen(1, res)
        self.assertDictEqual({'id': datetime(2016, 2, 1, 0, 0), 'val': 'a'}, res[0])

    def test_filterby_list_id(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.filterby([1, 3], 'id').sort('id')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[1])

    def test_filterby_tuple_id(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.filterby((1, 3), 'id').sort('id')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[1])

    def test_filterby_iterable_id(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.filterby(xrange(3), 'id').sort('id')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])

    def test_filterby_set_id(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.filterby({1, 3}, 'id').sort('id')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[1])

    def test_filterby_list_val(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.filterby(['a', 'b'], 'val').sort('id')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertColumnEqual([1, 2], res['id'])

    def test_filterby_xarray(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        a = XArray([1, 3])
        res = t.filterby(a, 'id').sort('id')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[1])
        self.assertColumnEqual([1, 3], res['id'])

    def test_filterby_list_exclude(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.filterby([1, 3], 'id', exclude=True).sort('id')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[0])
        self.assertDictEqual({'id': 4, 'val': 'd'}, res[1])
        self.assertColumnEqual([2, 4], res['id'])

    def test_filterby_bad_column_type_list(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(TypeError):
            t.filterby([1, 3], 'val')

    def test_filterby_xarray_exclude(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        a = XArray([1, 3])
        res = t.filterby(a, 'id', exclude=True).sort('id')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[0])
        self.assertDictEqual({'id': 4, 'val': 'd'}, res[1])
        self.assertColumnEqual([2, 4], res['id'])

    def test_filterby_bad_column_name_type(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(TypeError):
            t.filterby([1, 3], 1)

    def test_filterby_bad_column_name(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(KeyError):
            t.filterby([1, 3], 'xx')

    def test_filterby_bad_column_type_xarray(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        a = XArray([1, 3])
        with self.assertRaises(TypeError):
            t.filterby(a, 'val')

    def test_filterby_bad_list_empty(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            t.filterby([], 'id').sort('id')

    def test_filterby_bad_xarray_empty(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        a = XArray([])
        with self.assertRaises(TypeError):
            t.filterby(a, 'val')


class TestXFramePackColumnsList(XFrameUnitTestCase):
    """
    Tests XFrame pack_columns into list
    """

    def test_pack_columns(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new')
        self.assertEqualLen(4, res)
        self.assertEqual(1, res.num_columns())
        self.assertListEqual([list], res.dtype())
        self.assertListEqual(['new'], res.column_names())
        self.assertDictEqual({'new': [1, 'a']}, res[0])
        self.assertDictEqual({'new': [2, 'b']}, res[1])

    def test_pack_columns_all(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.pack_columns(new_column_name='new')
        self.assertEqualLen(4, res)
        self.assertEqual(1, res.num_columns())
        self.assertListEqual([list], res.dtype())
        self.assertListEqual(['new'], res.column_names())
        self.assertDictEqual({'new': [1, 'a']}, res[0])
        self.assertDictEqual({'new': [2, 'b']}, res[1])

    def test_pack_columns_prefix(self):
        t = XFrame({'x.id': [1, 2, 3, 4], 'x.val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(column_prefix='x', new_column_name='new')
        self.assertEqualLen(4, res)
        self.assertEqual(2, res.num_columns())
        self.assertListEqual([int, list], res.dtype())
        self.assertListEqual(['another', 'new'], res.column_names())
        self.assertDictEqual({'another': 10, 'new': [1, 'a']}, res[0])
        self.assertDictEqual({'another': 20, 'new': [2, 'b']}, res[1])

    def test_pack_columns_rest(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new')
        self.assertEqualLen(4, res)
        self.assertEqual(2, res.num_columns())
        self.assertListEqual([int, list], res.dtype())
        self.assertListEqual(['another', 'new'], res.column_names())
        self.assertDictEqual({'another': 10, 'new': [1, 'a']}, res[0])
        self.assertDictEqual({'another': 20, 'new': [2, 'b']}, res[1])

    def test_pack_columns_na(self):
        t = XFrame({'id': [1, 2, None, 4], 'val': ['a', 'b', 'c', None]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', fill_na='x')
        self.assertEqualLen(4, res)
        self.assertEqual(1, res.num_columns())
        self.assertListEqual([list], res.dtype())
        self.assertListEqual(['new'], res.column_names())
        self.assertDictEqual({'new': [1, 'a']}, res[0])
        self.assertDictEqual({'new': [2, 'b']}, res[1])
        self.assertDictEqual({'new': ['x', 'c']}, res[2])
        self.assertDictEqual({'new': [4, 'x']}, res[3])

    def test_pack_columns_fill_na(self):
        t = XFrame({'id': [1, 2, None, 4], 'val': ['a', 'b', 'c', None]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', fill_na=99)
        self.assertEqualLen(4, res)
        self.assertEqual(1, res.num_columns())
        self.assertListEqual([list], res.dtype())
        self.assertListEqual(['new'], res.column_names())
        self.assertDictEqual({'new': [1, 'a']}, res[0])
        self.assertDictEqual({'new': [2, 'b']}, res[1])
        self.assertDictEqual({'new': [99, 'c']}, res[2])
        self.assertDictEqual({'new': [4, 99]}, res[3])

    def test_pack_columns_def_new_name(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.pack_columns(columns=['id', 'val'])
        self.assertEqualLen(4, res)
        self.assertEqual(1, res.num_columns())
        self.assertListEqual([list], res.dtype())
        self.assertListEqual(['X.0'], res.column_names())
        self.assertDictEqual({'X.0': [1, 'a']}, res[0])
        self.assertDictEqual({'X.0': [2, 'b']}, res[1])

    def test_pack_columns_prefix_def_new_name(self):
        t = XFrame({'x.id': [1, 2, 3, 4], 'x.val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(column_prefix='x')
        self.assertEqualLen(4, res)
        self.assertEqual(2, res.num_columns())
        self.assertListEqual([int, list], res.dtype())
        self.assertListEqual(['another', 'x'], res.column_names())
        self.assertDictEqual({'another': 10, 'x': [1, 'a']}, res[0])
        self.assertDictEqual({'another': 20, 'x': [2, 'b']}, res[1])

    def test_pack_columns_bad_col_spec(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            t.pack_columns(columns='id', column_prefix='val')

    def test_pack_columns_bad_col_prefix_type(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(TypeError):
            t.pack_columns(column_prefix=1)

    def test_pack_columns_bad_col_prefix(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            t.pack_columns(column_prefix='xx')

    def test_pack_columns_bad_cols(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            t.pack_columns(columns=['xx'])

    def test_pack_columns_bad_cols_dup(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            t.pack_columns(columns=['id', 'id'])

    def test_pack_columns_bad_cols_single(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            t.pack_columns(columns=['id'])

    def test_pack_columns_bad_dtype(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            t.pack_columns(columns=['id', 'val'], dtype=int)

    def test_pack_columns_bad_new_col_name_type(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(TypeError):
            t.pack_columns(columns=['id', 'val'], new_column_name=1)

    def test_pack_columns_bad_new_col_name_dup_rest(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd'], 'another': [11, 12, 13, 14]})
        with self.assertRaises(KeyError):
            t.pack_columns(columns=['id', 'val'], new_column_name='another')

    def test_pack_columns_good_new_col_name_dup_key(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='id')
        self.assertListEqual(['id'], res.column_names())
        self.assertDictEqual({'id': [1, 'a']}, res[0])
        self.assertDictEqual({'id': [2, 'b']}, res[1])


class TestXFramePackColumnsDict(XFrameUnitTestCase):
    """
    Tests XFrame pack_columns into dict
    """

    def test_pack_columns(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=dict)
        self.assertEqualLen(4, res)
        self.assertEqual(1, res.num_columns())
        self.assertListEqual([dict], res.dtype())
        self.assertListEqual(['new'], res.column_names())
        self.assertDictEqual({'new': {'id': 1, 'val': 'a'}}, res[0])
        self.assertDictEqual({'new': {'id': 2, 'val': 'b'}}, res[1])

    def test_pack_columns_prefix(self):
        t = XFrame({'x.id': [1, 2, 3, 4], 'x.val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(column_prefix='x', dtype=dict)
        self.assertEqualLen(4, res)
        self.assertEqual(2, res.num_columns())
        self.assertListEqual([int, dict], res.dtype())
        self.assertListEqual(['another', 'x'], res.column_names())
        self.assertDictEqual({'another': 10, 'x': {'id': 1, 'val': 'a'}}, res[0])
        self.assertDictEqual({'another': 20, 'x': {'id': 2, 'val': 'b'}}, res[1])

    def test_pack_columns_prefix_named(self):
        t = XFrame({'x.id': [1, 2, 3, 4], 'x.val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(column_prefix='x', dtype=dict, new_column_name='new')
        self.assertEqualLen(4, res)
        self.assertEqual(2, res.num_columns())
        self.assertListEqual([int, dict], res.dtype())
        self.assertListEqual(['another', 'new'], res.column_names())
        self.assertDictEqual({'another': 10, 'new': {'id': 1, 'val': 'a'}}, res[0])
        self.assertDictEqual({'another': 20, 'new': {'id': 2, 'val': 'b'}}, res[1])

    def test_pack_columns_prefix_no_remove(self):
        t = XFrame({'x.id': [1, 2, 3, 4], 'x.val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(column_prefix='x', dtype=dict, remove_prefix=False)
        self.assertEqualLen(4, res)
        self.assertEqual(2, res.num_columns())
        self.assertListEqual([int, dict], res.dtype())
        self.assertListEqual(['another', 'x'], res.column_names())
        self.assertDictEqual({'another': 10, 'x': {'x.id': 1, 'x.val': 'a'}}, res[0])
        self.assertDictEqual({'another': 20, 'x': {'x.id': 2, 'x.val': 'b'}}, res[1])

    def test_pack_columns_drop_missing(self):
        t = XFrame({'id': [1, 2, None, 4], 'val': ['a', 'b', 'c', None]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=dict)
        self.assertEqualLen(4, res)
        self.assertEqual(1, res.num_columns())
        self.assertListEqual([dict], res.dtype())
        self.assertListEqual(['new'], res.column_names())
        self.assertDictEqual({'new': {'id': 1, 'val': 'a'}}, res[0])
        self.assertDictEqual({'new': {'id': 2, 'val': 'b'}}, res[1])
        self.assertDictEqual({'new': {'val': 'c'}}, res[2])
        self.assertDictEqual({'new': {'id': 4}}, res[3])

    def test_pack_columns_fill_na(self):
        t = XFrame({'id': [1, 2, None, 4], 'val': ['a', 'b', 'c', None]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=dict, fill_na=99)
        self.assertEqualLen(4, res)
        self.assertEqual(1, res.num_columns())
        self.assertListEqual([dict], res.dtype())
        self.assertListEqual(['new'], res.column_names())
        self.assertDictEqual({'new': {'id': 1, 'val': 'a'}}, res[0])
        self.assertDictEqual({'new': {'id': 2, 'val': 'b'}}, res[1])
        self.assertDictEqual({'new': {'id': 99, 'val': 'c'}}, res[2])
        self.assertDictEqual({'new': {'id': 4, 'val': 99}}, res[3])


class TestXFramePackColumnsArray(XFrameUnitTestCase):
    """
    Tests XFrame pack_columns into array
    """

    def test_pack_columns(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [10, 20, 30, 40]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=array.array)
        self.assertEqualLen(4, res)
        self.assertEqual(1, res.num_columns())
        self.assertListEqual([array.array], res.dtype())
        self.assertListEqual(['new'], res.column_names())
        self.assertDictEqual({'new': array.array('d', [1.0, 10.0])}, res[0])
        self.assertDictEqual({'new': array.array('d', [2.0, 20.0])}, res[1])

    def test_pack_columns_bad_fill_na_not_numeric(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [10, 20, 30, 40]})
        with self.assertRaises(ValueError):
            t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=array.array, fill_na='a')

    def test_pack_columns_bad_not_numeric(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a' 'b' 'c', 'd']})
        with self.assertRaises(TypeError):
            t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=array.array)


class TestXFrameUnpackList(XFrameUnitTestCase):
    """
    Tests XFrame unpack where the unpacked column contains a list
    """

    def test_unpack(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [[10, 'a'], [20, 'b'], [30, 'c'], [40, 'd']]})
        res = t.unpack('val')
        self.assertEqualLen(4, res)
        self.assertListEqual(['id', 'val.0', 'val.1'], res.column_names())
        self.assertListEqual([int, int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val.0': 10, 'val.1': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val.0': 20, 'val.1': 'b'}, res[1])

    def test_unpack_prefix(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [[10, 'a'], [20, 'b'], [30, 'c'], [40, 'd']]})
        res = t.unpack('val', column_name_prefix='x')
        self.assertEqualLen(4, res)
        self.assertListEqual(['id', 'x.0', 'x.1'], res.column_names())
        self.assertListEqual([int, int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'x.0': 10, 'x.1': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'x.0': 20, 'x.1': 'b'}, res[1])

    def test_unpack_types(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [[10, 'a'], [20, 'b'], [30, 'c'], [40, 'd']]})
        res = t.unpack('val', column_types=[str, str])
        self.assertEqualLen(4, res)
        self.assertListEqual(['id', 'val.0', 'val.1'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val.0': '10', 'val.1': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val.0': '20', 'val.1': 'b'}, res[1])

    def test_unpack_na_value(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [[10, 'a'], [20, 'b'], [None, 'c'], [40, None]]})
        res = t.unpack('val', na_value=99)
        self.assertEqualLen(4, res)
        self.assertListEqual(['id', 'val.0', 'val.1'], res.column_names())
        self.assertListEqual([int, int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val.0': 10, 'val.1': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val.0': 20, 'val.1': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val.0': 99, 'val.1': 'c'}, res[2])
        self.assertDictEqual({'id': 4, 'val.0': 40, 'val.1': '99'}, res[3])

    def test_unpack_limit(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [[10, 'a'], [20, 'b'], [30, 'c'], [40, 'd']]})
        res = t.unpack('val', limit=[1])
        self.assertEqualLen(4, res)
        self.assertListEqual(['id', 'val.1'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val.1': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val.1': 'b'}, res[1])


class TestXFrameUnpackDict(XFrameUnitTestCase):
    """
    Tests XFrame unpack where the unpacked column contains a dict
    """

    def test_unpack(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 1}, {'b': 2}, {'c': 3}, {'d': 4}]})
        res = t.unpack('val')
        self.assertEqualLen(4, res)
        self.assertListEqual(['id', 'val.a', 'val.c', 'val.b', 'val.d'], res.column_names())
        self.assertListEqual([int, int, int, int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'val.a': 1, 'val.c': None, 'val.b': None, 'val.d': None}, res[0])
        self.assertDictEqual({'id': 2, 'val.a': None, 'val.c': None, 'val.b': 2, 'val.d': None}, res[1])

    def test_unpack_mult(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        res = t.unpack('val')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val.a', 'val.b'], res.column_names())
        self.assertListEqual([int, int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'val.a': 1, 'val.b': None}, res[0])
        self.assertDictEqual({'id': 2, 'val.a': None, 'val.b': 2}, res[1])
        self.assertDictEqual({'id': 3, 'val.a': 1, 'val.b': 2}, res[2])

    def test_unpack_prefix(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        res = t.unpack('val', column_name_prefix='x')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'x.a', 'x.b'], res.column_names())
        self.assertListEqual([int, int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'x.a': 1, 'x.b': None}, res[0])
        self.assertDictEqual({'id': 2, 'x.a': None, 'x.b': 2}, res[1])
        self.assertDictEqual({'id': 3, 'x.a': 1, 'x.b': 2}, res[2])

    def test_unpack_types(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        res = t.unpack('val', column_types=[str, str], limit=['a', 'b'])
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val.a', 'val.b'], res.column_names())
        self.assertListEqual([int, str, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val.a': '1', 'val.b': None}, res[0])
        self.assertDictEqual({'id': 2, 'val.a': None, 'val.b': '2'}, res[1])
        self.assertDictEqual({'id': 3, 'val.a': '1', 'val.b': '2'}, res[2])

    def test_unpack_na_value(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        res = t.unpack('val', na_value=99)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val.a', 'val.b'], res.column_names())
        self.assertListEqual([int, int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'val.a': 1, 'val.b': 99}, res[0])
        self.assertDictEqual({'id': 2, 'val.a': 99, 'val.b': 2}, res[1])
        self.assertDictEqual({'id': 3, 'val.a': 1, 'val.b': 2}, res[2])

    def test_unpack_limit(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        res = t.unpack('val', limit=['b'])
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val.b'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertDictEqual({'id': 1, 'val.b': None}, res[0])
        self.assertDictEqual({'id': 2, 'val.b': 2}, res[1])
        self.assertDictEqual({'id': 3, 'val.b': 2}, res[2])

    def test_unpack_bad_types_no_limit(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        with self.assertRaises(ValueError):
            t.unpack('val', column_types=[str, str])


# TODO unpack array

class TestXFrameStackList(XFrameUnitTestCase):
    """
    Tests XFrame stack where column is a list
    """

    def test_stack_list(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        res = t.stack('val')
        self.assertListEqual(['id', 'X'], res.column_names())
        self.assertEqualLen(9, res)
        self.assertDictEqual({'id': 1, 'X': 'a1'}, res[0])
        self.assertDictEqual({'id': 3, 'X': None}, res[8])

    def test_stack_list_drop_na(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        res = t.stack('val', drop_na=True)
        self.assertListEqual(['id', 'X'], res.column_names())
        self.assertEqualLen(8, res)
        self.assertDictEqual({'id': 1, 'X': 'a1'}, res[0])
        self.assertDictEqual({'id': 3, 'X': 'c3'}, res[7])

    def test_stack_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        res = t.stack('val', new_column_name='flat_val')
        self.assertListEqual(['id', 'flat_val'], res.column_names())
        self.assertEqualLen(9, res)

    def test_stack_bad_col_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        with self.assertRaises(ValueError):
            t.stack('xx')

    def test_stack_bad_col_value(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            t.stack('val')

    def test_stack_bad_new_col_name_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            t.stack('val', new_column_name=1)

    def test_stack_new_col_name_dup_ok(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        res = t.stack('val', new_column_name='val')
        self.assertListEqual(['id', 'val'], res.column_names())

    def test_stack_bad_new_col_name_dup(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        with self.assertRaises(ValueError):
            t.stack('val', new_column_name='id')

    def test_stack_bad_no_data(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        t = t.head(0)
        with self.assertRaises(ValueError):
            t.stack('val', new_column_name='val')


class TestXFrameStackDict(XFrameUnitTestCase):
    """
    Tests XFrame stack where column is a dict
    """

    def test_stack_dict(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        res = t.stack('val')
        self.assertListEqual(['id', 'K', 'V'], res.column_names())
        self.assertEqualLen(7, res)
        self.assertDictEqual({'id': 1, 'K': 'a', 'V': 3}, res[0])
        self.assertDictEqual({'id': 4, 'K': None, 'V': None}, res[6])

    def test_stack_names(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        res = t.stack('val', ['new_k', 'new_v'])
        self.assertListEqual(['id', 'new_k', 'new_v'], res.column_names())
        self.assertEqualLen(7, res)
        self.assertDictEqual({'id': 1, 'new_k': 'a', 'new_v': 3}, res[0])
        self.assertDictEqual({'id': 4, 'new_k': None, 'new_v': None}, res[6])

    def test_stack_dropna(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        res = t.stack('val', drop_na=True)
        self.assertListEqual(['id', 'K', 'V'], res.column_names())
        self.assertEqualLen(6, res)
        self.assertDictEqual({'id': 1, 'K': 'a', 'V': 3}, res[0])
        self.assertDictEqual({'id': 3, 'K': 'd', 'V': 3}, res[5])

    def test_stack_bad_col_name(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        with self.assertRaises(ValueError):
            t.stack('xx')

    def test_stack_bad_new_col_name_type(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        with self.assertRaises(TypeError):
            t.stack('val', new_column_name=1)

    def test_stack_bad_new_col_name_len(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        with self.assertRaises(TypeError):
            t.stack('val', new_column_name=['a'])

    def test_stack_bad_new_col_name_dup(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        with self.assertRaises(ValueError):
            t.stack('val', new_column_name=['id', 'xx'])

    def test_stack_bad_no_data(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        t = t.head(0)
        with self.assertRaises(ValueError):
            t.stack('val', new_column_name=['k', 'v'])


class TestXFrameUnstackList(XFrameUnitTestCase):
    """
    Tests XFrame unstack where unstack column is list
    """

    def test_unstack(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1, 3], 'val': ['a1', 'b1', 'c1', 'a2', 'b2', 'a3', 'c3']})
        res = t.unstack('val')
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'unstack'], res.column_names())
        self.assertListEqual([int, list], res.column_types())
        self.assertDictEqual({'id': 1, 'unstack': ['a1', 'a2', 'a3']}, res[0])
        self.assertDictEqual({'id': 2, 'unstack': ['b1', 'b2']}, res[1])
        self.assertDictEqual({'id': 3, 'unstack': ['c1', 'c3']}, res[2])

    def test_unstack_name(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1, 3], 'val': ['a1', 'b1', 'c1', 'a2', 'b2', 'a3', 'c3']})
        res = t.unstack('val', new_column_name='vals')
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'vals'], res.column_names())
        self.assertListEqual([int, list], res.column_types())
        self.assertDictEqual({'id': 1, 'vals': ['a1', 'a2', 'a3']}, res[0])
        self.assertDictEqual({'id': 2, 'vals': ['b1', 'b2']}, res[1])
        self.assertDictEqual({'id': 3, 'vals': ['c1', 'c3']}, res[2])


class TestXFrameUnstackDict(XFrameUnitTestCase):
    """
    Tests XFrame unstack where unstack column is dict
    """

    # untested -- test after groupby
    def test_unstack(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1, 3],
                    'key': ['ka1', 'kb1', 'kc1', 'ka2', 'kb2', 'ka3', 'kc3'],
                    'val': ['a1', 'b1', 'c1', 'a2', 'b2', 'a3', 'c3']})
        res = t.unstack(['key', 'val'])
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'unstack'], res.column_names())
        self.assertListEqual([int, dict], res.column_types())
        self.assertDictEqual({'id': 1, 'unstack': {'ka1': 'a1', 'ka2': 'a2', 'ka3': 'a3'}}, res[0])
        self.assertDictEqual({'id': 2, 'unstack': {'kb1': 'b1', 'kb2': 'b2'}}, res[1])
        self.assertDictEqual({'id': 3, 'unstack': {'kc1': 'c1', 'kc3': 'c3'}}, res[2])

    def test_unstack_name(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1, 3],
                    'key': ['ka1', 'kb1', 'kc1', 'ka2', 'kb2', 'ka3', 'kc3'],
                    'val': ['a1', 'b1', 'c1', 'a2', 'b2', 'a3', 'c3']})
        res = t.unstack(['key', 'val'], new_column_name='vals')
        res = res.topk('id', reverse=True)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'vals'], res.column_names())
        self.assertListEqual([int, dict], res.column_types())
        self.assertDictEqual({'id': 1, 'vals': {'ka1': 'a1', 'ka2': 'a2', 'ka3': 'a3'}}, res[0])
        self.assertDictEqual({'id': 2, 'vals': {'kb1': 'b1', 'kb2': 'b2'}}, res[1])
        self.assertDictEqual({'id': 3, 'vals': {'kc1': 'c1', 'kc3': 'c3'}}, res[2])


class TestXFrameUnique(XFrameUnitTestCase):
    """
    Tests XFrame unique
    """

    def test_unique_noop(self):
        t = XFrame({'id': [3, 2, 1], 'val': ['c', 'b', 'a']})
        res = t.unique()
        self.assertEqualLen(3, res)

    def test_unique(self):
        t = XFrame({'id': [3, 2, 1, 1], 'val': ['c', 'b', 'a', 'a']})
        res = t.unique()
        self.assertEqualLen(3, res)

    def test_unique_part(self):
        t = XFrame({'id': [3, 2, 1, 1], 'val': ['c', 'b', 'a', 'x']})
        res = t.unique()
        self.assertEqualLen(4, res)


class TestXFrameSort(XFrameUnitTestCase):
    """
    Tests XFrame sort
    """

    def test_sort(self):
        t = XFrame({'id': [3, 2, 1], 'val': ['c', 'b', 'a']})
        res = t.sort('id')
        self.assertColumnEqual([1, 2, 3], res['id'])
        self.assertColumnEqual(['a', 'b', 'c'], res['val'])

    def test_sort_descending(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.sort('id', ascending=False)
        self.assertColumnEqual([3, 2, 1], res['id'])
        self.assertColumnEqual(['c', 'b', 'a'], res['val'])

    def test_sort_multi_col(self):
        t = XFrame({'id': [3, 2, 1, 1], 'val': ['c', 'b', 'b', 'a']})
        res = t.sort(['id', 'val'])
        self.assertColumnEqual([1, 1, 2, 3], res['id'])
        self.assertColumnEqual(['a', 'b', 'b', 'c'], res['val'])

    def test_sort_multi_col_asc_desc(self):
        t = XFrame({'id': [3, 2, 1, 1], 'val': ['c', 'b', 'b', 'a']})
        res = t.sort([('id', True), ('val', False)])
        self.assertColumnEqual([1, 1, 2, 3], res['id'])
        self.assertColumnEqual(['b', 'a', 'b', 'c'], res['val'])


class TestXFrameDropna(XFrameUnitTestCase):
    """
    Tests XFrame dropna
    """

    def test_dropna_no_drop(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.dropna()
        self.assertEqualLen(3, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_dropna_none(self):
        t = XFrame({'id': [1, None, 3], 'val': ['a', 'b', 'c']})
        res = t.dropna()
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[1])

    def test_dropna_nan(self):
        t = XFrame({'id': [1.0, float('nan'), 3.0], 'val': ['a', 'b', 'c']})
        res = t.dropna()
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1.0, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 3.0, 'val': 'c'}, res[1])

    def test_dropna_float_none(self):
        t = XFrame({'id': [1.0, None, 3.0], 'val': ['a', 'b', 'c']})
        res = t.dropna()
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1.0, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 3.0, 'val': 'c'}, res[1])

    def test_dropna_empty_list(self):
        t = XFrame({'id': [1, None, 3], 'val': ['a', 'b', 'c']})
        res = t.dropna(columns=[])
        self.assertEqualLen(3, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': None, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_dropna_any(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', None, 'c']})
        res = t.dropna()
        self.assertEqualLen(1, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])

    def test_dropna_all(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', None, 'c']})
        res = t.dropna(how='all')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': None, 'val': 'c'}, res[1])

    def test_dropna_col_val(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', None, 'c']})
        res = t.dropna(columns='val')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': None, 'val': 'c'}, res[1])

    def test_dropna_col_id(self):
        t = XFrame({'id': [1, 2, None], 'val': ['a', None, 'c']})
        res = t.dropna(columns='id')
        self.assertEqualLen(2, res)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': None}, res[1])

    def test_dropna_bad_col_arg(self):
        t = XFrame({'id': [1, 2, None], 'val': ['a', None, 'c']})
        with self.assertRaises(TypeError):
            t.dropna(columns=1)

    def test_dropna_bad_col_name_in_list(self):
        t = XFrame({'id': [1, 2, None], 'val': ['a', None, 'c']})
        with self.assertRaises(TypeError):
            t.dropna(columns=['id', 2])

    def test_dropna_bad_how(self):
        t = XFrame({'id': [1, 2, None], 'val': ['a', None, 'c']})
        with self.assertRaises(ValueError):
            t.dropna(how='xx')


class TestXFrameDropnaSplit(XFrameUnitTestCase):
    """
    Tests XFrame dropna_split
    """

    def test_dropna_split_no_drop(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res1, res2 = t.dropna_split()
        self.assertEqualLen(3, res1)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res1[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res1[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res1[2])
        self.assertEqualLen(0, res2)

    def test_dropna_split_none(self):
        t = XFrame({'id': [1, None, 3], 'val': ['a', 'b', 'c']})
        res1, res2 = t.dropna_split()
        self.assertEqualLen(2, res1)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res1[0])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res1[1])
        self.assertEqualLen(1, res2)
        self.assertDictEqual({'id': None, 'val': 'b'}, res2[0])

    def test_dropna_split_all(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', None, 'c']})
        res1, res2 = t.dropna_split(how='all')
        self.assertEqualLen(2, res1)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res1[0])
        self.assertDictEqual({'id': None, 'val': 'c'}, res1[1])
        self.assertEqualLen(1, res2)
        self.assertDictEqual({'id': None, 'val': None}, res2[0])


class TestXFrameFillna(XFrameUnitTestCase):
    """
    Tests XFrame fillna
    """

    def test_fillna(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', 'b', 'c']})
        res = t.fillna('id', 0)
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 0, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 0, 'val': 'c'}, res[2])

    def test_fillna_bad_col_name(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', 'b', 'c']})
        with self.assertRaises(ValueError):
            t.fillna('xx', 0)

    def test_fillna_bad_arg_type(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            t.fillna(1, 0)


class TestXFrameAddRowNumber(XFrameUnitTestCase):
    """
    Tests XFrame add_row_number
    """

    def test_add_row_number(self):
        t = XFrame({'ident': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.add_row_number()
        self.assertListEqual(['id', 'ident', 'val'], res.column_names())
        self.assertDictEqual({'id': 0, 'ident': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 1, 'ident': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 2, 'ident': 3, 'val': 'c'}, res[2])

    def test_add_row_number_start(self):
        t = XFrame({'ident': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.add_row_number(start=10)
        self.assertListEqual(['id', 'ident', 'val'], res.column_names())
        self.assertDictEqual({'id': 10, 'ident': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 11, 'ident': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 12, 'ident': 3, 'val': 'c'}, res[2])

    def test_add_row_number_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.add_row_number(column_name='row_number')
        self.assertListEqual(['row_number', 'id', 'val'], res.column_names())
        self.assertDictEqual({'row_number': 0, 'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'row_number': 1, 'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'row_number': 2, 'id': 3, 'val': 'c'}, res[2])


class TestXFrameShape(XFrameUnitTestCase):
    """
    Tests XFrame shape
    """

    def test_shape(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertEqual((3, 2), t.shape)

    def test_shape_empty(self):
        t = XFrame()
        self.assertEqual((0, 0), t.shape)


# noinspection SqlNoDataSourceInspection,SqlDialectInspection
class TestXFrameSql(XFrameUnitTestCase):
    """
    Tests XFrame sql
    """

    def test_sql(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.sql("SELECT * FROM xframe WHERE id > 1 ORDER BY id")
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[0])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[1])

    def test_sql_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.sql("SELECT * FROM tmp_tbl WHERE id > 1 ORDER BY id", table_name='tmp_tbl')
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[0])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[1])


if __name__ == '__main__':
    unittest.main()
