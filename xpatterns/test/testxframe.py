import unittest
import math
import copy
from datetime import datetime
import array
import pickle

# Check the spark configuration
import os
if not 'SPARK_HOME' in os.environ:
    print 'SPARK_HOME must be set'
spark_home = os.environ['SPARK_HOME']

# Set the python path
import sys
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(1, os.path.join(spark_home, 'python/lib/py4j-0.8.2.1-src.zip'))

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# python testxframe.py
# python -m unittest testxframe
# python -m unittest testxframe.TestXFrameConstructorLocal
# python -m unittest testxarray.TestXArrayConstructorLocal.test_construct_list_float_infer

from xpatterns import XArray
from xpatterns import XFrame
from xpatterns.aggregate import SUM, ARGMAX, ARGMIN, MAX, MIN, COUNT, AVG, MEAN, \
    VAR, VARIANCE, STD, STDV, SELECT_ONE, CONCAT, QUANTILE

def eq_array(expected, result):
    return (XArray(expected) == result).all()

class TestXFrameConstructor(unittest.TestCase):
    """
    Tests XFrame constructors that create data from local sources.
    """

    def test_construct_auto_dataframe(self):
        pass

    def test_construct_auto_str_csv(self):
        path = 'files/test-frame.csv'
        res = XFrame(path, verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_str_tsv(self):
        path = 'files/test-frame.tsv'
        res = XFrame(path, verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_str_psv(self):
        path = 'files/test-frame.psv'
        res = XFrame(path, verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_str_txt(self):
        # construct and XFrame given a text file
        # interpret as csv
        pass

    def test_construct_auto_str_xframe(self):
        # construct and XFrame given a file with unrecognized file extension
        # this refers to a graphlab internal file, and might be reinterpreted as reading from a 
        # spark internal checkpoint file
        pass

    def test_construct_xarray(self):
        # construct and XFrame given an XArray
        pass

    def test_construct_xframe(self):
        # construct an XFrame given another XFrame
        pass

    def test_construct_iteritems(self):
        # construct an XFrame from an object that has iteritems
        pass

    def test_construct_iter(self):
        # construct an XFrame from an object that has __iter__
        pass

    def test_construct_none(self):
        # construct an empty XFrame
        pass

    def test_construct_array(self):
        # construct an XFrame from an array
        pass

    def test_construct_dict_int(self):
        # construct an XFrame from a dict of int
        pass

    def test_construct_dict_float(self):
        # construct an XFrame from a dict of float
        pass

    def test_construct_dict_str(self):
        # construct an XFrame from a dict of str
        pass

    def test_construct_dict_int_str(self):
        # construct an XFrame from a dict of int and str
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertEqual(3, len(t))
        t = t.sort('id')
        self.assertTrue(eq_array([1, 2, 3], t['id']))
        self.assertEqual([int, str], t.column_types())
        self.assertEqual(['id', 'val'], t.column_names())

    def test_construct_binary(self):
        # make binary file
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame'
        t.save(path, format='binary')    ### File does not necessarily save in order
        res = XFrame(path).sort('id')    ### so let's sort after we read it back
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])

    def test_construct_rdd(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame(rdd)
        self.assertEqual(3, len(res))
        self.assertEqual({'X.0': 1, 'X.1': 'a'}, res[0])
        self.assertEqual({'X.0': 2, 'X.1': 'b'}, res[1])

    def test_construct_spark_dataframe(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        fields = [StructField('id', IntegerType(), True), StructField('val', StringType(), True)]
        schema = StructType(fields)
        sqlc = XFrame.spark_sql_context()
        s_rdd = sqlc.applySchema(rdd, schema)
        res = XFrame(s_rdd)
        self.assertEqual(3, len(res))
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])

class TestXFrameReadCsvWithErrors(unittest.TestCase):
    """
    Tests XFrame read_csv_with_errors
    """

    def test_read_csv_with_errors(self):
        pass

class TestXFrameReadCsv(unittest.TestCase):
    """
    Tests XFrame read_csv
    """

    def test_read_csv(self):
        path = 'files/test-frame.csv'
        res = XFrame.read_csv(path, verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_verbose(self):
        path = 'files/test-frame.csv'
        res = XFrame.read_csv(path)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_delim(self):
        path = 'files/test-frame.psv'
        res = XFrame.read_csv(path, delimiter='|', verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_no_header(self):
        path = 'files/test-frame-no-header.csv'
        res = XFrame.read_csv(path, header=False, verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['X.0', 'X.1'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'X.0': 1, 'X.1': 'a'}, res[0])
        self.assertEqual({'X.0': 2, 'X.1': 'b'}, res[1])
        self.assertEqual({'X.0': 3, 'X.1': 'c'}, res[2])

    def test_read_csv_comment(self):
        path = 'files/test-frame-comment.csv'
        res = XFrame.read_csv(path, comment_char='#', verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_escape(self):
        path = 'files/test-frame-escape.csv'
        res = XFrame.read_csv(path, verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a,a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b,b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c,c'}, res[2])

    def test_read_csv_escape_custom(self):
        path = 'files/test-frame-escape-custom.csv'
        res = XFrame.read_csv(path, escape_char='$', verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a,a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b,b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c,c'}, res[2])

    def test_read_csv_initial_space(self):
        path = 'files/test-frame-initial_space.csv'
        res = XFrame.read_csv(path, skip_initial_space=True, verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_hints_type(self):
        path = 'files/test-frame.csv'
        res = XFrame.read_csv(path, column_type_hints=str, verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([str, str], res.column_types())
        self.assertEqual({'id': '1', 'val': 'a'}, res[0])
        self.assertEqual({'id': '2', 'val': 'b'}, res[1])
        self.assertEqual({'id': '3', 'val': 'c'}, res[2])

    def test_read_csv_hints_list(self):
        path = 'files/test-frame-extra.csv'
        res = XFrame.read_csv(path, column_type_hints=[str, str, int], verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val1', 'val2'], res.column_names())
        self.assertEqual([str, str, int], res.column_types())
        self.assertEqual({'id': '1', 'val1': 'a', 'val2': 10}, res[0])
        self.assertEqual({'id': '2', 'val1': 'b', 'val2': 20}, res[1])
        self.assertEqual({'id': '3', 'val1': 'c', 'val2': 30}, res[2])

    def test_read_csv_hints_dict(self):
        path = 'files/test-frame-extra.csv'
        res = XFrame.read_csv(path, column_type_hints={'val2': int}, verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val1', 'val2'], res.column_names())
        self.assertEqual([str, str, int], res.column_types())
        self.assertEqual({'id': '1', 'val1': 'a', 'val2': 10}, res[0])
        self.assertEqual({'id': '2', 'val1': 'b', 'val2': 20}, res[1])
        self.assertEqual({'id': '3', 'val1': 'c', 'val2': 30}, res[2])

    def test_read_csv_na(self):
        path = 'files/test-frame-na.csv'
        res = XFrame.read_csv(path, na_values='None', verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'NA'}, res[0])
        self.assertEqual({'id': None, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_csv_na_mult(self):
        path = 'files/test-frame-na.csv'
        res = XFrame.read_csv(path, na_values=['NA', 'None'], verbose=False)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': None}, res[0])
        self.assertEqual({'id': None, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

class TestXFrameReadParquet(unittest.TestCase):
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
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_read_parquet_bool(self):
        t = XFrame({'id': [1, 2, 3], 'val': [True, False, True]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('id')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, bool], res.column_types())
        self.assertEqual({'id': 1, 'val': True}, res[0])
        self.assertEqual({'id': 2, 'val': False}, res[1])
        self.assertEqual({'id': 3, 'val': True}, res[2])

    def test_read_parquet_int(self):
        t = XFrame({'id': [1, 2, 3], 'val': [10, 20, 30]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('id')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, int], res.column_types())
        self.assertEqual({'id': 1, 'val': 10}, res[0])
        self.assertEqual({'id': 2, 'val': 20}, res[1])
        self.assertEqual({'id': 3, 'val': 30}, res[2])

    def test_read_parquet_float(self):
        t = XFrame({'id': [1, 2, 3], 'val': [1.0, 2.0, 3.0]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('id')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, float], res.column_types())
        self.assertEqual({'id': 1, 'val': 1.0}, res[0])
        self.assertEqual({'id': 2, 'val': 2.0}, res[1])
        self.assertEqual({'id': 3, 'val': 3.0}, res[2])

    def test_read_parquet_list(self):
        t = XFrame({'id': [1, 2, 3], 'val': [[1, 1], [2, 2], [3, 3]]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('id')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, list], res.column_types())
        self.assertEqual({'id': 1, 'val': [1, 1]}, res[0])
        self.assertEqual({'id': 2, 'val': [2, 2]}, res[1])
        self.assertEqual({'id': 3, 'val': [3, 3]}, res[2])

    def test_read_parquet_dict(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{1: 1}, {2: 2}, {3: 3}]})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')

        res = XFrame('tmp/frame-parquet.parquet')
        res = res.sort('id')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, dict], res.column_types())
        self.assertEqual({'id': 1, 'val': {1: 1}}, res[0])
        self.assertEqual({'id': 2, 'val': {2: 2}}, res[1])
        self.assertEqual({'id': 3, 'val': {3: 3}}, res[2])

class TestXFrameToSparkDataFrame(unittest.TestCase):
    """
    Tests XFrame to_spark_dataframe
    """

    def test_to_spark_dataframe_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        srdd = t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertEqual('a', row.val)

    def test_to_spark_dataframe_bool(self):
        t = XFrame({'id': [1, 2, 3], 'val': [True, False, True]})
        srdd = t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertEqual(True, row.val)

    def test_to_spark_dataframe_float(self):
        t = XFrame({'id': [1, 2, 3], 'val': [1.0, 2.0, 3.0]})
        srdd = t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertEqual(1.0, row.val)

    def test_to_spark_dataframe_int(self):
        t = XFrame({'id': [1, 2, 3], 'val': [1, 2, 3]})
        srdd = t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertEqual(1, row.val)

    def test_to_spark_dataframe_list(self):
        t = XFrame({'id': [1, 2, 3], 'val': [[1, 1],  [2, 2], [3, 3]]})
        srdd = t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        self.assertEqual([1, 1], row.val)

    def test_to_spark_dataframe_list_bad(self):
        t = XFrame({'id': [1, 2, 3], 'val': [[[1], 1],  [[2], 2], [[3], 3]]})
        with self.assertRaises(ValueError):
            srdd = t.to_spark_dataframe('tmp_tbl')

    def test_to_spark_dataframe_map(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'x': 1},  {'y': 2}, {'z': 3}]})
        srdd = t.to_spark_dataframe('tmp_tbl')
        sqlc = XFrame.spark_sql_context()
        results = sqlc.sql('SELECT * FROM tmp_tbl ORDER BY id')
        self.assertEqual(3, results.count())
        row = results.collect()[0]
        self.assertEqual(1, row.id)
        expected = {'x': 1}
        self.assertEqual(expected, row.val)

    def test_to_spark_dataframe_map_bad(self):
        t = XFrame({'id': [1, 2, 3], 'val': [None,  {'y': 2}, {'z': 3}]})
        with self.assertRaises(ValueError):
            srdd = t.to_spark_dataframe('tmp_tbl')

class TestXFrameToRdd(unittest.TestCase):
    """
    Tests XFrame to_rdd
    """

    def test_to_rdd(self):
        pass

class TestXFrameFromRdd(unittest.TestCase):
    """
    Tests XFrame from_rdd with regular rdd
    """

    def test_from_rdd(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame.from_rdd(rdd)
        self.assertEqual(3, len(res))
        self.assertEqual({'X.0': 1, 'X.1': 'a'}, res[0])
        self.assertEqual({'X.0': 2, 'X.1': 'b'}, res[1])

    def test_from_rdd_names(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame.from_rdd(rdd, column_names=('id', 'val'))
        self.assertEqual(3, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])

    def test_from_rdd_types(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(None, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame.from_rdd(rdd, column_types=(int, str))
        self.assertEqual(3, len(res))
        self.assertEqual((int, str), res.column_types())
        self.assertEqual({'X.0': None, 'X.1': 'a'}, res[0])
        self.assertEqual({'X.0': 2, 'X.1': 'b'}, res[1])

    def test_from_rdd_names_types(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(None, 'a'), (2, 'b'), (3, 'c')])
        res = XFrame.from_rdd(rdd, column_names = ('id', 'val'), column_types=(int, str))
        self.assertEqual(3, len(res))
        self.assertEqual((int, str), res.column_types())
        self.assertEqual({'id': None, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])

    def test_from_rdd_names_bad(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        with self.assertRaises(ValueError):
            res = XFrame.from_rdd(rdd, column_names=('id', ))

    def test_from_rdd_types_bad(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(None, 'a'), (2, 'b'), (3, 'c')])
        with self.assertRaises(ValueError):
            res = XFrame.from_rdd(rdd, column_types=(int, ))

class TestXFrameFromSparkDataFrame(unittest.TestCase):
    """
    Tests XFrame from_rdd with spark dataframe
    """

    def test_from_rdd(self):
        sc = XFrame.spark_context()
        rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
        fields = [StructField('id', IntegerType(), True), StructField('val', StringType(), True)]
        schema = StructType(fields)
        sqlc = XFrame.spark_sql_context()
        s_rdd = sqlc.applySchema(rdd, schema)

        res = XFrame.from_rdd(s_rdd)
        self.assertEqual(3, len(res))
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])

class TestXFramePrintRows(unittest.TestCase):
    """
    Tests XFrame print_rows
    """

    def test_print_rows(self):
        pass


class TestXFrameToStr(unittest.TestCase):
    """
    Tests XFrame __str__
    """

    def test_to_str(self):
        pass

class TestXFrameNonzero(unittest.TestCase):
    """
    Tests XFrame __nonzero__
    """
    def test_nonzero_true(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertTrue(t)

    def test_nonzero_false(self):
        t = XFrame()
        self.assertFalse(t)

    # TODO make an XFrame and then somehow delete all its rows, so the RDD
    # exists but is empty

class TestXFrameLen(unittest.TestCase):
    """
    Tests XFrame __len__
    """

    def test_len_nonzero(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertEquals(3, len(t))

    def test_len_zero(self):
        t = XFrame()
        self.assertEqual(0, len(t))

    # TODO make an XFrame and then somehow delete all its rows, so the RDD
    # exists but is empty

class TestXFrameCopy(unittest.TestCase):
    """
    Tests XFrame __copy__
    """

    def test_copy(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        x = copy.copy(t)
        self.assertEqual(3, len(x))
        self.assertTrue(eq_array([1, 2, 3], x['id']))
        self.assertEqual([int, str], x.column_types())
        self.assertEqual(['id', 'val'], x.column_names())

class TestXFrameDtype(unittest.TestCase):
    """
    Tests XFrame dtype
    """

    def test_dtype(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        dt = t.dtype()
        self.assertEqual(int, dt[0])
        self.assertEqual(str, dt[1])


class TestXFrameNumRows(unittest.TestCase):
    """
    Tests XFrame num_rows
    """

    def test_num_rows(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertEqual(3, t.num_rows())

class TestXFrameNumCols(unittest.TestCase):
    """
    Tests XFrame num_cols
    """

    def test_num_cols(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertEqual(2, t.num_cols())

class TestXFrameNumColumns(unittest.TestCase):
    """
    Tests XFrame num_columns
    """

    def test_num_columns(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertEqual(2, t.num_columns())

class TestXFrameColumnNames(unittest.TestCase):
    """
    Tests XFrame column_names
    """

    def test_column_names(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        names = t.column_names()
        self.assertEqual(['id', 'val'], names)

class TestXFrameColumnTypes(unittest.TestCase):
    """
    Tests XFrame column_types
    """

    def test_column_types(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        types = t.column_types()
        self.assertEqual([int, str], types)

class TestXFrameHead(unittest.TestCase):
    """
    Tests XFrame head
    """

    def test_head(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        hd = t.head(2)
        self.assertEqual(2, len(hd))
        self.assertTrue(eq_array([1, 2], hd['id']))
        self.assertTrue(eq_array(['a', 'b'], hd['val']))

class TestXFrameTail(unittest.TestCase):
    """
    Tests XFrame tail
    """

    def test_tail(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        tl = t.tail(2)
        self.assertEqual(2, len(tl))
        self.assertTrue(eq_array([2, 3], tl['id']))
        self.assertTrue(eq_array(['b', 'c'], tl['val']))

class TestXFrameToPandasDataframe(unittest.TestCase):
    """
    Tests XFrame to_pandas_dataframe
    """

    def test_to_pandas_dataframe_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        df = t.to_pandas_dataframe()
        self.assertEqual(3, len(df))
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual('a', df['val'][0])

    def test_to_pandas_dataframe_bool(self):
        t = XFrame({'id': [1, 2, 3], 'val': [True, False, True]})
        df = t.to_pandas_dataframe()
        self.assertEqual(3, len(df))
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual(True, df['val'][0])
        self.assertEqual(False, df['val'][1])

    def test_to_pandas_dataframe_float(self):
        t = XFrame({'id': [1, 2, 3], 'val': [1.0, 2.0, 3.0]})
        df = t.to_pandas_dataframe()
        self.assertEqual(3, len(df))
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual(1.0, df['val'][0])
        self.assertEqual(2.0, df['val'][1])

    def test_to_pandas_dataframe_int(self):
        t = XFrame({'id': [1, 2, 3], 'val': [1, 2, 3]})
        df = t.to_pandas_dataframe()
        self.assertEqual(3, len(df))
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual(1, df['val'][0])
        self.assertEqual(2, df['val'][1])

    def test_to_pandas_dataframe_list(self):
        t = XFrame({'id': [1, 2, 3], 'val': [[1, 1],  [2, 2], [3, 3]]})
        df = t.to_pandas_dataframe()
        self.assertEqual(3, len(df))
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual([1, 1], df['val'][0])
        self.assertEqual([2, 2], df['val'][1])

    def test_to_pandas_dataframe_map(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'x': 1},  {'y': 2}, {'z': 3}]})
        df = t.to_pandas_dataframe()
        self.assertEqual(3, len(df))
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual({'x': 1}, df['val'][0])
        self.assertEqual({'y': 2}, df['val'][1])


class TestXFrameToDataframeplus(unittest.TestCase):
    """
    Tests XFrame to_dataframeplus
    """

    def test_to_dataframeplus_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        df = t.to_dataframeplus()
        self.assertEqual(3, len(df))
        self.assertEqual(1, df['id'][0])
        self.assertEqual(2, df['id'][1])
        self.assertEqual('a', df['val'][0])

class TestXFrameApply(unittest.TestCase):
    """
    Tests XFrame apply
    """

    def test_apply(self):
        from xpatterns.xrdd import XRdd
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.apply(lambda row: row['id'] * 2)
        self.assertEqual(3, len(res))
        self.assertEqual(int, res.dtype())
        self.assertTrue(eq_array([2, 4, 6], res))

    def test_apply_float(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.apply(lambda row: row['id'] * 2, dtype=float)
        self.assertEqual(3, len(res))
        self.assertEqual(float, res.dtype())
        self.assertTrue(eq_array([2.0, 4.0, 6.0], res))

    def test_apply_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.apply(lambda row: row['id'] * 2, dtype=str)
        self.assertEqual(3, len(res))
        self.assertEqual(str, res.dtype())
        self.assertTrue(eq_array(['2', '4', '6'], res))

class TestXFrameFlatMap(unittest.TestCase):
    """
    Tests XFrame flat_map
    """

    def test_flat_map(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.flat_map(['number', 'letter'], 
                         lambda row: [list(row.itervalues()) for i in range(0, row['id'])],
                         column_types=[int, str])
        self.assertEqual(['number', 'letter'], res.column_names())
        self.assertEqual([int, str], res.dtype())
        self.assertEqual({'number': 1, 'letter': 'a'}, res[0])
        self.assertEqual({'number': 2, 'letter': 'b'}, res[1])
        self.assertEqual({'number': 2, 'letter': 'b'}, res[2])
        self.assertEqual({'number': 3, 'letter': 'c'}, res[3])
        self.assertEqual({'number': 3, 'letter': 'c'}, res[4])
        self.assertEqual({'number': 3, 'letter': 'c'}, res[5])

    def test_flat_map_identity(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.flat_map(['number', 'letter'], 
                         lambda row: [[row['id'], row['val']]],
                         column_types=[int, str])
        self.assertEqual(['number', 'letter'], res.column_names())
        self.assertEqual([int, str], res.dtype())
        self.assertEqual({'number': 1, 'letter': 'a'}, res[0])
        self.assertEqual({'number': 2, 'letter': 'b'}, res[1])
        self.assertEqual({'number': 3, 'letter': 'c'}, res[2])

    def test_flat_map_mapped(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.flat_map(['number', 'letter'], 
                         lambda row: [[row['id'] * 2, row['val'] + 'x']],
                         column_types=[int, str])
        self.assertEqual(['number', 'letter'], res.column_names())
        self.assertEqual([int, str], res.dtype())
        self.assertEqual({'number': 2, 'letter': 'ax'}, res[0])
        self.assertEqual({'number': 4, 'letter': 'bx'}, res[1])
        self.assertEqual({'number': 6, 'letter': 'cx'}, res[2])

    def test_flat_map_auto(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.flat_map(['number', 'letter'], 
                         lambda row: [[row['id'] * 2, row['val'] + 'x']])
        self.assertEqual(['number', 'letter'], res.column_names())
        self.assertEqual([int, str], res.dtype())
        self.assertEqual({'number': 2, 'letter': 'ax'}, res[0])
        self.assertEqual({'number': 4, 'letter': 'bx'}, res[1])
        self.assertEqual({'number': 6, 'letter': 'cx'}, res[2])

    # TODO: test auto error cases

class TestXFrameSample(unittest.TestCase):
    """
    Tests XFrame sample
    """

    @unittest.skip('depends on number of partitions')
    def test_sample_02(self):
        t = XFrame({'id': [1, 2, 3, 4, 5], 'val': ['a', 'b', 'c', 'd', 'e']})
        res = t.sample(0.2, 2)
        self.assertEqual(1, len(res))
        self.assertEqual({'id': 2, 'val': 'b'}, res[0])

    @unittest.skip('depends on number of partitions')
    def test_sample_08(self):
        t = XFrame({'id': [1, 2, 3, 4, 5], 'val': ['a', 'b', 'c', 'd', 'e']})
        res = t.sample(0.8, 3)
        self.assertEqual(3, len(res))
        self.assertEqual({'id': 2, 'val': 'b'}, res[0])
        self.assertEqual({'id': 4, 'val': 'd'}, res[1])
        self.assertEqual({'id': 5, 'val': 'e'}, res[2])

class TestXFrameRandomSplit(unittest.TestCase):
    """
    Tests XFrame random_split
    """

    @unittest.skip('depends on number of partitions')
    def test_random_split(self):
        t = XFrame({'id': [1, 2, 3, 4, 5], 'val': ['a', 'b', 'c', 'd', 'e']})
        res1, res2 = t.random_split(0.5, 1)
        self.assertEqual(3, len(res1))
        self.assertEqual({'id': 1, 'val': 'a'}, res1[0])
        self.assertEqual({'id': 4, 'val': 'd'}, res1[1])
        self.assertEqual({'id': 5, 'val': 'e'}, res1[2])
        self.assertEqual(2, len(res2))
        self.assertEqual({'id': 2, 'val': 'b'}, res2[0])
        self.assertEqual({'id': 3, 'val': 'c'}, res2[1])

class TestXFrameTopk(unittest.TestCase):
    """
    Tests XFrame topk
    """

    def test_topk_int(self):
        t = XFrame({'id': [10, 20, 30], 'val': ['a', 'b', 'c']})
        res = t.topk('id', 2)
        self.assertEqual(2, len(res))
        self.assertTrue((XArray([30, 20]) == res['id']).all())
        self.assertTrue(eq_array(['c', 'b'], res['val']))
        self.assertEqual([int, str], res.column_types())
        self.assertEqual(['id', 'val'], res.column_names())

    def test_topk_int_reverse(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['c', 'b', 'a']})
        res = t.topk('id', 2, reverse=True)
        self.assertEqual(2, len(res))
        self.assertTrue(eq_array([10, 20], res['id']))
        self.assertTrue(eq_array(['a', 'b'], res['val']))

    def test_topk_float(self):
        t = XFrame({'id': [10.0, 20.0, 30.0], 'val': ['a', 'b', 'c']})
        res = t.topk('id', 2)
        self.assertEqual(2, len(res))
        self.assertTrue((XArray([30.0, 20.0]) == res['id']).all())
        self.assertTrue(eq_array(['c', 'b'], res['val']))
        self.assertEqual([float, str], res.column_types())
        self.assertEqual(['id', 'val'], res.column_names())

    def test_topk_float_reverse(self):
        t = XFrame({'id': [30.0, 20.0, 10.0], 'val': ['c', 'b', 'a']})
        res = t.topk('id', 2, reverse=True)
        self.assertEqual(2, len(res))
        self.assertTrue(eq_array([10.0, 20.0], res['id']))
        self.assertTrue(eq_array(['a', 'b'], res['val']))

    def test_topk_str(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['a', 'b', 'c']})
        res = t.topk('val', 2)
        self.assertEqual(2, len(res))
        self.assertTrue(eq_array([10, 20], res['id']))
        self.assertTrue(eq_array(['c', 'b'], res['val']))
        self.assertEqual([int, str], res.column_types())
        self.assertEqual(['id', 'val'], res.column_names())

    def test_topk_str_reverse(self):
        t = XFrame({'id': [10, 20, 30], 'val': ['c', 'b', 'a']})
        res = t.topk('val', 2, reverse=True)
        self.assertEqual(2, len(res))
        self.assertTrue(eq_array([30, 20], res['id']))
        self.assertTrue(eq_array(['a', 'b'], res['val']))


class TestXFrameSaveBinary(unittest.TestCase):
    """
    Tests XFrame save binary format
    """

    def test_save(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame'
        t.save(path, format='binary')
        with open(os.path.join(path, '_metadata')) as f:
            metadata = pickle.load(f)
        self.assertEqual([['id', 'val'], [int,  str]], metadata)
        # TODO find some way to check the data


class TestXFrameSaveCSV(unittest.TestCase):
    """
    Tests XFrame save csv format
    """

    def test_save(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame-csv'
        t.save(path, format='csv')
        # TODO verify

class TestXFrameSaveParquet(unittest.TestCase):
    """
    Tests XFrame save for parquet files
    """
    def test_save(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['a', 'b', 'c']})
        path = 'tmp/frame-parquet'
        t.save(path, format='parquet')
        # TODO verify


class TestXFrameSelectColumn(unittest.TestCase):
    """
    Tests XFrame select_column
    """

    def test_select_column_id(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.select_column('id')
        self.assertTrue(eq_array([1, 2, 3], res))

    def test_select_column_val(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.select_column('val')
        self.assertTrue(eq_array(['a', 'b', 'c'], res))

    def test_select_column_bad_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(ValueError):
            res = t.select_column('xx')

    def test_select_column_bad_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            res = t.select_column(1)


class TestXFrameSelectColumns(unittest.TestCase):
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
            res = t.select_columns(1)

    def test_select_columns_bad_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(TypeError):
            res = t.select_columns(['id', 2])

    def test_select_columns_bad_dup(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(ValueError):
            res = t.select_columns(['id', 'id'])


class TestXFrameAddColumn(unittest.TestCase):
    """
    Tests XFrame add_column
    """

    def test_add_column_named(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta = XArray([3.0, 2.0, 1.0])
        tf.add_column(ta, name='another')
        self.assertEqual(['id', 'val', 'another'], tf.column_names())
        self.assertEqual({'id': 1, 'val': 'a', 'another': 3.0}, tf[0])

    def test_add_column_name_default(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta = XArray([3.0, 2.0, 1.0])
        tf.add_column(ta)
        self.assertEqual(['id', 'val', 'X2'], tf.column_names())
        self.assertEqual({'id': 1, 'val': 'a', 'X2': 3.0}, tf[0])

    def test_add_column_name_dup(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta = XArray([3.0, 2.0, 1.0])
        with self.assertRaises(ValueError):
            tf.add_column(ta, name='id')


class TestXFrameAddColumnsArray(unittest.TestCase):
    """
    Tests XFrame add_columns where data is array of XArray
    """

    def test_add_columns_one(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta = XArray([3.0, 2.0, 1.0])
        tf.add_columns([ta], namelist=['new1'])
        self.assertEqual(['id', 'val', 'new1'], tf.column_names())
        self.assertEqual([int, str, float], tf.column_types())
        self.assertEqual({'id': 1, 'val': 'a', 'new1': 3.0}, tf[0])
        
    def test_add_columns_two(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta1 = XArray([3.0, 2.0, 1.0])
        ta2 = XArray([30.0, 20.0, 10.0])
        tf.add_columns([ta1, ta2], namelist=['new1', 'new2'])
        self.assertEqual(['id', 'val', 'new1', 'new2'], tf.column_names())
        self.assertEqual([int, str, float, float], tf.column_types())
        self.assertEqual({'id': 1, 'val': 'a', 'new1': 3.0, 'new2': 30.0}, tf[0])
        
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

class TestXFrameAddColumnsFrame(unittest.TestCase):
    """
    Tests XFrame add_columns where data is XFrame
    """

    def test_add_columns(self):
        tf1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        tf2 = XFrame({'new1': [3.0, 2.0, 1.0], 'new2': [30.0, 20.0, 10.0]})
        tf1.add_columns(tf2)
        self.assertEqual(['id', 'val', 'new1', 'new2'], tf1.column_names())
        self.assertEqual([int, str, float, float], tf1.column_types())
        self.assertEqual({'id': 1, 'val': 'a', 'new1': 3.0, 'new2': 30.0}, tf1[0])

    def test_add_columns_dup_names(self):
        tf1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        tf2 = XFrame({'new1': [3.0, 2.0, 1.0], 'val': [30.0, 20.0, 10.0]})
        with self.assertRaises(ValueError):
            tf1.add_columns(tf2)


class TestXFrameRemoveColumn(unittest.TestCase):
    """
    Tests XFrame remove_column
    """

    def test_remove_column(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        t.remove_column('another')
        self.assertEqual({'id': 1, 'val': 'a'}, t[0])

    def test_remove_column_not_found(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            t.remove_column('xx')


class TestXFrameRemoveColumns(unittest.TestCase):
    """
    Tests XFrame remove_columns
    """

    def test_remove_columns(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'new1': [3.0, 2.0, 1.0], 'new2': [30.0, 20.0, 10.0]})
        t.remove_columns(['new1', 'new2'])
        self.assertEqual({'id': 1, 'val': 'a'}, t[0])

    def test_remove_column_not_iterable(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(TypeError):
            t.remove_columns('xx')

    def test_remove_column_not_found(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            t.remove_columns(['xx'])

class TestXFrameSwapColumns(unittest.TestCase):
    """
    Tests XFrame swap_columns
    """

    def test_swap_columns(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'x': [3.0, 2.0, 1.0]})
        t.swap_columns('val', 'x')
        self.assertEqual(['id', 'x', 'val'], t.column_names())
        self.assertEqual({'id': 1, 'x': 3.0, 'val': 'a'}, t[0])

    def test_swap_columns_bad_col_1(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            t.swap_columns('xx', 'another')

    def test_swap_columns_bad_col_2(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            t.swap_columns('val', 'xx')


class TestXFrameRename(unittest.TestCase):
    """
    Tests XFrame rename
    """

    def test_rename(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t.rename({'id': 'new_id'})
        self.assertEqual(['new_id', 'val'], t.column_names())
        self.assertEqual({'new_id': 1, 'val': 'a'}, t[0])

    def test_rename_arg_not_dict(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            t.rename(['id', 'new_id'])

    def test_rename_col_not_found(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(ValueError):
            t.rename({'xx': 'new_id'})

class TestXFrameGetitem(unittest.TestCase):
    """
    Tests XFrame __getitem__
    """

    def test_getitem_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t['id']
        self.assertTrue(eq_array([1, 2, 3], res))

    def test_getitem_int(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t[1]
        self.assertEqual({'id': 2, 'val': 'b'}, res)

    def test_getitem_int(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t[-2]
        self.assertEqual({'id': 2, 'val': 'b'}, res)

    def test_getitem_int_too_low(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(IndexError):
            res = t[-100]

    def test_getitem_int_too_high(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(IndexError):
            res = t[100]

    def test_getitem_slice(self):
        # TODO we could test more variations of slice
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t[:2]
        self.assertEqual(2, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])

    def test_getitem_list(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'x': [1.0, 2.0, 3.0]})
        res = t[['id', 'x']]
        self.assertEqual({'id': 2, 'x': 2.0}, res[1])

    def test_getitem_bad_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            res = t[{'a': 1}]

    # TODO: need to implement
    def test_getitem_xarray(self):
        pass



class TestXFrameGetattr(unittest.TestCase):
    """
    Tests XFrame __getattr__
    """

    def test_getattr(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.id
        self.assertTrue(eq_array([1, 2, 3], res))

class TestXFrameSetitem(unittest.TestCase):
    """
    Tests XFrame __setitem__
    """

    def test_setitem_str_const(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t['x'] = 5.0
        self.assertEqual(['id', 'val', 'x'], t.column_names())
        self.assertEqual({'id': 2, 'val': 'b', 'x': 5.0}, t[1])

    def test_setitem_list(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta1 = XArray([3.0, 2.0, 1.0])
        ta2 = XArray([30.0, 20.0, 10.0])
        tf[['new1', 'new2']] = [ta1, ta2]
        self.assertEqual(['id', 'val', 'new1', 'new2'], tf.column_names())
        self.assertEqual([int, str, float, float], tf.column_types())
        self.assertEqual({'id': 1, 'val': 'a', 'new1': 3.0, 'new2': 30.0}, tf[0])

    def test_setitem_str_iter(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t['x'] = [1.0, 2.0, 3.0]
        self.assertEqual(['id', 'val', 'x'], t.column_names())
        self.assertEqual({'id': 2, 'val': 'b', 'x': 2.0}, t[1])

    def test_setitem_str_xarray(self):
        tf = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        ta = XArray([3.0, 2.0, 1.0])
        tf['new'] = ta
        self.assertEqual(['id', 'val', 'new'], tf.column_names())
        self.assertEqual([int, str, float], tf.column_types())
        self.assertEqual({'id': 1, 'val': 'a', 'new': 3.0}, tf[0])

    def test_setitem_str_iter_replace(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t['val'] = [1.0, 2.0, 3.0]
        self.assertEqual(['id', 'val'], t.column_names())
        self.assertEqual({'id': 2, 'val': 2.0}, t[1])

    def test_setitem_bad_key(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            t[{'a': 1}] = [1.0, 2.0, 3.0]

    def test_setitem_str_iter_replace_one_col(self):
        t = XFrame({'val': ['a', 'b', 'c']})
        t['val'] = [1.0, 2.0, 3.0, 4.0]
        self.assertEqual(['val'], t.column_names())
        self.assertEqual(4, len(t))
        self.assertEqual({'val': 2.0}, t[1])


class TestXFrameSetattr(unittest.TestCase):
    """
    Tests XFrame __setattr__
    """

    def test_setitem_const(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t.x = 5.0
        self.assertEqual(['id', 'val', 'x'], t.column_names())
        self.assertEqual({'id': 2, 'val': 'b', 'x': 5.0}, t[1])

    def test_setitem_str_iter_replace(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t.val = [1.0, 2.0, 3.0]
        self.assertEqual(['id', 'val'], t.column_names())
        self.assertEqual({'id': 2, 'val': 2.0}, t[1])

class TestXFrameDelitem(unittest.TestCase):
    """
    Tests XFrame __delitem__
    """

    def test_delitem(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        del t['another']
        self.assertEqual({'id': 1, 'val': 'a'}, t[0])

    def test_delitem_not_found(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c'], 'another': [3.0, 2.0, 1.0]})
        with self.assertRaises(KeyError):
            del t['xx']


class TestXFrameHasSize(unittest.TestCase):
    """
    Tests XFrame __hassize__
    """

    def test_hassize_false(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertFalse(t.__has_size__())

    def test_hassize_true(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        len(t)
        self.assertTrue(t.__has_size__())

class TestXFrameIter(unittest.TestCase):
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

class TestXFrameAppend(unittest.TestCase):
    """
    Tests XFrame append
    """

    def test_append(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20, 30], 'val': ['aa', 'bb', 'cc']})
        res = t1.append(t2)
        self.assertEqual(6, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 10, 'val': 'aa'}, res[3])

    def test_append_bad_type(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(RuntimeError):
            res = t1.append(1)

    def test_append_both_empty(self):
        t1 = XFrame()
        t2 = XFrame()
        res = t1.append(t2)
        self.assertEqual(0, len(res))

    def test_append_first_empty(self):
        t1 = XFrame()
        t2 = XFrame({'id': [10, 20, 30], 'val': ['aa', 'bb', 'cc']})
        res = t1.append(t2)
        self.assertEqual(3, len(res))
        self.assertEqual({'id': 10, 'val': 'aa'}, res[0])

    def test_append_second_empty(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame()
        res = t1.append(t2)
        self.assertEqual(3, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])

    def test_append_unequal_col_length(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20, 30], 'val': ['aa', 'bb', 'cc'], 'another': [1.0, 2.0, 3.0]})
        with self.assertRaises(RuntimeError):
            res = t1.append(t2)

    def test_append_col_name_mismatch(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20], 'xx': ['aa', 'bb']})
        with self.assertRaises(RuntimeError):
            res = t1.append(t2)

    def test_append_col_type_mismatch(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20], 'val': [1.0, 2.0]})
        with self.assertRaises(RuntimeError):
            res = t1.append(t2)

class TestXFrameGroupby(unittest.TestCase):
    """
    Tests XFrame groupby
    """

    def test_groupby(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id'], res.column_names())
        self.assertEqual([int], res.column_types())
        self.assertEqual({'id': 1}, res[0])
        self.assertEqual({'id': 2}, res[1])
        self.assertEqual({'id': 3}, res[2])

    def test_groupby_bad_col_name_type(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        with self.assertRaises(TypeError):
            res = t.groupby(1, {})

    def test_groupby_bad_col_name_list_type(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        with self.assertRaises(TypeError):
            res = t.groupby([1], {})

    def test_groupby_bad_col_group_name(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        with self.assertRaises(KeyError):
            res = t.groupby('xx', {})

    def test_groupby_bad_group_type(self):
        t = XFrame({'id': [{1: 'a', 2: 'b'}], 
                    'val': ['a', 'b']})
        with self.assertRaises(TypeError):
            res = t.groupby('id', {})

    def test_groupby_bad_agg_group_name(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        with self.assertRaises(KeyError):
            res = t.groupby('id', SUM('xx'))

    def test_groupby_bad_agg_group_type(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        with self.assertRaises(TypeError):
            res = t.groupby('id', SUM(1))

class TestXFrameGroupbyAggregators(unittest.TestCase):
    """
    Tests XFrame groupby aggregators
    """

    def test_groupby_count(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'count': COUNT})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'count'], res.column_names())
        self.assertEqual([int, int], res.column_types())
        self.assertEqual({'id': 1, 'count': 3}, res[0])
        self.assertEqual({'id': 2, 'count': 2}, res[1])
        self.assertEqual({'id': 3, 'count': 1}, res[2])

    def test_groupby_sum(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'sum': SUM('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'sum'], res.column_names())
        self.assertEqual([int, int], res.column_types())
        self.assertEqual({'id': 1, 'sum': 110}, res[0])
        self.assertEqual({'id': 2, 'sum': 70}, res[1])
        self.assertEqual({'id': 3, 'sum': 30}, res[2])

    def test_groupby_sum_def(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', SUM('another'))
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'sum'], res.column_names())
        self.assertEqual([int, int], res.column_types())
        self.assertEqual({'id': 1, 'sum': 110}, res[0])
        self.assertEqual({'id': 2, 'sum': 70}, res[1])
        self.assertEqual({'id': 3, 'sum': 30}, res[2])

    def test_groupby_sum_sum_def(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', [SUM('another'), SUM('another')])
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'sum', 'sum.1'], res.column_names())
        self.assertEqual([int, int, int], res.column_types())
        self.assertEqual({'id': 1, 'sum': 110, 'sum.1': 110}, res[0])
        self.assertEqual({'id': 2, 'sum': 70, 'sum.1': 70}, res[1])
        self.assertEqual({'id': 3, 'sum': 30, 'sum.1': 30}, res[2])

    def test_groupby_sum_rename(self):
        t = XFrame({'sum': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('sum', SUM('another'))
        res = res.topk('sum', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['sum', 'sum.1'], res.column_names())
        self.assertEqual([int, int], res.column_types())
        self.assertEqual({'sum': 1, 'sum.1': 110}, res[0])
        self.assertEqual({'sum': 2, 'sum.1': 70}, res[1])
        self.assertEqual({'sum': 3, 'sum.1': 30}, res[2])

    def test_groupby_count_sum(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'count': COUNT, 'sum': SUM('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'count', 'sum'], res.column_names())
        self.assertEqual([int, int, int], res.column_types())
        self.assertEqual({'id': 1, 'count': 3, 'sum': 110}, res[0])
        self.assertEqual({'id': 2, 'count': 2, 'sum': 70}, res[1])
        self.assertEqual({'id': 3, 'count': 1, 'sum': 30}, res[2])

    def test_groupby_count_sum_def(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', [COUNT, SUM('another')])
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'count', 'sum'], res.column_names())
        self.assertEqual([int, int, int], res.column_types())
        self.assertEqual({'id': 1, 'count': 3, 'sum': 110}, res[0])
        self.assertEqual({'id': 2, 'count': 2, 'sum': 70}, res[1])
        self.assertEqual({'id': 3, 'count': 1, 'sum': 30}, res[2])

    def test_groupby_argmax(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'argmax': ARGMAX('another', 'val')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'argmax'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'argmax': 'f'}, res[0])
        self.assertEqual({'id': 2, 'argmax': 'e'}, res[1])
        self.assertEqual({'id': 3, 'argmax': 'c'}, res[2])

    def test_groupby_argmin(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'argmin': ARGMIN('another', 'val')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'argmin'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'argmin': 'a'}, res[0])
        self.assertEqual({'id': 2, 'argmin': 'b'}, res[1])
        self.assertEqual({'id': 3, 'argmin': 'c'}, res[2])

    def test_groupby_max(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'max': MAX('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'max'], res.column_names())
        self.assertEqual([int, int], res.column_types())
        self.assertEqual({'id': 1, 'max': 60}, res[0])
        self.assertEqual({'id': 2, 'max': 50}, res[1])
        self.assertEqual({'id': 3, 'max': 30}, res[2])

    def test_groupby_max_float(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'max': MAX('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'max'], res.column_names())
        self.assertEqual([int, float], res.column_types())
        self.assertEqual({'id': 1, 'max': 60.0}, res[0])
        self.assertEqual({'id': 2, 'max': 50.0}, res[1])
        self.assertEqual({'id': 3, 'max': 30.0}, res[2])

    def test_groupby_max_str(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'max': MAX('val')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'max'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'max': 'f'}, res[0])
        self.assertEqual({'id': 2, 'max': 'e'}, res[1])
        self.assertEqual({'id': 3, 'max': 'c'}, res[2])

    def test_groupby_min(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'min': MIN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'min'], res.column_names())
        self.assertEqual([int, int], res.column_types())
        self.assertEqual({'id': 1, 'min': 10}, res[0])
        self.assertEqual({'id': 2, 'min': 20}, res[1])
        self.assertEqual({'id': 3, 'min': 30}, res[2])

    def test_groupby_min_float(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'min': MIN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'min'], res.column_names())
        self.assertEqual([int, float], res.column_types())
        self.assertEqual({'id': 1, 'min': 10.0}, res[0])
        self.assertEqual({'id': 2, 'min': 20.0}, res[1])
        self.assertEqual({'id': 3, 'min': 30.0}, res[2])

    def test_groupby_min_str(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'min': MIN('val')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'min'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'min': 'a'}, res[0])
        self.assertEqual({'id': 2, 'min': 'b'}, res[1])
        self.assertEqual({'id': 3, 'min': 'c'}, res[2])

    def test_groupby_avg(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'avg': AVG('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'avg'], res.column_names())
        self.assertEqual([int, float], res.column_types())
        self.assertEqual({'id': 1, 'avg': 110.0/3.0}, res[0])
        self.assertEqual({'id': 2, 'avg': 70.0/2.0}, res[1])
        self.assertEqual({'id': 3, 'avg': 30.0}, res[2])

    def test_groupby_mean(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'mean': MEAN('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'mean'], res.column_names())
        self.assertEqual([int, float], res.column_types())
        self.assertEqual({'id': 1, 'mean': 110.0/3.0}, res[0])
        self.assertEqual({'id': 2, 'mean': 70.0/2.0}, res[1])
        self.assertEqual({'id': 3, 'mean': 30.0}, res[2])

    def test_groupby_var(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'var': VAR('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'var'], res.column_names())
        self.assertEqual([int, float], res.column_types())
        self.assertAlmostEqual(3800.0/9.0, res[0]['var'])
        self.assertAlmostEqual(225.0, res[1]['var'])
        self.assertEqual({'id': 3, 'var': 0.0}, res[2])

    def test_groupby_variance(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'variance': VARIANCE('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'variance'], res.column_names())
        self.assertEqual([int, float], res.column_types())
        self.assertAlmostEqual(3800.0/9.0, res[0]['variance'])
        self.assertAlmostEqual(225.0, res[1]['variance'])
        self.assertEqual({'id': 3, 'variance': 0.0}, res[2])

    def test_groupby_std(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'std': STD('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'std'], res.column_names())
        self.assertEqual([int, float], res.column_types())
        self.assertAlmostEqual(math.sqrt(3800.0/9.0), res[0]['std'])
        self.assertAlmostEqual(math.sqrt(225.0), res[1]['std'])
        self.assertEqual({'id': 3, 'std': 0.0}, res[2])

    def test_groupby_stdv(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'stdv': STDV('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'stdv'], res.column_names())
        self.assertEqual([int, float], res.column_types())
        self.assertAlmostEqual(math.sqrt(3800.0/9.0), res[0]['stdv'])
        self.assertAlmostEqual(math.sqrt(225.0), res[1]['stdv'])
        self.assertEqual({'id': 3, 'stdv': 0.0}, res[2])

    def test_groupby_select_one(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'select_one': SELECT_ONE('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'select_one'], res.column_names())
        self.assertEqual([int, int], res.column_types())
        self.assertEqual({'id': 1, 'select_one': 60}, res[0])
        self.assertEqual({'id': 2, 'select_one': 50}, res[1])
        self.assertEqual({'id': 3, 'select_one': 30}, res[2])

    def test_groupby_select_one_float(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'select_one': SELECT_ONE('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'select_one'], res.column_names())
        self.assertEqual([int, float], res.column_types())
        self.assertEqual({'id': 1, 'select_one': 60.0}, res[0])
        self.assertEqual({'id': 2, 'select_one': 50.0}, res[1])
        self.assertEqual({'id': 3, 'select_one': 30.0}, res[2])

    def test_groupby_select_one_str(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
        res = t.groupby('id', {'select_one': SELECT_ONE('val')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'select_one'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'select_one': 'f'}, res[0])
        self.assertEqual({'id': 2, 'select_one': 'e'}, res[1])
        self.assertEqual({'id': 3, 'select_one': 'c'}, res[2])

    def test_groupby_concat_list(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'concat': CONCAT('another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'concat'], res.column_names())
        self.assertEqual([int, list], res.column_types())
        self.assertEqual({'id': 1, 'concat': [10, 40, 60]}, res[0])
        self.assertEqual({'id': 2, 'concat': [20, 50]}, res[1])
        self.assertEqual({'id': 3, 'concat': [30]}, res[2])

    def test_groupby_concat_dict(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1], 
                    'val': ['a', 'b', 'c', 'd', 'e', 'f'], 
                    'another': [10, 20, 30, 40, 50, 60]})
        res = t.groupby('id', {'concat': CONCAT('val', 'another')})
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'concat'], res.column_names())
        self.assertEqual([int, dict], res.column_types())
        self.assertEqual({'id': 1, 'concat': {'a': 10, 'd': 40, 'f': 60}}, res[0])
        self.assertEqual({'id': 2, 'concat': {'b': 20, 'e': 50}}, res[1])
        self.assertEqual({'id': 3, 'concat': {'c': 30}}, res[2])


    def test_groupby_quantile(self):
        # not implemented
        pass

class TestXFrameJoin(unittest.TestCase):
    """
    Tests XFrame join
    """

    def test_join(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertEqual([int, str, str], res.column_types())
        self.assertEqual({'id': 3, 'val': 'c', 'doubled': 'cc'}, res.topk('id', 1)[0])

    def test_join_rename(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'val': ['aa', 'bb', 'cc']})
        res = t1.join(t2, on='id')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val', 'val.1'], res.column_names())
        self.assertEqual([int, str, str], res.column_types())
        self.assertEqual({'id': 3, 'val': 'c', 'val.1': 'cc'}, res.topk('id', 1)[0])

    def test_join_compound_key(self):
        t1 = XFrame({'id1': [1, 2, 3], 'id2': [10, 20, 30], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id1': [1, 2, 3], 'id2': [10, 20, 30], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2)
        self.assertEqual(3, len(res))
        self.assertEqual(['id1', 'id2', 'val', 'doubled'], res.column_names())
        self.assertEqual([int, int, str, str], res.column_types())
        self.assertEqual({'id1': 3, 'id2': 30, 'val': 'c', 'doubled': 'cc'}, res.topk('id1', 1)[0])

    def test_join_dict_key(self):
        t1 = XFrame({'id1': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id2': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2, on={'id1': 'id2'})
        self.assertEqual(3, len(res))
        self.assertEqual(['id1', 'val', 'doubled'], res.column_names())
        self.assertEqual([int, str, str], res.column_types())
        self.assertEqual({'id1': 3, 'val': 'c', 'doubled': 'cc'}, res.topk('id1', 1)[0])

    def test_join_partial(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 4], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2)
        self.assertEqual(2, len(res))
        self.assertEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertEqual([int, str, str], res.column_types())
        self.assertEqual({'id': 2, 'val': 'b', 'doubled': 'bb'}, res.topk('id', 1)[0])

    def test_join_empty(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [4, 5, 6], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2)
        self.assertEqual(0, len(res))
        self.assertEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertEqual([int, str, str], res.column_types())

    def test_join_on_val(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20, 30], 'val': ['a', 'b', 'c']})
        res = t1.join(t2, on='val')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val', 'id.1'], res.column_names())
        self.assertEqual([int, str, int], res.column_types())
        self.assertEqual({'id': 3, 'val': 'c', 'id.1': 30}, res.topk('id', 1)[0])

    def test_join_inner(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 4], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2, how='inner')
        self.assertEqual(2, len(res))
        self.assertEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertEqual([int, str, str], res.column_types())
        self.assertEqual({'id': 2, 'val': 'b', 'doubled': 'bb'}, res.topk('id', 1)[0])

    def test_join_left(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 4], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2, how='left')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertEqual([int, str, str], res.column_types())
        self.assertEqual({'id': 3, 'val': 'c', 'doubled': None}, res.topk('id', 1)[0])

    def test_join_right(self):
        # TODO is this the right expected result?
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 4], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2, how='right')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val', 'doubled'], res.column_names())
        self.assertEqual([int, str, str], res.column_types())
        top = res.topk('doubled', 2)
        self.assertEqual({'id': None, 'val': None, 'doubled': 'cc'}, top[0])
        self.assertEqual({'id': 2, 'val': 'b', 'doubled': 'bb'}, top[1])

    def test_join_outer(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [10, 20, 30], 'doubled': ['aa', 'bb', 'cc']})
        res = t1.join(t2, how='outer')
        self.assertEqual(9, len(res))
        self.assertEqual(['id', 'val', 'doubled', 'id.1'], res.column_names())
        self.assertEqual([int, str, str, int], res.column_types())
        top = res.topk('id', 3)
        top = top.topk('doubled', 2)
        self.assertEqual({'id': 3, 'val': 'c', 'doubled': 'cc', 'id.1': 30}, top[0])
        self.assertEqual({'id': 3, 'val': 'c', 'doubled': 'bb', 'id.1': 20}, top[1])

    def test_join_bad_how(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        with self.assertRaises(ValueError):
            res = t1.join(t2, how='xx')

    def test_join_bad_right(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            res = t1.join([1, 2, 3])

    def test_join_bad_on_list(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        with self.assertRaises(TypeError):
            res = t1.join(t2, on=['id', 1])

    def test_join_bad_on_type(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        with self.assertRaises(TypeError):
            res = t1.join(t2, on=1)

    def test_join_bad_on_col_name(self):
        t1 = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        t2 = XFrame({'id': [1, 2, 3], 'doubled': ['aa', 'bb', 'cc']})
        with self.assertRaises(ValueError):
            res = t1.join(t2, on='xx')


class TestXFrameSplitDatetime(unittest.TestCase):
    """
    Tests XFrame split_datetime
    """

    def test_split_datetime(self):
        t = XFrame({'id': [1, 2, 3], 'val': [datetime(2011, 1, 1), 
                                             datetime(2011, 2, 2),
                                             datetime(2011, 3, 3)]})
        with self.assertRaises(NotImplementedError):
            t.split_datetime('val')
        
    def test_split_datetime_bad_col(self):
        t = XFrame({'id': [1, 2, 3], 'val': [datetime(2011, 1, 1), 
                                             datetime(2011, 2, 2),
                                             datetime(2011, 3, 3)]})
        with self.assertRaises(KeyError):
            t.split_datetime('xx')
        

class TestXFrameFilterBy(unittest.TestCase):
    """
    Tests XFrame filter_by
    """

    # not tested -- test after group by
    def test_filter_by_list_id(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.filter_by([1, 3], 'id')
        top = res.topk('id')
        self.assertEquals(2, len(top))
        self.assertEquals({'id': 3, 'val': 'c'}, top[0])
        self.assertEquals({'id': 1, 'val': 'a'}, top[1])

    def test_filter_by_list_val(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.filter_by(['a', 'b'], 'val')
        top = res.topk('id')
        self.assertEquals(2, len(res))
        self.assertEquals({'id': 2, 'val': 'b'}, top[0])
        self.assertEquals({'id': 1, 'val': 'a'}, top[1])
#        TODO: following does not work
#        self.assertTrue(eq_array(['a', 'b'], res['val']))

    def test_filter_by_xarray(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        a = XArray([1, 3])
        res = t.filter_by(a, 'id')
        top = res.topk('id')
        self.assertEquals(2, len(res))
        self.assertEquals({'id': 3, 'val': 'c'}, top[0])
        self.assertEquals({'id': 1, 'val': 'a'}, top[1])
#        self.assertTrue(eq_array([1, 3], res['id']))

    def test_filter_by_list_exclude(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.filter_by([1, 3], 'id', exclude=True)
        top = res.topk('id')
        self.assertEquals(2, len(res))
        self.assertEquals({'id': 4, 'val': 'd'}, top[0])
        self.assertEquals({'id': 2, 'val': 'b'}, top[1])
#        self.assertTrue(eq_array([2, 4], res['id']))

    def test_filter_by_xarray_exclude(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        a = XArray([1, 3])
        res = t.filter_by(a, 'id', exclude=True)
        top = res.topk('id')
        self.assertEquals(2, len(res))
        self.assertEquals({'id': 4, 'val': 'd'}, top[0])
        self.assertEquals({'id': 2, 'val': 'b'}, top[1])
#        self.assertTrue(eq_array([2, 4], res['id']))

    def test_filter_by_bad_column_name_type(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(TypeError):
            res = t.filter_by([1, 3], 1)

    def test_filter_by_bad_column_name(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(KeyError):
            res = t.filter_by([1, 3], 'xx')

    def test_filter_by_bad_column_type(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(TypeError):
            res = t.filter_by([1, 3], 'val')


class TestXFramePackColumnsList(unittest.TestCase):
    """
    Tests XFrame pack_columns into list
    """

    def test_pack_columns(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new')
        self.assertEqual(4, len(res))
        self.assertEqual(1, res.num_columns())
        self.assertEqual([list], res.dtype())
        self.assertEqual(['new'], res.column_names())
        self.assertEqual({'new': [1, 'a']}, res[0])
        self.assertEqual({'new': [2, 'b']}, res[1])

    def test_pack_columns_all(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.pack_columns(new_column_name='new')
        self.assertEqual(4, len(res))
        self.assertEqual(1, res.num_columns())
        self.assertEqual([list], res.dtype())
        self.assertEqual(['new'], res.column_names())
        self.assertEqual({'new': [1, 'a']}, res[0])
        self.assertEqual({'new': [2, 'b']}, res[1])

    def test_pack_columns_prefix(self):
        t = XFrame({'x.id': [1, 2, 3, 4], 'x.val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(column_prefix='x', new_column_name='new')
        self.assertEqual(4, len(res))
        self.assertEqual(2, res.num_columns())
        self.assertEqual([int, list], res.dtype())
        self.assertEqual(['another', 'new'], res.column_names())
        self.assertEqual({'another': 10, 'new': [1, 'a']}, res[0])
        self.assertEqual({'another': 20, 'new': [2, 'b']}, res[1])

    def test_pack_columns_rest(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new')
        self.assertEqual(4, len(res))
        self.assertEqual(2, res.num_columns())
        self.assertEqual([int, list], res.dtype())
        self.assertEqual(['another', 'new'], res.column_names())
        self.assertEqual({'another': 10, 'new': [1, 'a']}, res[0])
        self.assertEqual({'another': 20, 'new': [2, 'b']}, res[1])

    def test_pack_columns_na(self):
        t = XFrame({'id': [1, 2, None, 4], 'val': ['a', 'b', 'c', None]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', fill_na='x')
        self.assertEqual(4, len(res))
        self.assertEqual(1, res.num_columns())
        self.assertEqual([list], res.dtype())
        self.assertEqual(['new'], res.column_names())
        self.assertEqual({'new': [1, 'a']}, res[0])
        self.assertEqual({'new': [2, 'b']}, res[1])
        self.assertEqual({'new': ['x', 'c']}, res[2])
        self.assertEqual({'new': [4, 'x']}, res[3])

    def test_pack_columns_fill_na(self):
        t = XFrame({'id': [1, 2, None, 4], 'val': ['a', 'b', 'c', None]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', fill_na=99)
        self.assertEqual(4, len(res))
        self.assertEqual(1, res.num_columns())
        self.assertEqual([list], res.dtype())
        self.assertEqual(['new'], res.column_names())
        self.assertEqual({'new': [1, 'a']}, res[0])
        self.assertEqual({'new': [2, 'b']}, res[1])
        self.assertEqual({'new': [99, 'c']}, res[2])
        self.assertEqual({'new': [4, 99]}, res[3])

    def test_pack_columns_def_new_name(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.pack_columns(columns=['id', 'val'])
        self.assertEqual(4, len(res))
        self.assertEqual(1, res.num_columns())
        self.assertEqual([list], res.dtype())
        self.assertEqual(['X0'], res.column_names())
        self.assertEqual({'X0': [1, 'a']}, res[0])
        self.assertEqual({'X0': [2, 'b']}, res[1])

    def test_pack_columns_prefix_def_new_name(self):
        t = XFrame({'x.id': [1, 2, 3, 4], 'x.val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(column_prefix='x')
        self.assertEqual(4, len(res))
        self.assertEqual(2, res.num_columns())
        self.assertEqual([int, list], res.dtype())
        self.assertEqual(['another', 'x'], res.column_names())
        self.assertEqual({'another': 10, 'x': [1, 'a']}, res[0])
        self.assertEqual({'another': 20, 'x': [2, 'b']}, res[1])

    def test_pack_columns_bad_col_spec(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            res = t.pack_columns(columns='id', column_prefix='val')

    def test_pack_columns_bad_col_prefix_type(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(TypeError):
            res = t.pack_columns(column_prefix=1)

    def test_pack_columns_bad_col_prefix(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            res = t.pack_columns(column_prefix='xx')

    def test_pack_columns_bad_cols(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(TypeError):
            res = t.pack_columns(columns='xx')

    def test_pack_columns_bad_cols(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            res = t.pack_columns(columns=['xx'])

    def test_pack_columns_bad_cols_dup(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            res = t.pack_columns(columns=['id', 'id'])

    def test_pack_columns_bad_cols_single(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            res = t.pack_columns(columns=['id'])

    def test_pack_columns_bad_dtype(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(ValueError):
            res = t.pack_columns(columns=['id', 'val'], dtype=int)
    
    def test_pack_columns_bad_new_col_name_type(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        with self.assertRaises(TypeError):
            res = t.pack_columns(columns=['id', 'val'], new_column_name=1)
    
    def test_pack_columns_bad_new_col_name_dup_rest(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd'], 'another': [11, 12, 13, 14]})
        with self.assertRaises(KeyError):
            res = t.pack_columns(columns=['id', 'val'], new_column_name='another')
    
    def test_pack_columns_good_new_col_name_dup_key(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='id')
        self.assertEqual(['id'], res.column_names())
        self.assertEqual({'id': [1, 'a']}, res[0])
        self.assertEqual({'id': [2, 'b']}, res[1])
    

class TestXFramePackColumnsDict(unittest.TestCase):
    """
    Tests XFrame pack_columns into dict
    """

    def test_pack_columns(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=dict)
        self.assertEqual(4, len(res))
        self.assertEqual(1, res.num_columns())
        self.assertEqual([dict], res.dtype())
        self.assertEqual(['new'], res.column_names())
        self.assertEqual({'new': {'id': 1, 'val': 'a'}}, res[0])
        self.assertEqual({'new': {'id': 2, 'val': 'b'}}, res[1])

    def test_pack_columns_prefix(self):
        t = XFrame({'x.id': [1, 2, 3, 4], 'x.val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(column_prefix='x', dtype=dict)
        self.assertEqual(4, len(res))
        self.assertEqual(2, res.num_columns())
        self.assertEqual([int, dict], res.dtype())
        self.assertEqual(['another', 'x'], res.column_names())
        self.assertEqual({'another': 10, 'x': {'id': 1, 'val': 'a'}}, res[0])
        self.assertEqual({'another': 20, 'x': {'id': 2, 'val': 'b'}}, res[1])

    def test_pack_columns_prefix_named(self):
        t = XFrame({'x.id': [1, 2, 3, 4], 'x.val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(column_prefix='x', dtype=dict, new_column_name='new')
        self.assertEqual(4, len(res))
        self.assertEqual(2, res.num_columns())
        self.assertEqual([int, dict], res.dtype())
        self.assertEqual(['another', 'new'], res.column_names())
        self.assertEqual({'another': 10, 'new': {'id': 1, 'val': 'a'}}, res[0])
        self.assertEqual({'another': 20, 'new': {'id': 2, 'val': 'b'}}, res[1])

    def test_pack_columns_prefix_no_remove(self):
        t = XFrame({'x.id': [1, 2, 3, 4], 'x.val': ['a', 'b', 'c', 'd'], 'another': [10, 20, 30, 40]})
        res = t.pack_columns(column_prefix='x', dtype=dict, remove_prefix=False)
        self.assertEqual(4, len(res))
        self.assertEqual(2, res.num_columns())
        self.assertEqual([int, dict], res.dtype())
        self.assertEqual(['another', 'x'], res.column_names())
        self.assertEqual({'another': 10, 'x': {'x.id': 1, 'x.val': 'a'}}, res[0])
        self.assertEqual({'another': 20, 'x': {'x.id': 2, 'x.val': 'b'}}, res[1])

    def test_pack_columns_drop_missing(self):
        t = XFrame({'id': [1, 2, None, 4], 'val': ['a', 'b', 'c', None]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=dict)
        self.assertEqual(4, len(res))
        self.assertEqual(1, res.num_columns())
        self.assertEqual([dict], res.dtype())
        self.assertEqual(['new'], res.column_names())
        self.assertEqual({'new': {'id': 1, 'val': 'a'}}, res[0])
        self.assertEqual({'new': {'id': 2, 'val': 'b'}}, res[1])
        self.assertEqual({'new': {'val': 'c'}}, res[2])
        self.assertEqual({'new': {'id': 4}}, res[3])

    def test_pack_columns_fill_na(self):
        t = XFrame({'id': [1, 2, None, 4], 'val': ['a', 'b', 'c', None]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=dict, fill_na=99)
        self.assertEqual(4, len(res))
        self.assertEqual(1, res.num_columns())
        self.assertEqual([dict], res.dtype())
        self.assertEqual(['new'], res.column_names())
        self.assertEqual({'new': {'id': 1, 'val': 'a'}}, res[0])
        self.assertEqual({'new': {'id': 2, 'val': 'b'}}, res[1])
        self.assertEqual({'new': {'id': 99, 'val': 'c'}}, res[2])
        self.assertEqual({'new': {'id': 4, 'val': 99}}, res[3])


class TestXFramePackColumnsArray(unittest.TestCase):
    """
    Tests XFrame pack_columns into array
    """

    def test_pack_columns(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [10, 20, 30, 40]})
        res = t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=array.array)
        self.assertEqual(4, len(res))
        self.assertEqual(1, res.num_columns())
        self.assertEqual([array.array], res.dtype())
        self.assertEqual(['new'], res.column_names())
        self.assertEqual({'new': array.array('d', [1.0, 10.0])}, res[0])
        self.assertEqual({'new': array.array('d', [2.0, 20.0])}, res[1])

    def test_pack_columns_bad_fill_na_not_numeric(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [10, 20, 30, 40]})
        with self.assertRaises(ValueError):
            res = t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=array.array, fill_na='a')

    def test_pack_columns_bad_not_numeric(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': ['a' 'b' 'c', 'd']})
        with self.assertRaises(TypeError):
            res = t.pack_columns(columns=['id', 'val'], new_column_name='new', dtype=array.array)

class TestXFrameUnpackList(unittest.TestCase):
    """
    Tests XFrame unpack where the unpacked column contains a list
    """
    def test_unpack(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [[10, 'a'], [20, 'b'], [30, 'c'], [40, 'd']]})
        res = t.unpack('val')
        self.assertEqual(4, len(res))
        self.assertEqual(['id', 'val.0', 'val.1'], res.column_names())
        self.assertEqual([int, int, str], res.column_types())
        self.assertEqual({'id': 1, 'val.0': 10, 'val.1': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val.0': 20, 'val.1': 'b'}, res[1])

    def test_unpack_prefix(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [[10, 'a'], [20, 'b'], [30, 'c'], [40, 'd']]})
        res = t.unpack('val', column_name_prefix='x')
        self.assertEqual(4, len(res))
        self.assertEqual(['id', 'x.0', 'x.1'], res.column_names())
        self.assertEqual([int, int, str], res.column_types())
        self.assertEqual({'id': 1, 'x.0': 10, 'x.1': 'a'}, res[0])
        self.assertEqual({'id': 2, 'x.0': 20, 'x.1': 'b'}, res[1])

    def test_unpack_types(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [[10, 'a'], [20, 'b'], [30, 'c'], [40, 'd']]})
        res = t.unpack('val', column_types=[str, str])
        self.assertEqual(4, len(res))
        self.assertEqual(['id', 'val.0', 'val.1'], res.column_names())
        self.assertEqual([int, str, str], res.column_types())
        self.assertEqual({'id': 1, 'val.0': '10', 'val.1': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val.0': '20', 'val.1': 'b'}, res[1])

    def test_unpack_na_value(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [[10, 'a'], [20, 'b'], [None, 'c'], [40, None]]})
        res = t.unpack('val', na_value=99)
        self.assertEqual(4, len(res))
        self.assertEqual(['id', 'val.0', 'val.1'], res.column_names())
        self.assertEqual([int, int, str], res.column_types())
        self.assertEqual({'id': 1, 'val.0': 10, 'val.1': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val.0': 20, 'val.1': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val.0': 99, 'val.1': 'c'}, res[2])
        self.assertEqual({'id': 4, 'val.0': 40, 'val.1': '99'}, res[3])

    def test_unpack_limit(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [[10, 'a'], [20, 'b'], [30, 'c'], [40, 'd']]})
        res = t.unpack('val', limit=[1])
        self.assertEqual(4, len(res))
        self.assertEqual(['id', 'val.1'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 1, 'val.1': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val.1': 'b'}, res[1])


class TestXFrameUnpackDict(unittest.TestCase):
    """
    Tests XFrame unpack where the unpacked column contains a dict
    """
    def test_unpack(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 1}, {'b': 2}, {'c': 3}, {'d': 4}]})
        res = t.unpack('val')
        self.assertEqual(4, len(res))
        self.assertEqual(['id', 'val.a', 'val.c', 'val.b', 'val.d'], res.column_names())
        self.assertEqual([int, int, int, int, int], res.column_types())
        self.assertEqual({'id': 1, 'val.a': 1, 'val.c': None, 'val.b': None, 'val.d': None}, res[0])
        self.assertEqual({'id': 2, 'val.a': None, 'val.c': None, 'val.b': 2, 'val.d': None}, res[1])

    def test_unpack_mult(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        res = t.unpack('val')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val.a', 'val.b'], res.column_names())
        self.assertEqual([int, int, int], res.column_types())
        self.assertEqual({'id': 1, 'val.a': 1, 'val.b': None}, res[0])
        self.assertEqual({'id': 2, 'val.a': None, 'val.b': 2}, res[1])
        self.assertEqual({'id': 3, 'val.a': 1, 'val.b': 2}, res[2])

    def test_unpack_prefix(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        res = t.unpack('val', column_name_prefix='x')
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'x.a', 'x.b'], res.column_names())
        self.assertEqual([int, int, int], res.column_types())
        self.assertEqual({'id': 1, 'x.a': 1, 'x.b': None}, res[0])
        self.assertEqual({'id': 2, 'x.a': None, 'x.b': 2}, res[1])
        self.assertEqual({'id': 3, 'x.a': 1, 'x.b': 2}, res[2])

    def test_unpack_types(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        res = t.unpack('val', column_types=[str, str], limit=['a', 'b'])
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val.a', 'val.b'], res.column_names())
        self.assertEqual([int, str, str], res.column_types())
        self.assertEqual({'id': 1, 'val.a': '1', 'val.b': None}, res[0])
        self.assertEqual({'id': 2, 'val.a': None, 'val.b': '2'}, res[1])
        self.assertEqual({'id': 3, 'val.a': '1', 'val.b': '2'}, res[2])

    def test_unpack_na_value(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        res = t.unpack('val', na_value=99)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val.a', 'val.b'], res.column_names())
        self.assertEqual([int, int, int], res.column_types())
        self.assertEqual({'id': 1, 'val.a': 1, 'val.b': 99}, res[0])
        self.assertEqual({'id': 2, 'val.a': 99, 'val.b': 2}, res[1])
        self.assertEqual({'id': 3, 'val.a': 1, 'val.b': 2}, res[2])

    def test_unpack_limit(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        res = t.unpack('val', limit=['b'])
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'val.b'], res.column_names())
        self.assertEqual([int, int], res.column_types())
        self.assertEqual({'id': 1, 'val.b': None}, res[0])
        self.assertEqual({'id': 2, 'val.b': 2}, res[1])
        self.assertEqual({'id': 3, 'val.b': 2}, res[2])

    def test_unpack_bad_types_no_limit(self):
        t = XFrame({'id': [1, 2, 3], 'val': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        with self.assertRaises(ValueError):
            res = t.unpack('val', column_types=[str, str])


# TODO unpack array

class TestXFrameStackList(unittest.TestCase):
    """
    Tests XFrame stack where column is a list
    """

    def test_stack_list(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        res = t.stack('val')
        self.assertEqual(['id', 'X'], res.column_names())
        self.assertEqual(9, len(res))
        self.assertEqual({'id': 1, 'X': 'a1'}, res[0])
        self.assertEqual({'id': 3, 'X': None}, res[8])

    def test_stack_list_drop_na(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        res = t.stack('val', drop_na=True)
        self.assertEqual(['id', 'X'], res.column_names())
        self.assertEqual(8, len(res))
        self.assertEqual({'id': 1, 'X': 'a1'}, res[0])
        self.assertEqual({'id': 3, 'X': 'c3'}, res[7])

    def test_stack_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        res = t.stack('val', new_column_name='flat_val')
        self.assertEqual(['id', 'flat_val'], res.column_names())
        self.assertEqual(9, len(res))

    def test_stack_bad_col_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        with self.assertRaises(ValueError):
            res = t.stack('xx')

    def test_stack_bad_col_value(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            res = t.stack('val')

    def test_stack_bad_new_col_name_type(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            res = t.stack('val', new_col_name=1)

    def test_stack_new_col_name_dup_ok(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        res = t.stack('val', new_column_name='val')
        self.assertEqual(['id', 'val'], res.column_names())

    def test_stack_bad_new_col_name_dup(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        with self.assertRaises(ValueError):
            res = t.stack('val', new_column_name='id')

    def test_stack_bad_no_data(self):
        t = XFrame({'id': [1, 2, 3], 'val': [['a1', 'b1', 'c1'], ['a2', 'b2'], ['a3', 'b3', 'c3', None]]})
        t = t.head(0)
        with self.assertRaises(ValueError):
            res = t.stack('val', new_column_name='val')

class TestXFrameStackDict(unittest.TestCase):
    """
    Tests XFrame stack where column is a dict
    """

    def test_stack_dict(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        res = t.stack('val')
        self.assertEqual(['id', 'K', 'V'], res.column_names())
        self.assertEqual(7, len(res))
        self.assertEqual({'id': 1, 'K': 'a', 'V': 3}, res[0])
        self.assertEqual({'id': 4, 'K': None, 'V': None}, res[6])

    def test_stack_names(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        res = t.stack('val', ['new_k', 'new_v'])
        self.assertEqual(['id', 'new_k', 'new_v'], res.column_names())
        self.assertEqual(7, len(res))
        self.assertEqual({'id': 1, 'new_k': 'a', 'new_v': 3}, res[0])
        self.assertEqual({'id': 4, 'new_k': None, 'new_v': None}, res[6])

    def test_stack_dropna(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        res = t.stack('val', drop_na=True)
        self.assertEqual(['id', 'K', 'V'], res.column_names())
        self.assertEqual(6, len(res))
        self.assertEqual({'id': 1, 'K': 'a', 'V': 3}, res[0])
        self.assertEqual({'id': 3, 'K': 'd', 'V': 3}, res[5])

    def test_stack_bad_col_name(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        with self.assertRaises(ValueError):
            res = t.stack('xx')

    def test_stack_bad_new_col_name_type(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        with self.assertRaises(TypeError):
            res = t.stack('val', new_column_name=1)

    def test_stack_bad_new_col_name_len(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        with self.assertRaises(TypeError):
            res = t.stack('val', new_column_name=['a'])

    def test_stack_bad_new_col_name_dup(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        with self.assertRaises(ValueError):
            res = t.stack('val', new_column_name=['id', 'xx'])

    def test_stack_bad_no_data(self):
        t = XFrame({'id': [1, 2, 3, 4], 'val': [{'a': 3, 'b': 2}, {'a': 2, 'c': 2}, {'c': 1, 'd': 3}, {}]})
        t = t.head(0)
        with self.assertRaises(ValueError):
            res = t.stack('val', new_column_name=['k', 'v'])

class TestXFrameUnstackList(unittest.TestCase):
    """
    Tests XFrame unstack where unstack column is list
    """

    def test_unstack(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1, 3], 'val':['a1', 'b1', 'c1', 'a2', 'b2', 'a3', 'c3']})
        res = t.unstack('val')
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'unstack'], res.column_names())
        self.assertEqual([int, list], res.column_types())
        self.assertEqual({'id': 1, 'unstack': ['a1', 'a2', 'a3']}, res[0])
        self.assertEqual({'id': 2, 'unstack': ['b1', 'b2']}, res[1])
        self.assertEqual({'id': 3, 'unstack': ['c1', 'c3']}, res[2])

    def test_unstack_name(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1, 3], 'val':['a1', 'b1', 'c1', 'a2', 'b2', 'a3', 'c3']})
        res = t.unstack('val', new_column_name='vals')
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'vals'], res.column_names())
        self.assertEqual([int, list], res.column_types())
        self.assertEqual({'id': 1, 'vals': ['a1', 'a2', 'a3']}, res[0])
        self.assertEqual({'id': 2, 'vals': ['b1', 'b2']}, res[1])
        self.assertEqual({'id': 3, 'vals': ['c1', 'c3']}, res[2])

class TestXFrameUnstackDict(unittest.TestCase):
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
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'unstack'], res.column_names())
        self.assertEqual([int, dict], res.column_types())
        self.assertEqual({'id': 1, 'unstack': {'ka1': 'a1', 'ka2': 'a2', 'ka3': 'a3'}}, res[0])
        self.assertEqual({'id': 2, 'unstack': {'kb1': 'b1', 'kb2': 'b2'}}, res[1])
        self.assertEqual({'id': 3, 'unstack': {'kc1': 'c1', 'kc3': 'c3'}}, res[2])


    def test_unstack_name(self):
        t = XFrame({'id': [1, 2, 3, 1, 2, 1, 3], 
                    'key': ['ka1', 'kb1', 'kc1', 'ka2', 'kb2', 'ka3', 'kc3'], 
                    'val': ['a1', 'b1', 'c1', 'a2', 'b2', 'a3', 'c3']})
        res = t.unstack(['key', 'val'], new_column_name='vals')
        res = res.topk('id', reverse=True)
        self.assertEqual(3, len(res))
        self.assertEqual(['id', 'vals'], res.column_names())
        self.assertEqual([int, dict], res.column_types())
        self.assertEqual({'id': 1, 'vals': {'ka1': 'a1', 'ka2': 'a2', 'ka3': 'a3'}}, res[0])
        self.assertEqual({'id': 2, 'vals': {'kb1': 'b1', 'kb2': 'b2'}}, res[1])
        self.assertEqual({'id': 3, 'vals': {'kc1': 'c1', 'kc3': 'c3'}}, res[2])


class TestXFrameUnique(unittest.TestCase):
    """
    Tests XFrame unique
    """

    def test_unique_noop(self):
        t = XFrame({'id': [3, 2, 1], 'val': ['c', 'b', 'a']})
        res = t.unique()
        self.assertEqual(3, len(res))

    def test_unique(self):
        t = XFrame({'id': [3, 2, 1, 1], 'val': ['c', 'b', 'a', 'a']})
        res = t.unique()
        self.assertEqual(3, len(res))

    def test_unique_part(self):
        t = XFrame({'id': [3, 2, 1, 1], 'val': ['c', 'b', 'a', 'x']})
        res = t.unique()
        self.assertEqual(4, len(res))

class TestXFrameSort(unittest.TestCase):
    """
    Tests XFrame sort
    """

    def test_sort(self):
        t = XFrame({'id': [3, 2, 1], 'val': ['c', 'b', 'a']})
        res = t.sort('id')
        self.assertTrue(eq_array([1, 2, 3], res['id']))
        self.assertTrue(eq_array(['a', 'b', 'c'], res['val']))

    def test_sort_descending(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.sort('id', ascending=False)
        self.assertTrue(eq_array([3, 2, 1], res['id']))
        self.assertTrue(eq_array(['c', 'b', 'a'], res['val']))

    def test_sort_multi_col(self):
        t = XFrame({'id': [3, 2, 1, 1], 'val': ['c', 'b', 'b', 'a']})
        res = t.sort(['id', 'val'])
        self.assertTrue(eq_array([1, 1, 2, 3], res['id']))
        self.assertTrue(eq_array(['a', 'b', 'b', 'c'], res['val']))

    def test_sort_multi_col_asc_desc(self):
        t = XFrame({'id': [3, 2, 1, 1], 'val': ['c', 'b', 'b', 'a']})
        res = t.sort([('id', True), ('val', False)])
        self.assertTrue(eq_array([1, 1, 2, 3], res['id']))
        self.assertTrue(eq_array(['b', 'a', 'b', 'c'], res['val']))

class TestXFrameDropna(unittest.TestCase):
    """
    Tests XFrame dropna
    """

    def test_dropna_no_drop(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.dropna()
        self.assertEqual(3, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_dropna_none(self):
        t = XFrame({'id': [1, None, 3], 'val': ['a', 'b', 'c']})
        res = t.dropna()
        self.assertEqual(2, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 3, 'val': 'c'}, res[1])

    def test_dropna_nan(self):
        t = XFrame({'id': [1.0, float('nan'), 3.0], 'val': ['a', 'b', 'c']})
        res = t.dropna()
        self.assertEqual(2, len(res))
        self.assertEqual({'id': 1.0, 'val': 'a'}, res[0])
        self.assertEqual({'id': 3.0, 'val': 'c'}, res[1])

    def test_dropna_empty_list(self):
        t = XFrame({'id': [1, None, 3], 'val': ['a', 'b', 'c']})
        res = t.dropna(columns=[])
        self.assertEqual(3, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': None, 'val': 'b'}, res[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res[2])

    def test_dropna_any(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', None, 'c']})
        res = t.dropna()
        self.assertEqual(1, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])

    def test_dropna_all(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', None, 'c']})
        res = t.dropna(how='all')
        self.assertEqual(2, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': None, 'val': 'c'}, res[1])

    def test_dropna_col_val(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', None, 'c']})
        res = t.dropna(columns='val')
        self.assertEqual(2, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': None, 'val': 'c'}, res[1])

    def test_dropna_col_id(self):
        t = XFrame({'id': [1, 2, None], 'val': ['a', None, 'c']})
        res = t.dropna(columns='id')
        self.assertEqual(2, len(res))
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 2, 'val': None}, res[1])

    def test_dropna_bad_col_arg(self):
        t = XFrame({'id': [1, 2, None], 'val': ['a', None, 'c']})
        with self.assertRaises(TypeError):
            res = t.dropna(columns=1)

    def test_dropna_bad_col_name_in_list(self):
        t = XFrame({'id': [1, 2, None], 'val': ['a', None, 'c']})
        with self.assertRaises(TypeError):
            res = t.dropna(columns=['id', 2])

    def test_dropna_bad_how(self):
        t = XFrame({'id': [1, 2, None], 'val': ['a', None, 'c']})
        with self.assertRaises(ValueError):
            res = t.dropna(how='xx')

class TestXFrameDropnaSplit(unittest.TestCase):
    """
    Tests XFrame dropna_split
    """

    def test_dropna_split_no_drop(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res1, res2 = t.dropna_split()
        self.assertEqual(3, len(res1))
        self.assertEqual({'id': 1, 'val': 'a'}, res1[0])
        self.assertEqual({'id': 2, 'val': 'b'}, res1[1])
        self.assertEqual({'id': 3, 'val': 'c'}, res1[2])
        self.assertEqual(0, len(res2))

    def test_dropna_split_none(self):
        t = XFrame({'id': [1, None, 3], 'val': ['a', 'b', 'c']})
        res1, res2 = t.dropna_split()
        self.assertEqual(2, len(res1))
        self.assertEqual({'id': 1, 'val': 'a'}, res1[0])
        self.assertEqual({'id': 3, 'val': 'c'}, res1[1])
        self.assertEqual(1, len(res2))
        self.assertEqual({'id': None, 'val': 'b'}, res2[0])

    def test_dropna_split_all(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', None, 'c']})
        res1, res2 = t.dropna_split(how='all')
        self.assertEqual(2, len(res1))
        self.assertEqual({'id': 1, 'val': 'a'}, res1[0])
        self.assertEqual({'id': None, 'val': 'c'}, res1[1])
        self.assertEqual(1, len(res2))
        self.assertEqual({'id': None, 'val': None}, res2[0])


class TestXFrameFillna(unittest.TestCase):
    """
    Tests XFrame fillna
    """

    def test_fillna(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', 'b', 'c']})
        res = t.fillna('id', 0)
        self.assertEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 0, 'val': 'b'}, res[1])
        self.assertEqual({'id': 0, 'val': 'c'}, res[2])

    def test_fillna_bad_col_name(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', 'b', 'c']})
        with self.assertRaises(ValueError):
            res = t.fillna('xx', 0)

    def test_fillna_bad_arg_type(self):
        t = XFrame({'id': [1, None, None], 'val': ['a', 'b', 'c']})
        with self.assertRaises(TypeError):
            res = t.fillna(1, 0)

class TestXFrameAddRowNumber(unittest.TestCase):
    """
    Tests XFrame add_row_number
    """

    def test_add_row_number(self):
        t = XFrame({'ident': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.add_row_number()
        self.assertEqual(['id', 'ident', 'val'], res.column_names())
        self.assertEqual({'id': 0, 'ident': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 1, 'ident': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 2, 'ident': 3, 'val': 'c'}, res[2])

    def test_add_row_number_start(self):
        t = XFrame({'ident': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.add_row_number(start=10)
        self.assertEqual(['id', 'ident', 'val'], res.column_names())
        self.assertEqual({'id': 10, 'ident': 1, 'val': 'a'}, res[0])
        self.assertEqual({'id': 11, 'ident': 2, 'val': 'b'}, res[1])
        self.assertEqual({'id': 12, 'ident': 3, 'val': 'c'}, res[2])

    def test_add_row_number_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.add_row_number(column_name='row_number')
        self.assertEqual(['row_number', 'id', 'val'], res.column_names())
        self.assertEqual({'row_number': 0, 'id': 1, 'val': 'a'}, res[0])
        self.assertEqual({'row_number': 1, 'id': 2, 'val': 'b'}, res[1])
        self.assertEqual({'row_number': 2, 'id': 3, 'val': 'c'}, res[2])

class TestXFrameShape(unittest.TestCase):
    """
    Tests XFrame shape
    """

    def test_shape(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        self.assertEqual((3, 2), t.shape)

    def test_shape_empty(self):
        t = XFrame()
        self.assertEqual((0, 0), t.shape)

class TestXFrameSql(unittest.TestCase):
    """
    Tests XFrame sql
    """

    def test_sql(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.sql("SELECT * FROM xframe WHERE id > 1 ORDER BY id")
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 2, 'val': 'b'}, res[0])
        self.assertEqual({'id': 3, 'val': 'c'}, res[1])

    def test_sql_name(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        res = t.sql("SELECT * FROM tmp_tbl WHERE id > 1 ORDER BY id", table_name='tmp_tbl')
        self.assertEqual(['id', 'val'], res.column_names())
        self.assertEqual([int, str], res.column_types())
        self.assertEqual({'id': 2, 'val': 'b'}, res[0])
        self.assertEqual({'id': 3, 'val': 'c'}, res[1])


if __name__ == '__main__':
    unittest.main()

