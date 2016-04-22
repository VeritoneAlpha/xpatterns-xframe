import unittest
import math
import os
import array
import datetime
import pickle
import shutil

# python testxarray.py
# python -m unittest testxarray
# python -m unittest testxarray.TestXArrayVersion
# python -m unittest testxarray.TestXArrayVersion.test_version

from xframes import XArray
from xframes import XFrame


def delete_file_or_dir(path):
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
    elif os.path.isfile(path):
        os.remove(path)


class XArrayUnitTestCase(unittest.TestCase):

    def assertEqualLen(self, expect, obj):
        return self.assertEqual(expect, len(obj))

    def assertColumnEqual(self, expect, obj):
        return self.assertListEqual(expect, list(obj))


class TestXArrayVersion(XArrayUnitTestCase):
    """
    Tests XArray version
    """

    def test_version(self):
        ver = XArray.version()
        self.assertIs(str, type(ver))


class TestXArrayConstructorLocal(XArrayUnitTestCase):
    """
    Tests XArray constructors that create data from local sources.
    """

    def test_construct_list_int_infer(self):
        t = XArray([1, 2, 3])
        self.assertEqualLen(3, t)
        self.assertEqual(1, t[0])
        self.assertIs(int, t.dtype())

    def test_construct_list_int(self):
        t = XArray([1, 2, 3], dtype=int)
        self.assertEqualLen(3, t)
        self.assertEqual(1, t[0])
        self.assertIs(int, t.dtype())

    def test_construct_list_str_infer(self):
        t = XArray(['a', 'b', 'c'])
        self.assertEqualLen(3, t)
        self.assertEqual('a', t[0])
        self.assertIs(str, t.dtype())

    def test_construct_list_str(self):
        t = XArray([1, 2, 3], dtype=str)
        self.assertEqualLen(3, t)
        self.assertEqual('1', t[0])
        self.assertIs(str, t.dtype())

    def test_construct_list_float_infer(self):
        t = XArray([1.0, 2.0, 3.0])
        self.assertEqualLen(3, t)
        self.assertEqual(1.0, t[0])
        self.assertIs(float, t.dtype())

    def test_construct_list_float(self):
        t = XArray([1, 2, 3], dtype=float)
        self.assertEqualLen(3, t)
        self.assertEqual(1.0, t[0])
        self.assertIs(float, t.dtype())

    def test_construct_list_bool_infer(self):
        t = XArray([True, False])
        self.assertEqualLen(2, t)
        self.assertTrue(t[0])
        self.assertIs(bool, t.dtype())

    def test_construct_list_bool(self):
        t = XArray([True, False], dtype=bool)
        self.assertEqualLen(2, t)
        self.assertTrue(t[0])
        self.assertIs(bool, t.dtype())

    def test_construct_list_list_infer(self):
        t = XArray([[1, 2, 3], [10]])
        self.assertEqualLen(2, t)
        self.assertListEqual([1, 2, 3], t[0])
        self.assertListEqual([10], t[1])
        self.assertIs(list, t.dtype())

    def test_construct_list_list(self):
        t = XArray([[1, 2, 3], [10]], dtype=list)
        self.assertEqualLen(2, t)
        self.assertListEqual([1, 2, 3], t[0])
        self.assertListEqual([10], t[1])
        self.assertIs(list, t.dtype())

    def test_construct_list_dict_infer(self):
        t = XArray([{'a': 1, 'b': 2}, {'x': 10}])
        self.assertEqualLen(2, t)
        self.assertDictEqual({'a': 1, 'b': 2}, t[0])
        self.assertIs(dict, t.dtype())

    def test_construct_list_dict(self):
        t = XArray([{'a': 1, 'b': 2}, {'x': 10}], dtype=dict)
        self.assertEqualLen(2, t)
        self.assertDictEqual({'a': 1, 'b': 2}, t[0])
        self.assertIs(dict, t.dtype())

    def test_construct_empty_list_infer(self):
        t = XArray([])
        self.assertEqualLen(0, t)
        self.assertIsNone(t.dtype())
    
    def test_construct_empty_list(self):
        t = XArray([], dtype=int)
        self.assertEqualLen(0, t)
        self.assertIs(int, t.dtype())

    def test_construct_list_int_cast_fail(self):
        with self.assertRaises(ValueError):
            t = XArray(['a', 'b', 'c'], dtype=int)
            print t     # force materialization

    def test_construct_list_int_cast_ignore(self):
        t = XArray(['1', '2', 'c'], dtype=int, ignore_cast_failure=True)
        self.assertEqualLen(3, t)
        self.assertEqual(1, t[0])
        self.assertIsNone(t[2])
        self.assertIs(int, t.dtype())


class TestXArrayConstructorRange(XArrayUnitTestCase):
    """
    Tests XArray constructors for sequential ranges.
    """

    # noinspection PyArgumentList
    def test_construct_none(self):
        with self.assertRaises(TypeError):
            XArray.from_sequence()

    # noinspection PyTypeChecker
    def test_construct_nonint_stop(self):
        with self.assertRaises(TypeError):
            XArray.from_sequence(1.0)

    # noinspection PyTypeChecker
    def test_construct_nonint_start(self):
        with self.assertRaises(TypeError):
            XArray.from_sequence(1.0, 10.0)

    def test_construct_stop(self):
        t = XArray.from_sequence(100, 200)
        self.assertEqualLen(100, t)
        self.assertEqual(100, t[0])
        self.assertIs(int, t.dtype())

    def test_construct_start(self):
        t = XArray.from_sequence(100)
        self.assertEqualLen(100, t)
        self.assertEqual(0, t[0])
        self.assertIs(int, t.dtype())


class TestXArrayConstructFromRdd(XArrayUnitTestCase):
    """
    Tests XArray from_rdd class method
    """

    def test_construct_from_rdd(self):
        # TODO test
        pass


class TestXArrayConstructorLoad(XArrayUnitTestCase):
    """
    Tests XArray constructors that loads from file.
    """

    def test_construct_local_file_int(self):
        t = XArray('files/test-array-int')
        self.assertEqualLen(4, t)
        self.assertIs(int, t.dtype())
        self.assertEqual(1, t[0])

    def test_construct_local_file_float(self):
        t = XArray('files/test-array-float')
        self.assertEqualLen(4, t)
        self.assertIs(float, t.dtype())
        self.assertEqual(1.0, t[0])

    def test_construct_local_file_str(self):
        t = XArray('files/test-array-str')
        self.assertEqualLen(4, t)
        self.assertIs(str, t.dtype())
        self.assertEqual('a', t[0])

    def test_construct_local_file_list(self):
        t = XArray('files/test-array-list')
        self.assertEqualLen(4, t)
        self.assertIs(list, t.dtype())
        self.assertListEqual([1, 2], t[0])

    def test_construct_local_file_dict(self):
        t = XArray('files/test-array-dict')
        self.assertEqualLen(4, t)
        self.assertIs(dict, t.dtype())
        self.assertDictEqual({1: 'a', 2: 'b'}, t[0])

    @unittest.skip('not working on jenkins')
    def test_construct_local_file_datetime(self):
        t = XArray('files/test-array-datetime')
        self.assertEqualLen(3, t)
        self.assertIs(datetime.datetime, t.dtype())
        self.assertEqual(datetime.datetime(2015, 8, 15), t[0])
        self.assertEqual(datetime.datetime(2016, 9, 16), t[1])
        self.assertEqual(datetime.datetime(2017, 10, 17), t[2])

    def test_construct_local_file_not_exist(self):
        with self.assertRaises(ValueError):
            _ = XArray('files/does-not-exist')


class TestXArrayReadText(XArrayUnitTestCase):
    """
    Tests XArray read_text class method
    """

    def test_read_text(self):
        t = XArray.read_text('files/test-array-int')
        self.assertEqual(4, len(t))
        self.assertListEqual(['1', '2', '3', '4'], list(t))


class TestXArrayFromConst(XArrayUnitTestCase):
    """
    Tests XArray constructed from const.
    """

    def test_from_const_int(self):
        t = XArray.from_const(1, 10)
        self.assertEqualLen(10, t)
        self.assertEqual(1, t[0])
        self.assertIs(int, t.dtype())

    def test_from_const_float(self):
        t = XArray.from_const(1.0, 10)
        self.assertEqualLen(10, t)
        self.assertEqual(1.0, t[0])
        self.assertIs(float, t.dtype())

    def test_from_const_str(self):
        t = XArray.from_const('a', 10)
        self.assertEqualLen(10, t)
        self.assertEqual('a', t[0])
        self.assertIs(str, t.dtype())

    def test_from_const_datetime(self):
        t = XArray.from_const(datetime.datetime(2015, 10, 11), 10)
        self.assertEqualLen(10, t)
        self.assertEqual(datetime.datetime(2015, 10, 11), t[0])
        self.assertIs(datetime.datetime, t.dtype())

    def test_from_const_list(self):
        t = XArray.from_const([1, 2], 10)
        self.assertEqualLen(10, t)
        self.assertListEqual([1, 2], t[0])
        self.assertIs(list, t.dtype())

    def test_from_const_dict(self):
        t = XArray.from_const({1: 'a'}, 10)
        self.assertEqualLen(10, t)
        self.assertDictEqual({1: 'a'}, t[0])
        self.assertIs(dict, t.dtype())

    def test_from_const_negint(self):
        with self.assertRaises(ValueError):
            XArray.from_const(1, -10)

    # noinspection PyTypeChecker
    def test_from_const_nonint(self):
        with self.assertRaises(TypeError):
            XArray.from_const(1, 'a')

    def test_from_const_bad_type(self):
        with self.assertRaises(TypeError):
            XArray.from_const((1, 1), 10)


class TestXArraySaveBinary(XArrayUnitTestCase):
    """
    Tests XArray save binary format
    """
    def test_save(self):
        t = XArray([1, 2, 3])
        path = 'tmp/array-binary'
        t.save(path)
        success_path = os.path.join(path, '_SUCCESS')
        self.assertTrue(os.path.isfile(success_path))

    def test_save_format(self):
        t = XArray([1, 2, 3])
        path = 'tmp/array-binary'
        t.save(path, format='binary')
        success_path = os.path.join(path, '_SUCCESS')
        self.assertTrue(os.path.isfile(success_path))

    def test_save_not_exist(self):
        t = XArray([1, 2, 3])
        path = 'xxx/does-not-exist'
        delete_file_or_dir('xxx')
        t.save(path, format='binary')
        self.assertTrue(os.path.isdir(path))


class TestXArraySaveText(XArrayUnitTestCase):
    """
    Tests XArray save text format
    """
    def test_save(self):
        t = XArray([1, 2, 3])
        path = 'tmp/array-text.txt'
        t.save(path)
        success_path = os.path.join(path, '_SUCCESS')
        self.assertTrue(os.path.isfile(success_path))

    def test_save_format(self):
        t = XArray([1, 2, 3])
        path = 'tmp/array-text'
        t.save(path, format='text')
        success_path = os.path.join(path, '_SUCCESS')
        self.assertTrue(os.path.isfile(success_path))


class TestXArraySaveCsv(XArrayUnitTestCase):
    """
    Tests XArray save csv format
    """
    def test_save(self):
        t = XArray([1, 2, 3])
        path = 'tmp/array-csv.csv'
        t.save(path)
        with open(path) as f:
            self.assertEqual('1', f.readline().strip())
            self.assertEqual('2', f.readline().strip())
            self.assertEqual('3', f.readline().strip())

    def test_save_format(self):
        t = XArray([1, 2, 3])
        path = 'tmp/array-csv'
        t.save(path, format='csv')
        with open(path) as f:
            self.assertEqual('1', f.readline().strip())
            self.assertEqual('2', f.readline().strip())
            self.assertEqual('3', f.readline().strip())


class TestXArrayRepr(XArrayUnitTestCase):
    """
    Tests XArray __repr__ function.
    """
    def test_repr(self):
        t = XArray([1, 2, 3])
        s = t.__repr__()
        self.assertEqual("""dtype: int
Rows: 3
[1, 2, 3]""", s)


class TestXArrayStr(XArrayUnitTestCase):
    """
    Tests XArray __str__ function.
    """
    def test_str(self):
        t = XArray(range(200))
        s = t.__repr__()
        self.assertEqual("dtype: int\nRows: 200\n[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11," +
                         " 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25," +
                         " 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41," +
                         " 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57," +
                         " 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73," +
                         " 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90," +
                         " 91, 92, 93, 94, 95, 96, 97, 98, 99, ... ]", s)


class TestXArrayNonzero(XArrayUnitTestCase):
    """
    Tests XArray __nonzero__ function
    """
    def test_nonzero_nonzero(self):
        t = XArray([0])
        self.assertTrue(bool(t))

    def test_nonzero_zero(self):
        t = XArray([])
        self.assertFalse(bool(t))


class TestXArrayLen(XArrayUnitTestCase):
    """
    Tests XArray __len__ function
    """
    def test_len_nonzero(self):
        t = XArray([0])
        self.assertEqualLen(1, t)

    def test_len_zero(self):
        t = XArray([])
        self.assertEqualLen(0, t)


class TestXArrayIterator(XArrayUnitTestCase):
    """
    Tests XArray iteration function
    """
    def test_iter_empty(self):
        t = XArray([])
        for _ in t:
            self.assertEquals(False, True, 'should not iterate')

    def test_iter_1(self):
        t = XArray([0])
        for elem in t:
            self.assertEquals(0, elem)

    def test_iter_3(self):
        t = XArray([0, 1, 2])
        for elem, expect in zip(t, [0, 1, 2]):
            self.assertEquals(expect, elem)


class TestXArrayAddScalar(XArrayUnitTestCase):
    """
    Tests XArray Scalar Addition
    """
    # noinspection PyAugmentAssignment
    # noinspection PyTypeChecker
    def test_add_scalar(self):
        t = XArray([1, 2, 3])
        self.assertEqualLen(3, t)
        self.assertEqual(1, t[0])
        self.assertIs(int, t.dtype())
        t = t + 2
        self.assertEqual(3, t[0])
        self.assertEqual(4, t[1])
        self.assertEqual(5, t[2])


class TestXArrayAddVector(XArrayUnitTestCase):
    """
    Tests XArray Vector Addition
    """
    def test_add_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 5, 6])
        t = t1 + t2
        self.assertEqualLen(3, t)
        self.assertIs(int, t.dtype())
        self.assertEqual(5, t[0])
        self.assertEqual(7, t[1])
        self.assertEqual(9, t[2])

    def test_add_vector_safe(self):
        t1 = XArray([1, 2, 3])
        t = t1 + t1
        self.assertEqualLen(3, t)
        self.assertIs(int, t.dtype())
        self.assertEqual(2, t[0])
        self.assertEqual(4, t[1])
        self.assertEqual(6, t[2])

        
class TestXArrayOpScalar(XArrayUnitTestCase):
    """
    Tests XArray Scalar operations other than addition
    """
    # noinspection PyTypeChecker
    def test_sub_scalar(self):
        t = XArray([1, 2, 3])
        res = t - 1
        self.assertEqual(0, res[0])
        self.assertEqual(1, res[1])
        self.assertEqual(2, res[2])

    # noinspection PyTypeChecker
    def test_mul_scalar(self):
        t = XArray([1, 2, 3])
        res = t * 2
        self.assertEqual(2, res[0])
        self.assertEqual(4, res[1])
        self.assertEqual(6, res[2])

    # noinspection PyTypeChecker
    def test_div_scalar(self):
        t = XArray([1, 2, 3])
        res = t / 2
        self.assertEqual(0, res[0])
        self.assertEqual(1, res[1])
        self.assertEqual(1, res[2])

    # noinspection PyTypeChecker
    def test_pow_scalar(self):
        t = XArray([1, 2, 3])
        res = t ** 2
        self.assertEqual(1, res[0])
        self.assertEqual(4, res[1])
        self.assertEqual(9, res[2])

    # noinspection PyUnresolvedReferences
    def test_lt_scalar(self):
        t = XArray([1, 2, 3])
        res = t < 3
        self.assertTrue(res[0])
        self.assertTrue(res[1])
        self.assertFalse(res[2])

    # noinspection PyUnresolvedReferences
    def test_le_scalar(self):
        t = XArray([1, 2, 3])
        res = t <= 2
        self.assertTrue(res[0])
        self.assertTrue(res[1])
        self.assertFalse(res[2])

    # noinspection PyUnresolvedReferences
    def test_gt_scalar(self):
        t = XArray([1, 2, 3])
        res = t > 2
        self.assertFalse(res[0])
        self.assertFalse(res[1])
        self.assertTrue(res[2])

    # noinspection PyUnresolvedReferences
    def test_ge_scalar(self):
        t = XArray([1, 2, 3])
        res = t >= 3
        self.assertFalse(res[0])
        self.assertFalse(res[1])
        self.assertTrue(res[2])

    # noinspection PyTypeChecker
    def test_radd_scalar(self):
        t = XArray([1, 2, 3])
        res = 1 + t
        self.assertEqual(2, res[0])
        self.assertEqual(3, res[1])
        self.assertEqual(4, res[2])

    # noinspection PyUnresolvedReferences
    def test_rsub_scalar(self):
        t = XArray([1, 2, 3])
        res = 1 - t
        self.assertEqual(0, res[0])
        self.assertEqual(-1, res[1])
        self.assertEqual(-2, res[2])

    # noinspection PyUnresolvedReferences
    def test_rmul_scalar(self):
        t = XArray([1, 2, 3])
        res = 2 * t
        self.assertEqual(2, res[0])
        self.assertEqual(4, res[1])
        self.assertEqual(6, res[2])

    # noinspection PyTypeChecker
    def test_rdiv_scalar(self):
        t = XArray([1, 2, 3])
        res = 12 / t
        self.assertEqual(12, res[0])
        self.assertEqual(6, res[1])
        self.assertEqual(4, res[2])

    # noinspection PyUnresolvedReferences
    def test_eq_scalar(self):
        t = XArray([1, 2, 3])
        res = t == 2
        self.assertFalse(res[0])
        self.assertTrue(res[1])
        self.assertFalse(res[2])

    # noinspection PyUnresolvedReferences
    def test_ne_scalar(self):
        t = XArray([1, 2, 3])
        res = t != 2
        self.assertTrue(res[0])
        self.assertFalse(res[1])
        self.assertTrue(res[2])

    def test_and_scalar(self):
        t = XArray([1, 2, 3])
        with self.assertRaises(TypeError):
            _ = t & True

    def test_or_scalar(self):
        t = XArray([1, 2, 3])
        with self.assertRaises(TypeError):
            _ = t | False


# noinspection PyUnresolvedReferences
class TestXArrayOpVector(XArrayUnitTestCase):
    """
    Tests XArray Vector operations other than addition
    """
    def test_sub_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 5, 6])
        t = t2 - t1
        self.assertEqual(3, t[0])
        self.assertEqual(3, t[1])
        self.assertEqual(3, t[2])

    def test_mul_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 5, 6])
        res = t1 * t2
        self.assertEqual(4, res[0])
        self.assertEqual(10, res[1])
        self.assertEqual(18, res[2])

    def test_div_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 6, 12])
        res = t2 / t1
        self.assertEqual(4, res[0])
        self.assertEqual(3, res[1])
        self.assertEqual(4, res[2])

    def test_lt_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 2, 2])
        res = t1 < t2
        self.assertTrue(res[0])
        self.assertFalse(res[1])
        self.assertFalse(res[2])

    def test_le_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 2, 2])
        res = t1 <= t2
        self.assertTrue(res[0])
        self.assertTrue(res[1])
        self.assertFalse(res[2])

    def test_gt_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 2, 2])
        res = t1 > t2
        self.assertFalse(res[0])
        self.assertFalse(res[1])
        self.assertTrue(res[2])

    def test_ge_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 2, 2])
        res = t1 >= t2
        self.assertFalse(res[0])
        self.assertTrue(res[1])
        self.assertTrue(res[2])

    def test_eq_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 2, 2])
        res = t1 == t2
        self.assertFalse(res[0])
        self.assertTrue(res[1])
        self.assertFalse(res[2])

    def test_ne_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 2, 2])
        res = t1 != t2
        self.assertTrue(res[0])
        self.assertFalse(res[1])
        self.assertTrue(res[2])

    def test_and_vector(self):
        t1 = XArray([0, 0, 1])
        t2 = XArray([0, 1, 1])
        res = t1 & t2
        self.assertEqual(0, res[0])
        self.assertEqual(0, res[1])
        self.assertEqual(1, res[2])

    def test_or_vector(self):
        t1 = XArray([0, 0, 1])
        t2 = XArray([0, 1, 1])
        res = t1 | t2
        self.assertEqual(0, res[0])
        self.assertEqual(1, res[1])
        self.assertEqual(1, res[2])


class TestXArrayOpUnary(XArrayUnitTestCase):
    """
    Tests XArray Unary operations
    """
    def test_neg_unary(self):
        t = XArray([1, -2, 3])
        res = -t
        self.assertEqual(-1, res[0])
        self.assertEqual(2, res[1])
        self.assertEqual(-3, res[2])

    def test_pos_unary(self):
        t = XArray([1, -2, 3])
        res = +t
        self.assertEqual(1, res[0])
        self.assertEqual(-2, res[1])
        self.assertEqual(3, res[2])

    def test_abs_unary(self):
        t = XArray([1, -2, 3])
        res = abs(t)
        self.assertEqual(1, res[0])
        self.assertEqual(2, res[1])
        self.assertEqual(3, res[2])


class TestXArrayLogicalFilter(XArrayUnitTestCase):
    """
    Tests XArray logical filter (XArray indexed by XArray)
    """
    def test_logical_filter_array(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([1, 0, 1])
        res = t1[t2]
        self.assertEqualLen(2, res)
        self.assertEqual(1, res[0])
        self.assertEqual(3, res[1])

    def test_logical_filter_test(self):
        t1 = XArray([1, 2, 3])
        res = t1[t1 != 2]
        self.assertEqualLen(2, res)
        self.assertEqual(1, res[0])
        self.assertEqual(3, res[1])

    def test_logical_filter_len_error(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([1, 0])
        with self.assertRaises(IndexError):
            _ = t1[t2]


class TestXArrayCopyRange(XArrayUnitTestCase):
    """
    Tests XArray integer and range indexing
    """
    def test_copy_range_pos(self):
        t = XArray([1, 2, 3])
        self.assertEqual(1, t[0])

    def test_copy_range_neg(self):
        t = XArray([1, 2, 3])
        self.assertEqual(3, t[-1])

    def test_copy_range_index_err(self):
        t = XArray([1, 2, 3])
        with self.assertRaises(IndexError):
            _ = t[3]
        
    def test_copy_range_slice(self):
        t = XArray([1, 2, 3])
        res = t[0:2]
        self.assertEqualLen(2, res)
        self.assertEqual(1, res[0])
        self.assertEqual(2, res[1])

    def test_copy_range_slice_neg_start(self):
        t = XArray([1, 2, 3, 4, 5])
        res = t[-3:4]
        self.assertEqualLen(2, res)
        self.assertEqual(3, res[0])
        self.assertEqual(4, res[1])

    def test_copy_range_slice_neg_stop(self):
        t = XArray([1, 2, 3, 4, 5])
        res = t[1:-2]
        self.assertEqualLen(2, res)
        self.assertEqual(2, res[0])
        self.assertEqual(3, res[1])

    def test_copy_range_slice_stride(self):
        t = XArray([1, 2, 3, 4, 5])
        res = t[0:4:2]
        self.assertEqualLen(2, res)
        self.assertEqual(1, res[0])
        self.assertEqual(3, res[1])

    def test_copy_range_bad_type(self):
        t = XArray([1, 2, 3])
        with self.assertRaises(IndexError):
            _ = t[{1, 2, 3}]


class TestXArraySize(XArrayUnitTestCase):
    """
    Tests XArray size operation
    """
    def test_size(self):
        t = XArray([1, 2, 3])
        self.assertEqual(3, t.size())


class TestXArrayDtype(XArrayUnitTestCase):
    """
    Tests XArray dtype operation
    """
    def test_dtype(self):
        t = XArray([1, 2, 3])
        self.assertIs(int, t.dtype())


class TestXArrayTableLineage(XArrayUnitTestCase):
    """
    Tests XArray ltable ineage operation
    """
    def test_lineage_program(self):
        res = XArray([1, 2, 3])
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = list(lineage)[0]
        self.assertEqual('PROGRAM', item)

    def test_lineage_file(self):
        res = XArray('files/test-array-int')
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = os.path.basename(list(lineage)[0])
        self.assertEqual('test-array-int', item)

    def test_lineage_apply(self):
        res = XArray('files/test-array-int').apply(lambda x: -x)
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = os.path.basename(list(lineage)[0])
        self.assertEqual('test-array-int', item)

    def test_lineage_range(self):
        res = XArray.from_sequence(100, 200)
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = list(lineage)[0]
        self.assertEqual('RANGE', item)

    def test_lineage_const(self):
        res = XArray.from_const(1, 10)
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = list(lineage)[0]
        self.assertEqual('CONST', item)

    def test_lineage_binary_op(self):
        res_int = XArray('files/test-array-int')
        res_float = XArray('files/test-array-float')
        res = res_int + res_float
        lineage = res.lineage()['table']
        self.assertEqualLen(2, lineage)
        basenames = set([os.path.basename(item) for item in lineage])
        self.assertTrue('test-array-int' in basenames)
        self.assertTrue('test-array-float' in basenames)

    # noinspection PyAugmentAssignment,PyUnresolvedReferences
    def test_lineage_left_op(self):
        res = XArray('files/test-array-int')
        res = res + 2
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = os.path.basename(list(lineage)[0])
        self.assertEqual('test-array-int', item)

    # noinspection PyAugmentAssignment,PyUnresolvedReferences
    def test_lineage_right_op(self):
        res = XArray('files/test-array-int')
        res = 2 + res
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = os.path.basename(list(lineage)[0])
        self.assertEqual('test-array-int', item)

    def test_lineage_unary(self):
        res = XArray('files/test-array-int')
        res = -res
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        item = os.path.basename(list(lineage)[0])
        self.assertEqual('test-array-int', item)

    def test_lineage_append(self):
        res1 = XArray('files/test-array-int')
        res2 = XArray('files/test-array-float')
        res3 = res2.apply(lambda x: int(x))
        res = res1.append(res3)
        lineage = res.lineage()['table']
        self.assertEqualLen(2, lineage)
        basenames = set([os.path.basename(item) for item in lineage])
        self.assertIn('test-array-int', basenames)
        self.assertIn('test-array-float', basenames)

    def test_lineage_save(self):
        res = XArray('files/test-array-int')
        path = '/tmp/xarray'
        res.save(path, format='binary')
        with open(os.path.join(path, '_metadata')) as f:
            metadata = pickle.load(f)
        self.assertIs(int, metadata)
        with open(os.path.join(path, '_lineage')) as f:
            lineage = pickle.load(f)
            table_lineage = lineage[0]
            self.assertEqualLen(1, table_lineage)
            basenames = set([os.path.basename(item) for item in table_lineage])
            self.assertIn('test-array-int', basenames)

    def test_lineage_save_text(self):
        res = XArray('files/test-array-str')
        path = '/tmp/xarray'
        res.save(path, format='text')
        with open(os.path.join(path, '_metadata')) as f:
            metadata = pickle.load(f)
        self.assertIs(str, metadata)
        with open(os.path.join(path, '_lineage')) as f:
            lineage = pickle.load(f)
            table_lineage = lineage[0]
            self.assertEqualLen(1, table_lineage)
            basenames = set([os.path.basename(item) for item in table_lineage])
            self.assertIn('test-array-str', basenames)

    def test_lineage_load(self):
        res = XArray('files/test-array-int')
        path = 'tmp/array'
        res.save(path, format='binary')
        res = XArray(path)
        lineage = res.lineage()['table']
        self.assertEqualLen(1, lineage)
        basenames = set([os.path.basename(item) for item in lineage])
        self.assertIn('test-array-int', basenames)


class TestXArrayColumnLineage(XArrayUnitTestCase):
    """
    Tests XArray column lineage operation
    """

    # helper function
    def get_column_lineage(self, xa, keys=None):
        lineage = xa.lineage()['column']
        keys = keys or ['_XARRAY']
        keys = sorted(keys)
        count = len(keys)
        self.assertEqual(count, len(lineage))
        self.assertListEqual(keys, sorted(lineage.keys()))
        return lineage

    def test_construct_empty(self):
        t = XArray()
        lineage = self.get_column_lineage(t)
        self.assertSetEqual({('EMPTY', '_XARRAY')}, lineage['_XARRAY'])

    def test_construct_from_xarrayt(self):
        t = XArray([1, 2, 3])
        res = XArray(t)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_construct_list_int(self):
        t = XArray([1, 2, 3])
        lineage = self.get_column_lineage(t)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_construct_from_const(self):
        t = XArray.from_const(1, 3)
        lineage = self.get_column_lineage(t)
        self.assertSetEqual({('CONST', '_XARRAY')}, lineage['_XARRAY'])

    def test_lineage_file(self):
        path = 'files/test-array-int'
        realpath = os.path.realpath(path)
        res = XArray(path)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({(realpath, '_XARRAY')}, lineage['_XARRAY'])

    def test_save(self):
        t = XArray([1, 2, 3])
        path = 'tmp/array-binary'
        t.save(path)
        lineage = self.get_column_lineage(t)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_save_as_text(self):
        t = XArray([1, 2, 3])
        path = 'tmp/array-text'
        t.save(path, format='text')
        lineage = self.get_column_lineage(t)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_from_rdd(self):
        t = XArray([1, 2, 3])
        rdd = t.to_rdd()
        res = XArray.from_rdd(rdd, int)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('RDD', '_XARRAY')}, lineage['_XARRAY'])

    def test_topk_index(self):
        t = XArray([1, 2, 3])
        res = t.topk_index(0)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_add_vector(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([4, 5, 6])
        res = t1 + t2
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    # noinspection PyTypeChecker
    def test_add_scalar(self):
        t = XArray([1, 2, 3])
        self.assertEqualLen(3, t)
        self.assertEqual(1, t[0])
        self.assertIs(int, t.dtype())
        res = t + 2
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_sample_no_seed(self):
        t = XArray(range(10))
        res = t.sample(0.3)
        self.assertTrue(len(res) < 10)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_logical_filter_array(self):
        t1 = XArray([1, 2, 3])
        t2 = XArray([1, 0, 1])
        res = t1[t2]
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_from_sequence(self):
        res = XArray.from_sequence(100, 200)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('RANGE', '_XARRAY')}, lineage['_XARRAY'])

    def test_range(self):
        t = XArray([1, 2, 3])
        res = t[1:2]
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('RANGE', '_XARRAY')}, lineage['_XARRAY'])

    def test_filter(self):
        t = XArray([1, 2, 3])
        res = t.filter(lambda x: x == 2)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_append(self):
        t = XArray([1, None, 3])
        path = 'files/test-array-int'
        realpath = os.path.realpath(path)
        u = XArray(path)
        res = t.append(u)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY'), (realpath, '_XARRAY')}, lineage['_XARRAY'])

    def test_apply(self):
        t = XArray([1, 2, 3])
        res = t.apply(lambda x: x * 2)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_flat_map(self):
        t = XArray([[1], [1, 2], [1, 2, 3]])
        res = t.flat_map(lambda x: x)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_astype_int_float(self):
        t = XArray([1, 2, 3])
        res = t.astype(float)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_clip_int_clip(self):
        t = XArray([1, 2, 3])
        res = t.clip(2, 2)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_fillna(self):
        t = XArray([1, 2, 3])
        res = t.fillna(10)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_unpack_list(self):
        t = XArray([[1, 0, 1],
                    [1, 1, 1],
                    [0, 1]])
        res = t.unpack()
        lineage = self.get_column_lineage(res, ['X.0', 'X.1', 'X.2'])
        self.assertIn('X.0', lineage)
        self.assertIn('X.1', lineage)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['X.0'])
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['X.1'])

    def test_sort(self):
        t = XArray([3, 2, 1])
        res = t.sort()
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_split_datetime_all(self):
        t = XArray([datetime.datetime(2011, 1, 1, 1, 1, 1),
                    datetime.datetime(2012, 2, 2, 2, 2, 2),
                    datetime.datetime(2013, 3, 3, 3, 3, 3)])
        res = t.split_datetime('date')
        lineage = self.get_column_lineage(res, ['date.year', 'date.month', 'date.day',
                                                'date.hour', 'date.minute', 'date.second'])
        self.assertIn('date.year', lineage)
        self.assertIn('date.month', lineage)
        self.assertIn('date.day', lineage)
        self.assertSetEqual({('RDD', 'date.year')}, lineage['date.year'])
        self.assertSetEqual({('RDD', 'date.month')}, lineage['date.month'])
        self.assertSetEqual({('RDD', 'date.day')}, lineage['date.day'])

    def test_datetime_to_str(self):
        t = XArray([datetime.datetime(2015, 8, 21),
                    datetime.datetime(2016, 9, 22),
                    datetime.datetime(2017, 10, 23)])
        res = t.datetime_to_str('%Y %m %d')
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_str_to_datetime(self):
        t = XArray(['2015 08 21', '2015 08 22', '2015 08 23'])
        res = t.str_to_datetime('%Y %m %d')
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_dict_trim_by_keys_include(self):
        t = XArray([{'a': 0, 'b': 0, 'c': 0}, {'x': 1}])
        res = t.dict_trim_by_keys(['a'], exclude=False)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_dict_trim_by_values(self):
        t = XArray([{'a': 0, 'b': 1, 'c': 2, 'd': 3}, {'x': 1}])
        res = t.dict_trim_by_values(1, 2)
        lineage = self.get_column_lineage(res)
        self.assertSetEqual({('PROGRAM', '_XARRAY')}, lineage['_XARRAY'])

    def test_dict_keys(self):
        t = XArray([{'a': 0, 'b': 0, 'c': 0}, {'x': 1, 'y': 2, 'z': 3}])
        res = t.dict_keys()
        lineage = self.get_column_lineage(res, ['X.0', 'X.1', 'X.2'])
        self.assertIn('X.0', lineage)
        self.assertIn('X.1', lineage)
        self.assertIn('X.2', lineage)
        self.assertSetEqual({('RDD', 'X.0')}, lineage['X.0'])
        self.assertSetEqual({('RDD', 'X.1')}, lineage['X.1'])

    def test_values(self):
        t = XArray([{'a': 0, 'b': 1, 'c': 2}, {'x': 10, 'y': 20, 'z': 30}])
        res = t.dict_values()
        lineage = self.get_column_lineage(res, ['X.0', 'X.1', 'X.2'])
        self.assertIn('X.0', lineage)
        self.assertIn('X.1', lineage)
        self.assertIn('X.2', lineage)
        self.assertSetEqual({('RDD', 'X.0')}, lineage['X.0'])
        self.assertSetEqual({('RDD', 'X.1')}, lineage['X.1'])


class TestXArrayHead(XArrayUnitTestCase):
    """
    Tests XArray head operation
    """
    def test_head(self):
        t = XArray([1, 2, 3])
        self.assertEqualLen(3, t.head())

    def test_head_10(self):
        t = XArray(range(100))
        self.assertEqualLen(10, t.head())

    def test_head_5(self):
        t = XArray(range(100))
        self.assertEqualLen(5, t.head(5))


class TestXArrayVectorSlice(XArrayUnitTestCase):
    """
    Tests XArray vector_slice operation
    """
    def test_vector_slice_start_0(self):
        t = XArray([[1, 2, 3], [10, 11, 12]])
        res = t.vector_slice(0)
        self.assertEqualLen(2, res)
        self.assertEqual(1, res[0])
        self.assertEqual(10, res[1])

    def test_vector_slice_start_1(self):
        t = XArray([[1, 2, 3], [10, 11, 12]])
        res = t.vector_slice(1)
        self.assertEqualLen(2, res)
        self.assertEqual(2, res[0])
        self.assertEqual(11, res[1])

    def test_vector_slice_start_end(self):
        t = XArray([[1, 2, 3], [10, 11, 12]])
        res = t.vector_slice(0, 2)
        self.assertEqualLen(2, res)
        self.assertListEqual([1, 2], res[0])
        self.assertListEqual([10, 11], res[1])

    def test_vector_slice_start_none(self):
        t = XArray([[1], [1, 2], [1, 2, 3]])
        res = t.vector_slice(2)
        self.assertEqualLen(3, res)
        self.assertIsNone(res[0])
        self.assertIsNone(res[1])
        self.assertEqual(3, res[2])

    def test_vector_slice_start_end_none(self):
        t = XArray([[1], [1, 2], [1, 2, 3]])
        res = t.vector_slice(0, 2)
        self.assertEqualLen(3, res)
        self.assertIsNone(res[0])
        self.assertTrue([1, 2], res[1])
        self.assertTrue([1, 2], res[2])


class TestXArrayCountWords(XArrayUnitTestCase):
    """
    Tests XArray count_words
    """
    def test_count_words(self):
        pass


class TestXArrayCountNgrams(XArrayUnitTestCase):
    """
    Tests XArray count_ngrams
    """
    def test_count_ngrams(self):
        pass


class TestXArrayApply(XArrayUnitTestCase):
    """
    Tests XArray apply
    """
    def test_apply_int(self):
        t = XArray([1, 2, 3])
        res = t.apply(lambda x: x * 2)
        self.assertEqualLen(3, res)
        self.assertIs(int, res.dtype())
        self.assertEqual(2, res[0])
        self.assertEqual(4, res[1])
        self.assertEqual(6, res[2])

    def test_apply_float_cast(self):
        t = XArray([1, 2, 3])
        res = t.apply(lambda x: x * 2, float)
        self.assertEqualLen(3, res)
        self.assertIs(float, res.dtype())
        self.assertEqual(2.0, res[0])
        self.assertEqual(4.0, res[1])
        self.assertEqual(6.0, res[2])

    def test_apply_skip_undefined(self):
        t = XArray([1, 2, 3, None])
        res = t.apply(lambda x: x * 2)
        self.assertEqualLen(4, res)
        self.assertIs(int, res.dtype())
        self.assertEqual(2, res[0])
        self.assertEqual(4, res[1])
        self.assertEqual(6, res[2])
        self.assertIsNone(res[3])

    def test_apply_type_err(self):
        t = XArray([1, 2, 3, None])
        with self.assertRaises(ValueError):
            t.apply(lambda x: x * 2, skip_undefined=False)

    def test_apply_fun_err(self):
        t = XArray([1, 2, 3, None])
        with self.assertRaises(TypeError):
            t.apply(1)


class TestXArrayFlatMap(XArrayUnitTestCase):
    """
    Tests XArray flat_map
    """
    def test_flat_map(self):
        t = XArray([[1], [1, 2], [1, 2, 3]])
        res = t.flat_map(lambda x: x)
        self.assertEqualLen(6, res)
        self.assertIs(int, res.dtype())
        self.assertEqual(1, res[0])
        self.assertEqual(1, res[1])
        self.assertEqual(2, res[2])
        self.assertEqual(1, res[3])
        self.assertEqual(2, res[4])
        self.assertEqual(3, res[5])

    def test_flat_map_int(self):
        t = XArray([[1], [1, 2], [1, 2, 3]])
        res = t.flat_map(lambda x: [v * 2 for v in x])
        self.assertEqualLen(6, res)
        self.assertIs(int, res.dtype())
        self.assertEqual(2, res[0])
        self.assertEqual(2, res[1])
        self.assertEqual(4, res[2])
        self.assertEqual(2, res[3])
        self.assertEqual(4, res[4])
        self.assertEqual(6, res[5])

    def test_flat_map_str(self):
        t = XArray([['a'], ['a', 'b'], ['a', 'b', 'c']])
        res = t.flat_map(lambda x: x)
        self.assertEqualLen(6, res)
        self.assertIs(str, res.dtype())
        self.assertEqual('a', res[0])
        self.assertEqual('a', res[1])
        self.assertEqual('b', res[2])
        self.assertEqual('a', res[3])
        self.assertEqual('b', res[4])
        self.assertEqual('c', res[5])

    def test_flat_map_float_cast(self):
        t = XArray([[1], [1, 2], [1, 2, 3]])
        res = t.flat_map(lambda x: x, dtype=float)
        self.assertEqualLen(6, res)
        self.assertIs(float, res.dtype())
        self.assertEqual(1.0, res[0])
        self.assertEqual(1.0, res[1])
        self.assertEqual(2.0, res[2])
        self.assertEqual(1.0, res[3])
        self.assertEqual(2.0, res[4])
        self.assertEqual(3.0, res[5])

    def test_flat_map_skip_undefined(self):
        t = XArray([[1], [1, 2], [1, 2, 3], None, [None]])
        res = t.flat_map(lambda x: x)
        self.assertEqualLen(6, res)
        self.assertIs(int, res.dtype())
        self.assertEqual(1, res[0])
        self.assertEqual(1, res[1])
        self.assertEqual(2, res[2])
        self.assertEqual(1, res[3])
        self.assertEqual(2, res[4])
        self.assertEqual(3, res[5])

    def test_flat_map_no_fun(self):
        t = XArray([[1], [1, 2], [1, 2, 3]])
        res = t.flat_map()
        self.assertEqualLen(6, res)
        self.assertIs(int, res.dtype())
        self.assertEqual(1, res[0])
        self.assertEqual(1, res[1])
        self.assertEqual(2, res[2])
        self.assertEqual(1, res[3])
        self.assertEqual(2, res[4])
        self.assertEqual(3, res[5])

    def test_flat_map_type_err(self):
        t = XArray([[1], [1, 2], [1, 2, 3], [None]])
        with self.assertRaises(ValueError):
            t.flat_map(lambda x: x * 2, skip_undefined=False)


class TestXArrayFilter(XArrayUnitTestCase):
    """
    Tests XArray filter
    """
    def test_filter(self):
        t = XArray([1, 2, 3])
        res = t.filter(lambda x: x == 2)
        self.assertEqual(1, len(res))

    def test_filter_empty(self):
        t = XArray([1, 2, 3])
        res = t.filter(lambda x: x == 10)
        self.assertEqual(0, len(res))


class TestXArraySample(XArrayUnitTestCase):
    """
    Tests XArray sample
    """
    def test_sample_no_seed(self):
        t = XArray(range(10))
        res = t.sample(0.3)
        self.assertTrue(len(res) < 10)

    @unittest.skip('depends on number of partitions')
    def test_sample_seed(self):
        t = XArray(range(10))
        res = t.sample(0.3, seed=1)
        # get 3, 6, 9 with this seed
        self.assertEqualLen(3, res)
        self.assertEqual(3, res[0])
        self.assertEqual(6, res[1])
        
    def test_sample_zero(self):
        t = XArray(range(10))
        res = t.sample(0.0)
        self.assertEqualLen(0, res)

    def test_sample_err_gt(self):
        t = XArray(range(10))
        with self.assertRaises(ValueError):
            t.sample(2, seed=1)

    def test_sample_err_lt(self):
        t = XArray(range(10))
        with self.assertRaises(ValueError):
            t.sample(-0.5, seed=1)


class TestXArrayAll(XArrayUnitTestCase):
    """
    Tests XArray all
    """
    # int
    def test_all_int_none(self):
        t = XArray([1, None])
        self.assertFalse(t.all())

    def test_all_int_zero(self):
        t = XArray([1, 0])
        self.assertFalse(t.all())

    def test_all_int_true(self):
        t = XArray([1, 2])
        self.assertTrue(t.all())

    # float
    def test_all_float_nan(self):
        t = XArray([1.0, float('nan')])
        self.assertFalse(t.all())

    def test_all_float_none(self):
        t = XArray([1.0, None])
        self.assertFalse(t.all())

    def test_all_float_zero(self):
        t = XArray([1.0, 0.0])
        self.assertFalse(t.all())

    def test_all_float_true(self):
        t = XArray([1.0, 2.0])
        self.assertTrue(t.all())

    # str
    def test_all_str_empty(self):
        t = XArray(['hello', ''])
        self.assertFalse(t.all())

    def test_all_str_none(self):
        t = XArray(['hello', None])
        self.assertFalse(t.all())

    def test_all_str_true(self):
        t = XArray(['hello', 'world'])
        self.assertTrue(t.all())

    # list
    def test_all_list_empty(self):
        t = XArray([[1, 2], []])
        self.assertFalse(t.all())

    def test_all_list_none(self):
        t = XArray([[1, 2], None])
        self.assertFalse(t.all())

    def test_all_list_true(self):
        t = XArray([[1, 2], [2, 3]])
        self.assertTrue(t.all())

    # dict
    def test_all_dict_empty(self):
        t = XArray([{1: 'a'}, {}])
        self.assertFalse(t.all())

    def test_all_dict_none(self):
        t = XArray([{1: 'a'}, None])
        self.assertFalse(t.all())

    def test_all_dict_true(self):
        t = XArray([{1: 'a'}, {2: 'b'}])
        self.assertTrue(t.all())

    # empty
    def test_all_empty(self):
        t = XArray([])
        self.assertTrue(t.all())


class TestXArrayAny(XArrayUnitTestCase):
    """
    Tests XArray any
    """
    # int
    def test_any_int(self):
        t = XArray([1, 2])
        self.assertTrue(t.any())

    def test_any_int_true(self):
        t = XArray([0, 1])
        self.assertTrue(t.any())

    def test_any_int_false(self):
        t = XArray([0, 0])
        self.assertFalse(t.any())

    def test_any_int_missing_true(self):
        t = XArray([1, None])
        self.assertTrue(t.any())

    def test_any_int_missing_false(self):
        t = XArray([None, 0])
        self.assertFalse(t.any())

    # float
    def test_any_float(self):
        t = XArray([1., 2.])
        self.assertTrue(t.any())

    def test_any_float_true(self):
        t = XArray([0.0, 1.0])
        self.assertTrue(t.any())

    def test_any_float_false(self):
        t = XArray([0.0, 0.0])
        self.assertFalse(t.any())

    def test_any_float_missing_true(self):
        t = XArray([1.0, None])
        self.assertTrue(t.any())

    def test_any_float_missing_true_nan(self):
        t = XArray([1.0, float('nan')])
        self.assertTrue(t.any())

    def test_any_float_missing_true_none(self):
        t = XArray([1.0, None])
        self.assertTrue(t.any())

    def test_any_float_missing_false(self):
        t = XArray([None, 0.0])
        self.assertFalse(t.any())

    def test_any_float_missing_false_nan(self):
        t = XArray([float('nan'), 0.0])
        self.assertFalse(t.any())

    def test_any_float_missing_false_none(self):
        t = XArray([None, 0.0])
        self.assertFalse(t.any())

    # str
    def test_any_str(self):
        t = XArray(['a', 'b'])
        self.assertTrue(t.any())

    def test_any_str_true(self):
        t = XArray(['', 'a'])
        self.assertTrue(t.any())

    def test_any_str_false(self):
        t = XArray(['', ''])
        self.assertFalse(t.any())

    def test_any_str_missing_true(self):
        t = XArray(['a', None])
        self.assertTrue(t.any())

    def test_any_str_missing_false(self):
        t = XArray([None, ''])
        self.assertFalse(t.any())

    # list
    def test_any_list(self):
        t = XArray([[1], [2]])
        self.assertTrue(t.any())

    def test_any_list_true(self):
        t = XArray([[], ['a']])
        self.assertTrue(t.any())

    def test_any_list_false(self):
        t = XArray([[], []])
        self.assertFalse(t.any())

    def test_any_list_missing_true(self):
        t = XArray([['a'], None])
        self.assertTrue(t.any())

    def test_any_list_missing_false(self):
        t = XArray([None, []])
        self.assertFalse(t.any())

    # dict
    def test_any_dict(self):
        t = XArray([{'a': 1, 'b': 2}])
        self.assertTrue(t.any())

    def test_any_dict_true(self):
        t = XArray([{}, {'a': 1}])
        self.assertTrue(t.any())

    def test_any_dict_false(self):
        t = XArray([{}, {}])
        self.assertFalse(t.any())

    def test_any_dict_missing_true(self):
        t = XArray([{'a': 1}, None])
        self.assertTrue(t.any())

    def test_any_dict_missing_false(self):
        t = XArray([None, {}])
        self.assertFalse(t.any())

    # empty
    def test_any_empty(self):
        t = XArray([])
        self.assertFalse(t.any())


class TestXArrayMax(XArrayUnitTestCase):
    """
    Tests XArray max
    """
    def test_max_empty(self):
        t = XArray([])
        self.assertIsNone(t.max())

    def test_max_err(self):
        t = XArray(['a'])
        with self.assertRaises(TypeError):
            t.max()

    def test_max_int(self):
        t = XArray([1, 2, 3])
        self.assertEqual(3, t.max())

    def test_max_float(self):
        t = XArray([1.0, 2.0, 3.0])
        self.assertEqual(3.0, t.max())


class TestXArrayMin(XArrayUnitTestCase):
    """
    Tests XArray min
    """
    def test_min_empty(self):
        t = XArray([])
        self.assertIsNone(t.min())

    def test_min_err(self):
        t = XArray(['a'])
        with self.assertRaises(TypeError):
            t.min()

    def test_min_int(self):
        t = XArray([1, 2, 3])
        self.assertEqual(1, t.min())

    def test_min_float(self):
        t = XArray([1.0, 2.0, 3.0])
        self.assertEqual(1.0, t.min())


class TestXArraySum(XArrayUnitTestCase):
    """
    Tests XArray sum
    """
    def test_sum_empty(self):
        t = XArray([])
        self.assertIsNone(t.sum())

    def test_sum_err(self):
        t = XArray(['a'])
        with self.assertRaises(TypeError):
            t.sum()

    def test_sum_int(self):
        t = XArray([1, 2, 3])
        self.assertEqual(6, t.sum())

    def test_sum_float(self):
        t = XArray([1.0, 2.0, 3.0])
        self.assertEqual(6.0, t.sum())

    def test_sum_array(self):
        t = XArray([array.array('l', [10, 20, 30]), array.array('l', [40, 50, 60])])
        self.assertEqual(array.array('l', [50, 70, 90]), t.sum())

    def test_sum_list(self):
        t = XArray([[10, 20, 30], [40, 50, 60]])
        self.assertListEqual([50, 70, 90], t.sum())

    def test_sum_dict(self):
        t = XArray([{'x': 1, 'y': 2}, {'x': 3, 'y': 4}])
        self.assertDictEqual({'x': 4, 'y': 6}, t.sum())


class TestXArrayMean(XArrayUnitTestCase):
    """
    Tests XArray mean
    """
    def test_mean_empty(self):
        t = XArray([])
        self.assertIsNone(t.mean())

    def test_mean_err(self):
        t = XArray(['a'])
        with self.assertRaises(TypeError):
            t.mean()

    def test_mean_int(self):
        t = XArray([1, 2, 3])
        self.assertEqual(2, t.mean())

    def test_mean_float(self):
        t = XArray([1.0, 2.0, 3.0])
        self.assertEqual(2.0, t.mean())


class TestXArrayStd(XArrayUnitTestCase):
    """
    Tests XArray std
    """
    def test_std_empty(self):
        t = XArray([])
        self.assertIsNone(t.std())

    def test_std_err(self):
        t = XArray(['a'])
        with self.assertRaises(TypeError):
            t.std()

    def test_std_int(self):
        t = XArray([1, 2, 3])
        expect = math.sqrt(2.0 / 3.0)
        self.assertEqual(expect, t.std())

    def test_std_float(self):
        t = XArray([1.0, 2.0, 3.0])
        expect = math.sqrt(2.0 / 3.0)
        self.assertEqual(expect, t.std())


class TestXArrayVar(XArrayUnitTestCase):
    """
    Tests XArray var
    """
    def test_var_empty(self):
        t = XArray([])
        self.assertIsNone(t.var())

    def test_var_err(self):
        t = XArray(['a'])
        with self.assertRaises(TypeError):
            t.var()

    def test_var_int(self):
        t = XArray([1, 2, 3])
        expect = 2.0 / 3.0
        self.assertEqual(expect, t.var())

    def test_var_float(self):
        t = XArray([1.0, 2.0, 3.0])
        expect = 2.0 / 3.0
        self.assertEqual(expect, t.var())


class TestXArrayNumMissing(XArrayUnitTestCase):
    """
    Tests XArray num_missing
    """
    def test_num_missing_empty(self):
        t = XArray([])
        self.assertEqual(0, t.num_missing())

    def test_num_missing_zero(self):
        t = XArray([1, 2, 3])
        self.assertEqual(0, t.num_missing())

    def test_num_missing_int_none(self):
        t = XArray([1, 2, None])
        self.assertEqual(1, t.num_missing())

    def test_num_missing_int_all(self):
        t = XArray([None, None, None], dtype=int)
        self.assertEqual(3, t.num_missing())

    def test_num_missing_float_none(self):
        t = XArray([1.0, 2.0, None])
        self.assertEqual(1, t.num_missing())

    def test_num_missing_float_nan(self):
        t = XArray([1.0, 2.0, float('nan')])
        self.assertEqual(1, t.num_missing())


class TestXArrayNumNonzero(XArrayUnitTestCase):
    """
    Tests XArray nnz
    """
    def test_nnz_empty(self):
        t = XArray([])
        self.assertEqual(0, t.nnz())

    def test_nnz_zero_int(self):
        t = XArray([0, 0, 0])
        self.assertEqual(0, t.nnz())

    def test_nnz_zero_float(self):
        t = XArray([0.0, 0.0, 0.0])
        self.assertEqual(0, t.nnz())

    def test_nnz_int_none(self):
        t = XArray([1, 2, None])
        self.assertEqual(2, t.nnz())

    def test_nnz_int_all(self):
        t = XArray([None, None, None], dtype=int)
        self.assertEqual(0, t.nnz())

    def test_nnz_float_none(self):
        t = XArray([1.0, 2.0, None])
        self.assertEqual(2, t.nnz())

    def test_nnz_float_nan(self):
        t = XArray([1.0, 2.0, float('nan')])
        self.assertEqual(2, t.nnz())


class TestXArrayDatetimeToStr(XArrayUnitTestCase):
    """
    Tests XArray datetime_to_str
    """
    def test_datetime_to_str(self):
        t = XArray([datetime.datetime(2015, 8, 21),
                    datetime.datetime(2016, 9, 22),
                    datetime.datetime(2017, 10, 23)])
        res = t.datetime_to_str('%Y %m %d')
        self.assertIs(str, res.dtype())
        self.assertEqual('2015 08 21', res[0])
        self.assertEqual('2016 09 22', res[1])
        self.assertEqual('2017 10 23', res[2])

    def test_datetime_to_str_bad_type(self):
        t = XArray([1, 2, 3])
        with self.assertRaises(TypeError):
            t.datetime_to_str('%Y %M %d')


class TestXArrayStrToDatetime(XArrayUnitTestCase):
    """
    Tests XArray str_to_datetime
    """
    def test_str_to_datetime(self):
        t = XArray(['2015 08 21', '2015 08 22', '2015 08 23'])
        res = t.str_to_datetime('%Y %m %d')
        self.assertIs(datetime.datetime, res.dtype())
        self.assertEqual(datetime.datetime(2015, 8, 21), res[0])
        self.assertEqual(datetime.datetime(2015, 8, 22), res[1])
        self.assertEqual(datetime.datetime(2015, 8, 23), res[2])

    def test_str_to_datetime_parse(self):
        t = XArray(['2015 8 21', '2015 Aug 22', '23 Aug 2015', 'Aug 24 2015'])
        res = t.str_to_datetime()
        self.assertIs(datetime.datetime, res.dtype())
        self.assertEqual(datetime.datetime(2015, 8, 21), res[0])
        self.assertEqual(datetime.datetime(2015, 8, 22), res[1])
        self.assertEqual(datetime.datetime(2015, 8, 23), res[2])
        self.assertEqual(datetime.datetime(2015, 8, 24), res[3])

    def test_str_to_datetime_bad_type(self):
        t = XArray([1, 2, 3])
        with self.assertRaises(TypeError):
            t.str_to_datetime()


class TestXArrayAstype(XArrayUnitTestCase):
    """
    Tests XArray astype
    """
    def test_astype_empty(self):
        t = XArray([])
        res = t.astype(int)
        self.assertIs(int, res.dtype())

    def test_astype_int_int(self):
        t = XArray([1, 2, 3])
        res = t.astype(int)
        self.assertIs(int, res.dtype())
        self.assertEqual(1, res[0])

    def test_astype_int_float(self):
        t = XArray([1, 2, 3])
        res = t.astype(float)
        self.assertIs(float, res.dtype())
        self.assertEqual(1.0, res[0])

    def test_astype_float_float(self):
        t = XArray([1.0, 2.0, 3.0])
        res = t.astype(float)
        self.assertIs(float, res.dtype())
        self.assertEqual(1.0, res[0])

    def test_astype_float_int(self):
        t = XArray([1.0, 2.0, 3.0])
        res = t.astype(int)
        self.assertIs(int, res.dtype())
        self.assertEqual(1, res[0])

    def test_astype_int_str(self):
        t = XArray([1, 2, 3])
        res = t.astype(str)
        self.assertIs(str, res.dtype())
        self.assertEqual('1', res[0])

    def test_astype_str_list(self):
        t = XArray(['[1, 2, 3]', '[4, 5, 6]'])
        res = t.astype(list)
        self.assertIs(list, res.dtype())
        self.assertListEqual([1, 2, 3], res[0])

    def test_astype_str_dict(self):
        t = XArray(['{"a": 1, "b": 2}', '{"x": 3}'])
        res = t.astype(dict)
        self.assertIs(dict, res.dtype())
        self.assertDictEqual({'a': 1, 'b': 2}, res[0])

    # noinspection PyTypeChecker
    def test_astype_str_array(self):
        t = XArray(['[1, 2, 3]', '[4, 5, 6]'])
        res = t.astype(array)
        self.assertIs(array, res.dtype())
        self.assertColumnEqual([1, 2, 3], res[0])

    def test_astype_str_datetime(self):
        t = XArray(['Aug 23, 2015', '2015 8 24'])
        res = t.astype(datetime.datetime)
        self.assertIs(datetime.datetime, res.dtype())
        self.assertTrue(datetime.datetime(2015, 8, 23), res[0])
        self.assertTrue(datetime.datetime(2015, 8, 24), res[1])


class TestXArrayClip(XArrayUnitTestCase):
    """
    Tests XArray clip
    """
    # noinspection PyTypeChecker
    def test_clip_int_nan(self):
        nan = float('nan')
        t = XArray([1, 2, 3])
        res = t.clip(nan, nan)
        self.assertColumnEqual([1, 2, 3], res)

    def test_clip_int_none(self):
        t = XArray([1, 2, 3])
        res = t.clip(None, None)
        self.assertColumnEqual([1, 2, 3], res)

    def test_clip_int_def(self):
        t = XArray([1, 2, 3])
        res = t.clip()
        self.assertColumnEqual([1, 2, 3], res)

    # noinspection PyTypeChecker
    def test_clip_float_nan(self):
        nan = float('nan')
        t = XArray([1.0, 2.0, 3.0])
        res = t.clip(nan, nan)
        self.assertColumnEqual([1.0, 2.0, 3.0], res)

    def test_clip_float_none(self):
        t = XArray([1.0, 2.0, 3.0])
        res = t.clip(None, None)
        self.assertColumnEqual([1.0, 2.0, 3.0], res)

    def test_clip_float_def(self):
        t = XArray([1.0, 2.0, 3.0])
        res = t.clip()
        self.assertColumnEqual([1.0, 2.0, 3.0], res)

    def test_clip_int_all(self):
        t = XArray([1, 2, 3])
        res = t.clip(1, 3)
        self.assertColumnEqual([1, 2, 3], res)

    # noinspection PyTypeChecker
    def test_clip_float_all(self):
        t = XArray([1.0, 2.0, 3.0])
        res = t.clip(1.0, 3.0)
        self.assertColumnEqual([1.0, 2.0, 3.0], res)

    def test_clip_int_clip(self):
        t = XArray([1, 2, 3])
        res = t.clip(2, 2)
        self.assertColumnEqual([2, 2, 2], res)

    # noinspection PyTypeChecker
    def test_clip_float_clip(self):
        t = XArray([1.0, 2.0, 3.0])
        res = t.clip(2.0, 2.0)
        self.assertColumnEqual([2.0, 2.0, 2.0], res)

    # noinspection PyTypeChecker
    def test_clip_list_nan(self):
        nan = float('nan')
        t = XArray([[1, 2, 3]])
        res = t.clip(nan, nan)
        self.assertColumnEqual([[1, 2, 3]], res)

    def test_clip_list_none(self):
        t = XArray([[1, 2, 3]])
        res = t.clip(None, None)
        self.assertColumnEqual([[1, 2, 3]], res)

    def test_clip_list_def(self):
        t = XArray([[1, 2, 3]])
        res = t.clip()
        self.assertColumnEqual([[1, 2, 3]], res)

    def test_clip_list_all(self):
        t = XArray([[1, 2, 3]])
        res = t.clip(1, 3)
        self.assertColumnEqual([[1, 2, 3]], res)

    def test_clip_list_clip(self):
        t = XArray([[1, 2, 3]])
        res = t.clip(2, 2)
        self.assertColumnEqual([[2, 2, 2]], res)


class TestXArrayClipLower(XArrayUnitTestCase):
    """
    Tests XArray clip_lower
    """
    def test_clip_lower_int_all(self):
        t = XArray([1, 2, 3])
        res = t.clip_lower(1)
        self.assertColumnEqual([1, 2, 3], res)

    def test_clip_int_clip(self):
        t = XArray([1, 2, 3])
        res = t.clip_lower(2)
        self.assertColumnEqual([2, 2, 3], res)

    def test_clip_lower_list_all(self):
        t = XArray([[1, 2, 3]])
        res = t.clip_lower(1)
        self.assertColumnEqual([[1, 2, 3]], res)

    def test_clip_lower_list_clip(self):
        t = XArray([[1, 2, 3]])
        res = t.clip_lower(2)
        self.assertColumnEqual([[2, 2, 3]], res)


class TestXArrayClipUpper(XArrayUnitTestCase):
    """
    Tests XArray clip_upper
    """
    def test_clip_upper_int_all(self):
        t = XArray([1, 2, 3])
        res = t.clip_upper(3)
        self.assertColumnEqual([1, 2, 3], res)

    def test_clip_int_clip(self):
        t = XArray([1, 2, 3])
        res = t.clip_upper(2)
        self.assertColumnEqual([1, 2, 2], res)

    def test_clip_upper_list_all(self):
        t = XArray([[1, 2, 3]])
        res = t.clip_upper(3)
        self.assertColumnEqual([[1, 2, 3]], res)

    def test_clip_upper_list_clip(self):
        t = XArray([[1, 2, 3]])
        res = t.clip_upper(2)
        self.assertColumnEqual([[1, 2, 2]], res)


class TestXArrayTail(XArrayUnitTestCase):
    """
    Tests XArray tail
    """
    def test_tail(self):
        t = XArray(range(1, 100))
        res = t.tail(10)
        self.assertEqual(range(90, 100), res)

    def test_tail_all(self):
        t = XArray(range(1, 100))
        res = t.tail(100)
        self.assertEqual(range(1, 100), res)


class TestXArrayCountna(XArrayUnitTestCase):
    """
    Tests XArray countna
    """
    def test_countna_not(self):
        t = XArray([1, 2, 3])
        res = t.countna()
        self.assertEqual(0, res)

    def test_countna_none(self):
        t = XArray([1, 2, None])
        res = t.countna()
        self.assertEqual(1, res)

    def test_countna_nan(self):
        t = XArray([1.0, 2.0, float('nan')])
        res = t.countna()
        self.assertEqual(1, res)

    def test_countna_float_none(self):
        t = XArray([1.0, 2.0, None])
        res = t.countna()
        self.assertEqual(1, res)


class TestXArrayDropna(XArrayUnitTestCase):
    """
    Tests XArray dropna
    """
    def test_dropna_not(self):
        t = XArray([1, 2, 3])
        res = t.dropna()
        self.assertColumnEqual([1, 2, 3], res)

    def test_dropna_none(self):
        t = XArray([1, 2, None])
        res = t.dropna()
        self.assertColumnEqual([1, 2], res)

    def test_dropna_nan(self):
        t = XArray([1.0, 2.0, float('nan')])
        res = t.dropna()
        self.assertColumnEqual([1.0, 2.0], res)

    def test_dropna_float_none(self):
        t = XArray([1.0, 2.0, None])
        res = t.dropna()
        self.assertColumnEqual([1.0, 2.0], res)


class TestXArrayFillna(XArrayUnitTestCase):
    """
    Tests XArray fillna
    """
    def test_fillna_not(self):
        t = XArray([1, 2, 3])
        res = t.fillna(10)
        self.assertColumnEqual([1, 2, 3], res)

    def test_fillna_none(self):
        t = XArray([1, 2, None])
        res = t.fillna(10)
        self.assertColumnEqual([1, 2, 10], res)

    def test_fillna_none_cast(self):
        t = XArray([1, 2, None])
        res = t.fillna(10.0)
        self.assertColumnEqual([1, 2, 10], res)

    def test_fillna_nan(self):
        t = XArray([1.0, 2.0, float('nan')])
        res = t.fillna(10.0)
        self.assertColumnEqual([1.0, 2.0, 10.0], res)

    def test_fillna_float_none(self):
        t = XArray([1.0, 2.0, None])
        res = t.fillna(10.0)
        self.assertColumnEqual([1.0, 2.0, 10.0], res)

    def test_fillna_nan_cast(self):
        t = XArray([1.0, 2.0, float('nan')])
        res = t.fillna(10)
        self.assertColumnEqual([1.0, 2.0, 10.0], res)

    def test_fillna_none_float_cast(self):
        t = XArray([1.0, 2.0, None])
        res = t.fillna(10)
        self.assertColumnEqual([1.0, 2.0, 10.0], res)


class TestXArrayTopkIndex(XArrayUnitTestCase):
    """
    Tests XArray topk_index
    """
    def test_topk_index_0(self):
        t = XArray([1, 2, 3])
        res = t.topk_index(0)
        self.assertColumnEqual([0, 0, 0], res)

    def test_topk_index_1(self):
        t = XArray([1, 2, 3])
        res = t.topk_index(1)
        self.assertColumnEqual([0, 0, 1], res)

    def test_topk_index_2(self):
        t = XArray([1, 2, 3])
        res = t.topk_index(2)
        self.assertColumnEqual([0, 1, 1], res)

    def test_topk_index_3(self):
        t = XArray([1, 2, 3])
        res = t.topk_index(3)
        self.assertColumnEqual([1, 1, 1], res)

    def test_topk_index_4(self):
        t = XArray([1, 2, 3])
        res = t.topk_index(4)
        self.assertColumnEqual([1, 1, 1], res)

    def test_topk_index_float_1(self):
        t = XArray([1.0, 2.0, 3.0])
        res = t.topk_index(1)
        self.assertColumnEqual([0, 0, 1], res)

    def test_topk_index_str_1(self):
        t = XArray(['a', 'b', 'c'])
        res = t.topk_index(1)
        self.assertColumnEqual([0, 0, 1], res)

    def test_topk_index_list_1(self):
        t = XArray([[1, 2, 3], [2, 3, 4], [3, 4, 5]])
        res = t.topk_index(1)
        self.assertColumnEqual([0, 0, 1], res)

    def test_topk_index_reverse_int(self):
        t = XArray([1, 2, 3])
        res = t.topk_index(1, reverse=True)
        self.assertColumnEqual([1, 0, 0], res)

    def test_topk_index_reverse_float(self):
        t = XArray([1.0, 2.0, 3.0])
        res = t.topk_index(1, reverse=True)
        self.assertColumnEqual([1, 0, 0], res)

    def test_topk_index_reverse_str(self):
        t = XArray(['a', 'b', 'c'])
        res = t.topk_index(1, reverse=True)
        self.assertColumnEqual([1, 0, 0], res)

    def test_topk_index_reverse_list(self):
        t = XArray([[1, 2, 3], [2, 3, 4], [3, 4, 5]])
        res = t.topk_index(1, reverse=True)
        self.assertColumnEqual([1, 0, 0], res)


class TestXArraySketchSummary(XArrayUnitTestCase):
    """
    Tests XArray sketch_summary
    """
    def test_sketch_summary_size(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertEqual(5, ss.size())

    def test_sketch_summary_min(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertEqual(1, ss.min())

    def test_sketch_summary_max(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertEqual(5, ss.max())

    def test_sketch_summary_mean(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertEqual(3.0, ss.mean())

    def test_sketch_summary_sum(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertEqual(15, ss.sum())

    def test_sketch_summary_var(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertEqual(2.0, ss.var())

    def test_sketch_summary_std(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertAlmostEqual(math.sqrt(2.0), ss.std())

    def test_sketch_summary_num_undefined(self):
        t = XArray([1, None, 3, None, 5])
        ss = t.sketch_summary()
        self.assertEqual(2, ss.num_undefined())

    def test_sketch_summary_num_unique(self):
        t = XArray([1, 3, 3, 3, 5])
        ss = t.sketch_summary()
        self.assertEqual(3, ss.num_unique())

    # TODO files on multiple workers
    # probably something wrong with combiner
    def test_sketch_summary_frequent_items(self):
        t = XArray([1, 3, 3, 3, 5])
        ss = t.sketch_summary()
        self.assertDictEqual({1: 1, 3: 3, 5: 1}, ss.frequent_items())

    def test_sketch_summary_frequency_count(self):
        t = XArray([1, 3, 3, 3, 5])
        ss = t.sketch_summary()
        self.assertEqual(1, ss.frequency_count(1))
        self.assertEqual(3, ss.frequency_count(3))
        self.assertEqual(1, ss.frequency_count(5))


class TestXArrayAppend(XArrayUnitTestCase):
    """
    Tests XArray append
    """
    def test_append(self):
        t = XArray([1, 2, 3])
        u = XArray([10, 20, 30])
        res = t.append(u)
        self.assertColumnEqual([1, 2, 3, 10, 20, 30], res)

    def test_append_empty_t(self):
        t = XArray([], dtype=int)
        u = XArray([10, 20, 30])
        res = t.append(u)
        self.assertColumnEqual([10, 20, 30], res)

    def test_append_empty_u(self):
        t = XArray([1, 2, 3])
        u = XArray([], dtype=int)
        res = t.append(u)
        self.assertColumnEqual([1, 2, 3], res)

    def test_append_int_float_err(self):
        t = XArray([1, 2, 3])
        u = XArray([10., 20., 30.])
        with self.assertRaises(RuntimeError):
            t.append(u)

    def test_append_int_str_err(self):
        t = XArray([1, 2, 3])
        u = XArray(['a', 'b', 'c'])
        with self.assertRaises(RuntimeError):
            t.append(u)


class TestXArrayUnique(XArrayUnitTestCase):
    """
    Tests XArray unique
    """
    def test_unique_dict_err(self):
        t = XArray([{'a': 1, 'b': 2, 'c': 3}])
        with self.assertRaises(TypeError):
            t.unique()

    def test_unique_int_noop(self):
        t = XArray([1, 2, 3])
        res = t.unique()
        self.assertEqualLen(3, res)
        self.assertListEqual([1, 2, 3], sorted(list(res)))

    def test_unique_float_noop(self):
        t = XArray([1.0, 2.0, 3.0])
        res = t.unique()
        self.assertEqualLen(3, res)
        self.assertListEqual([1.0, 2.0, 3.0], sorted(list(res)))

    def test_unique_str_noop(self):
        t = XArray(['1', '2', '3'])
        res = t.unique()
        self.assertEqualLen(3, res)
        self.assertListEqual(['1', '2', '3'], sorted(list(res)))

    def test_unique_int(self):
        t = XArray([1, 2, 3, 1, 2])
        res = t.unique()
        self.assertEqualLen(3, res)
        self.assertListEqual([1, 2, 3], sorted(list(res)))

    def test_unique_float(self):
        t = XArray([1.0, 2.0, 3.0, 1.0, 2.0])
        res = t.unique()
        self.assertEqualLen(3, res)
        self.assertListEqual([1.0, 2.0, 3.0], sorted(list(res)))

    def test_unique_str(self):
        t = XArray(['1', '2', '3', '1', '2'])
        res = t.unique()
        self.assertEqualLen(3, res)
        self.assertListEqual(['1', '2', '3'], sorted(list(res)))


class TestXArrayItemLength(XArrayUnitTestCase):
    """
    Tests XArray item_length
    """
    def test_item_length_int(self):
        t = XArray([1, 2, 3])
        with self.assertRaises(TypeError):
            t.item_length()

    def test_item_length_float(self):
        t = XArray([1.0, 2.0, 3.0])
        with self.assertRaises(TypeError):
            t.item_length()

    def test_item_length_str(self):
        t = XArray(['a', 'bb', 'ccc'])
        res = t.item_length()
        self.assertColumnEqual([1, 2, 3], res)
        self.assertIs(int, res.dtype())

    def test_item_length_list(self):
        t = XArray([[1], [1, 2], [1, 2, 3]])
        res = t.item_length()
        self.assertColumnEqual([1, 2, 3], res)
        self.assertIs(int, res.dtype())

    def test_item_length_dict(self):
        t = XArray([{1: 'a'}, {1: 'a', 2: 'b'}, {1: 'a', 2: 'b', 3: '3'}])
        res = t.item_length()
        self.assertColumnEqual([1, 2, 3], res)
        self.assertIs(int, res.dtype())


class TestXArraySplitDatetime(XArrayUnitTestCase):
    """
    Tests XArray split_datetime
    """

    def test_split_datetime_year(self):
        t = XArray([datetime.datetime(2011, 1, 1),
                    datetime.datetime(2012, 2, 2),
                    datetime.datetime(2013, 3, 3)])
        res = t.split_datetime('date', limit='year')
        self.assertTrue(isinstance(res, XFrame))
        self.assertListEqual(['date.year'], res.column_names())
        self.assertListEqual([int], res.column_types())
        self.assertEqualLen(3, res)
        self.assertColumnEqual([2011, 2012, 2013], res['date.year'])

    def test_split_datetime_year_mo(self):
        t = XArray([datetime.datetime(2011, 1, 1),
                    datetime.datetime(2012, 2, 2),
                    datetime.datetime(2013, 3, 3)])
        res = t.split_datetime('date', limit=['year', 'month'])
        self.assertTrue(isinstance(res, XFrame))
        self.assertListEqual(['date.year', 'date.month'], res.column_names())
        self.assertListEqual([int, int], res.column_types())
        self.assertEqualLen(3, res)
        self.assertColumnEqual([2011, 2012, 2013], res['date.year'])
        self.assertColumnEqual([1, 2, 3], res['date.month'])

    def test_split_datetime_all(self):
        t = XArray([datetime.datetime(2011, 1, 1, 1, 1, 1),
                    datetime.datetime(2012, 2, 2, 2, 2, 2),
                    datetime.datetime(2013, 3, 3, 3, 3, 3)])
        res = t.split_datetime('date')
        self.assertTrue(isinstance(res, XFrame))
        self.assertListEqual(['date.year', 'date.month', 'date.day',
                              'date.hour', 'date.minute', 'date.second'], res.column_names())
        self.assertListEqual([int, int, int, int, int, int], res.column_types())
        self.assertEqualLen(3, res)
        self.assertColumnEqual([2011, 2012, 2013], res['date.year'])
        self.assertColumnEqual([1, 2, 3], res['date.month'])
        self.assertColumnEqual([1, 2, 3], res['date.day'])
        self.assertColumnEqual([1, 2, 3], res['date.hour'])
        self.assertColumnEqual([1, 2, 3], res['date.minute'])
        self.assertColumnEqual([1, 2, 3], res['date.second'])

    def test_split_datetime_year_no_prefix(self):
        t = XArray([datetime.datetime(2011, 1, 1),
                    datetime.datetime(2012, 2, 2),
                    datetime.datetime(2013, 3, 3)])
        res = t.split_datetime(limit='year')
        self.assertTrue(isinstance(res, XFrame))
        self.assertListEqual(['X.year'], res.column_names())
        self.assertListEqual([int], res.column_types())
        self.assertEqualLen(3, res)
        self.assertColumnEqual([2011, 2012, 2013], res['X.year'])

    def test_split_datetime_year_null_prefix(self):
        t = XArray([datetime.datetime(2011, 1, 1),
                    datetime.datetime(2012, 2, 2),
                    datetime.datetime(2013, 3, 3)])
        res = t.split_datetime(column_name_prefix=None, limit='year')
        self.assertTrue(isinstance(res, XFrame))
        self.assertListEqual(['year'], res.column_names())
        self.assertListEqual([int], res.column_types())
        self.assertEqualLen(3, res)
        self.assertColumnEqual([2011, 2012, 2013], res['year'])

    def test_split_datetime_bad_col_type(self):
        t = XArray([1, 2, 3])
        with self.assertRaises(TypeError):
            t.split_datetime('date')

    # noinspection PyTypeChecker
    def test_split_datetime_bad_prefix_type(self):
        t = XArray([datetime.datetime(2011, 1, 1),
                    datetime.datetime(2011, 2, 2),
                    datetime.datetime(2011, 3, 3)])
        with self.assertRaises(TypeError):
            t.split_datetime(1)

    def test_split_datetime_bad_limit_val(self):
        t = XArray([datetime.datetime(2011, 1, 1),
                    datetime.datetime(2011, 2, 2),
                   datetime. datetime(2011, 3, 3)])
        with self.assertRaises(ValueError):
            t.split_datetime('date', limit='xx')

    # noinspection PyTypeChecker
    def test_split_datetime_bad_limit_type(self):
        t = XArray([datetime.datetime(2011, 1, 1),
                    datetime.datetime(2011, 2, 2),
                    datetime.datetime(2011, 3, 3)])
        with self.assertRaises(TypeError):
            t.split_datetime('date', limit=1)

    def test_split_datetime_bad_limit_not_list(self):
        t = XArray([datetime.datetime(2011, 1, 1),
                    datetime.datetime(2011, 2, 2),
                    datetime.datetime(2011, 3, 3)])
        with self.assertRaises(TypeError):
            t.split_datetime('date', limit=datetime.datetime(2011, 1, 1))


class TestXArrayUnpackErrors(XArrayUnitTestCase):
    """
    Tests XArray unpack errors
    """
    def test_unpack_str(self):
        t = XArray(['a', 'b', 'c'])
        with self.assertRaises(TypeError):
            t.unpack()

    # noinspection PyTypeChecker
    def test_unpack_bad_prefix(self):
        t = XArray([[1, 2, 3], [4, 5, 6]])
        with self.assertRaises(TypeError):
            t.unpack(column_name_prefix=1)

    # noinspection PyTypeChecker
    def test_unpack_bad_limit_type(self):
        t = XArray([[1, 2, 3], [4, 5, 6]])
        with self.assertRaises(TypeError):
            t.unpack(limit=1)

    def test_unpack_bad_limit_val(self):
        t = XArray([[1, 2, 3], [4, 5, 6]])
        with self.assertRaises(TypeError):
            t.unpack(limit=['a', 1])

    def test_unpack_bad_limit_dup(self):
        t = XArray([[1, 2, 3], [4, 5, 6]])
        with self.assertRaises(ValueError):
            t.unpack(limit=[1, 1])

    # noinspection PyTypeChecker
    def test_unpack_bad_column_types(self):
        t = XArray([[1, 2, 3], [4, 5, 6]])
        with self.assertRaises(TypeError):
            t.unpack(column_types=1)

    # noinspection PyTypeChecker
    def test_unpack_bad_column_types_bool(self):
        t = XArray([[1, 2, 3], [4, 5, 6]])
        with self.assertRaises(TypeError):
            t.unpack(column_types=[True])

    def test_unpack_column_types_limit_mismatch(self):
        t = XArray([[1, 2, 3], [4, 5, 6]])
        with self.assertRaises(ValueError):
            t.unpack(limit=[1], column_types=[int, int])

    def test_unpack_dict_column_types_no_limit(self):
        t = XArray([{'a': 1, 'b': 2}, {'c': 3, 'd': 4}])
        with self.assertRaises(ValueError):
            t.unpack(column_types=[int, int])

    def test_unpack_empty_no_column_types(self):
        t = XArray([], dtype=list)
        with self.assertRaises(RuntimeError):
            t.unpack()

    def test_unpack_empty_list_column_types(self):
        t = XArray([[]], dtype=list)
        with self.assertRaises(RuntimeError):
            t.unpack()


class TestXArrayUnpack(XArrayUnitTestCase):
    """
    Tests XArray unpack list
    """
    def test_unpack_list(self):
        t = XArray([[1, 0, 1],
                    [1, 1, 1],
                    [0, 1]])
        res = t.unpack()
        self.assertListEqual(['X.0', 'X.1', 'X.2'], res.column_names())
        self.assertDictEqual({'X.0': 1, 'X.1': 0, 'X.2': 1}, res[0])
        self.assertDictEqual({'X.0': 1, 'X.1': 1, 'X.2': 1}, res[1])
        self.assertDictEqual({'X.0': 0, 'X.1': 1, 'X.2': None}, res[2])

    def test_unpack_list_limit(self):
        t = XArray([[1, 0, 1],
                    [1, 1, 1],
                    [0, 1]])
        res = t.unpack(limit=[1])
        self.assertListEqual(['X.1'], res.column_names())
        self.assertDictEqual({'X.1': 0}, res[0])
        self.assertDictEqual({'X.1': 1}, res[1])
        self.assertDictEqual({'X.1': 1}, res[2])

    def test_unpack_list_na_values(self):
        t = XArray([[1, 0, 1],
                    [1, 1, 1],
                    [0, 1]])
        res = t.unpack(na_value=0)
        self.assertListEqual(['X.0', 'X.1', 'X.2'], res.column_names())
        self.assertDictEqual({'X.0': 1, 'X.1': 0, 'X.2': 1}, res[0])
        self.assertDictEqual({'X.0': 1, 'X.1': 1, 'X.2': 1}, res[1])
        self.assertDictEqual({'X.0': 0, 'X.1': 1, 'X.2': 0}, res[2])

    def test_unpack_list_na_values_col_types(self):
        t = XArray([[1, 0, 1],
                    [1, 1, 1],
                    [0, 1]])
        res = t.unpack(column_types=[int, int, int], na_value=0)
        self.assertListEqual(['X.0', 'X.1', 'X.2'], res.column_names())
        self.assertDictEqual({'X.0': 1, 'X.1': 0, 'X.2': 1}, res[0])
        self.assertDictEqual({'X.0': 1, 'X.1': 1, 'X.2': 1}, res[1])
        self.assertDictEqual({'X.0': 0, 'X.1': 1, 'X.2': 0}, res[2])

    def test_unpack_list_cast_str(self):
        t = XArray([[1, 0, 1],
                    [1, 1, 1],
                    [0, 1]])
        res = t.unpack(column_types=[str, str, str])
        self.assertListEqual(['X.0', 'X.1', 'X.2'], res.column_names())
        self.assertDictEqual({'X.0': '1', 'X.1': '0', 'X.2': '1'}, res[0])
        self.assertDictEqual({'X.0': '1', 'X.1': '1', 'X.2': '1'}, res[1])
        self.assertDictEqual({'X.0': '0', 'X.1': '1', 'X.2': None}, res[2])

    def test_unpack_list_no_prefix(self):
        t = XArray([[1, 0, 1],
                    [1, 1, 1],
                    [0, 1]])
        res = t.unpack(column_name_prefix='')
        self.assertListEqual(['0', '1', '2'], res.column_names())
        self.assertDictEqual({'0': 1, '1': 0, '2': 1}, res[0])
        self.assertDictEqual({'0': 1, '1': 1, '2': 1}, res[1])
        self.assertDictEqual({'0': 0, '1': 1, '2': None}, res[2])

    def test_unpack_dict_limit(self):
        t = XArray([{'word': 'a', 'count': 1},
                    {'word': 'cat', 'count': 2},
                    {'word': 'is', 'count': 3},
                    {'word': 'coming', 'count': 4}])
        res = t.unpack(limit=['word', 'count'], column_types=[str, int])
        self.assertListEqual(['X.word', 'X.count'], res.column_names())
        self.assertDictEqual({'X.word': 'a', 'X.count': 1}, res[0])
        self.assertDictEqual({'X.word': 'cat', 'X.count': 2}, res[1])
        self.assertDictEqual({'X.word': 'is', 'X.count': 3}, res[2])
        self.assertDictEqual({'X.word': 'coming', 'X.count': 4}, res[3])

    def test_unpack_dict_limit_word(self):
        t = XArray([{'word': 'a', 'count': 1},
                    {'word': 'cat', 'count': 2},
                    {'word': 'is', 'count': 3},
                    {'word': 'coming', 'count': 4}])
        res = t.unpack(limit=['word'])
        self.assertListEqual(['X.word'], res.column_names())
        self.assertDictEqual({'X.word': 'a'}, res[0])
        self.assertDictEqual({'X.word': 'cat'}, res[1])
        self.assertDictEqual({'X.word': 'is'}, res[2])
        self.assertDictEqual({'X.word': 'coming'}, res[3])

    def test_unpack_dict_limit_count(self):
        t = XArray([{'word': 'a', 'count': 1},
                    {'word': 'cat', 'count': 2},
                    {'word': 'is', 'count': 3},
                    {'word': 'coming', 'count': 4}])
        res = t.unpack(limit=['count'])
        self.assertListEqual(['X.count'], res.column_names())
        self.assertDictEqual({'X.count': 1}, res[0])
        self.assertDictEqual({'X.count': 2}, res[1])
        self.assertDictEqual({'X.count': 3}, res[2])
        self.assertDictEqual({'X.count': 4}, res[3])

    def test_unpack_dict_incomplete(self):
        t = XArray([{'word': 'a', 'count': 1},
                    {'word': 'cat', 'count': 2},
                    {'word': 'is'},
                    {'word': 'coming', 'count': 4}])
        res = t.unpack(limit=['word', 'count'], column_types=[str, int])
        self.assertListEqual(['X.word', 'X.count'], res.column_names())
        self.assertDictEqual({'X.count': 1, 'X.word': 'a'}, res[0])
        self.assertDictEqual({'X.count': 2, 'X.word': 'cat'}, res[1])
        self.assertDictEqual({'X.count': None, 'X.word': 'is'}, res[2])
        self.assertDictEqual({'X.count': 4, 'X.word': 'coming'}, res[3])

    def test_unpack_dict(self):
        t = XArray([{'word': 'a', 'count': 1},
                    {'word': 'cat', 'count': 2},
                    {'word': 'is', 'count': 3},
                    {'word': 'coming', 'count': 4}])
        res = t.unpack()
        self.assertListEqual(['X.count', 'X.word'], res.column_names())
        self.assertDictEqual({'X.count': 1, 'X.word': 'a'}, res[0])
        self.assertDictEqual({'X.count': 2, 'X.word': 'cat'}, res[1])
        self.assertDictEqual({'X.count': 3, 'X.word': 'is'}, res[2])
        self.assertDictEqual({'X.count': 4, 'X.word': 'coming'}, res[3])

    def test_unpack_dict_no_prefix(self):
        t = XArray([{'word': 'a', 'count': 1},
                    {'word': 'cat', 'count': 2},
                    {'word': 'is', 'count': 3},
                    {'word': 'coming', 'count': 4}])
        res = t.unpack(column_name_prefix=None)
        self.assertListEqual(['count', 'word'], res.column_names())
        self.assertDictEqual({'count': 1, 'word': 'a'}, res[0])
        self.assertDictEqual({'count': 2, 'word': 'cat'}, res[1])
        self.assertDictEqual({'count': 3, 'word': 'is'}, res[2])
        self.assertDictEqual({'count': 4, 'word': 'coming'}, res[3])


class TestXArraySort(XArrayUnitTestCase):
    """
    Tests XArray sort
    """
    def test_sort_int(self):
        t = XArray([3, 2, 1])
        res = t.sort()
        self.assertColumnEqual([1, 2, 3], res)

    def test_sort_float(self):
        t = XArray([3, 2, 1])
        res = t.sort()
        self.assertColumnEqual([1.0, 2.0, 3.0], res)

    def test_sort_str(self):
        t = XArray(['c', 'b', 'a'])
        res = t.sort()
        self.assertColumnEqual(['a', 'b', 'c'], res)

    def test_sort_list(self):
        t = XArray([[3, 4], [2, 3], [1, 2]])
        with self.assertRaises(TypeError):
            t.sort()

    def test_sort_dict(self):
        t = XArray([{'c': 3}, {'b': 2}, {'a': 1}])
        with self.assertRaises(TypeError):
            t.sort()

    def test_sort_int_desc(self):
        t = XArray([1, 2, 3])
        res = t.sort(ascending=False)
        self.assertColumnEqual([3, 2, 1], res)

    def test_sort_float_desc(self):
        t = XArray([1.0, 2.0, 3.0])
        res = t.sort(ascending=False)
        self.assertColumnEqual([3.0, 2.0, 1.0], res)

    def test_sort_str_desc(self):
        t = XArray(['a', 'b', 'c'])
        res = t.sort(ascending=False)
        self.assertColumnEqual(['c', 'b', 'a'], res)


class TestXArrayDictTrimByKeys(XArrayUnitTestCase):
    """
    Tests XArray dict_trim_by_keys
    """
    def test_dict_trim_by_keys_bad_type(self):
        t = XArray([3, 2, 1])
        with self.assertRaises(TypeError):
            t.dict_trim_by_keys(['a'])

    def test_dict_trim_by_keys_include(self):
        t = XArray([{'a': 0, 'b': 0, 'c': 0}, {'x': 1}])
        res = t.dict_trim_by_keys(['a'], exclude=False)
        self.assertColumnEqual([{'a': 0}, {}], res)

    def test_dict_trim_by_keys_exclude(self):
        t = XArray([{'a': 0, 'b': 1, 'c': 2}, {'x': 1}])
        res = t.dict_trim_by_keys(['a'])
        self.assertColumnEqual([{'b': 1, 'c': 2}, {'x': 1}], res)


class TestXArrayDictTrimByValues(XArrayUnitTestCase):
    """
    Tests XArray dict_trim_by_values
    """
    def test_dict_trim_by_values_bad_type(self):
        t = XArray([3, 2, 1])
        with self.assertRaises(TypeError):
            t.dict_trim_by_values(1, 2)

    def test_dict_trim_by_values(self):
        t = XArray([{'a': 0, 'b': 1, 'c': 2, 'd': 3}, {'x': 1}])
        res = t.dict_trim_by_values(1, 2)
        self.assertColumnEqual([{'b': 1, 'c': 2}, {'x': 1}], res)


class TestXArrayDictKeys(XArrayUnitTestCase):
    """
    Tests XArray dict_keys
    """
    # noinspection PyArgumentList
    def test_dict_keys_bad_type(self):
        t = XArray([3, 2, 1])
        with self.assertRaises(TypeError):
            t.dict_keys(['a'])

    def test_dict_keys_bad_len(self):
        t = XArray([{'a': 0, 'b': 0, 'c': 0}, {'x': 1}])
        with self.assertRaises(ValueError):
            t.dict_keys()

    def test_dict_keys(self):
        t = XArray([{'a': 0, 'b': 0, 'c': 0}, {'x': 1, 'y': 2, 'z': 3}])
        res = t.dict_keys()
        self.assertEqualLen(2, res)
        self.assertDictEqual({'X.0': 'a', 'X.1': 'c', 'X.2': 'b'}, res[0])
        self.assertDictEqual({'X.0': 'y', 'X.1': 'x', 'X.2': 'z'}, res[1])


class TestXArrayDictValues(XArrayUnitTestCase):
    """
    Tests XArray dict_values
    """
    # noinspection PyArgumentList
    def test_values_bad_type(self):
        t = XArray([3, 2, 1])
        with self.assertRaises(TypeError):
            t.dict_values(['a'])

    def test_values_bad_len(self):
        t = XArray([{'a': 0, 'b': 1, 'c': 2}, {'x': 10}])
        with self.assertRaises(ValueError):
            t.dict_values()

    def test_values(self):
        t = XArray([{'a': 0, 'b': 1, 'c': 2}, {'x': 10, 'y': 20, 'z': 30}])
        res = t.dict_values()
        self.assertEqualLen(2, res)
        self.assertDictEqual({'X.0': 0, 'X.1': 2, 'X.2': 1}, res[0])
        self.assertDictEqual({'X.0': 20, 'X.1': 10, 'X.2': 30}, res[1])


class TestXArrayDictHasAnyKeys(XArrayUnitTestCase):
    """
    Tests XArray dict_has_any_keys
    """
    def test_dict_has_any_keys_bad(self):
        t = XArray([3, 2, 1])
        with self.assertRaises(TypeError):
            t.dict_has_any_keys(['a'])

    def test_dict_has_any_keys(self):
        t = XArray([{'a': 0, 'b': 0, 'c': 0}, {'x': 1}])
        res = t.dict_has_any_keys(['a'])
        self.assertColumnEqual([True, False], res)


class TestXArrayDictHasAllKeys(XArrayUnitTestCase):
    """
    Tests XArray dict_has_all_keys
    """
    def test_dict_has_all_keys_bad(self):
        t = XArray([3, 2, 1])
        with self.assertRaises(TypeError):
            t.dict_has_all_keys(['a'])

    def test_dict_has_all_keys(self):
        t = XArray([{'a': 0, 'b': 0, 'c': 0}, {'a': 1, 'b': 1}])
        res = t.dict_has_all_keys(['a', 'b', 'c'])
        self.assertColumnEqual([True, False], res)

if __name__ == '__main__':
    unittest.main()
