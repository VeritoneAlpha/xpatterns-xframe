import unittest
import os
import pickle

# python testxarray.py
# python -m unittest testxarray
# python -m unittest testxarray.TestXArrayVersion
# python -m unittest testxarray.TestXArrayVersion.test_version

from xframes import XArray, XFrame
from xframes import fileio

hdfs_prefix = 'hdfs://localhost:8020'

# Needs to be tested
# XArray(saved file w/ _metadata)
# XArray save as text
# XArray save as csv
# XFrame(saved file w/ _metadata)
# XFrame save
# XFrame save as csv


class XFrameUnitTestCase(unittest.TestCase):

    def assertEqualLen(self, expect, obj):
        return self.assertEqual(expect, len(obj))

    def assertColumnEqual(self, expect, obj):
        return self.assertListEqual(expect, list(obj))


class TestXArrayConstructorLoad(XFrameUnitTestCase):
    """
    Tests XArray constructors that loads from file.
    """

    def test_construct_file_int(self):
        path = '{}/user/xpatterns/files/test-array-int'.format(hdfs_prefix)
        t = XArray(path)
        self.assertEqualLen(4, t)
        self.assertIs(int, t.dtype())
        self.assertEqual(1, t[0])

    def test_construct_local_file_float(self):
        t = XArray('{}/user/xpatterns/files/test-array-float'.format(hdfs_prefix))
        self.assertEqualLen(4, t)
        self.assertIs(float, t.dtype())
        self.assertEqual(1.0, t[0])

    def test_construct_local_file_str(self):
        t = XArray('{}/user/xpatterns/files/test-array-str'.format(hdfs_prefix))
        self.assertEqualLen(4, t)
        self.assertIs(str, t.dtype())
        self.assertEqual('a', t[0])

    def test_construct_local_file_list(self):
        t = XArray('{}/user/xpatterns/files/test-array-list'.format(hdfs_prefix))
        self.assertEqualLen(4, t)
        self.assertIs(list, t.dtype())
        self.assertListEqual([1, 2], t[0])

    def test_construct_local_file_dict(self):
        t = XArray('{}/user/xpatterns/files/test-array-dict'.format(hdfs_prefix))
        self.assertEqualLen(4, t)
        self.assertIs(dict, t.dtype())
        self.assertDictEqual({1: 'a', 2: 'b'}, t[0])


class TestXArraySaveCsv(XFrameUnitTestCase):
    """
    Tests XArray save csv format
    """
    def test_save(self):
        t = XArray([1, 2, 3])
        path = '{}/tmp/array-csv.csv'.format(hdfs_prefix)
        t.save(path)
        with fileio.open_file(path) as f:
            self.assertEqual('1', f.readline().strip())
            self.assertEqual('2', f.readline().strip())
            self.assertEqual('3', f.readline().strip())
        fileio.delete(path)

    def test_save_format(self):
        t = XArray([1, 2, 3])
        path = '{}/tmp/array-csv'.format(hdfs_prefix)
        t.save(path, format='csv')
        with fileio.open_file(path) as f:
            self.assertEqual('1', f.readline().strip())
            self.assertEqual('2', f.readline().strip())
            self.assertEqual('3', f.readline().strip())
        fileio.delete(path)


class TestXArraySaveText(XFrameUnitTestCase):
    """
    Tests XArray save text format
    """
    def test_save(self):
        t = XArray([1, 2, 3])
        path = '{}/tmp/array-csv'.format(hdfs_prefix)
        t.save(path)
        success_path = os.path.join(path, '_SUCCESS')
        self.assertTrue(fileio.is_file(success_path))
        fileio.delete(path)

    def test_save_format(self):
        t = XArray([1, 2, 3])
        path = '{}/tmp/array-csv'.format(hdfs_prefix)
        t.save(path, format='text')
        success_path = os.path.join(path, '_SUCCESS')
        self.assertTrue(fileio.is_file(success_path))
        fileio.delete(path)


class TestXFrameConstructor(XFrameUnitTestCase):
    """
    Tests XFrame constructors that create data from local sources.
    """

    def test_construct_auto_dataframe(self):
        path = '{}/user/xpatterns/files/test-frame-auto.csv'.format(hdfs_prefix)
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
        path = '{}/user/xpatterns/files/test-frame.csv'.format(hdfs_prefix)
        res = XFrame(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_str_tsv(self):
        path = '{}/user/xpatterns/files/test-frame.tsv'.format(hdfs_prefix)
        res = XFrame(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_str_psv(self):
        path = '{}/user/xpatterns/files/test-frame.psv'.format(hdfs_prefix)
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
        path = '{}/user/xpatterns/files/test-frame.txt'.format(hdfs_prefix)
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
        path = '{}/user/xpatterns/files/test-frame'.format(hdfs_prefix)
        res = XFrame(path)
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_auto_str_xframe(self):
        # construct an XFrame given a file with unrecognized file extension
        path = '{}/user/xpatterns/files/test-frame'.format(hdfs_prefix)
        res = XFrame(path)
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_str_csv(self):
        # construct and XFrame given a text file
        # interpret as csv
        path = '{}/user/xpatterns/files/test-frame.txt'.format(hdfs_prefix)
        res = XFrame(path, format='csv')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])

    def test_construct_str_xframe(self):
        # construct and XFrame given a saved xframe
        path = '{}/user/xpatterns/files/test-frame'.format(hdfs_prefix)
        res = XFrame(path, format='xframe')
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])


class TestXFrameReadCsv(XFrameUnitTestCase):
    """
    Tests XFrame read_csv
    """

    def test_read_csv(self):
        path = '{}/user/xpatterns/files/test-frame.csv'.format(hdfs_prefix)
        res = XFrame.read_csv(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])


class TestXFrameReadText(XFrameUnitTestCase):
    """
    Tests XFrame read_text
    """

    def test_read_text(self):
        path = '{}/user/xpatterns/files/test-frame-text.txt'.format(hdfs_prefix)
        res = XFrame.read_text(path)
        self.assertEqualLen(3, res)
        self.assertListEqual(['text', ], res.column_names())
        self.assertListEqual([str], res.column_types())
        self.assertDictEqual({'text': 'This is a test'}, res[0])
        self.assertDictEqual({'text': 'of read_text.'}, res[1])
        self.assertDictEqual({'text': 'Here is another sentence.'}, res[2])


class TestXFrameReadParquet(XFrameUnitTestCase):
    """
    Tests XFrame read_parquet
    """

    def test_read_parquet_str(self):
        t = XFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
        path = '{}/tmp/frame-parquet'.format(hdfs_prefix)
        t.save(path, format='parquet')

        res = XFrame('{}/tmp/frame-parquet.parquet'.format(hdfs_prefix))
        # results may not come back in the same order
        res = res.sort('id')
        self.assertEqualLen(3, res)
        self.assertListEqual(['id', 'val'], res.column_names())
        self.assertListEqual([int, str], res.column_types())
        self.assertDictEqual({'id': 1, 'val': 'a'}, res[0])
        self.assertDictEqual({'id': 2, 'val': 'b'}, res[1])
        self.assertDictEqual({'id': 3, 'val': 'c'}, res[2])
        fileio.delete(path)


class TestXFrameSaveBinary(XFrameUnitTestCase):
    """
    Tests XFrame save binary format
    """

    def test_save(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['a', 'b', 'c']})
        path = '{}/tmp/frame'.format(hdfs_prefix)
        t.save(path, format='binary')
        with fileio.open_file(os.path.join(path, '_metadata')) as f:
            metadata = pickle.load(f)
        self.assertListEqual([['id', 'val'], [int, str]], metadata)
        # TODO find some way to check the data
        fileio.delete(path)


class TestXFrameSaveCsv(XFrameUnitTestCase):
    """
    Tests XFrame save csv format
    """

    def test_save(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['a', 'b', 'c']})
        path = '{}/tmp/frame-csv'.format(hdfs_prefix)
        t.save(path, format='csv')

        with fileio.open_file(path + '.csv') as f:
            heading = f.readline().rstrip()
            self.assertEqual('id,val', heading)
            self.assertEqual('30,a', f.readline().rstrip())
            self.assertEqual('20,b', f.readline().rstrip())
            self.assertEqual('10,c', f.readline().rstrip())
        fileio.delete(path + '.csv')


class TestXFrameSaveParquet(XFrameUnitTestCase):
    """
    Tests XFrame save for parquet files
    """
    def test_save(self):
        t = XFrame({'id': [30, 20, 10], 'val': ['a', 'b', 'c']})
        path = '{}/tmp/frame-parquet'.format(hdfs_prefix)
        t.save(path, format='parquet')
        # TODO verify
        fileio.delete(path + '.parquet')


if __name__ == '__main__':
    unittest.main()
