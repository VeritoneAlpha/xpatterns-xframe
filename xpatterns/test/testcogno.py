import unittest

import os
import shutil

# Do some tests with cognosante data.
# Use 1000 lines of data

import sys
sys.path.insert(0, '/home/ubuntu/spark/python')
sys.path.insert(1, '/home/ubuntu/spark/python/lib/py4j-0.8.2.1-src.zip')

# python testcogno.py
# python -m unittest testcogno
# python -m unittest testcogno.TestCognoLoadRaw
# python -m unittest testcogno.TestCognoLoadRaw.test_read

from xpatterns.xarray import XArray
from xpatterns.xframe import XFrame
from xpatterns.aggregate import SUM, ARGMAX, ARGMIN, MAX, MIN, COUNT, AVG, MEAN, \
    VAR, VARIANCE, STD, STDV, SELECT_ONE, CONCAT, QUANTILE

def setUpModule():
        in_path = '../data/SB179010.PROF.PAC.1000.csv'
        out_path = '../data-files/SB179010.PROF.PAC.1000'
        res = XFrame.read_csv(in_path, column_type_hints=str, verbose=False)
        res.save(out_path)

def tearDownModule():
        path = '../data-files/SB179010.PROF.PAC.1000'
        shutil.rmtree(path)

class TestWithFile(unittest.TestCase):
    def setUp(self):
        path = '../data-files/SB179010.PROF.PAC.1000'
        frame = XFrame.load(path)
        self.assertEqual(999, len(frame))
        self.frame = frame

class TestCognoProperties(TestWithFile):
    """
    Test properties
    """

    def test_shape(self):
        shape = self.frame.shape
        self.assertEqual((999, 244), shape)

    def test_column_names(self):
        names = self.frame.column_names()
        self.assertEqual(244, len(names))
        self.assertEqual('CLMI-CLM-PRCS-RCRD-CD', names[0])

    def test_column_types(self):
        types = self.frame.column_types()
        self.assertEqual(244, len(types))
        self.assertEqual(str, types[0])

    def test_dtype(self):
        types = self.frame.dtype()
        self.assertEqual(244, len(types))
        self.assertEqual(str, types[0])

    def test_num_rows(self):
        self.assertEqual(999, len(self.frame))
        self.assertEqual(999, self.frame.num_rows())

    def test_num_columns(self):
        self.assertEqual(244, self.frame.num_columns())

    def test_topk(self):
        top = self.frame.topk('CLMI-INTNL-CTL-NO')
        first_row = top[0]
        self.assertEqual('402013357002273', first_row[4])
        self.assertEqual(['M', '37', 'D', 'ND', '402013357002273'], first_row[0:5])
        self.assertEqual(['M', '37', 'D', 'ND', '402013357002272'], top[1][0:5])

    def test_head(self):
        head = self.frame.head()
        first_row = head[0]
        self.assertEqual('402013329001041', first_row[4])
        self.assertEqual(['M', '37', 'D', 'ND', '402013329001041'], first_row[0:5])
        self.assertEqual(['M', '37', 'D', 'ND', '402013324006402'], head[1][0:5])


class TestCognoManipulation(TestWithFile):
    """
    Test manipulation
    """

    def test_add_column(self):
        names = self.frame.column_names()
        col = '*' + self.frame['CLMI-INTNL-CTL-NO'] + '*'
        self.frame.add_column(col, 'new-col')
        self.assertEqual(245, self.frame.num_columns())
        self.frame.remove_column('new-col')
        self.assertEqual(244, self.frame.num_columns())
        self.assertEqual(names, self.frame.column_names())

    def test_add_columns(self):
        names = self.frame.column_names()
        cols = self.frame.select_columns(['CLMI-INTNL-CTL-NO', 'CLMI-CLM-TRN-SRT-1', 'CLMI-CLM-TRN-SRT-2'])
        cols.rename({'CLMI-INTNL-CTL-NO': 'new-col-1', 
                     'CLMI-CLM-TRN-SRT-1': 'new-col-2', 
                     'CLMI-CLM-TRN-SRT-2': 'new-col-3'})
        self.frame.add_columns(cols)
        self.assertEqual(247, self.frame.num_columns())
        self.frame.remove_columns(['new-col-1', 'new-col-2', 'new-col-3'])
        self.assertEqual(244, self.frame.num_columns())
        self.assertEqual(names, self.frame.column_names())

    def test_add_row_number(self):
        numbered = self.frame.add_row_number('row-number')
        head = numbered.head()
        for i in range(0, 10):
            self.assertEqual(i, numbered[i][0])

    def test_swap_columns(self):
        names = self.frame.column_names()
        self.frame.swap_columns('CLMI-CLM-PRCS-RCRD-CD', 'CLMI-CLM-PRCS-TXCD')
        new_names = names[:]
        new_names[0], new_names[1] = new_names[1], new_names[0]
        self.assertEqual(new_names, self.frame.column_names())
        self.assertEqual('37', self.frame[0][0])
        self.assertEqual('M', self.frame[0][1])
        self.frame.swap_columns('CLMI-CLM-PRCS-RCRD-CD', 'CLMI-CLM-PRCS-TXCD')
        self.assertEqual(names, self.frame.column_names())
        
    def test_unique(self):
        col = self.frame['CLMI-CLM-ICN-CHK-DIG']
        u = col.unique()
        self.assertEqual(range(0, 8), u.sort())


    def test_sort(self):
        sorted = self.frame.sort('CLMI-INTNL-CTL-NO')
        col = sorted['CLMI-INTNL-CTL-NO']
        last =col[0]
        for i in col:
            self.assertTrue(last <= i)
            last = i

    def test_append(self):
        head = self.frame.head()
        extended = self.frame.append(head)
        self.assertEqual(1009, len(extended))

    def test_apply(self):
        names = self.frame.column_names()
        self.frame['check-squared'] = self.frame.apply(lambda row: int(row['CLMI-CLM-ICN-CHK-DIG']) ** 2)
        diff = self.frame.apply(lambda row: 1 if row['check-squared'] != int(row['CLMI-CLM-ICN-CHK-DIG']) **2 else 0)
        self.assertEqual(0, diff.sum())
        del self.frame['check-squared']
        self.assertEqual(names, self.frame.column_names())

    def test_filter_by(self):
        filt = self.frame.filter_by(['402013203714005'], 'CLMI-INTNL-CTL-NO')
        self.assertEqual(9, len(filt))
        for row in filt:
            self.assertEqual('402013203714005', row[4])

    def test_groupby(self):
        grouped = self.frame.groupby('CLMI-INTNL-CTL-NO', 
                                     {'count': COUNT,
                                      'seq-no': CONCAT('CLMI-CLM-SEQ-NO')})
        check = grouped.apply(lambda row: abs(row['count'] - len(row['seq-no'])))
        self.assertEqual(0, check.sum())
        self.assertEqual(13, grouped['count'].max())

    def test_pack_columns(self):
        names = self.frame.column_names()
        nf_names = [name for name in names if name != 'FILLER']
        fixed_frame = self.frame[nf_names]
        packed = fixed_frame.pack_columns(['CLMI-CLM-PRCS-RCRD-CD', 'CLMI-CLM-PRCS-TXCD', 'CLMI-CLM-STCD', 'CLMI-AGENCY-CODE'],
                                         new_column_name='composite')
        self.assertEqual(['M', '37', 'D', 'ND'], packed['composite'][0])

    def test_pack_unpack(self):
        names = self.frame.column_names()
        nf_names = [name for name in names if name != 'FILLER']
        fixed_frame = self.frame[nf_names]
        packed = fixed_frame.pack_columns(['CLMI-CLM-PRCS-RCRD-CD', 'CLMI-CLM-PRCS-TXCD', 'CLMI-CLM-STCD', 'CLMI-AGENCY-CODE'],
                                         dtype=dict, new_column_name='composite')
        unpack = packed.unpack('composite', column_name_prefix='')
        cols =  unpack[['CLMI-CLM-PRCS-RCRD-CD', 'CLMI-CLM-PRCS-TXCD', 'CLMI-CLM-STCD', 'CLMI-AGENCY-CODE']]
        self.assertEqual(['M', '37', 'D', 'ND'], cols[0])
        
    def test_unstack_stack(self):
        subset = self.frame[['CLMI-INTNL-CTL-NO', 'CLMI-CLM-SEQ-NO']]
        unstacked = subset.unstack('CLMI-CLM-SEQ-NO', new_column_name='seq-no')
        unstacked['count'] = unstacked['seq-no'].apply(lambda x: len(x))
        self.assertEqual(13, unstacked['count'].max())

        stacked = unstacked.stack('seq-no')
        self.assertEqual(len(self.frame), len(stacked))

        # flat_map


if __name__ == '__main__':
    unittest.main()
