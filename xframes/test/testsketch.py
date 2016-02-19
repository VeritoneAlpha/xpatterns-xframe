import unittest

# python testsketch.py
# python -m unittest testsketch
# python -m unittest testsketch.TestSketchConstructor
# python -m unittest testsketch.TestSketchConstructor.test_construct

from xframes.xarray import XArray


def eq_list(expected, result):
    return (XArray(expected) == result).all()


class TestSketchConstructor(unittest.TestCase):
    """
    Tests sketch constructor
    """

    def test_construct(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertEqual(5, ss.size())
        self.assertEqual(5, ss.max())
        self.assertEqual(1, ss.min())
        self.assertEqual(15, ss.sum())
        self.assertEqual(3, ss.mean())
        self.assertAlmostEqual(1.4142135623730951, ss.std())
        self.assertAlmostEqual(2.0, ss.var())

    def test_avg_length_int(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertEqual(1, ss.avg_length())

    def test_avg_length_float(self):
        t = XArray([1.0, 2.0, 3.0, 4.0, 5.0])
        ss = t.sketch_summary()
        self.assertEqual(1, ss.avg_length())

    def test_avg_length_list(self):
        t = XArray([[1, 2, 3, 4], [5, 6]])
        ss = t.sketch_summary()
        self.assertEqual(3, ss.avg_length())

    def test_avg_length_dict(self):
        t = XArray([{1: 1, 2: 2, 3: 3, 4: 4}, {5: 5, 6: 6}])
        ss = t.sketch_summary()
        self.assertEqual(3, ss.avg_length())

    def test_avg_length_str(self):
        t = XArray(['a', 'bb', 'ccc', 'dddd', 'eeeee'])
        ss = t.sketch_summary()
        self.assertEqual(3, ss.avg_length())

    def test_avg_length_empty(self):
        t = XArray([])
        ss = t.sketch_summary()
        self.assertEqual(0, ss.avg_length())

    def test_num_undefined(self):
        t = XArray([1, 2, 3, 4, 5, None])
        ss = t.sketch_summary()
        self.assertEqual(1, ss.num_undefined())

    def test_num_unique(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertEqual(5, ss.num_unique())

    def test_frequent_items(self):
        t = XArray([1, 2, 3, 2])
        ss = t.sketch_summary()
        self.assertEqual({1: 1, 2: 2, 3: 1}, ss.frequent_items())

    def test_tf_idf_list(self):
        t = XArray([['this', 'is', 'a', 'test'], ['another', 'test']])
        ss = t.sketch_summary()
        tf_idf = ss.tf_idf()
        self.assertEqual({'this': 0.4054651081081644,
                          'a': 0.4054651081081644,
                          'is': 0.4054651081081644,
                          'test': 0.0},
                         tf_idf[0])
        self.assertEqual({'test': 0.0,
                          'another': 0.4054651081081644},
                         tf_idf[1])

    def test_tf_idf_str(self):
        t = XArray(['this is a test', 'another test'])
        ss = t.sketch_summary()
        tf_idf = ss.tf_idf()
        self.assertEqual({'this': 0.4054651081081644,
                          'a': 0.4054651081081644,
                          'is': 0.4054651081081644,
                          'test': 0.0},
                         tf_idf[0])
        self.assertEqual({'test': 0.0,
                          'another': 0.4054651081081644},
                         tf_idf[1])

    def test_quantile(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertAlmostEqual(3, ss.quantile(0.5), places=1)
        self.assertAlmostEqual(4, ss.quantile(0.8), places=1)
        self.assertAlmostEqual(5, ss.quantile(0.9), places=1)
        self.assertAlmostEqual(5, ss.quantile(0.99), places=1)

    def test_frequency_count(self):
        t = XArray([1, 2, 3, 4, 5, 3])
        ss = t.sketch_summary()
        self.assertEqual(2, ss.frequency_count(3))

    def test_missing(self):
        t = XArray([None], dtype=int)
        ss = t.sketch_summary()
        self.assertIsNone(ss.min())
        self.assertIsNone(ss.max())
        self.assertEqual(0, ss.mean())
        self.assertEqual(0.0, ss.sum())
        self.assertIsNone(ss.var())
        self.assertIsNone(ss.std())
        self.assertIsNone(ss.max())
        self.assertEqual(0, ss.avg_length())



if __name__ == '__main__':
    unittest.main()

