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


if __name__ == '__main__':
    unittest.main()

