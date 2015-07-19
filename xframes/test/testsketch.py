import unittest

# python testsketch.py
# python -m unittest testsketch
# python -m unittest testsketch.TestSketchConstructor
# python -m unittest testsketch.TestSketchConstructor.test_construct

from xpatterns.xarray import XArray


def eq_list(expected, result):
    return (XArray(expected) == result).all()


class TestSketchConstructor(unittest.TestCase):
    """
    Tests sketch constructor
    """

    def test_construct(self):
        t = XArray([1, 2, 3, 4, 5])
        ss = t.sketch_summary()
        self.assertAlmostEqual(3, ss.quantile(0.5), places=1)
        self.assertAlmostEqual(4, ss.quantile(0.8), places=1)
        self.assertAlmostEqual(5, ss.quantile(0.9), places=1)
        self.assertAlmostEqual(5, ss.quantile(0.99), places=1)


if __name__ == '__main__':
    unittest.main()

