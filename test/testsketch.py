import unittest

 # Configure the necessary Spark environment
import os
os.environ['SPARK_HOME'] = '/home/ubuntu/spark/'

# And Python path
import sys
sys.path.insert(0, '/home/ubuntu/spark/python')
sys.path.insert(1, '/home/ubuntu/spark/python/lib/py4j-0.8.2.1-src.zip')


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
        self.assertAlmostEqual(3, ss.quantile(0.5), places=2)
        self.assertAlmostEqual(4, ss.quantile(0.8), places=2)
        self.assertAlmostEqual(5, ss.quantile(0.9), places=2)
        self.assertAlmostEqual(5, ss.quantile(0.99), places=2)


