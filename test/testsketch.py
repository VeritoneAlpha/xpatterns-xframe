import unittest

# Check the spark configuration
import os
if not 'SPARK_HOME' in os.environ:
    print 'SPARK_HOME must be set'
spark_home = os.environ['SPARK_HOME']

# Set the python path
import sys
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(1, os.path.join(spark_home, 'python/lib/py4j-0.8.2.1-src.zip'))


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


