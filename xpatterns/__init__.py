import os
import sys

if 'SPARK_HOME' in os.environ:
    spark_home = os.environ['SPARK_HOME']
    sys.path.insert(0, os.path.join(spark_home, 'python'))
    sys.path.insert(1, os.path.join(spark_home, 'python/lib/py4j-0.8.2.1-src.zip'))

__all__ = ['xframe', 'xarray', 'xplot', 'sketch']


from xpatterns.xarray import XArray
from xpatterns.xframe import XFrame
from xpatterns.xrdd import XRdd
from xpatterns.sketch import Sketch
from xpatterns.xplot import XPlot
