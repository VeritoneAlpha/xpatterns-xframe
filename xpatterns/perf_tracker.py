
from pprint import pprint

from xpatterns.xrdd import xRdd
from xpatterns.stdXArrayImpl import StdXArrayImpl
from xpatterns.stdXFrameImpl import StdXFrameImpl
 

class PerfTracker:
    @staticmethod
    def xrdd_track(enable=True):
        xRdd.set_perf_count(enable)

    @staticmethod
    def xarray_track(enable=True):
        StdXArrayImpl.set_perf_count(enable)

    @staticmethod
    def xframe_track(enable=True):
        StdXFrameImpl.set_perf_count(enable)

    @staticmethod
    def xpatterns_track(enable=True):
        StdXFrameImpl.set_perf_count(enable)
        StdXArrayImpl.set_perf_count(enable)

    @staticmethod
    def print_perf():
        perf = xRdd.get_perf_count()
        if perf:
            print 'xRDD'
            pprint(perf)
        perf = StdXArrayImpl.get_perf_count()
        if perf:
            print 'XArray'
            pprint(perf)
        perf = StdXFrameImpl.get_perf_count()
        if perf:
            print 'XFrame'
            pprint(perf)
