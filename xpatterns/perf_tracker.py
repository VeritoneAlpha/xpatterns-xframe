
from pprint import pprint

from xpatterns.xrdd import XRdd
from xpatterns.XArray_impl import XArrayImpl
from xpatterns.XFrame_impl import XFrameImpl
 

class PerfTracker(object):
    @staticmethod
    def xrdd_track(enable=True):
        XRdd.set_perf_count(enable)

    @staticmethod
    def xarray_track(enable=True):
        XArrayImpl.set_perf_count(enable)

    @staticmethod
    def xframe_track(enable=True):
        XFrameImpl.set_perf_count(enable)

    @staticmethod
    def xpatterns_track(enable=True):
        XFrameImpl.set_perf_count(enable)
        XArrayImpl.set_perf_count(enable)

    @staticmethod
    def print_perf():
        perf = XRdd.get_perf_count()
        if perf:
            print 'XRDD'
            pprint(perf)
        perf = XArrayImpl.get_perf_count()
        if perf:
            print 'XArray'
            pprint(perf)
        perf = XFrameImpl.get_perf_count()
        if perf:
            print 'XFrame'
            pprint(perf)
