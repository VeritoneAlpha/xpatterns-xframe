
from pprint import pprint
from sys import stderr


from xframes.xrdd import XRdd
from xframes.xarray_impl import XArrayImpl
from xframes.xframe_impl import XFrameImpl
 

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
    def xframes_track(enable=True):
        XFrameImpl.set_perf_count(enable)
        XArrayImpl.set_perf_count(enable)

    @staticmethod
    def print_perf():
        perf = XRdd.get_perf_count()
        if perf:
            print >>stderr, 'XRDD'
            pprint(perf, stream=stderr)
        perf = XArrayImpl.get_perf_count()
        if perf:
            print >>stderr, 'XArray'
            pprint(perf, stream=stderr)
        perf = XFrameImpl.get_perf_count()
        if perf:
            print >>stderr, 'XFrame'
            pprint(perf, stream=stderr)
