"""
This module provides an implementation of Sketch using pySpark RDDs.
"""

import inspect
import math

from xpatterns.common_impl import CommonSparkContext
from xpatterns.xframe_impl import XFrameImpl
from xpatterns.xrdd import XRdd
from xpatterns.dsq import QuantileAccumulator

__all__ = ['Sketch']

class NotReadyError(Exception):
    pass

def is_missing(x):
    if x is None: return True
    if isinstance(x, float) and math.isnan(x): return True
    return False

class SketchImpl:

    entry_trace = False
    exit_trace = False

    def __init__(self):
        self._entry()
        self.quantile_accum = None
        self.sketch_ready = False
        self._exit()
            
    def _entry(self, *args):
        if SketchImpl.entry_trace:
            print 'enter sketch', inspect.stack()[1][3], args

    def _exit(self):
        if SketchImpl.exit_trace:
            print 'exit sketch', inspect.stack()[1][3]
        pass
        
    @classmethod
    def set_trace(cls, entry_trace=None, exit_trace=None):
        cls.entry_trace = entry_trace or cls.entry_trace
        cls.exit_trace = exit_trace or cls.exit_trace

    def construct_from_xarray(self, xa, background=None, sub_sketch_keys=None):
        self._entry(background, sub_sketch_keys)
        if background:
            raise NotImplementedError('background mode not implemented')
        if sub_sketch_keys is not None:
            raise NotImplementedError('sub_sketch_keys mode not implemented')

        # use xa to create the quantile accumulator
        # TODO calculate these in one pass
        res = xa.rdd.filter(lambda x: not is_missing(x))
        
        lower_bound = res.min()
        upper_bound = res.max()
#       With these values, the system runs out of memory
#        num_levels = 12
#        epsilon = 0.001
#        delta = 0.01
#       With these, it is OK.
        num_levels = 10
        epsilon = 0.01
        delta = 0.1
        self.accumulator = QuantileAccumulator(lower_bound, upper_bound, num_levels, epsilon, delta)
        accumulators = res.mapPartitions(self.accumulator)
        self.quantile_accum = accumulators.reduce(lambda x, y: x.merge(y))
        self.sketch_ready = True
        self._exit()

    def size(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('size not implemented')

    def max(self):
        if not self.sketch_ready:
            raise NotReadyError()
        return self.accumulator.upper_bound

    def min(self):
        if not self.sketch_ready:
            raise NotReadyError()
        return self.accumulator.lower_bound

    def sum(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('sum not implemented')

    def mean(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('mean not implemented')

    def var(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('var not implemented')

    def num_undefined(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('num_undefined not implemented')

    def num_unique(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('num_unique not implemented')

    def frequent_items(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('frequent_items not implemented')

    def get_quantile(self, quantile_val):
        if not self.sketch_ready:
            raise NotReadyError()
        return self.quantile_accum.ppf(quantile_val)

    def frequency_count(self, element):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('frequency_count not implemented')

    def sketch_ready(self):
        return self.sketch_ready

    def num_elements_processed(self):
        if not self.sketch_ready:
            raise NotReadyError()
        pass

    def element_length_summary(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('element_length_summary not implemented')

    def dict_key_summary(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('dict_key_summary not implemented')

    def dict_value_summary(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('dict_value_summary not implemented')

    def element_summary(self):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('element_summary not implemented')

    def element_sub_sketch(self, keys):
        if not self.sketch_ready:
            raise NotReadyError()
        raise NotImplementedError('sub_sketch not implemented')

    def cancel(self):
        pass
