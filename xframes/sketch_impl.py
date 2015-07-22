"""
This module provides an implementation of Sketch using pySpark RDDs.
"""

import inspect
import math
from sys import stderr


from xframes.dsq import QuantileAccumulator
from xframes.frequent import FreqSketch
import xframes.util as util

__all__ = ['Sketch']


def is_missing(x):
    if x is None: return True
    if isinstance(x, float) and math.isnan(x): return True
    return False


class SketchImpl(object):

    entry_trace = False
    exit_trace = False

    def __init__(self):
        self._entry()
        self._rdd = None
        self.sketch_type = None
        self.count = 0
        self.min_val = util.nan
        self.max_val = util.nan
        self.mean_val = 0
        self.sum_val = 0
        self.variance_val = 0.0
        self.stdev_val = 0.0
        self.num_undefined_val = None
        self.num_unique_val = None
        self.quantile_accumulator = None
        self.frequency_sketch = None
        self.quantile_accum = None
        self._exit()
            
    @staticmethod
    def _entry(*args):
        if SketchImpl.entry_trace:
            print >>stderr, 'Enter sketch', inspect.stack()[1][3], args

    @staticmethod
    def _exit():
        if SketchImpl.exit_trace:
            print >>stderr, 'Exit sketch', inspect.stack()[1][3]
        pass
        
    @classmethod
    def set_trace(cls, entry_trace=None, exit_trace=None):
        cls.entry_trace = entry_trace or cls.entry_trace
        cls.exit_trace = exit_trace or cls.exit_trace

    def construct_from_xarray(self, xa, sub_sketch_keys=None):
        self._entry(sub_sketch_keys)
        if sub_sketch_keys is not None:
            raise NotImplementedError('sub_sketch_keys mode not implemented')

        # calculate some basic statistics in one pass
        defined = xa.to_rdd().filter(lambda x: not is_missing(x))
        self.sketch_type = 'numeric' if xa.dtype() in (int, float) else 'non-numeric'
        if self.sketch_type == 'numeric':
            stats = defined.stats()
            self.count = stats.count()
            self.min_val = stats.min()
            self.max_val = stats.max()
            self.mean_val = stats.mean()
            self.sum_val = stats.sum()
            self.variance_val = stats.variance()
            self.stdev_val = stats.stdev()
        else:
            self.count = defined.count()

        # compute these later if needed
        self._rdd = xa.to_rdd()

        self._exit()

    def _create_quantile_accumulator(self):
        num_levels = 12
        epsilon = 0.001
        delta = 0.01
        accumulator = QuantileAccumulator(self.min_val, self.max_val, num_levels, epsilon, delta)
        accumulators = self._rdd.mapPartitions(accumulator)
        return accumulators.reduce(lambda x, y: x.merge(y))

    def _create_frequency_sketch(self):
        num_items = 500
        epsilon = 0.0001
        delta = 0.01
        accumulator = FreqSketch(num_items, epsilon, delta)
        accumulators = self._rdd.mapPartitions(accumulator.iterate_values)
        return accumulators.aggregate(FreqSketch.initial_accumulator_value(), 
                                      FreqSketch.merge_accumulator_value, 
                                      FreqSketch.merge_accumulators)

    def size(self):
        return self.count

    def max(self):
        if self.sketch_type == 'numeric':
            return self.max_val
        raise ValueError('max only available for numeric types')

    def min(self):
        if self.sketch_type == 'numeric':
            return self.min_val
        raise ValueError('max only available for numeric types')

    def sum(self):
        if self.sketch_type == 'numeric':
            return self.sum_val
        raise ValueError('max only available for numeric types')

    def mean(self):
        if self.sketch_type == 'numeric':
            return self.mean_val
        raise ValueError('max only available for numeric types')

    def var(self):
        if self.sketch_type == 'numeric':
            return self.variance_val
        raise ValueError('max only available for numeric types')

    def num_undefined(self):
        if self.num_undefined_val is None:
            self.num_undefined_val = self._rdd.filter(lambda x: is_missing(x)).count()
        return self.num_undefined_val

    def num_unique(self):
        if self.num_unique_val is None:
            self.num_unique_val = self._rdd.distinct().count()
        return self.num_unique_val

    def frequent_items(self):
        if self.frequency_sketch is None:
            self.frequency_sketch = self._create_frequency_sketch()
        return self.frequency_sketch

    def get_quantile(self, quantile_val):
        if self.sketch_type == 'numeric':
            if self.quantile_accumulator is None:
                self.quantile_accumulator = self._create_quantile_accumulator()
            return self.quantile_accumulator.ppf(quantile_val)
        raise ValueError('max only available for numeric types')

    def frequency_count(self, element):
        if self.frequency_sketch is None:
            self.frequency_sketch = self._create_frequency_sketch()
        return self.frequency_sketch.get(element)

    def element_length_summary(self):
        raise NotImplementedError('element_length_summary not implemented')

    def dict_key_summary(self):
        raise NotImplementedError('dict_key_summary not implemented')

    def dict_value_summary(self):
        raise NotImplementedError('dict_value_summary not implemented')

    def element_summary(self):
        raise NotImplementedError('element_summary not implemented')

    def element_sub_sketch(self, keys):
        raise NotImplementedError('sub_sketch not implemented')
