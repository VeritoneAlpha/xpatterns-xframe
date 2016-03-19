"""
This module provides an implementation of Sketch using pySpark RDDs.
"""

import math
import datetime
from collections import Counter
import copy
import logging


from xframes.xobject_impl import XObjectImpl
from xframes.traced_object import TracedObject
from xframes.dsq import QuantileAccumulator
from xframes.frequent import FreqSketch
from xframes import util
from xframes import xarray_impl

__all__ = ['Sketch']


def is_missing(x):
    if x is None:
        return True
    if isinstance(x, (float, int)) and (
            math.isnan(x) or math.isinf(x)):
        return True
    return False


def normalize_number(x):
    return None if is_missing(x) else x


class SketchImpl(XObjectImpl, TracedObject):

    entry_trace = False
    exit_trace = False

    def __init__(self):
        super(SketchImpl, self).__init__(None)
        self.defined = None
        self.dtype = None
        self.sketch_type = None
        self.count = 0
        self.stats = None
        self.min_val = None
        self.max_val = None
        self.mean_val = None
        self.sum_val = None
        self.variance_val = None
        self.stdev_val = None
        self.avg_len = None
        self.num_undefined_val = None
        self.num_unique_val = None
        self.quantile_accumulator = None
        self.quantile_accumulator_num_levels = None
        self.quantile_accumulator_epsilon = None
        self.quantile_accumulator_delta = None
        self.frequency_sketch = None
        self.frequency_sketch_num_items = None
        self.frequency_sketch_epsilon = None
        self.frequency_sketch_delta = None

    def set_quantile_accumulator_params(self, num_levels, epsilon, delta):
        self.quantile_accumulator_num_levels = num_levels
        self.quantile_accumulator_epsilon = epsilon
        self.quantile_accumulator_delta = delta

    def set_frequency_sketch_params(self, num_items, epsilon, delta):
        self.frequency_sketch_num_items = num_items
        self.frequency_sketch_epsilon = epsilon
        self.frequency_sketch_delta = delta

    def construct_from_xarray(self, xa, sub_sketch_keys=None):
        self._entry(sub_sketch_keys=sub_sketch_keys)
        if sub_sketch_keys is not None:
            raise NotImplementedError('sub_sketch_keys mode not implemented')

        # these are not going through the xrdd layer -- should they?
        defined = xa.to_rdd().filter(lambda x: not is_missing(x))
        defined.cache()
        self.dtype = xa.dtype()
        self.count = defined.count()
        if util.is_numeric_type(self.dtype):
            self.sketch_type = 'numeric'
        elif util.is_date_type(self.dtype):
            self.sketch_type = 'date'
        else:
            self.sketch_type = 'non-numeric'

        # compute others later if needed
        self._rdd = xa.to_rdd()
        self.defined = defined

    def _create_stats(self):
        # calculate some basic statistics
        if self.stats is None:
            if util.is_date_type(self.dtype):
                self.min_val = normalize_number(self.defined.min())
                self.max_val = normalize_number(self.defined.max())
            else:
                stats = self.defined.stats()
                self.min_val = normalize_number(stats.min())
                self.max_val = normalize_number(stats.max())
                self.mean_val = normalize_number(stats.mean())
                self.sum_val = normalize_number(stats.sum())
                self.variance_val = normalize_number(stats.variance())
                self.stdev_val = normalize_number(stats.stdev())
                self.stats = stats

    def _create_quantile_accumulator(self):
        num_levels = self.quantile_accumulator_num_levels or 12
        epsilon = self.quantile_accumulator_epsilon or 0.001
        delta = self.quantile_accumulator_delta or 0.01
        accumulator = QuantileAccumulator(self.min_val, self.max_val, num_levels, epsilon, delta)
        accumulators = self._rdd.mapPartitions(accumulator)
        return accumulators.reduce(lambda x, y: x.merge(y))

    def _create_frequency_sketch(self):
        num_items = self.frequency_sketch_num_items or 500
        epsilon = self.frequency_sketch_epsilon or 0.0001
        delta = self.frequency_sketch_delta or 0.01
        accumulator = FreqSketch(num_items, epsilon, delta)
        accumulators = self._rdd.mapPartitions(accumulator.iterate_values)
        return accumulators.aggregate(FreqSketch.initial_accumulator_value(),
                                      FreqSketch.merge_accumulator_value,
                                      FreqSketch.merge_accumulators)

    def size(self):
        return self.count

    def max(self):
        if self.sketch_type in ['numeric', 'date']:
            self._create_stats()
            return self.max_val
        raise ValueError('max only available for numeric or date types')

    def min(self):
        if self.sketch_type in ['numeric', 'date']:
            self._create_stats()
            return self.min_val
        raise ValueError('min only available for numeric or date types')

    def sum(self):
        if self.sketch_type == 'numeric':
            self._create_stats()
            return self.sum_val
        raise ValueError('sum only available for numeric types')

    def mean(self):
        if self.sketch_type == 'numeric':
            self._create_stats()
            return self.mean_val
        raise ValueError('mean only available for numeric types')

    def var(self):
        if self.sketch_type == 'numeric':
            self._create_stats()
            return self.variance_val
        raise ValueError('var only available for numeric types')

    def avg_length(self):
        if self.avg_len is None:
            if self.count == 0:
                self.avg_len = 0
            elif self.dtype in [int, float, datetime.datetime]:
                self.avg_len = 1
            elif self.dtype in [list, dict, str]:
                lens = self.defined.map(lambda x: len(x))
                self.avg_len = lens.sum() / float(self.count)
            else:
                self.avg_len = 0
        return self.avg_len

    def num_undefined(self):
        if self.num_undefined_val is None:
            self.num_undefined_val = self._rdd.filter(lambda x: is_missing(x)).count()
        return self.num_undefined_val

    def num_unique(self):
        if self.num_unique_val is None:
            # distinct fails if the values are not hashable
            if self.dtype in [list, dict]:
                rdd = self._rdd.map(lambda x: str(x))
            else:
                rdd = self._rdd
            self.num_unique_val = rdd.distinct().count()
        return self.num_unique_val

    def frequent_items(self):
        if self.frequency_sketch is None:
            self.frequency_sketch = self._create_frequency_sketch()
        return self.frequency_sketch

    def tf_idf(self):
        """ Returns an RDD of td-idf dicts, one for each document. """
        def normalize_doc(doc):
            if doc is None:
                return []
            if not isinstance(doc, str):
                logging.warn('Document should be str -- is {}: {}'.format(type(doc).__name__, doc))
                return []
            return doc.strip().lower().split()
        if self.dtype is str:
            docs = self._rdd.map(normalize_doc)
        else:
            docs = self._rdd.map(lambda doc: doc or [])
        docs.cache()

        # build TF
        def build_tf(doc):
            # Build term Frequency for a document (cell)
            if len(doc) == 0:
                return {}
            counts = Counter()
            counts.update(doc)
            # use augmented frequency (see en.wikipedia.org/wiki/Tf-idf)
            k = 0.5   # regularization factor
            max_count = float(counts.most_common(1)[0][1])
            return {word: k + (((1.0 - k) * count) / max_count)
                    for word, count in counts.iteritems()}
        tf = docs.map(build_tf)

        # get all terms
        # we can afford to do this because there aren't that many words
        all_terms = docs.flatMap(lambda x: x if x else '').distinct().collect()
        term_count = {term: 0 for term in all_terms}

        def seq_op(count, doc):
            # Spark documentaton says you can modify first arg, but
            #  it is not true!
            counts = Counter(count)
            counts.update(doc)
            return dict(counts)

        def comb_op(count1, count2):
            counts = copy.copy(count1)
            for term, count in count2.iteritems():
                counts[term] += count
            return counts

        # create idf counts per document
        counts = docs.aggregate(term_count, seq_op, comb_op)

        doc_count = float(docs.count())
        # add 1.0 to denominator, as suggested by wiki article cited above
        idf = {term: math.log((doc_count + 1.0) / (count + 1.0))
               for term, count in counts.iteritems()}

        def build_tfidf(tf):
            return {term: tf_count * idf[term] for term, tf_count in tf.iteritems()}
        tfidf = tf.map(build_tfidf)
        return xarray_impl.XArrayImpl(tfidf, dict)

    def get_quantile(self, quantile_val):
        if self.sketch_type == 'numeric' or self.sketch_type == 'date':
            if self.quantile_accumulator is None:
                self._create_stats()
                self.quantile_accumulator = self._create_quantile_accumulator()
            return self.quantile_accumulator.ppf(quantile_val)
        raise ValueError('get_quantile only available for numeric or date types')

    def frequency_count(self, element):
        if self.frequency_sketch is None:
            self.frequency_sketch = self._create_frequency_sketch()
        return self.frequency_sketch.get(element, 0)

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
