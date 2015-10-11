"""
This module provides an implementation of Sketch using pySpark RDDs.
"""

import inspect
import math
import datetime
from sys import stderr
from collections import Counter
import copy


from xframes.dsq import QuantileAccumulator
from xframes.frequent import FreqSketch
from xframes import util
import xframes

__all__ = ['Sketch']


def is_missing(x):
    if x is None:
        return True
    if isinstance(x, float) and math.isnan(x):
        return True
    return False


class SketchImpl(object):

    entry_trace = False
    exit_trace = False

    def __init__(self):
        import xframes.util as util
        self._entry()
        self._rdd = None
        self.defined = None
        self.dtype = None
        self.sketch_type = None
        self.count = 0
        self.stats = None
        self.min_val = util.nan
        self.max_val = util.nan
        self.mean_val = 0
        self.sum_val = 0
        self.variance_val = 0.0
        self.stdev_val = 0.0
        self.avg_len = None
        self.num_undefined_val = None
        self.num_unique_val = None
        self.quantile_accumulator = None
        self.frequency_sketch = None
        self.tfidf = None
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

        defined = xa.to_rdd().filter(lambda x: not is_missing(x))
        defined.cache()
        self.dtype = xa.dtype()
        self.count = defined.count()
        self.sketch_type = 'numeric' if self.dtype in util.numeric_types else 'non-numeric'

        # compute others later if needed
        self._rdd = xa.to_rdd()
        self.defined = defined

        self._exit()

    def _create_stats(self):
        # calculate some basic statistics
        if self.stats is None:
            stats = self.defined.stats()
            self.min_val = stats.min()
            self.max_val = stats.max()
            self.mean_val = stats.mean()
            self.sum_val = stats.sum()
            self.variance_val = stats.variance()
            self.stdev_val = stats.stdev()
            self.stats = stats

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
            self._create_stats()
            return self.max_val
        raise ValueError('max only available for numeric types')

    def min(self):
        if self.sketch_type == 'numeric':
            self._create_stats()
            return self.min_val
        raise ValueError('min only available for numeric types')

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

    def tf_idf_rdd(self):
        """ Returns an RDD of td-idf dicts, one for each document. """
        import xframes
        def normalize_doc(doc):
            if doc is None:
                return []
            if type(doc) != str:
                print >>stderr, 'document should be str -- is {}: {}'.format(type(doc), doc)
                return []
            return doc.strip().lower().split()
        if self.dtype is str:
            docs = self._rdd.map(normalize_doc)
        else:
            docs = self._rdd.map(lambda doc: doc or [])
        docs.cache()

        def build_tf(doc):
            """ Build term Frequency for a document (cell)"""
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
            return count1

        # create idf counts per document
        counts = docs.aggregate(term_count, seq_op, comb_op)

        doc_count = float(docs.count())
        # add 1.0 to denominator, as suggested by wiki article cited above
        idf = {term: math.log(doc_count / (count + 1.0))
               for term, count in counts.iteritems()}

        def build_tfidf(tf):
            return {term: tf_count * idf[term] for term, tf_count in tf.iteritems()}
        return tf.map(build_tfidf)

    def tf_idf(self):
        if self.tfidf is None:
            self.tfidf = self.tf_idf_rdd()
        return xframes.xarray_impl.XArrayImpl(self.tfidf, dict)

    def tf_idf_col(self):
        if self.tfidf is None:
            self.tfidf = self.tf_idf_rdd()

        def combine_tfrdf(map1, map2):
            map = copy.copy(map1)
            for k, v in map2.iteritems():
                if k not in map:
                    map[k] = v
                elif map[k] < v:
                    map[k] = v
            return map
        return self.tfidf.reduce(combine_tfrdf)

    def get_quantile(self, quantile_val):
        if self.sketch_type == 'numeric':
            if self.quantile_accumulator is None:
                self._create_stats()
                self.quantile_accumulator = self._create_quantile_accumulator()
            return self.quantile_accumulator.ppf(quantile_val)
        raise ValueError('get_quantile only available for numeric types')

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
