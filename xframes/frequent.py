import sys
import random
import heapq

import numpy as np

#
#    This code is derived from that found on the webpage:
#         http://tech.shareaholic.com/2012/12/03/the-count-min-sketch-how-to-count-over-large-keyspaces-when-about-right-is-good-enough/
#    The algorithm described above is used in each partition, and then the results are merged using
#       the methods at the end.
#
BIG_PRIME = 9223372036854775783


def _random_parameter():
    return random.randrange(0, BIG_PRIME - 1)


def _generate_hash_function_params():
    """
    Returns parameters used in hash function.
    """
    a, b = _random_parameter(), _random_parameter()
    return a, b


class FreqSketch(object):
    def __init__(self, k, epsilon, delta, seed=None):
        """
        Setup a new count-min sketch with parameters num_levels, epsilon, and delta.

        The parameters epsilon and delta control the accuracy of the
        estimates of the sketch

        Cormode and Muthukrishnan prove that for an item i with count a_i, the
        estimate from the sketch a_i_hat will satisfy the relation

        a_hat_i <= a_i + epsilon * ||a||_1

        with probability at least 1 - delta, where a is the the vector of all
        all counts and ||x||_1 is the L1 norm of a vector x

        Parameters
        ----------
        k : int
            A positive integer that sets the number of top items counted
        epsilon : float
            A value in the unit interval that sets the precision of the sketch
        delta : float
            A value in the unit interval that sets the precision of the sketch

        Examples
        --------
        >>> s = FreqSketch(40, 0.005, 10**-7)

        Raises
        ------
        ValueError
            If if k is not a positive integer, or epsilon or delta are not in the unit interval.
        """

        seed = seed or 1729
        random.seed(seed)
        if k < 1:
            raise ValueError("k must be a positive integer")
        if epsilon <= 0 or epsilon >= 1:
            raise ValueError("epsilon must be between 0 and 1, exclusive")
        if delta <= 0 or delta >= 1:
            raise ValueError("delta must be between 0 and 1, exclusive")

        self.k = k
        self.width = int(np.ceil(np.exp(1) / epsilon))
        self.depth = int(np.ceil(np.log(1 / delta)))
        self.hash_function_params = [_generate_hash_function_params() for _ in range(self.depth)]
        self.count = np.zeros((self.depth, self.width), dtype='int32')
        self.heap = []
        self.top_k = {}       # top_k => [estimate, key] pairs

    def _check_compatibility(self, other):
        """Check if another FreqSketch is compatible with this one for merge.
        
        Compatibility requires same width, depth, and hash_functions.
        """
        if self.width != other.width or self.depth != other.depth:
            raise ValueError("FreqSketch dimensions do not match.")
        if self.hash_function_params != other.hash_function_params:
            raise ValueError("FreqSketch hashes do not match")

    def increment(self, key):
        """
        Increments the sketch for the item with name of key.

        Parameters
        ----------
        key : string
            The item to update the value of in the sketch

        Examples
        --------
        >>> s = FreqSketch(40, 0.005, 10**-7)
        >>> s.increment('http://www.cnn.com/')

        """
        self.update(key, 1)

    def hash_function(self, x, params):
        a, b = params
        res = (a * x + b) % BIG_PRIME % self.width
        return res

    def update(self, key, increment):
        """
        Updates the sketch for the item with name of key by the amount
        specified in increment

        Parameters
        ----------
        key : string
            The item to update the value of in the sketch
        increment : integer
            The amount to update the sketch by for the given key

        Examples
        --------
        >>> s = FreqSketch(40, 0.005, 10**-7)
        >>> s.update('http://www.cnn.com/', 1)

        """
        for row, hash_function_params in enumerate(self.hash_function_params):
            column = self.hash_function(abs(hash(key)), hash_function_params)
            self.count[row, column] += increment

        self.update_heap(key)

    def update_heap(self, key):
        """
        Updates the class's heap that keeps track of the top k items for a
        given key

        For the given key, it checks whether the key is present in the heap,
        updating accordingly if so, and adding it to the heap if it is
        absent

        Parameters
        ----------
        key : string
            The item to check against the heap

        """
        estimate = self.get(key)

        if not self.heap or estimate >= self.heap[0][0]:
            if key in self.top_k:
                old_pair = self.top_k.get(key)
                old_pair[0] = estimate
                heapq.heapify(self.heap)
            else:
                if len(self.top_k) < self.k:
                    heapq.heappush(self.heap, [estimate, key])
                    self.top_k[key] = [estimate, key]
                else:
                    new_pair = [estimate, key]
                    old_pair = heapq.heappushpop(self.heap, new_pair)
                    try: 
                        del self.top_k[old_pair[1]]
                    except:
                        pass
                    self.top_k[key] = new_pair

    def get(self, key):
        """
        Fetches the sketch estimate for the given key

        Parameters
        ----------
        key : string
            The item to produce an estimate for

        Returns
        -------
        estimate : int
            The best estimate of the count for the given key based on the
            sketch

        Examples
        --------
        >>> s = FreqSketch(40, 0.005, 10**-7)
        >>> s.update('http://www.cnn.com/', 1)
        >>> s.get('http://www.cnn.com/')
        1

        """
        value = sys.maxint
        for row, hash_function_params in enumerate(self.hash_function_params):
            column = self.hash_function(abs(hash(key)), hash_function_params)
            value = min(self.count[row, column], value)

        return value

    def frequent_items(self):
        """
        Returns the most frequent items.
        
        These are the frequent items from the heap.
        """
        return {key: self.get(key) for key in self.top_k}
            
    def iterate_values(self, value_iterator):
        """Makes FreqSketch usable with PySpark .mapPartitions().
        
        An RDD's .mapPartitions method takes a function that consumes an
        iterator of records and spits out an iterable for the next RDD
        downstream.
        """
        for value in value_iterator:
            self.increment(value)
        yield self

    @staticmethod
    def initial_accumulator_value():
        """
        Initial value used with aggregate function.
        """
        return dict()

    @staticmethod
    def merge_accumulator_value(acc, value):
        """
        Add an accumulator and a value, for use with aggregate.
        """
        acc2 = value.frequent_items()
        return FreqSketch.merge_accumulators(acc, acc2)

    @staticmethod
    def merge_accumulators(acc1, acc2):
        """
        Add two accumulators, for use with aggregate.

        Notes
        -----
        If the dictionaries contain keys of float('nan') then this will not work.
        To begin with, dictionaries treat different instances of float('nan') as distinct
        so there may be many keys that look alike.  Even if you use the singleton np.nan, spark
        serialization does not seem to preserve this property.

        It is recommended that the caller transform NaN into None before doing frequency
        counts to work around this limitation.
        """
        ans = dict(acc1)
        for key in acc2:
            ans[key] = ans[key] + acc2[key] if key in ans else acc2[key]
        return ans
