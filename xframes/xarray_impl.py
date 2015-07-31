"""
This module provides an implementation of XArray using pySpark RDDs.
"""
import math
import inspect
import array
import os
import pickle
import ast
import csv
import copy
import StringIO
import random
from sys import stderr


from xframes.xobject_impl import XObjectImpl
from xframes.spark_context import spark_context
from xframes.util import infer_type_of_list, cache, uncache
from xframes.util import delete_file_or_dir, infer_type, infer_types
from xframes.util import is_missing
from xframes.util import distribute_seed
from xframes.xrdd import XRdd
import xframes


class ReverseCmp(object):
    """ Reverse comparison.

    Wraps a comparable class so that comparisons are reversed.
    """
    def __init__(self, obj):
        self.obj = obj

    def __lt__(self, other):
        return self.obj > other.obj

    def __gt__(self, other):
        return self.obj < other.obj

    def __eq__(self, other):
        return self.obj == other.obj

    def __le__(self, other):
        return self.obj >= other.obj

    def __ge__(self, other):
        return self.obj <= other.obj

    def __ne__(self, other):
        return self.obj != other.obj


class XArrayImpl(XObjectImpl):
    # What is missing:
    # sum over arrays
    # datetime functions
    # text processing functions
    # array.array and numpy.array values
    entry_trace = False
    exit_trace = False
    perf_count = None
    
    def __init__(self, rdd=None, elem_type=None):
        # The RDD holds all the data for the XArray.
        # The rows must be of a single type.
        # Types permitted include int, long, float, string, list, and dict.
        # We record the element type here.
        self._entry(elem_type)
        super(XArrayImpl, self).__init__(rdd)
        self.elem_type = elem_type
        self.materialized = False
        self.iter_pos = 0
        self._exit()

    def _rv(self, rdd, typ=None):
        """
        Return a new XArrayImpl containing the rdd and element type
        """
        return XArrayImpl(rdd, typ or self.elem_type)

    @staticmethod
    def _rv_frame(rdd, col_names=None, types=None):
        """
        Return a new XFrameImpl containing the rdd, column names, and element types
        """
        return xframes.xframe_impl.XFrameImpl(rdd, col_names, types)

    def _replace(self, rdd, dtype):
        self._replace_rdd(rdd)
        self.elem_type = dtype
        self.materialized = False

    def _count(self):
        count = self._rdd.count()     # action
        self.materialized = True
        return count

    @staticmethod
    def _entry(*args):
        if not XArrayImpl.entry_trace and not XArrayImpl.perf_count: return
        stack = inspect.stack()
        caller = stack[1]
        if XArrayImpl.entry_trace:
            print >>stderr, 'Enter xArray', caller[3], args
        if XArrayImpl.perf_count is not None:
            my_fun = caller[3]
            if my_fun not in XArrayImpl.perf_count:
                XArrayImpl.perf_count[my_fun] = 0
            XArrayImpl.perf_count[my_fun] += 1

    @staticmethod
    def _exit():
        if XArrayImpl.exit_trace:
            print >>stderr, 'Exit xArray', inspect.stack()[1][3]
        pass

    def rdd(self):
        return self._rdd

    @staticmethod
    def create_sequential_xarray(size, start, reverse):
        """
        Create RDD with sequential integer values of given size and starting pos.
        """
        if not reverse:
            stop = start + size
            step = 1
        else:
            stop = start - size
            step = -1
        sc = spark_context()
        rdd = XRdd(sc.parallelize(range(start, stop, step)))
        return XArrayImpl(rdd, int)

    # Load
    @classmethod
    def load_from_iterable(cls, values, dtype, ignore_cast_failure):
        """
        Load RDD from values given by iterable.

        Note
        ----
        Values must not only be iterable, but also it must support len and __getitem__
        
        Modifies the existing RDD: does not return a new XArray.
        """
        cls._entry(values, dtype, ignore_cast_failure)
        dtype = dtype or None
        sc = spark_context()
        try:
            if len(values) == 0:
                cls._exit()
                return XArrayImpl(XRdd(sc.parallelize([])), dtype)
                dtype = dtype or infer_type_of_list(values[0:100])
        except TypeError:
            # get here if values does not support len or __getitem
            pass

        if dtype is None:
            # try iterating and see if we get something
            cpy = copy.copy(values)
            for val in cpy:
                dtype = infer_type_of_list([val])
                break

        if dtype is None:
            raise TypeError('Cannot determine types.')

        def do_cast(x, dtype, ignore_cast_failure):
            if is_missing(x): return x
            if type(x) == dtype:
                return x
            try:
                return dtype(x)
            except (ValueError, TypeError):
                # TODO: this does not seem to cach as it should
                return None if ignore_cast_failure else ValueError

        raw_rdd = XRdd(sc.parallelize(values))
        rdd = raw_rdd.map(lambda x: do_cast(x, dtype, ignore_cast_failure))
        if not ignore_cast_failure:
            errs = len(rdd.filter(lambda x: x is ValueError).take(1)) == 1
            if errs: raise ValueError

        cls._exit()
        return cls(rdd, dtype)

    @classmethod
    def load_from_const(cls, value, size):
        """
        Load RDD from const value.
        """
        cls._entry(value, size)
        values = [value for _ in xrange(0, size)]
        sc = spark_context()
        cls._exit()
        return cls(XRdd(sc.parallelize(values)), type(value))

    @classmethod
    def load_autodetect(cls, path, dtype):
        """
        Load from the given path.
        
        This can be anything that spark will read from: local file or HDFS file.
        It can also be a directory, and spark will read and concatenate them all.
        """
        # Read the file as string
        # Examine the first 100 lines, and cast if necessary to int or float
        cls._entry(path, dtype)
        # If the path is a directory, then look for sarray-data file in the directory.
        # If the path is a file, look for that file
        # Use type inference to determine the element type.
        # Passed-in dtype is always str and is ignored.
        sc = spark_context()
        if os.path.isdir(path):
            res = XRdd(sc.pickleFile(path))
            metadata_path = os.path.join(path, '_metadata')
            with open(metadata_path) as f:
                dtype = pickle.load(f)
        else:
            res = XRdd(sc.textFile(path, use_unicode=False))
            dtype = infer_type(res)

        if dtype != str:
            if dtype in (list, dict):
                res = res.map(lambda x: ast.literal_eval(x))
            else:
                res = res.map(lambda x: dtype(x))
        cls._exit()
        return cls(res, dtype)

    def get_content_identifier(self):
        """
        Returns the unique identifier of the content that backs the XArray
        """
        self._entry()
        self._exit()
        return self._rdd.name()

    # Save
    def save(self, path):
        """
        Saves the RDD to file in pickled form.
        """
        self._entry(path)
        # this only works for local files
        delete_file_or_dir(path)
        try:
            self._rdd.saveAsPickleFile(path)          # action ?
        except:
            # TODO distinguish between filesystem errors and pickle errors
            raise TypeError('The XArray save failed.')
        metadata = self.elem_type
        metadata_path = os.path.join(path, '_metadata')
        with open(metadata_path, 'w') as md:
            # TODO detect filesystem errors
            pickle.dump(metadata, md)
        self._exit()

    def save_as_text(self, path):
        """
        Saves the RDD to file as text.
        """
        self._entry(path)
        # this only works for local files
        delete_file_or_dir(path)
        try:
            self._rdd.saveAsTextFile(path)           # action ?
        except:
            # TODO distinguish between filesystem errors and pickle errors
            raise TypeError('The XArray save failed.')
        metadata = self.elem_type
        metadata_path = os.path.join(path, 'metadata')
        with open(metadata_path, 'w') as md:
            # TODO detect filesystem errors
            pickle.dump(metadata, md)
        self._exit()

    def save_as_csv(self, path, **params):
        """
        Saves the RDD to file as text.
        """
        self._entry(path)

        def to_csv(row, **params):
            sio = StringIO.StringIO()
            writer = csv.writer(sio, **params)
            try:
                writer.writerow([row], **params)
                ret = sio.getvalue()
                return ret
            except IOError:
                return ''

        delete_file_or_dir(path)
        with open(path, 'w') as f:
            self.begin_iterator()
            elems_at_a_time = 10000
            ret = self.iterator_get_next(elems_at_a_time)
            while True:
                for row in ret:
                    line = to_csv(row, **params)
                    f.write(line)
                if len(ret) == elems_at_a_time:
                    ret = self.iterator_get_next(elems_at_a_time)
                else:
                    break
        self._exit()

    def to_rdd(self, number_of_partitions=None):
        """
        Returns the internal rdd.
        """
        self._entry(number_of_partitions)
        if number_of_partitions:
            self._replace_rdd(self._rdd.repartition(number_of_partitions))
        res = self._rdd.RDD()
        self._exit()
        return res

    @classmethod
    def from_rdd(cls, rdd, dtype):
        return cls(rdd, dtype)

    # Array Information
    def size(self):
        """
        Returns the number of rows in the RDD.
        """
        self._entry()
        count = self._count()             # action
        self._exit()
        return count

    def dtype(self):
        """
        Returns the type of the RDD elements.
        """
        self._entry()
        self._exit()
        return self.elem_type

    # Get Data
    def head(self, n):
        self._entry(n)
        pairs = self._rdd.zipWithIndex()
        filtered_pairs = pairs.filter(lambda x: x[1] < n)
        res = filtered_pairs.keys()
        self._exit()
        return self._rv(res)

    def head_as_list(self, n):
        self._entry(n)

        lst = self._rdd.take(n)     # action
        self._exit()
        return lst

    def tail(self, n):
        self._entry(n)
        pairs = self._rdd.zipWithIndex()
        cache(pairs)
        start = pairs.count() - n
        filtered_pairs = pairs.filter(lambda x: x[1] >= start)
        uncache(pairs)
        res = filtered_pairs.keys()
        self._exit()
        return self._rv(res)

    def topk_index(self, topk, reverse):
        """
        Create an RDD indicating which elements are in the top k.

        Entries are '1' if the corresponding element in the current RDD is a
        part of the top k elements, and '0' if that corresponding element is
        not. 
        """
        self._entry(topk, reverse)
        if type(topk) is not int:
            raise TypeError("'Topk_index' -- topk must be integer ({})".format(topk))

        if topk == 0:
            res = self._rdd.map(lambda x: 0)
        else:
            pairs = self._rdd.zipWithIndex()
            # we are going to use this twice
            cache(pairs)
            # takeOrdered always sorts ascending
            # topk needs to sort descending if reverse is False, ascending if True
            if reverse:
                order_fn = lambda x: x
            else:
                order_fn = lambda x: ReverseCmp(x)
            top_pairs = pairs.takeOrdered(topk, lambda x: order_fn(x[0]))
            top_ranks = [x[1] for x in top_pairs]
            res = pairs.map(lambda x: x[1] in top_ranks)
            uncache(pairs)
        self._exit()
        return self._rv(res)

    # Materialization
    def materialize(self):
        """
        For an RDD that is lazily evaluated, force the persistence of the
        RDD, committing all lazy evaluated operations.
        """
        self._entry()
        self._count()       # action
        self._exit()

    def is_materialized(self):
        """
        Returns whether or not the RDD has been materialized.
        """
        self._entry()
        self._exit()
        return self.materialized

    # Iteration
    def begin_iterator(self):
        """ Resets the iterator. """
        self._entry()
        self._exit()
        self.iter_pos = 0

    def iterator_get_next(self, elems_at_a_time):
        """ Gets a group of elements for the iterator. """

        self._entry(elems_at_a_time)
        buf_rdd = self._rdd.zipWithIndex()
        low = self.iter_pos
        high = self.iter_pos + elems_at_a_time
        filtered_rdd = buf_rdd.filter(lambda row: row[1] >= low < high)
        trimmed_rdd = filtered_rdd.map(lambda row: row[0])
        iter_buf = trimmed_rdd.collect()
        self.iter_pos += elems_at_a_time
        self._exit()
        return iter_buf

    # Operate on Vectors
    def vector_operator(self, other, op):
        """
        Performs an element-wise operation on the two RDDs.
        """
        self._entry(other, op)
        res_type = self.elem_type
        pairs = self._rdd.zip(other.rdd())
        if op == '+':
            res = pairs.map(lambda x: x[0] + x[1])
        elif op == '-':
            res = pairs.map(lambda x: x[0] - x[1])
        elif op == '*':
            res = pairs.map(lambda x: x[0] * x[1])
        elif op == '/':
            res = pairs.map(lambda x: x[0] / x[1])
        elif op == '<':
            res = pairs.map(lambda x: x[0] < x[1])
            res_type = int
        elif op == '>':
            res = pairs.map(lambda x: x[0] > x[1])
            res_type = int
        elif op == '<=':
            res = pairs.map(lambda x: x[0] <= x[1])
            res_type = int
        elif op == '>=':
            res = pairs.map(lambda x: x[0] >= x[1])
            res_type = int
        elif op == '==':
            res = pairs.map(lambda x: x[0] == x[1])
            res_type = int
        elif op == '!=':
            res = pairs.map(lambda x: x[0] != x[1])
            res_type = int
        elif op == '&':
            res = pairs.map(lambda x: x[0] and x[1])
            res_type = int
        elif op == '|':
            res = pairs.map(lambda x: x[0] or x[1])
            res_type = int
        else:
            raise NotImplementedError(op)
        self._exit()
        return self._rv(res, res_type)

    def left_scalar_operator(self, other, op):
        """
        Performs a scalar operation on the RDD.
        """
        self._entry(other, op)
        res_type = self.elem_type
        if op == '+':
            res = self._rdd.map(lambda x: x + other)
        elif op == '-':
            res = self._rdd.map(lambda x: x - other)
        elif op == '*':
            res = self._rdd.map(lambda x: x * other)
        elif op == '/':
            res = self._rdd.map(lambda x: x / other)
        elif op == '**':
            res = self._rdd.map(lambda x: x ** other)
        elif op == '<':
            res = self._rdd.map(lambda x: x < other)
            res_type = int
        elif op == '>':
            res = self._rdd.map(lambda x: x > other)
        elif op == '<=':
            res = self._rdd.map(lambda x: x <= other)
            res_type = int
        elif op == '>=':
            res = self._rdd.map(lambda x: x >= other)
            res_type = int
        elif op == '==':
            res = self._rdd.map(lambda x: x == other)
            res_type = int
        elif op == '!=':
            res = self._rdd.map(lambda x: x != other)
            res_type = int
        else:
            raise NotImplementedError(op)
        self._exit()
        return self._rv(res, res_type)

    def right_scalar_operator(self, other, op):
        """
        Performs a scalar operation on the RDD.
        """
        self._entry(other, op)
        if op == '+':
            res = self._rdd.map(lambda x: other + x)
        elif op == '-':
            res = self._rdd.map(lambda x: other - x)
        elif op == '*':
            res = self._rdd.map(lambda x: other * x)
        elif op == '/':
            res = self._rdd.map(lambda x: other / x)
        else:
            raise NotImplementedError(op)
        self._exit()
        return self._rv(res)

    def unary_operator(self, op):
        """
        Performs unary operations on an RDD.
        """
        self._entry(op)
        if op == '+':
            res = self._rdd
        elif op == '-':
            res = self._rdd.map(lambda x: -x)
        elif op == 'abs':
            res = self._rdd.map(lambda x: abs(x))
        else:
            raise NotImplementedError(op)
        self._exit()
        return self._rv(res)

    # Sample
    def sample(self, fraction, seed):
        """
        Create an RDD which contains a subsample of the current RDD.
        """
        self._entry(fraction, seed)
        res = self._rdd.sample(False, fraction, seed)
        self._exit()
        return self._rv(res)

    # Row Manipulation
    def logical_filter(self, other):
        """
        Selects all the elements in this RDD
        where the corresponding value in the other RDD is True.
        Self and other are of the same length.
        """
        self._entry(other)
        pairs = self._rdd.zip(other.rdd())
        res = pairs.filter(lambda p: p[1]).map(lambda p: p[0])
        self._exit()
        return self._rv(res)

    def copy_range(self, start, step, stop):
        """
        Returns an RDD consisting of the values between start and stop, counting by step.
        """
        self._entry(start, step, stop)

        def select_row(x, start, step, stop):
            if x < start or x >= stop: return False
            return (x - start) % step == 0
        pairs = self._rdd.zipWithIndex()
        res = pairs.filter(lambda x: select_row(x[1], start, step, stop)).map(lambda x: x[0])
        self._exit()
        return self._rv(res)

    def vector_slice(self, start, end):
        """
        This RDD contains lists or arrays.  Returns a new RDD
        containing each individual vector sliced, between start and end, exclusive.
        """
        self._entry(start, end)

        def slice_start(x, start):
            try:
                return x[start]
            except IndexError:
                return None

        def slice_start_end(x, start, end):
            l = len(x)
            if end > l:
                return None
            try:
                return x[start:end]
            except IndexError:
                return None
        if start == end - 1:
            res = self._rdd.map(lambda x: slice_start(x, start))
        else:
            res = self._rdd.map(lambda x: slice_start_end(x, start, end))
        self._exit()
        return self._rv(res)

    def filter(self, fn, skip_undefined, seed):
        """
        Filter this RDD by a function.

        Returns a new RDD filtered by this RDD.  If `fn` evaluates an
        element to True, this element is copied to the new RDD. If not, it
        isn't. Throws an exception if the return type of `fn` is not castable
        to a boolean value.
        """
        self._entry(fn, skip_undefined, seed)

        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)
        def apply_filter(x, fn, skip_undefined):
            if x is None and skip_undefined: return None
            return fn(x)
        res = self._rdd.filter(lambda x: apply_filter(x, fn, skip_undefined))
        self._exit()
        return self._rv(res)

    def drop_missing_values(self):
        """
        Create new RDD containing only the non-missing values of the
        RDD.

        A missing value shows up in an RDD as 'None'.  This will also drop
        float('nan').
        """
        self._entry()
        res = self._rdd.filter(lambda x: not is_missing(x))
        self._exit()
        return self._rv(res)

    def append(self, other):
        """
        Append an RDD to the current RDD. Creates a new RDD with the
        rows from both RDDs. Both RDDs must be of the same type.
        """
        self._entry(other)
        if self.elem_type != other.elem_type:
            raise TypeError('Types must match in append: {} {}'.format(self.elem_type, other.elem_type))
        res = self._rdd.union(other.rdd())
        self._exit()
        return self._rv(res)

    # Data Transformation
    def transform(self, fn, dtype, skip_undefined, seed):
        """
        Implementation of apply(fn, dtype, skip_undefined, seed).

        Transform each element of the RDD by a given function. The result
        RDD is of type ``dtype``. ``fn`` should be a function that returns
        exactly one value which can be cast into the type specified by
        ``dtype``. 
        """
        self._entry(fn, dtype, skip_undefined, seed)
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)

        def apply_and_cast(x, fn, dtype, skip_undefined):
            if is_missing(x) and skip_undefined: return None
            try:
                fnx = fn(x)
            except Exception:
                return ValueError('Error evaluating function on "{}"'.format(x))
            if is_missing(fnx) and skip_undefined: return None
            try:
                return dtype(fnx)
            except TypeError:
                return ValueError('Error converting "{}" to {}'.format(fnx, dtype))

        res = self._rdd.map(lambda x: apply_and_cast(x, fn, dtype, skip_undefined))
        # search for type error and throw exception
        errs = res.filter(lambda x: type(x) is ValueError).collect()
        if len(errs) > 0:
            raise ValueError('Transformation failures: ({}) {}'.format(len(errs), errs[0].args[0]))
        self._exit()
        return self._rv(res, dtype)

    def flat_map(self, fn, dtype, skip_undefined, seed):
        """
        Implementation of flat_map(fn, dtype, skip_undefined, seed).

        Transform each element of the RDD by a given function, then flatten. The result
        RDD is of type ``dtype``. ``fn`` should be a function that returns
        a list of values which can be cast into the type specified by
        ``dtype``. 
        """
        self._entry(fn, dtype, skip_undefined, seed)
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)

        def apply_and_cast(x, fn, dtype, skip_undefined):
            if is_missing(x) and skip_undefined: return []
            try:
                if skip_undefined:
                    return [dtype(item) for item in fn(x) if not is_missing(item)]
                return [dtype(item) for item in fn(x)]
            except TypeError:
                return TypeError

        res = self._rdd.flatMap(lambda x: apply_and_cast(x, fn, dtype, skip_undefined))

        # search for type error and throw exception
        try:
            errs = res.filter(lambda x: x is TypeError).take(1)
        except Exception:
            raise ValueError('type conversion failure')
        if len(errs) > 0:
            raise ValueError('type conversion failure')
        self._exit()
        return self._rv(res, dtype)

    def astype(self, dtype, undefined_on_failure):
        """
        Create a new rdd with all values cast to the given type. Throws an
        exception if the types are not castable to the given type.
        """
        # can parse strings that look like list and dict into corresponding types
        # does not do this now
        self._entry(dtype, undefined_on_failure)

        def convert_type(x, dtype):
            try:
                if dtype in (int, long, float):
                    x = 0 if x == '' else x
                    return dtype(x)
                elif dtype == str:
                    return dtype(x)
                elif dtype in (list, dict):
                    res = ast.literal_eval(x)
                    if isinstance(res, dtype): return res
                raise ValueError
            except ValueError as e:
                if undefined_on_failure:
                    return util.nan if dtype == float else None
                raise e
        res = self._rdd.map(lambda x: convert_type(x, dtype))
        self._exit()
        return self._rv(res, dtype)

    def clip(self, lower, upper):
        """
        Create a new XArray with each value clipped to be within the given
        bounds.
        This function can operate on XArrays of
        numeric type as well as array type, in which case each individual
        element in each array is clipped.
        """
        self._entry(lower, upper)

        def clip_val(x, lower, upper):
            if not math.isnan(lower) and x < lower: return lower
            elif not math.isnan(upper) and x > upper: return upper
            else: return x

        def clip_list(x, lower, upper):
            return [clip_val(v, lower, upper) for v in x]
        if self.elem_type == list:
            res = self._rdd.map(lambda x: clip_list(x, lower, upper))
        else:
            res = self._rdd.map(lambda x: clip_val(x, lower, upper))
        self._exit()
        return self._rv(res)

    def fill_missing_values(self, value):
        """
        Create new rdd with all missing values (None or NaN) filled in
        with the given value.

        The size of the new rdd will be the same as the original rdd. If
        the given value is not the same type as the values in the rdd,
        `fill_missing_values` will attempt to convert the value to the original rdd's
        type. If this fails, an error will be raised.
        """
        self._entry(value)
        res = self._rdd.map(lambda x: value if is_missing(x) else x)
        self._exit()
        return self._rv(res)

    def unpack(self, column_name_prefix, limit, column_types, na_value):
        """
        Convert an XArray of list, array, or dict type to an XFrame with
        multiple columns.

        `unpack` expands an XArray using the values of each list/array/dict as
        elements in a new XFrame of multiple columns. For example, an XArray of
        lists each of length 4 will be expanded into an XFrame of 4 columns,
        one for each list element. An XArray of lists/arrays of varying size
        will be expand to a number of columns equal to the longest list/array.
        An XArray of dictionaries will be expanded into as many columns as
        there are keys.

        When unpacking an XArray of list or array type, new columns are named:
        `column_name_prefix`.0, `column_name_prefix`.1, etc. If unpacking a
        column of dict type, unpacked columns are named
        `column_name_prefix`.key1, `column_name_prefix`.key2, etc.

        When unpacking an XArray of list or dictionary types, missing values in
        the original element remain as missing values in the resultant columns.
        If the `na_value` parameter is specified, all values equal to this
        given value are also replaced with missing values. In an XArray of
        array.array type, NaN is interpreted as a missing value.

        :py:func:`xframes.XFrame.pack_columns()` is the reverse effect of unpack
        """
        self._entry(column_name_prefix, limit, column_types, na_value)

        prefix = column_name_prefix if len(column_name_prefix) == 0 else column_name_prefix + '.'
        column_names = [prefix + str(elem) for elem in limit]
        n_cols = len(column_names)

        def select_elems(row, limit):
            if type(row) == list:
                return [row[elem] if 0 <= elem < len(row) else None for elem in limit]
            else:
                return [row[elem] if elem in row else None for elem in limit]

        def extend(row, n_cols, na_value):
            if na_value is not None:
                if isinstance(row, list):
                    row = [na_value if is_missing(x) else x for x in row]
                else:
                    row = {x: na_value if is_missing(row[x]) else row[x] for x in row}
            if len(row) < n_cols:
                if isinstance(row, list):
                    for i in range(len(row), n_cols):
                        row.append(na_value)
                else:
                    for i in limit:
                        if i not in row: row[i] = na_value
            return row

        def narrow(row, n_cols):
            if len(row) > n_cols:
                row = row[0:n_cols]
            return row

        def cast_elem(x, dtype):
            return None if x is None else dtype(x)

        def cast_row(row, column_types):
            return [cast_elem(x, dtype) for x, dtype in zip(row, column_types)]
        res = self._rdd.map(lambda row: extend(row, n_cols, na_value))
        res = res.map(lambda row: select_elems(row, limit))
        res = res.map(lambda row: narrow(row, n_cols))
        res = res.map(lambda row: cast_row(row, column_types))
        res = res.map(tuple)
        self._exit()
        return self._rv_frame(res, column_names, column_types)

    def sort(self, ascending):
        """
        Sort all values in this XArray.

        Sort only works for xarray of type str, int and float, otherwise TypeError
        will be raised. Creates a new, sorted XArray.
        """
        self._entry(ascending)
        res = self._rdd.sortBy((lambda x: x), ascending)
        self._exit()
        return self._rv(res)

    # Data Summarizers
    def unique(self):
        """
        Get all unique values in the current RDD.

        Raises a TypeError if the RDD is of dictionary type. Will not
        necessarily preserve the order of the given RDD in the new RDD.
        """
        self._entry()
        if self.elem_type == dict:
            raise TypeError('unique: type is dict')
        res = self._rdd.distinct()
        self._exit()
        return self._rv(res)

    def all(self):
        """
        Return True if every element of the rdd evaluates to True. For
        numeric RDDs zeros and missing values (``None``) evaluate to False,
        while all non-zero, non-missing values evaluate to True. For string,
        list, and dictionary RDDs, empty values (zero length strings, lists
        or dictionaries) or missing values (``None``) evaluate to False. All
        other values evaluate to True.

        Returns True on an empty RDD.
        """
        self._entry()
        count = self._count()     # action
        if count == 0:
            return True

        def do_all(val1, val2):
            if isinstance(val1, float) and math.isnan(val1): val1 = False
            if isinstance(val2, float) and math.isnan(val2): val2 = False
            if val1 is None: val1 = False
            if val2 is None: val2 = False
            return bool(val1 and val2)

        def combine(acc1, acc2): 
            return acc1 and acc2
        self._exit()
        return self._rdd.aggregate(True, do_all, combine)       # action

    def any(self):
        """
        Return True if any element of the RDD evaluates to True. For numeric
        RDDs any non-zero value evaluates to True. For string, list, and
        dictionary RDDs, any element of non-zero length evaluates to True.

        Returns False on an empty RDD.
        """
        self._entry()
        count = self._count()     # action
        if count == 0:
            return False

        def do_any(val1, val2):
            if isinstance(val1, float) and math.isnan(val1): val1 = False
            if isinstance(val2, float) and math.isnan(val2): val2 = False
            if val1 is None: val1 = False
            if val2 is None: val2 = False
            return bool(val1 or val2)

        def combine(acc1, acc2): 
            return acc1 or acc2
        self._exit()
        res = self._rdd.aggregate(False, do_any, combine)    # action
        return bool(res)

    def max(self):
        """
        Get maximum numeric value in the RDD.

        Returns None on an empty RDD. Raises an exception if called on an
        RDD with non-numeric type.
        """
        self._entry()
        count = self._count()     # action
        if count == 0:     # action
            return None
        if self.elem_type not in (int, long, float):
            raise TypeError('max: non numeric type')
        self._exit()
        return self._rdd.max()          # action

    def min(self):
        """
        Get minimum numeric value in the RDD.

        Returns None on an empty RDD. Raises an exception if called on an
        RDD with non-numeric type.
        """
        self._entry()
        count = self._count()     # action
        if count == 0:     # action
            return None
        if self.elem_type not in (int, long, float):
            raise TypeError('sum: non numeric type')
        self._exit()
        return self._rdd.min()      # action

    def sum(self):
        """
        Sum of all values in the RDD.

        Raises an exception if called on an RDD of strings, lists, or
        dictionaries. If the RDD contains numeric arrays (array.array) and
        all the arrays are the same length, the sum over all the arrays will be
        returned (NOT IMPLEMENTED). Returns None on an empty RDD. For large values, this may
        overflow without warning.
        """
        self._entry()
        count = self._count()     # action
        if count == 0:     # action
            return None

        if self.elem_type not in (int, long, float, bool):
            raise TypeError('sum: non numeric type')
        self._exit()
        return self._rdd.sum()    # action

    def mean(self):
        """
        Mean of all the values in the RDD.

        Returns None on an empty RDD. Raises an exception if called on an
        RDD with non-numeric type.
        """
        self._entry()
        count = self._count()     # action
        if count == 0:     # action
            return None
        if self.elem_type not in (int, long, float):
            raise TypeError('mean: non numeric type')
        self._exit()
        return self._rdd.mean()       # action

    def std(self, ddof):
        """
        Standard deviation of all the values in the rdd.

        Returns None on an empty RDD. Raises an exception if called on an
        RDD with non-numeric type or if `ddof` >= length of RDD.
        """
        self._entry(ddof)
        count = self._count()     # action
        if count == 0:      # action
            self._exit()
            return None
        if self.elem_type not in (int, long, float):
            raise TypeError('mean: non numeric type')
        if ddof < 0 or ddof > 1 or ddof >= count:
            raise ValueError('std: invalid ddof {}'.format(ddof))
        if ddof == 0:
            res = self._rdd.stdev()
        else:
            res = self._rdd.sampleStdev()       # action
        self._exit()
        return res

    def var(self, ddof):
        """
        Variance of all the values in the RDD.

        Returns None on an empty RDD. Raises an exception if called on an
        RDD with non-numeric type or if `ddof` >= length of XArray.
        """
        self._entry(ddof)
        count = self._count()     # action
        if count == 0:      # action
            return None
        if self.elem_type not in (int, long, float):
            raise TypeError('mean: non numeric type')
        if ddof < 0 or ddof > 1 or ddof >= count:
            raise ValueError('std: invalid ddof {}'.format(ddof))
        if ddof == 0:
            res = self._rdd.variance()     # action
        else:
            res = self._rdd.sampleVariance()     # action
        self._exit()
        return res

    def num_missing(self):
        """
        Number of missing elements in the RDD.
        """
        self._entry()
        self.materialized = True
        res = self._rdd.aggregate(0,             # action
                                  lambda acc, v: acc + 1 if is_missing(v) else acc,
                                  lambda acc1, acc2: acc1 + acc2)
        self._exit()
        return res

    def nnz(self):
        """
        Number of non-zero elements in the RDD.
        """
        self._entry()
        self.materialized = True

        def ne_zero(x):
            if is_missing(x): return False
            return x != 0
        res = self._rdd.aggregate(0,            # action
                                  lambda acc, v: acc + 1 if ne_zero(v) else acc,
                                  lambda acc1, acc2: acc1 + acc2)
        self._exit()
        return res

    def item_length(self):
        """
        Length of each element in the current XArray.

        Only works on XArrays of dict, array, or list type. If a given element
        is a missing value, then the output elements is also a missing value.
        This function is equivalent to the following but more performant:

            sa_item_len =  sa.apply(lambda x: len(x) if x is not None else None)
        """
        self._entry()
        if self.elem_type not in (str, dict, array, list):
            raise TypeError('item_length: must be string, dict, array, or list {}'.format(self.elem_type))
        res = self._rdd.map(lambda x: len(x) if x is not None else None, preservesPartitioning=True)
        self._exit()
        return self._rv(res, int)

    # Date/Time Handling
    def expand(self, column_name_prefix, limit, column_types):
        """
        Used only in split_datetime.
        """
        raise NotImplementedError('datetime_to_str')

    def datetime_to_str(self, str_format):
        """
        Create a new RDD with all the values cast to str. The string format is
        specified by the 'str_format' parameter.
        """
        self._entry(str_format)
        raise NotImplementedError('datetime_to_str')

    def str_to_datetime(self, str_format):
        """
        Create a new RDD with all the values cast to datetime. The string format is
        specified by the 'str_format' parameter.
        """
        self._entry(str_format)
        raise NotImplementedError('str_to_datetime')

    # Text Processing
    def count_bag_of_words(self, options):
        raise NotImplementedError('count_bag_of_words')

    def count_ngrams(self, n, options):
        self._entry(n, options)
        raise NotImplementedError('count_ngrams')

    def count_character_ngrams(self, n, options):
        self._entry(n, options)
        raise NotImplementedError('count_character_ngrams')

    def dict_trim_by_keys(self, keys, exclude):
        """
        Filter an RDD of dictionary type by the given keys. By default, all
        keys that are in the provided list in ``keys`` are *excluded* from the
        returned RDD.
        """
        self._entry(keys, exclude)
        if self.dtype() != dict:
            raise TypeError('type must be dict: {}'.format(self.dtype))

        def trim_keys(items):
            if exclude:
                return {k: items[k] for k in items if k not in keys}
            else:
                return {k: items[k] for k in items if k in keys}

        res = self._rdd.map(trim_keys)
        self._exit()
        return self._rv(res, dict)

    def dict_trim_by_values(self, lower, upper):
        """
        Filter dictionary values to a given range (inclusive). Trimming is only
        performed on values which can be compared to the bound values. Fails on
        RDDs whose data type is not ``dict``.
        """
        self._entry(lower, upper)
        if self.dtype() != dict:
            raise TypeError('type must be dict: {}'.format(self.dtype))

        def trim_values(items):
            return {k: items[k] for k in items if lower <= items[k] <= upper}
        res = self._rdd.map(trim_values)
        self._exit()
        return self._rv(res, dict)

    def dict_keys(self):
        """
        Create an RDD that contains all the keys from each dictionary
        element as a list. Fails on RDDs whose data type is not ``dict``.
        """
        self._entry()
        if self.dtype() != dict:
            raise TypeError('type must be dict: {}'.format(self.dtype))

        res = self._rdd.map(lambda item: item.keys())
        column_types = infer_types(res)
        column_names = ['X.{}'.format(i) for i in range(len(column_types))]
        self._exit()
        return self._rv_frame(res, column_names, column_types)

    def dict_values(self):
        """
        Create an RDD that contains all the values from each dictionary
        element as a list. Fails on RDDs whose data type is not ``dict``.
        """
        self._entry()
        if self.dtype() != dict:
            raise TypeError('type must be dict: {}'.format(self.dtype))

        res = self._rdd.map(lambda item: item.values())
        column_types = infer_types(res)
        column_names = ['X.{}'.format(i) for i in range(len(column_types))]
        self._exit()
        return self._rv_frame(res, column_names, column_types)

    def dict_has_any_keys(self, keys):
        """
        Create a boolean RDD by checking the keys of an RDD of
        dictionaries. An element of the output RDD is True if the
        corresponding input element's dictionary has any of the given keys.
        Fails on RDDs whose data type is not ``dict``.
        """
        self._entry(keys)
        if self.dtype() != dict:
            raise TypeError('type must be dict: {}'.format(self.dtype))

        def has_any_keys(items):
            return all(key in items for key in keys)
        res = self._rdd.map(has_any_keys)
        self._exit()
        return self._rv(res, bool)

    def dict_has_all_keys(self, keys):
        """
        Create a boolean RDD by checking the keys of an RDD of
        dictionaries. An element of the output RDD is True if the
        corresponding input element's dictionary has all of the given keys.
        Fails on RDDs whose data type is not ``dict``.
        """
        self._entry(keys)
        if self.dtype() != dict:
            raise TypeError('type must be dict: {}'.format(self.dtype))

        def has_all_keys(items):
            return all(key in items for key in keys)
        res = self._rdd.map(has_all_keys)
        self._exit()
        return self._rv(res, bool)
