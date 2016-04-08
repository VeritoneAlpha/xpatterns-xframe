"""
This module provides an implementation of XArray using pySpark RDDs.
"""
import math
import array
import os
import pickle
import ast
import csv
import copy
import StringIO
import random
import datetime
from dateutil import parser
import logging

from xframes.lineage import Lineage
import xframes
from xframes.xobject_impl import XObjectImpl
from xframes.traced_object import TracedObject
from xframes.spark_context import CommonSparkContext
import xframes.fileio as fileio
from xframes.util import infer_type_of_list, cache, uncache
from xframes.util import infer_type, infer_types, is_numeric_type
from xframes.util import is_missing
from xframes.util import distribute_seed
from xframes.xrdd import XRdd


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


class ApplyError(object):
    def __init__(self, msg):
        self.msg = msg


# noinspection PyIncorrectDocstring
class XArrayImpl(XObjectImpl, TracedObject):
    # What is missing:
    # sum over arrays
    # datetime functions
    # text processing functions
    # array.array and numpy.array values

    def __init__(self, rdd=None, elem_type=None, lineage=None):
        # The RDD holds all the data for the XArray.
        # The rows must be of a single type.
        # Types permitted include int, long, float, string, list, and dict.
        # We record the element type here.
        self._entry(elem_type=elem_type)
        super(XArrayImpl, self).__init__(rdd)
        self.elem_type = elem_type
        self.lineage = lineage or Lineage.init_array_lineage()
        self.materialized = False
        self.iter_pos = 0

    def _rv(self, rdd, typ=None, lineage=None):
        """
        Return a new XArrayImpl containing the rdd,element type, and lineage.
        """
        return XArrayImpl(rdd, typ or self.elem_type, lineage or self.lineage)

    @staticmethod
    def _rv_frame(rdd, col_names=None, col_types=None, lineage=None):
        """
        Return a new XFrameImpl containing the rdd, column names, and element types
        """
        # noinspection PyUnresolvedReferences
        lineage = lineage or Lineage.init_frame_lineage(Lineage.RDD, col_names)
        return xframes.xframe_impl.XFrameImpl(rdd, col_names, col_types, lineage)

#    def _replace(self, rdd, dtype=None, lineage=None):
#        self._replace_rdd(rdd)
#        self.elem_type = dtype or self.elem_type
#        self.lineage = lineage or self.lineage
#        self.materialized = False

    def _count(self):
        count = self._rdd.count()
        self.materialized = True
        return count

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
        sc = CommonSparkContext.spark_context()
        rdd = XRdd(sc.parallelize(range(start, stop, step)))
        return XArrayImpl(rdd, int, Lineage.init_array_lineage(Lineage.RANGE))

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
        cls._entry(dtype=dtype, ignore_cast_failure=ignore_cast_failure)
        dtype = dtype or None
        sc = CommonSparkContext.spark_context()
        try:
            if len(values) == 0:
                dtype = dtype or infer_type_of_list(values[0:100])
                return XArrayImpl(XRdd(sc.parallelize([])), dtype)
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

        # noinspection PyShadowingNames
        def do_cast(x, dtype, ignore_cast_failure):
            if is_missing(x):
                return x
            if isinstance(x, str) and dtype is datetime.datetime:
                return parser.parse(x)
            if isinstance(x, dtype):
                return x
            try:
                return dtype(x)
            except (ValueError, TypeError):
                # TODO: this does not seem to catch as it should
                return None if ignore_cast_failure else ValueError

        raw_rdd = XRdd(sc.parallelize(values))
        rdd = raw_rdd.map(lambda x: do_cast(x, dtype, ignore_cast_failure))
        if not ignore_cast_failure:
            errs = len(rdd.filter(lambda x: x is ValueError).take(1)) == 1
            if errs:
                raise ValueError

        return cls(rdd, dtype, Lineage.init_array_lineage(Lineage.PROGRAM))

    @classmethod
    def load_from_const(cls, value, size):
        """
        Load RDD from const value.
        """
        cls._entry(value=value, size=size)
        values = [value for _ in xrange(0, size)]
        sc = CommonSparkContext.spark_context()
        return cls(XRdd(sc.parallelize(values)), type(value), Lineage.init_array_lineage(Lineage.CONST))

    @classmethod
    def load_autodetect(cls, path, dtype):
        """
        Load from the given path.

        This can be anything that spark will read from: local file or HDFS file.
        It can also be a directory, and spark will read and concatenate them all.
        """
        # Read the file as string
        # Examine the first 100 lines, and cast if necessary to int, float, or datetime
        cls._entry(path=path, dtype=dtype)
        # If the path is a directory, then look for sarray-data file in the directory.
        # If the path is a file, look for that file
        # Use type inference to determine the element type.
        # Passed-in dtype is always str and is ignored.
        lineage = Lineage.init_array_lineage(path)
        sc = CommonSparkContext.spark_context()
        if os.path.isdir(path):
            res = XRdd(sc.pickleFile(path))
            metadata_path = os.path.join(path, '_metadata')
            with fileio.open_file(metadata_path) as f:
                dtype = pickle.load(f)
            lineage_path = os.path.join(path, '_lineage')
            if fileio.exists(lineage_path):
                lineage = Lineage.load(lineage_path)
        else:
            res = XRdd(sc.textFile(path, use_unicode=False))
            dtype = infer_type(res)

        if dtype != str:
            if dtype in (list, dict):
                res = res.map(lambda x: ast.literal_eval(x))
            elif dtype is datetime.datetime:
                res = res.map(lambda x: parser.parse(x))
            else:
                res = res.map(lambda x: dtype(x))
        return cls(res, dtype, lineage)

    def get_content_identifier(self):
        """
        Returns the unique identifier of the content that backs the XArray
        """
        self._entry()
        return self._rdd.name()

    # Save
    def save(self, path):
        """
        Saves the RDD to file in pickled form.
        """
        self._entry(path=path)
        # this only works for local files
        fileio.delete(path)
        try:
            self._rdd.saveAsPickleFile(path)          # action ?
        except:
            # TODO distinguish between filesystem errors and pickle errors
            raise TypeError('The XArray save failed.')
        metadata = self.elem_type
        metadata_path = os.path.join(path, '_metadata')
        with fileio.open_file(metadata_path, 'w') as f:
            # TODO detect filesystem errors
            pickle.dump(metadata, f)

        lineage_path = os.path.join(path, '_lineage')
        self.lineage.save(lineage_path)

    def save_as_text(self, path):
        """
        Saves the RDD to file as text.
        """
        self._entry(path=path)
        fileio.delete(path)
        try:
            self._rdd.saveAsTextFile(path)
        except:
            # TODO distinguish between filesystem errors and pickle errors
            raise TypeError('The XArray save failed.')
        metadata = self.elem_type
        metadata_path = os.path.join(path, '_metadata')
        with fileio.open_file(metadata_path, 'w') as f:
            # TODO detect filesystem errors
            pickle.dump(metadata, f)

        lineage_path = os.path.join(path, '_lineage')
        self.lineage.save(lineage_path)

    def save_as_csv(self, path, **params):
        """
        Saves the RDD to file as text.
        """
        self._entry(path=path)

        # noinspection PyShadowingNames
        def to_csv(row, **params):
            sio = StringIO.StringIO()
            writer = csv.writer(sio, **params)
            try:
                writer.writerow([row], **params)
                ret = sio.getvalue()
                return ret
            except IOError:
                return ''

        fileio.delete(path)
        with fileio.open_file(path, 'w') as f:
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

    def to_rdd(self, number_of_partitions=None):
        """
        Returns the internal rdd.
        """
        self._entry(number_of_partitions=number_of_partitions)
        if number_of_partitions:
            self._replace_rdd(self._rdd.repartition(number_of_partitions))
        res = self._rdd.RDD()
        return res

    @classmethod
    def from_rdd(cls, rdd, dtype):
        return cls(rdd, dtype, Lineage.init_array_lineage(Lineage.RDD))

    # Array Information
    def size(self):
        """
        Returns the number of rows in the RDD.
        """
        self._entry()
        count = self._count()             # action
        return count

    def dtype(self):
        """
        Returns the type of the RDD elements.
        """
        self._entry()
        return self.elem_type

    def lineage_as_dict(self):
        """
        Returns the lineage as a dictionary
        """
        self._entry()
        return {'table': self.lineage.table_lineage,
                'column': self.lineage.column_lineage}

    # Get Data
    def head(self, n):
        self._entry(n=n)
        pairs = self._rdd.zipWithIndex()
        filtered_pairs = pairs.filter(lambda x: x[1] < n)
        res = filtered_pairs.keys()
        return self._rv(res)

    def head_as_list(self, n):
        self._entry(n=n)

        lst = self._rdd.take(n)     # action
        return lst

    def tail(self, n):
        self._entry(n=n)
        pairs = self._rdd.zipWithIndex()
        cache(pairs)
        start = pairs.count() - n
        filtered_pairs = pairs.filter(lambda x: x[1] >= start)
        uncache(pairs)
        res = filtered_pairs.keys()
        return self._rv(res)

    def topk_index(self, topk, reverse):
        """
        Create an RDD indicating which elements are in the top k.

        Entries are '1' if the corresponding element in the current RDD is a
        part of the top k elements, and '0' if that corresponding element is
        not. 
        """
        self._entry(topk=topk, reverse=reverse)
        if not isinstance(topk, int):
            raise TypeError("'Topk_index' -- topk must be integer ({})".format(topk))

        if topk == 0:
            res = self._rdd.map(lambda y: 0)
        else:
            pairs = self._rdd.zipWithIndex()
            # we are going to use this twice
            cache(pairs)
            # takeOrdered always sorts ascending
            # topk needs to sort descending if reverse is False, ascending if True
            if reverse:
                top_pairs = pairs.takeOrdered(topk, lambda x: x[0])
            else:
                top_pairs = pairs.takeOrdered(topk, lambda x: ReverseCmp(x[0]))
            top_ranks = [v[1] for v in top_pairs]
            res = pairs.map(lambda z: z[1] in top_ranks)
            uncache(pairs)
        return self._rv(res)

    # Materialization
    def materialize(self):
        """
        For an RDD that is lazily evaluated, force the persistence of the
        RDD, committing all lazy evaluated operations.
        """
        self._entry()
        self._count()

    def is_materialized(self):
        """
        Returns whether or not the RDD has been materialized.
        """
        self._entry()
        return self.materialized

    # Iteration
    def begin_iterator(self):
        """ Resets the iterator. """
        self._entry()
        self.iter_pos = 0

    def iterator_get_next(self, elems_at_a_time):
        """ Gets a group of elements for the iterator. """

        self._entry(elems_at_a_time=elems_at_a_time)
        buf_rdd = self._rdd.zipWithIndex()
        low = self.iter_pos
        high = self.iter_pos + elems_at_a_time
        filtered_rdd = buf_rdd.filter(lambda row: row[1] >= low < high)
        trimmed_rdd = filtered_rdd.map(lambda row: row[0])
        iter_buf = trimmed_rdd.collect()
        self.iter_pos += elems_at_a_time
        return iter_buf

    # Operate on Vectors
    def vector_operator(self, other, op):
        """
        Performs an element-wise operation on the two RDDs.
        """
        self._entry(op=op)
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
        lineage = self.lineage.merge(other.lineage)  # TODO lineage
        return self._rv(res, res_type, lineage)

    def left_scalar_operator(self, other, op):
        """
        Performs a scalar operation on the RDD.
        """
        self._entry(op=op)
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
        return self._rv(res, res_type)

    def right_scalar_operator(self, other, op):
        """
        Performs a scalar operation on the RDD.
        """
        self._entry(op=op)
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
        return self._rv(res)

    def unary_operator(self, op):
        """
        Performs unary operations on an RDD.
        """
        self._entry(op=op)
        if op == '+':
            res = self._rdd
        elif op == '-':
            res = self._rdd.map(lambda x: -x)
        elif op == 'abs':
            res = self._rdd.map(lambda x: abs(x))
        else:
            raise NotImplementedError(op)
        return self._rv(res)

    # Sample
    def sample(self, fraction, seed):
        """
        Create an RDD which contains a subsample of the current RDD.
        """
        self._entry(fraction=fraction, seed=seed)
        res = self._rdd.sample(False, fraction, seed)
        return self._rv(res)

    # Row Manipulation
    def logical_filter(self, other):
        """
        Selects all the elements in this RDD
        where the corresponding value in the other RDD is True.
        Self and other are of the same length.
        """
        self._entry()
        pairs = self._rdd.zip(other.rdd())
        res = pairs.filter(lambda p: p[1]).map(lambda p: p[0])
        return self._rv(res)

    def copy_range(self, start, step, stop):
        """
        Returns an RDD consisting of the values between start and stop, counting by step.
        """
        self._entry(start=start, step=step, stop=stop)

        # noinspection PyShadowingNames
        def select_row(x, start, step, stop):
            if x < start or x >= stop:
                return False
            return (x - start) % step == 0
        pairs = self._rdd.zipWithIndex()
        res = pairs.filter(lambda x: select_row(x[1], start, step, stop)).map(lambda x: x[0])
        return self._rv(res)  # TODO lineage

    def vector_slice(self, start, end):
        """
        This RDD contains lists or arrays.  Returns a new RDD
        containing each individual vector sliced, between start and end, exclusive.
        """
        self._entry(start=start, end=end)

        # noinspection PyShadowingNames
        def slice_start(x, start):
            try:
                return x[start]
            except IndexError:
                return None

        # noinspection PyShadowingNames
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
        return self._rv(res)

    def filter(self, fn, skip_undefined, seed):
        """
        Filter this RDD by a function.

        Returns a new RDD filtered by this RDD.  If `fn` evaluates an
        element to True, this element is copied to the new RDD. If not, it
        isn't. Throws an exception if the return type of `fn` is not castable
        to a boolean value.
        """
        self._entry(skip_undefined=skip_undefined, seed=seed)

        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)

        # noinspection PyShadowingNames
        def apply_filter(x, fn, skip_undefined):
            if x is None and skip_undefined:
                return None
            return fn(x)
        res = self._rdd.filter(lambda x: apply_filter(x, fn, skip_undefined))
        return self._rv(res)

    def count_missing_values(self):
        """
        Count missing values.

        A missing value shows up in an RDD as 'NaN' or 'None'.
        """
        self._entry()
        res = self._rdd.map(lambda x: 1 if is_missing(x) else 0)
        total = res.sum()
        return total

    def drop_missing_values(self):
        """
        Create new RDD containing only the non-missing values of the
        RDD.

        A missing value shows up in an RDD as 'None'.  This will also drop
        float('nan').
        """
        self._entry()
        res = self._rdd.filter(lambda x: not is_missing(x))
        return self._rv(res)

    def append(self, other):
        """
        Append an RDD to the current RDD. Creates a new RDD with the
        rows from both RDDs. Both RDDs must be of the same type.
        """
        self._entry()
        if self.elem_type != other.elem_type:
            raise TypeError('Types must match in append: {} {}'.format(self.elem_type, other.elem_type))
        res = self._rdd.union(other.rdd())
        lineage = self.lineage.merge(other.lineage)
        return self._rv(res, lineage=lineage)

    # Data Transformation
    def transform(self, fn, dtype, skip_undefined, seed):
        """
        Implementation of apply(fn, dtype, skip_undefined, seed).

        Transform each element of the RDD by a given function. The result
        RDD is of type ``dtype``. ``fn`` should be a function that returns
        exactly one value which can be cast into the type specified by
        ``dtype``. 
        """
        self._entry(dtype=dtype, skip_undefined=skip_undefined, seed=seed)
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)

        def array_typecode(val):
            if isinstance(val, int):
                return 'l'
            if isinstance(val, float):
                return 'd'
            return None

        # noinspection PyShadowingNames
        def apply_and_cast(x, fn, dtype, skip_undefined):
            if is_missing(x) and skip_undefined:
                return None
            # noinspection PyBroadException
            try:
                fnx = fn(x)
            except Exception:
                return ApplyError('Error evaluating function on "{}"'.format(x))
            if is_missing(fnx) and skip_undefined:
                return None
            if dtype is None:
                return fnx
            try:
                if dtype in [array.array]:
                    return array.array(array_typecode(fnx[0]), fnx)
                else:
                    return dtype(fnx)
            except TypeError:
                return ApplyError('Error converting "{}" to {}'.format(fnx, dtype))

        res = self._rdd.map(lambda x: apply_and_cast(x, fn, dtype, skip_undefined))
        # search for type error and raise exception
        # TODO this forces evaluatuion -- consider not doing it
        errs = res.filter(lambda x: type(x) is ApplyError).take(100)
        if len(errs) > 0:
            raise ValueError('Transformation failures: errs {}'.format(len(errs)))
        return self._rv(res, dtype)

    def flat_map(self, fn, dtype, skip_undefined, seed):
        """
        Implementation of flat_map(fn, dtype, skip_undefined, seed).

        Transform each element of the RDD by a given function, then flatten. The result
        RDD is of type ``dtype``. ``fn`` should be a function that returns
        a list of values which can be cast into the type specified by
        ``dtype``. 
        """
        self._entry(dtype=dtype, skip_undefined=skip_undefined, seed=seed)
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)

        # noinspection PyShadowingNames
        def apply_and_cast(x, fn, dtype, skip_undefined):
            if is_missing(x) and skip_undefined:
                return []
            try:
                # It is tempting to define the lambda function on the fly, but that
                #  leads to serilization difficulties.
                if skip_undefined:
                    if dtype is None:
                        return [item for item in fn(x) if not is_missing(item)]
                    return [dtype(item) for item in fn(x) if not is_missing(item)]
                if dtype is None:
                    return [item for item in fn(x)]
                return [dtype(item) for item in fn(x)]
            except TypeError:
                return [ApplyError('TypeError')]

        res = self._rdd.flatMap(lambda x: apply_and_cast(x, fn, dtype, skip_undefined))

        # search for type error and raise exception
        try:
            errs = res.filter(lambda x: type(x) is ApplyError).take(100)
        except Exception:
            raise ValueError('Type conversion failure: {}'.format(dtype))
        if len(errs) > 0:
            raise ValueError('Type conversion failures  errs: {}'.format(len(errs)))
        return self._rv(res, dtype)

    def astype(self, dtype, undefined_on_failure):
        """
        Create a new rdd with all values cast or converted to the given type. Raises an
        exception if the values cannot be converted to the given type.
        """
        # can parse strings that look like list and dict into corresponding types
        # does not do this now
        self._entry(dtype=dtype, undefined_on_failure=undefined_on_failure)

        # noinspection PyShadowingNames
        def convert_type(x, dtype):
            try:
                if dtype in (int, long, float):
                    x = 0 if x == '' else x
                    return dtype(x)
                if dtype is str:
                    return dtype(x)
                if dtype is datetime.datetime:
                    dt = parser.parse(x)
                    if isinstance(dt, datetime.datetime):
                        return dt
                    raise ValueError
                if dtype in (list, dict):
                    res = ast.literal_eval(x)
                    if isinstance(res, dtype):
                        return res
                    raise ValueError
                if dtype is array:
                    res = ast.literal_eval(x)
                    if isinstance(res, list) and len(res) > 0:
                        dtype = type(res[0])
                        if dtype in (int, long):
                            # TODO this could be too long and if so it will fail
                            return array.array('i', res)
                        if dtype is float:
                            return array.array('d', res)
                        if dtype is str:
                            return array.array('c', res)
                    raise ValueError('astype -- array not handled:{} type: {}'.format(res, dtype))
                raise ValueError('astype -- type not handled: {}'.format(dtype))
            except ValueError as e:
                if undefined_on_failure:
                    return None
                raise e
        res = self._rdd.map(lambda x: convert_type(x, dtype))
        return self._rv(res, dtype)

    def clip(self, lower, upper):
        """
        Create a new XArray with each value clipped to be within the given
        bounds.
        This function can operate on XArrays of
        numeric type as well as array type, in which case each individual
        element in each array is clipped.
        """
        self._entry(lower=lower, upper=upper)

        # noinspection PyShadowingNames
        def clip_val(x, lower, upper):
            if x is None:
                return None
            if lower is not None and not math.isnan(lower) and x < lower:
                return lower
            elif upper is not None and not math.isnan(upper) and x > upper:
                return upper
            else:
                return x

        # noinspection PyShadowingNames
        def clip_list(x, lower, upper):
            return [clip_val(v, lower, upper) for v in x]
        if self.elem_type == list:
            res = self._rdd.map(lambda x: clip_list(x, lower, upper))
        else:
            res = self._rdd.map(lambda x: clip_val(x, lower, upper))
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
        self._entry(value=value)
        res = self._rdd.map(lambda x: value if is_missing(x) else x)
        return self._rv(res)

    def unpack(self, column_name_prefix, limit, column_types, na_value):
        """
        Convert an XArray of list, tuple, array, or dict type to an XFrame with
        multiple columns.

        `unpack` expands an XArray using the values of each list/tuple/array/dict as
        elements in a new XFrame of multiple columns. For example, an XArray of
        lists each of length 4 will be expanded into an XFrame of 4 columns,
        one for each list element. An XArray of lists/arrays of varying size
        will be expand to a number of columns equal to the longest list/tuple/array.
        An XArray of dictionaries will be expanded into as many columns as
        there are keys.

        When unpacking an XArray of list, tuple, or array type, new columns are named:
        `column_name_prefix`.0, `column_name_prefix`.1, etc. If unpacking a
        column of dict type, unpacked columns are named
        `column_name_prefix`.key1, `column_name_prefix`.key2, etc.

        When unpacking an XArray of list, tuple or dictionary types, missing values in
        the original element remain as missing values in the resultant columns.
        If the `na_value` parameter is specified, all values equal to this
        given value are also replaced with missing values. In an XArray of
        array.array type, NaN is interpreted as a missing value.

        :py:func:`xframes.XFrame.pack_columns()` is the reverse effect of unpack
        """
        self._entry(column_name_prefix=column_name_prefix, limit=limit,
                    column_types=column_types, na_value=na_value)

        prefix = column_name_prefix if len(column_name_prefix) == 0 else column_name_prefix + '.'
        column_names = [prefix + str(elem) for elem in limit]
        n_cols = len(column_names)

        # noinspection PyShadowingNames
        def select_elems(row, limit):
            if isinstance(row, (list, tuple, array.array)):
                return [row[elem] if 0 <= elem < len(row) else None for elem in limit]
            else:
                return [row[elem] if elem in row else None for elem in limit]

        # noinspection PyShadowingNames
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
                        if i not in row:
                            row[i] = na_value
            return row

        # noinspection PyShadowingNames
        def narrow(row, n_cols):
            if len(row) > n_cols:
                row = row[0:n_cols]
            return row

        def cast_elem(x, dtype):
            return None if x is None else dtype(x)

        # noinspection PyShadowingNames
        def cast_row(row, column_types):
            return [cast_elem(x, dtype) for x, dtype in zip(row, column_types)]
        res = self._rdd.map(lambda row: extend(row, n_cols, na_value))
        res = res.map(lambda row: select_elems(row, limit))
        res = res.map(lambda row: narrow(row, n_cols))
        res = res.map(lambda row: cast_row(row, column_types))
        res = res.map(tuple)
        return self._rv_frame(res, column_names, column_types)  # TODO lineage

    def sort(self, ascending):
        """
        Sort all values in this XArray.

        Sort only works for xarray of type str, int and float, otherwise TypeError
        will be raised. Creates a new, sorted XArray.
        """
        self._entry(ascending=ascending)
        res = self._rdd.sortBy((lambda x: x), ascending)
        return self._rv(res)

    # Data Summarizers
    def unique(self):
        """
        Get all unique values in the current RDD.

        Raises a TypeError if the RDD is of dictionary type. Will not
        necessarily preserve the order of the given RDD in the new RDD.
        """
        self._entry()
        if self.elem_type is dict:
            raise TypeError('Unique: cannot take unique of dict type.')
        res = self._rdd.distinct()
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
            if isinstance(val1, float) and math.isnan(val1):
                val1 = False
            if isinstance(val2, float) and math.isnan(val2):
                val2 = False
            if val1 is None:
                val1 = False
            if val2 is None:
                val2 = False
            return bool(val1 and val2)

        def combine(acc1, acc2): 
            return acc1 and acc2
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
            if isinstance(val1, float) and math.isnan(val1):
                val1 = False
            if isinstance(val2, float) and math.isnan(val2):
                val2 = False
            if val1 is None:
                val1 = False
            if val2 is None:
                val2 = False
            return bool(val1 or val2)

        def combine(acc1, acc2): 
            return acc1 or acc2
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
        if not is_numeric_type(self.elem_type):
            raise TypeError('max: non numeric type')
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
        if not is_numeric_type(self.elem_type):
            raise TypeError('sum: non numeric type')
        return self._rdd.min()      # action

    def sum(self):
        """
        Sum of all values in the RDD.

        Raises an exception if called on an RDD of strings, lists, or
        dictionaries. If the RDD contains numeric lists or arrays (array.array) and
        all the arrays are the same length, the sum over all the arrays will be
        returned. Returns None on an empty RDD. For large values, this may
        overflow without warning.
        """
        self._entry()
        count = self._count()     # action
        if count == 0:     # action
            return None

        if is_numeric_type(self.elem_type):
            total = self._rdd.sum()    # action
        elif self.elem_type is array.array:
            def array_sum(x, y):
                if x.typecode != y.typecode:
                    logging.warn('Sum: arrays are not compatible')
                total = array.array(x.typecode)
                total.fromlist([a + b for a, b in zip(x, y)])
                return total
            total = self._rdd.reduce(array_sum)
        elif self.elem_type is list:
            def list_sum(x, y):
                return [a + b for a, b in zip(x, y)]
            total = self._rdd.reduce(list_sum)
        elif self.elem_type is dict:
            def dict_sum(x, y):
                return {k: x.get(k, 0) + y.get(k, 0) for k in set(x) & set(y)}
            total = self._rdd.reduce(dict_sum)

        else:
            raise TypeError('sum: non numeric type')
        return total

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
        if not is_numeric_type(self.elem_type):
            raise TypeError('mean: non numeric type')
        return self._rdd.mean()       # action

    def std(self, ddof):
        """
        Standard deviation of all the values in the rdd.

        Returns None on an empty RDD. Raises an exception if called on an
        RDD with non-numeric type or if `ddof` >= length of RDD.
        """
        self._entry(ddof=ddof)
        count = self._count()     # action
        if count == 0:      # action
            return None
        if not is_numeric_type(self.elem_type):
            raise TypeError('mean: non numeric type')
        if ddof < 0 or ddof > 1 or ddof >= count:
            raise ValueError('std: invalid ddof {}'.format(ddof))
        if ddof == 0:
            res = self._rdd.stdev()
        else:
            res = self._rdd.sampleStdev()       # action
        return res

    def var(self, ddof):
        """
        Variance of all the values in the RDD.

        Returns None on an empty RDD. Raises an exception if called on an
        RDD with non-numeric type or if `ddof` >= length of XArray.
        """
        self._entry(ddof=ddof)
        count = self._count()     # action
        if count == 0:      # action
            return None
        if not is_numeric_type(self.elem_type):
            raise TypeError('mean: non numeric type')
        if ddof < 0 or ddof > 1 or ddof >= count:
            raise ValueError('std: invalid ddof {}'.format(ddof))
        if ddof == 0:
            res = self._rdd.variance()     # action
        else:
            res = self._rdd.sampleVariance()     # action
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
        return res

    def nnz(self):
        """
        Number of non-zero elements in the RDD.
        """
        self._entry()
        self.materialized = True

        def ne_zero(x):
            if is_missing(x):
                return False
            return x != 0
        res = self._rdd.aggregate(0,            # action
                                  lambda acc, v: acc + 1 if ne_zero(v) else acc,
                                  lambda acc1, acc2: acc1 + acc2)
        return res

    def item_length(self):
        """
        Length of each element in the current XArray.

        Only works on XArrays of str, dict, array, or list type. If a given element
        is a missing value, then the output elements is also a missing value.
        This function is equivalent to the following but more performant:

            sa_item_len =  sa.apply(lambda x: len(x) if x is not None else None)
        """
        self._entry()
        if self.elem_type not in (str, dict, array, list):
            raise TypeError('item_length: must be string, dict, array, or list {}'.format(self.elem_type))
        res = self._rdd.map(lambda x: len(x) if x is not None else None, preserves_partitioning=True)
        return self._rv(res, int)

    # Date/Time Handling
    def split_datetime(self, column_name_prefix, limit, column_types):
        """
        Split a datatime value into separate columns.
        """
        # generate new column names
        if column_name_prefix != '':
            new_names = [column_name_prefix + '.' + name for name in limit]
        else:
            new_names = [name for name in limit]

        def expand_datetime_field(val, limit):
            if limit == 'year':
                return val.year
            if limit == 'month':
                return val.month
            if limit == 'day':
                return val.day
            if limit == 'hour':
                return val.hour
            if limit == 'minute':
                return val.minute
            if limit == 'second':
                return val.second
            return None

        def expand_datetime(val, limit):
            return tuple([expand_datetime_field(val, lim) for lim in limit])
        res = self._rdd.map(lambda x: expand_datetime(x, limit))
        return self._rv_frame(res.RDD(), new_names, column_types)

    def datetime_to_str(self, str_format):
        """
        Create a new RDD with all the values converted to str according to str_format.
        """
        self._entry(str_format=str_format)
        res = self._rdd.map(lambda x: x.strftime(str_format))
        return self._rv(res, str)

    def str_to_datetime(self, str_format):
        """
        Create a new RDD with all the values converted to datetime by datetime.strptime.
        If not str_format is given, use dateutil.parser.
        """
        self._entry(str_format=str_format)
        if str_format is None:
            res = self._rdd.map(lambda x: parser.parse(x))
        else:
            res = self._rdd.map(lambda x: datetime.datetime.strptime(x, str_format))
        return self._rv(res, datetime.datetime)

    # Text Processing
    def count_bag_of_words(self, options):
        raise NotImplementedError('count_bag_of_words')

    def count_ngrams(self, n, options):
        self._entry(n=n, options=options)
        raise NotImplementedError('count_ngrams')

    def count_character_ngrams(self, n, options):
        self._entry(n=n, options=options)
        raise NotImplementedError('count_character_ngrams')

    def dict_trim_by_keys(self, keys, exclude):
        """
        Filter an RDD of dictionary type by the given keys. By default, all
        keys that are in the provided list in ``keys`` are *excluded* from the
        returned RDD.
        """
        self._entry(keys=keys, exclude=exclude)
        if not issubclass(self.dtype(), dict):
            raise TypeError('XArray dtype must be dict: {}'.format(self.dtype))

        def trim_keys(items):
            if exclude:
                return {k: items[k] for k in items if k not in keys}
            else:
                return {k: items[k] for k in items if k in keys}

        res = self._rdd.map(trim_keys)
        return self._rv(res, dict)

    def dict_trim_by_values(self, lower, upper):
        """
        Filter dictionary values to a given range (inclusive). Trimming is only
        performed on values which can be compared to the bound values. Fails on
        RDDs whose data type is not ``dict``.
        """
        self._entry(lower=lower, upper=upper)
        if not issubclass(self.dtype(), dict):
            raise TypeError('XArray dtype must be dict: {}'.format(self.dtype))

        def trim_values(items):
            return {k: items[k] for k in items if lower <= items[k] <= upper}
        res = self._rdd.map(trim_values)
        return self._rv(res, dict)

    def dict_keys(self):
        """
        Create an RDD that contains all the keys from each dictionary
        element as a list. Fails on RDDs whose data type is not ``dict``.
        """
        self._entry()
        if not issubclass(self.dtype(), dict):
            raise TypeError('XArray dtype must be dict: {}'.format(self.dtype))

        res = self._rdd.map(lambda item: item.keys())
        column_types = infer_types(res)
        column_names = ['X.{}'.format(i) for i in range(len(column_types))]
        return self._rv_frame(res, column_names, column_types)

    def dict_values(self):
        """
        Create an RDD that contains all the values from each dictionary
        element as a list. Fails on RDDs whose data type is not ``dict``.
        """
        self._entry()
        if not issubclass(self.dtype(), dict):
            raise TypeError('XArray dtype must be dict: {}'.format(self.dtype))

        res = self._rdd.map(lambda item: item.values())
        column_types = infer_types(res)
        column_names = ['X.{}'.format(i) for i in range(len(column_types))]
        return self._rv_frame(res, column_names, column_types)

    def dict_has_any_keys(self, keys):
        """
        Create a boolean RDD by checking the keys of an RDD of
        dictionaries. An element of the output RDD is True if the
        corresponding input element's dictionary has any of the given keys.
        Fails on RDDs whose data type is not ``dict``.
        """
        self._entry(keys=keys)
        if not issubclass(self.dtype(), dict):
            raise TypeError('XArray dtype must be dict: {}'.format(self.dtype))

        def has_any_keys(items):
            return all(key in items for key in keys)
        res = self._rdd.map(has_any_keys)
        return self._rv(res, bool)

    def dict_has_all_keys(self, keys):
        """
        Create a boolean RDD by checking the keys of an RDD of
        dictionaries. An element of the output RDD is True if the
        corresponding input element's dictionary has all of the given keys.
        Fails on RDDs whose data type is not ``dict``.
        """
        self._entry(keys=keys)
        if not issubclass(self.dtype(), dict):
            raise TypeError('XArray dtype must be dict: {}'.format(self.dtype))

        def has_all_keys(items):
            return all(key in items for key in keys)
        res = self._rdd.map(has_all_keys)
        return self._rv(res, bool)
