"""
This module provides an implementation of XFrame using pySpark RDDs.
"""
import os
import json
import random
import array
import pickle
import csv
import StringIO
import ast
import shutil
import re
import copy
from sys import stderr

import numpy

from xframes.deps import HAS_PANDAS
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField

import xframes.fileio as fileio
from xframes.xobject_impl import XObjectImpl
from xframes.traced_object import TracedObject
from xframes.util import infer_type_of_rdd
from xframes.util import cache, uncache, persist
from xframes.util import is_missing, is_missing_or_empty
from xframes.util import to_ptype, to_schema_type, pytype_from_dtype, safe_cast_val
from xframes.util import distribute_seed
import xframes
from xframes.xrdd import XRdd
from xframes.cmp_rows import CmpRows
from xframes.aggregator_impl import aggregator_properties


def name_col(existing_col_names, proposed_name):
    """ Give a column a unique name.

    If the name already exists, create a unique name
    by appending a number.
    """
    # if there is a dup, add .<n> to make name unique
    candidate = proposed_name
    i = 1
    while candidate in existing_col_names:
        candidate = '{}.{}'.format(proposed_name, i)
        i += 1
    return candidate


# noinspection PyUnresolvedReferences,PyShadowingNames
class XFrameImpl(XObjectImpl, TracedObject):
    """ Implementation for XFrame. """

    def __init__(self, rdd=None, col_names=None, column_types=None):
        """ Instantiate a XFrame implementation.

        The RDD holds all the data for the XFrame.
        The rows in the rdd are stored as a list.
        Each column must be of uniform type.
        Types permitted include int, long, float, string, list, and dict.
        """
        self._entry(col_names=col_names, column_types=column_types)
        super(XFrameImpl, self).__init__(rdd)
        col_names = col_names or []
        column_types = column_types or []
        self.col_names = list(col_names)
        self.column_types = list(column_types)
        self.iter_pos = None
        self._num_rows = None

        self.materialized = False
        self._exit()

    def _rv(self, rdd, col_names=None, column_types=None):
        """
        Return a new XFrameImpl containing the RDD, column names, and column types.

        Column names and types default to the existing ones.
        This is typically used when a function returns a new XFrame.
        """
        # only use defaults if values are None, not []
        col_names = self.col_names if col_names is None else col_names
        column_types = self.column_types if column_types is None else column_types
        return XFrameImpl(rdd, col_names, column_types)

    def _reset(self):
        self._rdd = None
        self.col_names = []
        self.column_types = []
        self.materialized = False

    def _replace(self, rdd, col_names=None, column_types=None):
        """
        Replaces the existing RDD, column names, and column types with new values.

        Column names and types default to the existing ones.
        This is typically used when a function modifies the current XFrame.
        """
        self._replace_rdd(rdd)
        if col_names is not None:
            self.col_names = col_names
        if column_types is not None:
            self.column_types = column_types
        self._num_rows = None
        self.materialized = False
        return self

    def _count(self):
        persist(self._rdd)
        count = self._rdd.count()     # action
        self.materialized = True
        return count

    def rdd(self):
        return self._rdd

    @staticmethod
    def is_rdd(rdd):
        return XRdd.is_rdd(rdd)

    @staticmethod
    def is_dataframe(rdd):
        return XRdd.is_dataframe(rdd)

    # Load
    @classmethod
    def load_from_pandas_dataframe(cls, data):
        """
        Load from a pandas.DataFrame.
        """
        cls._entry()
        if not HAS_PANDAS:
            raise NotImplementedError('Pandas is required.')

        # build something we can parallelize
        # list of rows, each row is a tuple
        columns = data.columns
        dtypes = data.dtypes
        column_names = [col for col in columns]
        column_types = [type(numpy.zeros(1, dtype).tolist()[0]) for dtype in dtypes]

        res = []
        for row in data.iterrows():
            rowval = row[1]
            cols = [rowval[col] for col in columns]
            res.append(tuple(cols))
        sc = cls.spark_context()
        rdd = sc.parallelize(res)
        cls._exit()
        return XFrameImpl(rdd, column_names, column_types)

    @classmethod
    def load_from_xframe_index(cls, path):
        """
        Load from a saved xframe.
        """
        cls._entry(path=path)
        sc = cls.spark_context()
        res = sc.pickleFile(path)
        # read metadata from the same directory
        metadata_path = os.path.join(path, '_metadata')
        with fileio.open_file(metadata_path) as f:
            names, types = pickle.load(f)
        cls._exit()
        return cls(res, names, types)

    @classmethod
    def load_from_spark_dataframe(cls, rdd):
        """
        Load data from an existing spark dataframe.
        """
        cls._entry()
        schema = rdd.schema
        xf_names = [str(col.name) for col in schema.fields]
        xf_types = [to_ptype(col.dataType) for col in schema.fields]

        def row_to_tuple(row):
            return tuple([row[i] for i in range(len(row))])
        xf_rdd = rdd.map(row_to_tuple)
        cls._exit()
        return cls(xf_rdd, xf_names, xf_types)

    @classmethod
    def load_from_hive(cls, dataset):
        """
        Load data from a hive dataset.  This is normally given as database.table.
        """
        cls._entry()
        hc = cls.hive_context()
        # guard agains SQL injection attack
        if not re.match('[A-Za-z0-9.]+', dataset):
            raise ValueError('Hive dataset name must contain only alphnum and period.')
        hive_dataframe = hc.sql('SELECT * from {}'.format(dataset))
        xf_names = [str(col) for col in hive_dataframe.columns]
        type_names = [name_type[1] for name_type in hive_dataframe.dtypes]
        xf_types = [pytype_from_dtype(type_name) for type_name in type_names]

        def row_to_tuple(row):
            return tuple([row[i] for i in range(len(row))])
        rdd = hive_dataframe.map(row_to_tuple)
        return cls(rdd, xf_names, xf_types)

    @classmethod
    def load_from_rdd(cls, rdd, names=None, types=None):
        cls._entry(names=names, types=types)
        first_row = rdd.take(1)[0]
        if names is not None:
            if len(names) != len(first_row):
                raise ValueError('Length of names does not match RDD.')
        if types is not None:
            if len(types) != len(first_row):
                raise ValueError('Length of types does not match RDD.')
        names = names or ['X.{}'.format(i) for i in range(len(first_row))]
        types = types or [type(elem) for elem in first_row]
        # TODO sniff types using more of the rdd
        cls._exit()
        return cls(rdd, names, types)

    def load_from_csv(self, path, parsing_config, type_hints):
        """
        Load RDD from a csv file
        """
        self._entry(path=path, parsing_config=parsing_config, type_hints=type_hints)

        def get_config(name):
            return parsing_config[name] if name in parsing_config else None
        row_limit = get_config('row_limit')
        use_header = get_config('use_header')
        comment_char = get_config('comment_char')
        na_values = get_config('na_values')
        if not type(na_values) == list:
            na_values = [na_values]
        store_errors = get_config('store_errors')
        errors = {}

        sc = self.spark_context()
        raw = XRdd(sc.textFile(path))
        # parsing_config
        # 'row_limit': 100, 
        # 'use_header': True, 

        # 'double_quote': True, 
        # 'skip_initial_space': True, 
        # 'delimiter': '\n', 
        # 'quote_char': '"', 
        # 'escape_char': '\\'

        # 'comment_char': '', 
        # 'na_values': ['NA'], 
        # 'continue_on_failure': True, 
        # 'store_errors': False, 

        def apply_comment(line, comment_char):
            return line.partition(comment_char)[0].rstrip()
        if comment_char:
            raw = raw.map(lambda line: apply_comment(line, comment_char))

        def to_format_params(config):
            params = {}
            parm_map = {
                # parse_config: read_csv
                'delimiter': 'delimiter',
                'doublequote': 'doublequote',
                'escape_char': 'escapechar',
                'quote_char': 'quotechar',
                'skip_initial_space': 'skipinitialspace'
            }
            for pc, rc in parm_map.iteritems():
                if pc in config:
                    params[rc] = config[pc]
            return params

        params = to_format_params(parsing_config)
        if row_limit:
            if row_limit > 100:
                pairs = raw.zipWithIndex()
                cache(pairs)
                filtered_pairs = pairs.filter(lambda x: x[1] < row_limit)
                uncache(pairs)
                raw = filtered_pairs.keys()
            else:
                lines = raw.take(row_limit)
                raw = XRdd(sc.parallelize(lines))

        # TODO: Use per partition operations to create a reader once per partition
        # See p 106: Learning Spark
        # See mapPartitions
        def csv_to_array(line, params):
            line = line.replace('\r', '').replace('\n', '') + '\n'
            reader = csv.reader([line.encode('utf-8')], **params)
            try:
                res = reader.next()
                return res
            except IOError:
                print 'Malformed line:', line
                return ''
            except Exception as e:
                print 'Error', e
                return ''
        res = raw.map(lambda row: csv_to_array(row, params))

        # use first row, if available, to make column names
        first = res.first()
        if use_header:
            col_names = [item.strip() for item in first]
        else:
            col_names = ['X.{}'.format(i) for i in range(len(first))]
        col_count = len(col_names)

        # filter out all rows that match header
        # this could potentialy filter data rows, but we will ignore that for now
        if use_header:
            res = res.filter(lambda row: row != first)

        if store_errors:
            before_count = res.count()
            res = res.filter(lambda row: len(row) == col_count)
            after_count = res.count()
            filter_diff = before_count - after_count
            if filter_diff > 0:
                print >>stderr, '{} rows dropped because of incorrect column count'.format(filter_diff)
                errors['dropped'] = filter_diff
        else:
            # filter out rows with invalid col count
            res = res.filter(lambda row: len(row) == col_count)

        # Transform hints: __X{}__ ==> name.
        # If it is not of this form, leave it alone.
        def extract_index(s):
            if s.startswith('__X') and s.endswith('__'):
                index = s[3:-2]
                return int(index)
            return None

        def map_col(col, col_names):
            # Change key on hints from generated names __X<n>__
            #   into the actual column name
            index = extract_index(col)
            if index is None:
                return col
            return col_names[index]

        # get desired column types
        if '__all_columns__' in type_hints:
            # all cols are of the given type
            typ = type_hints['__all_columns__']
            types = [typ for _ in first]
        else:
            # all cols are str, except the one(s) mentioned
            types = [str for _ in first]
            # change generated hint key to actual column name
            type_hints = {map_col(col, col_names): typ for col, typ in type_hints.iteritems()}
            for col in col_names:
                if col in type_hints:
                    types[col_names.index(col)] = type_hints[col]
        column_types = types

        # apply na values to value
        def apply_na(row, na_values):
            return [None if val in na_values else val for val in row]
        res = res.map(lambda row: apply_na(row, na_values))

        # drop columns with empty header
        remove_cols = [col_index for col_index, col_name in enumerate(col_names) if len(col_name) == 0]

        def remove_columns(row):
            return [val for index, val in enumerate(row) if index not in remove_cols]
        res = res.map(remove_columns)
        col_names = remove_columns(col_names)

        # cast to desired type
        def cast_val(val, typ, name):
            if val is None:
                return None
            if len(val) == 0:
                if typ is int:
                    return 0
                if typ is float:
                    return 0.0
                if typ is str:
                    return ''
                if typ is dict:
                    return {}
                if typ is list:
                    return []
            try:
                if typ == dict or typ == list:
                    return ast.literal_eval(val)
                return typ(val)
            except ValueError:
                raise ValueError('Cast failed: ({}) {}  col: {}'.format(typ, val, name))

        # This is where the result is cast as a tuple
        def cast_row(row, types, names):
            return tuple([cast_val(val, typ, name) for val, typ, name in zip(row, types, names)])

        # TODO -- if cast fails, then don't cast and adjust type appropriately ??
        res = res.map(lambda row: cast_row(row, types, col_names))
        if row_limit is None:
            persist(res)
        self._replace(res, col_names, column_types)

        self._exit()
        # returns a dict of errors
        return errors

    # noinspection PyUnusedLocal
    def read_from_text(self, path, delimiter, nrows, verbose):
        """
        Load RDD from a text file
        """
        # TODO handle nrows, verbose
        self._entry(path=path)
        sc = self.spark_context()
        if delimiter is None:
            rdd = sc.textFile(path)
            res = rdd.map(lambda line: [line.encode('utf-8')])
        else:
            conf = {'textinputformat.record.delimiter': delimiter}
            rdd = sc.newAPIHadoopFile(path,
                                      "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                      "org.apache.hadoop.io.Text",
                                      "org.apache.hadoop.io.Text",
                                      conf=conf)

            def fixup_line(line):
                return str(line).replace('\n', ' ').strip()
            res = rdd.values().map(lambda line: (fixup_line(line), ))
        col_names = ['text']
        col_types = [str]
        self._exit()
        return self._replace(res, col_names, col_types)

    def load_from_parquet(self, path):
        """
        Load RDD from a parquet file
        """
        self._entry(path=path)
        sqlc = self.spark_sql_context()
        s_rdd = sqlc.parquetFile(path)
        schema = s_rdd.schema
        col_names = [str(col.name) for col in schema.fields]
        col_types = [to_ptype(col.dataType) for col in schema.fields]

        def row_to_tuple(row):
            return tuple([row[i] for i in range(len(row))])
        rdd = s_rdd.map(row_to_tuple)
        self._exit()
        return self._replace(rdd, col_names, col_types)

    # Save
    def save(self, path):
        """
        Save to a file.  

        Saved in an efficient internal format, intended for reading back into an RDD.
        """
        self._entry(path=path)
        fileio.delete(path)
        # save rdd
        self._rdd.saveAsPickleFile(path)        # action ?
        # save metadata in the same directory
        # TODO have to write this with HDFS lib if on hdfs
        metadata_path = os.path.join(path, '_metadata')
        metadata = [self.col_names, self.column_types]

        with fileio.open_file(metadata_path, 'w') as f:
            # TODO detect filesystem errors
            pickle.dump(metadata, f)
        self._exit()

    # noinspection PyArgumentList
    def save_as_csv(self, path, **params):
        """
        Save to a text file in csv format.
        """
        # Transform into RDD of csv-encoded lines, then write
        self._entry(path=path, **params)

        def to_csv(row, **params):
            sio = StringIO.StringIO()
            writer = csv.writer(sio, **params)
            try:
                writer.writerow(row)
                return sio.getvalue()
            except IOError:
                return ''

        # create heading with line terminator
        heading = to_csv(self.column_names(), **params)
        # create rows without the line terminator
        params['lineterminator'] = ''
        csv_data = self._rdd.map(lambda row: to_csv(row, **params))

        # Make a CSV file from an RDD

        # this will ensure that we get everything in one fie
        data = csv_data.repartition(1)

        # save the data in the part file
        temp_file_name = fileio.temp_file_name(path)
        data.saveAsTextFile(temp_file_name)
        in_path = os.path.join(temp_file_name, 'part-00000')
        fileio.delete(path)
        # copy the part file to the output file
        with fileio.open_file(path, 'w') as f:
            f.write(heading)
            with fileio.open_file(in_path) as rd:
                shutil.copyfileobj(rd, f)
        fileio.delete(temp_file_name)

        self._exit()

    def save_as_parquet(self, url, number_of_partitions):
        """
        Save to a parquet file.
        """
        self._entry(url=url, number_of_partitions=number_of_partitions)
        fileio.delete(url)
        dataframe = self.to_spark_dataframe(table_name=None,
                                            number_of_partitions=number_of_partitions)
        dataframe.saveAsParquetFile(url)
        self._exit()

    def to_rdd(self, number_of_partitions=None):
        """
        Returns the underlying RDD.

        Discards the column name and type information.
        """
        self._entry(number_of_partitions=number_of_partitions)
        res = self._rdd.repartition(number_of_partitions) if number_of_partitions is not None else self._rdd
        self._exit()
        return res.RDD()

    def to_spark_dataframe(self, table_name, number_of_partitions=None):
        """
        Adds column name and type information to the rdd and returns it.
        """
        # TODO: add the option to give schema type hints, or look further to find
        #   types for list and dict

        self._entry(table_name=table_name, number_of_partitions=number_of_partitions)
        if isinstance(self._rdd, DataFrame):
            res = self._rdd
        else:
            first_row = self.head_as_list(1)[0]
            fields = [StructField(name, to_schema_type(typ, first_row[i]), True)
                      for i, (name, typ) in enumerate(zip(self.col_names, self.column_types))]
            schema = StructType(fields)
            rdd = self._rdd.repartition(number_of_partitions) if number_of_partitions is not None else self._rdd
            sqlc = self.spark_sql_context()
            res = sqlc.createDataFrame(rdd.RDD(), schema)
            if table_name is not None:
                sqlc.registerDataFrameAsTable(res, table_name)
        self._exit()
        return res

    # Table Information
    # noinspection PyUnresolvedReferences
    def width(self):
        """
        Diagnostic function: count the number in the RDD tuple.
        """
        if self._rdd is None:
            return 0
        res = self._rdd.map(lambda row: len(row))
        return xframes.xarray_impl.XArrayImpl(res, int)

    def num_rows(self):
        """
        Returns the number of rows of the RDD.
        """
        self._entry()
        if self._rdd is None:
            return 0
        if self._num_rows is not None:
            return self._num_rows
        self._num_rows = self._count()      # action
        self._exit(num_rows=self._num_rows)
        return self._num_rows

    def num_columns(self):
        """
        Returns the number of columns in the XFrame.
        """
        self._entry()
        num_cols = len(self.col_names)
        self._exit(num_cols=num_cols)
        return num_cols

    def column_names(self):
        """
        Returns the column names in the XFrame.
        """
        self._entry()
        col_names = self.col_names
        self._exit(col_names=col_names)
        return col_names

    def dtype(self):
        """
        Returns the column data types in the XFrame.
        """
        self._entry()
        column_types = self.column_types
        self._exit(column_types=column_types)
        return column_types

    # Get Data
    def head(self, n):
        """
        Return the first n rows of the RDD as an XFrame.
        """
        # Returns an XFrame, otherwise we would use take(n)
        # TODO: this is really inefficient: it numbers the whole thing, and
        #  then filters most of it out.
        # Maybe it would be better to use take(n) then parallelize ?
        self._entry(n=n)
        if n <= 100:
            data = self._rdd.take(n)
            sc = self.spark_context()
            res = sc.parallelize(data)
            self._exit()
            return self._rv(res)
        pairs = self._rdd.zipWithIndex()
        cache(pairs)
        filtered_pairs = pairs.filter(lambda x: x[1] < n)
        uncache(pairs)
        res = filtered_pairs.keys()
        self.materialized = True
        self._exit()
        return self._rv(res)

    def head_as_list(self, n):
        # Used in xframe when doing dry runs to determine type
        self._entry(n=n)
        lst = self._rdd.take(n)      # action
        self._exit()
        return lst

    def tail(self, n):
        """
        Return the last n rows of the RDD as an XFrame.
        """
        self._entry(n=n)
        pairs = self._rdd.zipWithIndex()
        cache(pairs)
        start = pairs.count() - n
        filtered_pairs = pairs.filter(lambda x: x[1] >= start)
        uncache(pairs)
        res = filtered_pairs.map(lambda x: x[0])
        self._exit()
        return self._rv(res)

    # Sampling
    def sample(self, fraction, seed):
        """
        Sample the current RDDs rows as an XFrame.
        """
        self._entry(fraction=fraction, seed=seed)
        res = self._rdd.sample(False, fraction, seed)
        self._exit()
        return self._rv(res)

    def random_split(self, fraction, seed):
        """
        Randomly split the rows of an XFrame into two XFrames. The first XFrame
        contains *M* rows, sampled uniformly (without replacement) from the
        original XFrame. *M* is approximately the fraction times the original
        number of rows. The second XFrameD contains the remaining rows of the
        original XFrame.
        """
        # There is random split in the scala RDD interface, but not in pySpark.
        # Assign random number to each row and filter the two sets.
        self._entry(fraction=fraction, seed=seed)
        distribute_seed(self._rdd, seed)
        rng = random.Random(seed)
        rand_col = self._rdd.map(lambda row: rng.uniform(0.0, 1.0))
        labeled_rdd = self._rdd.zip(rand_col)
#        cache(labeled_rdd)
        rdd1 = labeled_rdd.filter(lambda row: row[1] < fraction).keys()
        rdd2 = labeled_rdd.filter(lambda row: row[1] >= fraction).keys()
        uncache(labeled_rdd)
        self._exit()
        return self._rv(rdd1), self._rv(rdd2)

    # Materialization
    def materialize(self):
        """
        For an RDD that is lazily evaluated, force the persistence of the
        RDD, committing all lazy evaluated operations.
        """
        self._entry()
        self._count()
        self._exit()

    def is_materialized(self):
        """
        Returns whether or not the RDD has been materialized.
        """
        self._entry()
        materialized = self.materialized
        self._exit(materialized=materialized)
        return materialized

    def has_size(self):
        """
        Returns whether or not the size of the XFrame is known.
        """
        self._entry()
        materialized = self.materialized
        self._exit(materialized=materialized)
        return materialized

    # Column Manipulation
    def select_column(self, column_name):
        """
        Get the array RDD that corresponds with
        the given column_name as an XArray.
        """
        self._entry(column_name=column_name)
        if column_name not in self.col_names:
            raise ValueError("Column name does not exist: '{}'.".format(column_name))

        col = self.col_names.index(column_name)
        res = self._rdd.map(lambda row: row[col])
        col_type = self.column_types[col]
        self._exit(col_type=col_type)
        return xframes.xarray_impl.XArrayImpl(res, col_type)

    def select_columns(self, keylist):
        """
        Creates RDD composed only of the columns referred to in the given list of
        keys, as an XFrame.
        """
        self._entry(keylist=keylist)

        def get_columns(row, cols):
            return tuple([row[col] for col in cols])
        cols = [self.col_names.index(key) for key in keylist]
        names = [self.col_names[col] for col in cols]
        types = [self.column_types[col] for col in cols]
        res = self._rdd.map(lambda row: get_columns(row, cols))
        self._exit(names=names, types=types)
        return self._rv(res, names, types)

    def copy(self):
        """
        Creates a copy of the XFrameImpl.

        The underlying RDD is immutale, so we just need to copy the metadata.
        """
        self._entry()
        self._exit()
        return self._rv(self._rdd)

    def add_column(self, col, name):
        """
        Create a new xFrame with one additional column.

        The number of elements in the data given
        must match the length of every other column of the XFrame. If no
        name is given, a default name is chosen.
        """
        self._entry(name=name)
        col_index = len(self.col_names)
        if name == '':
            name = 'X{}'.format(col_index)
        if name in self.col_names:
            raise ValueError("Column name already exists: '{}'.".format(name))
        col_names = copy.copy(self.col_names)
        col_names.append(name)
        col_types = copy.copy(self.column_types)
        col_types.append(col.elem_type)
        # zip the data into the rdd, then shift into the tuple
        if self._rdd is None:
            res = col.rdd().map(lambda x: (x,))
        else:
            res = self._rdd.zip(col.rdd())

            def move_inside(old_val, new_elem):
                return tuple(old_val + (new_elem, ))
            res = res.map(lambda pair: move_inside(pair[0], pair[1]))
        self._exit()
        return self._rv(res, col_names, col_types)

    def add_column_in_place(self, data, name):
        """
        Add a column to this XFrame.

        The number of elements in the data given
        must match the length of every other column of the XFrame. If no
        name is given, a default name is chosen.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry(name=name)
        col_index = len(self.col_names)
        if name == '':
            name = 'X{}'.format(col_index)
        if name in self.col_names:
            raise ValueError("Column name already exists: '{}'.".format(name))
        self.col_names.append(name)
        self.column_types.append(data.elem_type)
        # zip the data into the rdd, then shift into the tuple
        if self._rdd is None:
            res = data.rdd().map(lambda x: (x, ))
        else:
            res = self._rdd.zip(data.rdd())

            def move_inside(old_val, new_elem):
                return tuple(old_val + (new_elem, ))
            res = res.map(lambda pair: move_inside(pair[0], pair[1]))
        self._exit()
        return self._replace(res)

    def add_columns_array(self, cols, namelist):
        """
        Adds multiple columns to this XFrame.

        The number of elements in all
        columns must match the length of every other column of the RDDs.
        Each column added is an XArray.

        This operation returns a new XFrame.
        """
        self._entry(namelist=namelist)
        names = self.col_names + namelist
        types = self.column_types + [col._impl.elem_type for col in cols]
        rdd = self._rdd
        for col in cols:
            rdd = rdd.zip(col._impl.rdd())

            def move_inside(old_val, new_elem):
                return tuple(old_val + (new_elem, ))
            rdd = rdd.map(lambda pair: move_inside(pair[0], pair[1]))
        self._exit(names=names, types=types)
        return self._rv(rdd, names, types)

    def add_columns_array_in_place(self, cols, namelist):
        """
        Adds multiple columns to this XFrame. 

        The number of elements in all
        columns must match the length of every other column of the RDDs. 
        Each column added is an XArray.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry(namelist=namelist)
        names = self.col_names + namelist
        types = self.column_types + [col._impl.elem_type for col in cols]
        rdd = self._rdd
        for col in cols:
            rdd = rdd.zip(col._impl.rdd())

            def move_inside(old_val, new_elem):
                return tuple(old_val + (new_elem, ))
            rdd = rdd.map(lambda pair: move_inside(pair[0], pair[1]))
        self._exit(names=names, types=types)
        return self._replace(rdd, names, types)

    def add_columns_frame(self, other):
        """
        Adds multiple columns to this XFrame.

        The number of elements in all
        columns must match the length of every other column of the RDD.
        The columns to be added are in an XFrame.

        This operation returns a new XFrame.
        """
        self._entry()
        names = self.col_names + other._impl.col_names
        types = self.column_types + other._impl.column_types

        def merge(old_cols, new_cols):
            return old_cols + new_cols

        rdd = self._rdd.zip(other._impl.rdd())
        res = rdd.map(lambda pair: merge(pair[0], pair[1]))
        self._exit(names=names, types=types)
        return self._rv(res, names, types)

    def add_columns_frame_in_place(self, other):
        """
        Adds multiple columns to this XFrame. 

        The number of elements in all
        columns must match the length of every other column of the RDD. 
        The columns to be added are in an XFrame.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry()
        names = self.col_names + other._impl.col_names
        types = self.column_types + other._impl.column_types

        def merge(old_cols, new_cols):
            return old_cols + new_cols

        rdd = self._rdd.zip(other._impl.rdd())
        res = rdd.map(lambda pair: merge(pair[0], pair[1]))
        self._exit(names=names, types=types)
        return self._replace(res, names, types)

    def remove_column(self, name):
        """
        Remove a column from the RDD. 

        This operation creates a new xframe_impl and returns it.
        """
        self._entry(name=name)
        col = self.col_names.index(name)
        col_names = copy.copy(self.col_names)
        col_types = copy.copy(self.column_types)
        col_names.pop(col)
        col_types.pop(col)

        def pop_col(row, col):
            lst = list(row)
            lst.pop(col)
            return tuple(lst)
        res = self._rdd.map(lambda row: pop_col(row, col))
        self._exit()
        return self._rv(res, col_names, col_types)

    def remove_column_in_place(self, name):
        """
        Remove a column from the RDD.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry(name=name)
        col = self.col_names.index(name)
        self.col_names.pop(col)
        self.column_types.pop(col)

        def pop_col(row, col):
            lst = list(row)
            lst.pop(col)
            return tuple(lst)
        res = self._rdd.map(lambda row: pop_col(row, col))
        self._exit()
        return self._replace(res)

    def remove_columns(self, col_names):
        """
        Remove columns from the RDD. 

        This operation creates a new xframe_impl and returns it.
        """
        self._entry(col_names=col_names)
        cols = [self.col_names.index(name) for name in col_names]
        # pop from highets to lowest does not foul up indexes
        cols.sort(reverse=True)
        col_names = copy.copy(self.col_names)
        col_types = copy.copy(self.column_types)
        for col in cols:
            col_names.pop(col)
            col_types.pop(col)

        def pop_cols(row, cols):
            lst = list(row)
            for col in cols:
                lst.pop(col)
            return tuple(lst)
        res = self._rdd.map(lambda row: pop_cols(row, cols))
        self._exit()
        return self._rv(res, col_names, col_types)

    def swap_columns(self, column_1, column_2):
        """
        Creates an RDD with the given columns swapped.

        This operation
        """
        self._entry(column_1=column_1, column_2=column_2)

        def swap_list(lst, col1, col2):
            new_list = list(lst)
            new_list[col1] = lst[col2]
            new_list[col2] = lst[col1]
            return new_list

        def swap_cols(row, col1, col2):
            # is it OK to modify
            # the row ?
            try:
                lst = list(row)
                lst[col1], lst[col2] = lst[col2], lst[col1]
                return tuple(lst)
            except IndexError:
                print >>stderr, 'Swap index error', col1, col2, row, len(row)
        col1 = self.col_names.index(column_1)
        col2 = self.col_names.index(column_2)
        names = swap_list(self.col_names, col1, col2)
        types = swap_list(self.column_types, col1, col2)
        res = self._rdd.map(lambda row: swap_cols(row, col1, col2))
        self._exit(names=names, types=types)
        return self._rv(res, names, types)

    def reorder_columns(self, column_names):
        """
        Reorder columns of the RDD.
        """
        self._entry(column_names=column_names)

        def reorder_list(lst, column_indexes):
            return [lst[i] for i in column_indexes]

        def reorder_cols(row, column_indexes):
            return tuple([row[i] for i in column_indexes])

        column_indexes = [self.col_names.index(col) for col in column_names]

        names = reorder_list(self.col_names, column_indexes)
        types = reorder_list(self.column_types, column_indexes)
        res = self._rdd.map(lambda row: reorder_cols(row, column_indexes))
        self._exit(names=names, types=types)
        return self._rv(res, names, types)

    def set_column_name(self, old_name, new_name):
        """
        Rename the given column.

        No return value.
        """
        self._entry(old_name=old_name, new_name=new_name)
        col = self.col_names.index(old_name)
        self.col_names[col] = new_name
        self._exit()

    def replace_column_names(self, new_names):
        """
        Rename all the column names.

        No return value.
        """
        self._entry(new_names=new_names)
        self.col_names = new_names
        self._exit()

    # Iteration

    # Begin_iterator is called by a generator function, local to __iter__.
        # It calls iterator_get_next to fetch a group of items, then returns them one by one
        # using yield.  It keeps calling iterator_get_next as long as there are elements 
        # remaining.  It seems like only one iterator at a time can be operating because
        # the position is stored here.  Would it be better to let the caller handle the iter_pos?
        #
        # This uses zipWithIndex, which needs to process the whole data set.  
        # Is it better to use take or collect ?  OR are they effectively the same since zipWithIndex 
        # has just run?
    def begin_iterator(self):
        self._entry()
        self._exit()
        self.iter_pos = 0

    def iterator_get_next(self, elems_at_a_time):
        self._entry(elems_at_a_time=elems_at_a_time)
        low = self.iter_pos
        high = self.iter_pos + elems_at_a_time
        buf_rdd = self._rdd.zipWithIndex()
        filtered_rdd = buf_rdd.filter(lambda row: low <= row[1] < high)
        trimmed_rdd = filtered_rdd.keys()
        iter_buf = trimmed_rdd.collect()
        self.iter_pos += elems_at_a_time
        self._exit()
        return iter_buf

    def add_column_const_in_place(self, name, value):
        """
        Add a new column at the end of the RDD with a const value.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry(name=name, value=value)

        def add_col(row, value):
            row = list(row)
            row.append(value)
            return tuple(row)
        res = self._rdd.map(lambda row: add_col(row, value))

        self.col_names.append(name)
        col_type = type(value)
        self.column_types.append(col_type)
        self._exit()
        return self._replace(res)

    def replace_column_const_in_place(self, name, value):
        """
        Replace thge given column of the RDD with a const value.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry(name=name, value=value)
        col_num = self.col_names.index(name)

        def replace_col(row, col_num, value):
            row = list(row)
            row[col_num] = value
            return tuple(row)
        res = self._rdd.map(lambda row: replace_col(row, col_num, value))

        col_type = type(value)
        self.column_types[col_num] = col_type
        self._exit()
        return self._replace(res)

    def replace_single_column_in_place(self, col):
        """
        Replace the column in a single-column table with the given one.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry()
        res = col._impl.rdd().map(lambda item: (item, ))
        col_type = infer_type_of_rdd(col._impl.rdd())
        self.column_types[0] = col_type
        self._exit()
        return self._replace(res)

    def replace_selected_column(self, column_name, col):
        """
        Replace the given column  with the given one.

        This operation returns a new XFrame.
        """
        self._entry(column_name=column_name)
        rdd = self._rdd.zip(col._impl.rdd())
        col_num = self.col_names.index(column_name)

        def replace_col(row_col, col_num):
            row = list(row_col[0])
            col = row_col[1]
            row[col_num] = col
            return tuple(row)
        res = rdd.map(lambda row_col: replace_col(row_col, col_num))
        col_names = copy.copy(self.column_names())
        col_names[col_num] = column_name
        col_type = infer_type_of_rdd(col._impl.rdd())
        col_types = copy.copy(self.column_types)
        col_types[col_num] = col_type
        self._exit()
        return self._rv(res, col_names, col_types)

    def replace_selected_column_in_place(self, column_name, col):
        """
        Replace the given column  with the given one.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry(column_name=column_name)
        rdd = self._rdd.zip(col._impl.rdd())
        col_num = self.col_names.index(column_name)

        def replace_col(row_col, col_num):
            row = list(row_col[0])
            col = row_col[1]
            row[col_num] = col
            return tuple(row)
        res = rdd.map(lambda row_col: replace_col(row_col, col_num))
        col_type = infer_type_of_rdd(col._impl.rdd())
        self.column_types[col_num] = col_type
        self._exit()
        return self._replace(res)

    # Row Manipulation
    def flat_map(self, fn, column_names, column_types, seed):
        """
        Map each row of the RDD to multiple rows in a new RDD via a
        function.

        The input to `fn` is a dictionary of column/value pairs.
        The output of `fn` must have type List[List[...]].  Each inner list
        will be a single row in the new output, and the collection of these
        rows within the outer list make up the data for the output RDD.
        """
        self._entry(column_names=column_names, column_types=column_types, seed=seed)
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)
        names = self.col_names
        # fn needs the row as a dict
        res = self._rdd.flatMap(lambda row: fn(dict(zip(names, row))))
        res = res.map(tuple)
        self._exit(column_names=column_names, column_types=column_types)
        return self._rv(res, column_names, column_types)

    def logical_filter(self, other):
        """
        Where other is an array RDD of identical length as the current one,
        this returns a selection of a subset of rows in the current RDD
        where the corresponding row in the selector is non-zero.
        """
        self._entry()
        # zip restriction: data must match in length and partition structure

        pairs = self._rdd.zip(other.rdd())

        res = pairs.filter(lambda p: p[1]).map(lambda p: p[0])
        self._exit()
        return self._rv(res)

    def stack_list(self, column_name, new_column_names, new_column_types, drop_na):
        """
        Convert a "wide" list column of an XFrame to one or two "tall" columns by
        stacking all values.
        
        new_column_names and new_column_types are lists of 1 or 2 items
        """
        self._entry(column_name=column_name, new_column_names=new_column_names,
                    new_column_types=new_column_types, drop_na=drop_na)
        col_num = self.col_names.index(column_name)

        def subs_row(row, col, val):
            new_row = list(row)
            new_row[col] = val
            return tuple(new_row)

        def stack_row(row, col, drop_na):
            res = []
            for val in row[col]:
                if drop_na and is_missing_or_empty(val):
                    continue
                res.append(subs_row(row, col, val))
            if len(res) > 0 or drop_na:
                return res
            return [subs_row(row, col, None)]
        res = self._rdd.flatMap(lambda row: stack_row(row, col_num, drop_na))

        column_names = list(self.col_names)
        new_name = new_column_names[0]
        if new_name == '':
            new_name = name_col(column_names, 'X')
        column_names[col_num] = new_name
        column_types = list(self.column_types)
        column_types[col_num] = new_column_types[0]
        self._exit(column_names=column_names, column_types=column_types)
        return self._rv(res, column_names, column_types)

    def stack_dict(self, column_name, new_column_names, new_column_types, drop_na):
        """
        Convert a "wide" dict column of an XFrame to two "tall" columns by
        stacking all values.
        
        new_column_names and new_column_types are lists of 2 items
        """
        self._entry(column_name=column_name, new_column_names=new_column_names,
                    new_column_types=new_column_types, drop_na=drop_na)
        col_num = self.col_names.index(column_name)

        def subs_row(row, col, key, val):
            new_row = list(row)
            new_row[col] = key
            new_row.insert(col + 1, val)
            return tuple(new_row)

        def stack_row(row, col, drop_na):
            res = []
            for key, val in row[col].iteritems():
                if drop_na and is_missing_or_empty(val):
                    continue
                res.append(subs_row(row, col, key, val))
            if len(res) > 0 or drop_na:
                return res
            return [subs_row(row, col, None, None)]
        res = self._rdd.flatMap(lambda row: stack_row(row, col_num, drop_na))

        column_names = list(self.col_names)
        new_name_k = new_column_names[0]
        if new_name_k == '':
            new_name_k = name_col(column_names, 'K')
        column_names[col_num] = new_name_k
        new_name_v = new_column_names[1]
        if new_name_v == '':
            new_name_v = name_col(column_names, 'V')
        column_names.insert(col_num + 1, new_name_v)
        column_types = list(self.column_types)
        column_types[col_num] = new_column_types[0]
        column_types.insert(col_num + 1, new_column_types[1])
        self._exit(column_names=column_names, column_types=column_types)
        return self._rv(res, column_names, column_types)

    def append(self, other):
        """
        Add the rows of an RDD to the end of this RDD.

        Both RDDs must have the same set of columns with the same column
        names and column types.
        """
        self._entry()
        res = self._rdd.union(other.rdd())
        self._exit()
        return self._rv(res)

    def copy_range(self, start, step, stop):
        """
        Returns an RDD consisting of the values between start and stop, counting by step.
        """
        self._entry(start=start, step=step, stop=stop)

        def select_row(x, start, step, stop):
            if x < start or x >= stop:
                return False
            return (x - start) % step == 0
        pairs = self._rdd.zipWithIndex()
        res = pairs.filter(lambda x: select_row(x[1], start, step, stop)).map(lambda x: x[0])
        self._exit()
        return self._rv(res)

    def drop_missing_values(self, columns, all_behavior, split):
        """
        Remove missing values from an RDD. A missing value is either ``None``
        or ``NaN``.  If ``all_behavior`` is False, a row will be removed if any of the
        columns in the ``columns`` parameter contains at least one missing
        value.  If ``all_behavior`` is True, a row will be removed if all of the columns
        in the ``columns`` parameter are missing values.

        If the ``columns`` parameter is the empty list, 
        consider all columns when searching for missing values.
        """
        self._entry(columns=columns, all_behavior=all_behavior, split=split)

        def keep_row_all(row, cols):
            for col in cols:
                if not is_missing(row[col]):
                    return True
            return False

        def keep_row_any(row, cols):
            for col in cols:
                if is_missing(row[col]):
                    return False
            return True

        column_names = self.col_names if len(columns) == 0 else columns
        cols = [self.col_names.index(col) for col in column_names]
        f = keep_row_all if all_behavior else keep_row_any
        if not split:
            res = self._rdd.filter(lambda row: f(row, cols))
            self._exit()
            return self._rv(res)
        else:
            res1 = self._rdd.filter(lambda row: f(row, cols))
            res2 = self._rdd.filter(lambda row: not f(row, cols))
            self._exit()
            return self._rv(res1), self._rv(res2)

    def add_row_number(self, column_name, start):
        """
        Returns a new RDD with a new column that numbers each row
        sequentially. By default the count starts at 0, but this can be changed
        to a positive or negative number.  The new column will be named with
        the given column name.  
        Make sure the row number is the first column.
        """
        self._entry(column_name=column_name, start=start)

        def pull_up(pair, start):
            row = list(pair[0])
            row.insert(0, pair[1] + start)
            return tuple(row)
        names = list(self.col_names)
        names.insert(0, column_name)
        types = list(self.column_types)
        types.insert(0, int)
        res = self._rdd.zipWithIndex().map(lambda row: pull_up(row, start))
        self._exit(names=names, types=types)
        return self._rv(res, names, types)

    # Data Transformations Within Columns
    def pack_columns(self, columns, dict_keys, dtype, fill_na):
        """
        Pack two or more columns of the current XFrame into one single
        column.The result is a new XFrame with the unaffected columns from the
        original XFrame plus the newly created column.

        The list of columns that are packed is chosen through either the
        ``columns`` or ``column_prefix`` parameter. Only one of the parameters
        is allowed to be provided. ``columns`` explicitly specifies the list of
        columns to pack, while ``column_prefix`` specifies that all columns that
        have the given prefix are to be packed.

        The type of the resulting column is decided by the ``dtype`` parameter.
        Allowed values for ``dtype`` are dict, array.array and list:

         - *dict*: pack to a dictionary XArray where column name becomes
           dictionary key and column value becomes dictionary value

         - *array.array*: pack all values from the packing columns into an array

         - *list*: pack all values from the packing columns into a list.
        """
        self._entry(columns=columns, dict_keys=dict_keys, dtype=dtype, fill_na=fill_na)
        cols = [self.col_names.index(col) for col in columns]
        keys = self._rdd.map(lambda row: [row[col] for col in cols])

        def substitute_missing(v, fill_na):
            return fill_na if is_missing(v) and fill_na else v

        def pack_row_list(row, fill_na):
            return [substitute_missing(v, fill_na) for v in row]

        def pack_row_array(row, fill_na, typecode):
            lst = [substitute_missing(v, fill_na) for v in row]
            return array.array(typecode, lst)

        def pack_row_dict(row, dict_keys, fill_na):
            d = {}
            for val, key in zip(row, dict_keys):
                new_val = substitute_missing(val, fill_na)
                if new_val is not None:
                    d[key] = new_val
            return d

        if dtype == list:
            res = keys.map(lambda row: pack_row_list(row, fill_na))
        elif dtype == array.array:
            typecode = 'd'
            res = keys.map(lambda row: pack_row_array(row, fill_na, typecode))
        elif dtype == dict:
            res = keys.map(lambda row: pack_row_dict(row, dict_keys, fill_na))
        else:
            raise NotImplementedError
        self._exit(dtype=dtype)
        return xframes.xarray_impl.XArrayImpl(res, dtype)

    def transform(self, fn, dtype, seed):
        """
        Transform each row to an XArray according to a
        specified function. Returns a array RDD of ``dtype`` where each element
        in this array RDD is transformed by `fn(x)` where `x` is a single row in
        the xframe represented as a dictionary.  The ``fn`` should return
        exactly one value which is or can be cast into type ``dtype``. 
        """
        self._entry(dtype=dtype, seed=seed)
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)
        names = self.col_names
        # fn needs the row as a dict

        def transformer(row):
            row_as_dict = dict(zip(names, row))
            result = fn(row_as_dict)
            if type(result) != dtype:
                cast_result = safe_cast_val(result, dtype)
                return cast_result
            return result
        res = self._rdd.map(transformer)
        self._exit(dtype=dtype)
        return xframes.xarray_impl.XArrayImpl(res, dtype)

    def transform_col(self, col, fn, dtype, seed):
        """
        Transform a single column according to a specified function. 
        The remaining columns are not modified.
        The type of the transformed column becomes ``dtype``, with
        the new value being the result of `fn(x)` where `x` is a single row in
        the xframe represented as a dictionary.  The ``fn`` should return
        exactly one value which can be cast into type ``dtype``. If ``dtype`` is
        not specified, the first 100 rows of the XFrame are used to make a guess
        of the target data type.
        """
        self._entry(col=col, dtype=dtype, seed=seed)
        if col not in self.col_names:
            raise ValueError("Column name does not exist: '{}'.".format(col))
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)
        col_index = self.col_names.index(col)
        names = self.col_names

        def transformer(row):
            row_as_dict = dict(zip(names, row))
            result = fn(row_as_dict)
            if type(result) != dtype:
                result = dtype(result)
            lst = list(row)
            lst[col_index] = result
            return tuple(lst)

        res = self._rdd.map(transformer)
        new_col_types = list(self.column_types)
        new_col_types[col_index] = dtype
        self._exit(new_col_types=new_col_types)
        return self._rv(res, column_types=new_col_types)

    def transform_cols(self, cols, fn, dtypes, seed):
        """
        Transform multiple columns according to a specified function. 
        The remaining columns are not modified.
        The type of the transformed column types are given by  ``dtypes``, with
        the new values being the result of `fn(x)` where `x` is a single row in
        the xframe represented as a dictionary.  The ``fn`` should return
        a value for each element of cols, which can be cast into the corresponding ``dtype``. 
        If ``dtype`` is not specified, the first 100 rows of the XFrame are 
        used to make a guess of the target data types.
        """
        self._entry(cols=cols, dtypes=dtypes, seed=seed)
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)
        for col in cols:
            if col not in self.col_names:
                raise ValueError("Column name does not exist: '{}'.".format(col))
        col_indexes = [self.col_names.index(col) for col in cols]
        names = self.col_names

        def transformer(row):
            row_as_dict = dict(zip(names, row))
            result = fn(row_as_dict)
            lst = list(row)
            for dtype_index, col_index in enumerate(col_indexes):
                dtype = dtypes[dtype_index]
                result_item = result[dtype_index]
                if result_item is None:
                    lst[col_index] = None
                elif type(result_item) == dtype:
                    lst[col_index] = result_item
                else:
                    lst[col_index] = safe_cast_val(result_item, dtype)
            return tuple(lst)

        res = self._rdd.map(transformer)
        new_col_types = list(self.column_types)
        for dtype_index, col_index in enumerate(col_indexes):
            new_col_types[col_index] = dtypes[dtype_index]
        self._exit(new_col_types=new_col_types)
        return self._rv(res, column_types=new_col_types)

    def filter(self, values, column_name, exclude):
        """
        Perform simple filtering on a single column by values in a collection.
        For now, values is always a set.
        """
        col_index = self.col_names.index(column_name)

        def filter_fun(row):
            val = row[col_index]
            return val not in values if exclude else val in values

        res = self._rdd.filter(filter_fun)
        return self._rv(res)


    def groupby_aggregate(self, key_columns_array, group_columns, group_output_columns, group_ops):
        """
        Perform a group on the key_columns followed by aggregations on the
        columns listed in operations.

        Group_columns, group_output_columns and group_ops are all arrays of equal length
        """
        self._entry(key_columns_array=key_columns_array, group_columns=group_columns,
                    group_output_columns=group_output_columns, group_ops=group_ops)

        # make key column indexes
        key_cols = [self.col_names.index(col) for col in key_columns_array]

        # make group column indexes
        group_cols = [[self.col_names.index(col) if col != '' else None for col in cols]
                      for cols in group_columns]

        # look up operators
        # make new column names
        default_group_output_columns = [aggregator_properties[op].default_col_name
                                        for op in group_ops]
        group_output_columns = [col if col != '' else deflt
                                for col, deflt in zip(group_output_columns,
                                                      default_group_output_columns)]
        new_col_names = key_columns_array + group_output_columns
        # make sure col names are unique
        unique_col_names = []
        for col_name in new_col_names:
            unique_name = name_col(unique_col_names, col_name)
            unique_col_names.append(unique_name)
        new_col_names = unique_col_names

        def get_group_types(cols):
            return [self.column_types[col] if type(col) is int else None for col in cols]

        # make new column types
        new_col_types = [self.column_types[index] for index in key_cols]
        # get existing types of group columns
        group_types = [get_group_types(cols) for cols in group_cols]
        agg_types = [aggregator_properties[op].get_output_type(group_type)
                     for op, group_type in zip(group_ops, group_types)]

        new_col_types.extend(agg_types)

        # make RDD into K,V pairs where key incorporates the key column values
        def make_key(row, key_cols):
            key = [row[col] for col in key_cols]
            return json.dumps(key)
        keyed_rdd = self._rdd.map(lambda row: (make_key(row, key_cols), row))

        grouped = keyed_rdd.groupByKey()
        grouped = grouped.map(lambda pair: (json.loads(pair[0]), pair[1]))
        # (key, [row ...]) ...
        # run the aggregator on y: count --> len(y); sum --> sum(y), etc

        def build_aggregates(rows, aggregators, group_cols):
            # apply each of the aggregator functions and collect their results into a list
            return [aggregator(rows, cols)
                    for aggregator, cols in zip(aggregators, group_cols)]
        aggregators = [aggregator_properties[op].agg_function for op in group_ops]
        aggregates = grouped.map(lambda (x, y): (x, build_aggregates(y, aggregators, group_cols)))

        def concatenate(old_vals, new_vals):
            return old_vals + new_vals
        res = aggregates.map(lambda pair: concatenate(pair[0], pair[1]))
        res = res.map(tuple)
        persist(res)
        self._exit(new_col_names=new_col_names, new_col_types=new_col_types)
        return self._rv(res, new_col_names, new_col_types)

    def join(self, right, how, join_keys):
        """
        Merge two XFrames. Merges the current (left) XFrame with the given
        (right) XFrame using a SQL-style equi-join operation by columns.

        join_keys is a dict of left-right column names
        how = [left, right, outer, inner]
        """
        self._entry(how=how, join_keys=join_keys)
        # new columns are made up of:
        # 1) left columns
        # 2) right columns exculding join_keys.values()
        # Duplicate remaining right columns need to be renamed

        # make lists of left and right key indexes
        # these are the positions of the key columns in left and right
        # put the pieces together
        # one of the pairs may be None in all cases except inner
        if how == 'cartesian':
            # outer join is substantially different
            # so do it separately
            new_col_names = list(self.col_names)
            new_col_types = list(self.column_types)
            for col in right.col_names:
                new_col_names.append(name_col(new_col_names, col))
            for t in right.column_types:
                new_col_types.append(t)
            left_count = len(self.col_names)
            right_count = len(right.col_names)
            pairs = self._rdd.cartesian(right.rdd())

            def combine_results(left_row, right_row, left_count, right_count):
                if left_row is None:
                    left_row = tuple([None] * left_count)
                if right_row is None:
                    right_row = tuple([None] * right_count)
                return tuple(left_row + right_row)

            res = pairs.map(lambda row: combine_results(row[0], row[1],
                            left_count, right_count))
        else:
            # inner, left, right, full
            left_key_indexes = []
            right_key_indexes = []
            for left_key, right_key in join_keys.iteritems():
                left_index = self.col_names.index(left_key)
                left_key_indexes.append(left_index)
                right_index = right.col_names.index(right_key)
                right_key_indexes.append(right_index)
                right_key_indexes.sort(reverse=True)

            # make a list of the right column names and types
            right_col_names = list(right.col_names)
            right_col_types = list(right.column_types)
            for i in right_key_indexes:
                right_col_names.pop(i)
                right_col_types.pop(i)

            # make a list of the result column names and types
            # rename duplicate names
            new_col_names = list(self.col_names)
            new_col_types = list(self.column_types)
            for col in right_col_names:
                new_col_names.append(name_col(new_col_names, col))
            for t in right_col_types:
                new_col_types.append(t)
            left_count = len(self.col_names)
            right_count = len(right.col_names)

            # build a key from the column values
            # spark cannot handle tuples as keys, so make it a string using json
            def build_key(row, indexes):
                key = [row[i] for i in indexes]
                return json.dumps(key)

            if len(left_key_indexes) == 0 or len(right_key_indexes) == 0:
                raise ValueError("Empty join columns -- left: '{}' right: '{}'."
                                 .format(left_key_indexes, right_key_indexes))

            # add keys to left and right
            keyed_left = self._rdd.map(lambda row: (build_key(row, left_key_indexes), row))
            keyed_right = right.rdd().map(lambda row: (build_key(row, right_key_indexes), row))

            if how == 'inner':
                joined = keyed_left.join(keyed_right)
            elif how == 'left':
                joined = keyed_left.leftOuterJoin(keyed_right)
            elif how == 'right':
                joined = keyed_left.rightOuterJoin(keyed_right)
            elif how == 'full':
                joined = keyed_left.fullOuterJoin(keyed_right)
            else:
                raise ValueError("'How' argument is not 'inner', 'left', 'right', 'full' or 'cartesian'.")

            # throw away key in the joined table
            pairs = joined.values()

#            print 'left', keyed_left.collect()
#            print 'right', keyed_right.collect()
#            print 'pairs', pairs.collect()

            def combine_results(left_row, right_row, left_count, right_count):
                if left_row is None:
                    left_row = tuple([None] * left_count)
                if right_row is None:
                    right_row = tuple([None] * right_count)
                return (left_row, right_row)

            #res = pairs.map(lambda row: combine_results(row[0], row[1],
            #                left_count, right_count))

#            print 'res', res.collect()

            # remove redundant key fields from the right
            # take into account any missing any missing rows
            def fixup(left_row, right_row, left_count, right_count, left_key_indexes, right_key_indexes):
                left_list = list([None] * left_count) if left_row is None else list(left_row)
                right_list = list([None] * right_count) if right_row is None else list(right_row)
                for left_index, right_index in zip(left_key_indexes, right_key_indexes):
                    if left_list[left_index] is None:
                        left_list[left_index] = right_list[right_index]
                for i in right_key_indexes:
                    right_list.pop(i)
                return tuple(tuple(left_list) + tuple(right_list))

            res = pairs.map(lambda row: fixup(row[0], row[1],
                                              left_count, right_count,
                                              left_key_indexes, right_key_indexes))

        persist(res)

        self._exit(new_col_names=new_col_names, new_col_types=new_col_types)
        return self._rv(res, new_col_names, new_col_types)

    def unique(self):
        """
        Remove duplicate rows of the XFrame. Will not necessarily preserve the
        order of the given XFrame in the new XFrame.
        """

        self._entry()
        as_json = self._rdd.map(lambda row: json.dumps(row))
        unique_rows = as_json.distinct()
        res = unique_rows.map(lambda s: json.loads(s))
        self._exit()
        return self._rv(res)

    def sort(self, sort_column_names, sort_column_orders):
        """
        Sort current XFrame by the given columns, using the given sort order.
        Only columns that are type of str, int and float can be sorted.

        sort_column_orders is an array of boolean; True is ascending
        """
        self._entry(sort_column_names=sort_column_names, sort_column_orders=sort_column_orders)

        sort_column_indexes = [self.col_names.index(name) for name in sort_column_names]
        key_fn = lambda row: CmpRows(row, sort_column_indexes, sort_column_orders)

        res = self._rdd.sortBy(keyfunc=key_fn)
        self._exit()
        return self._rv(res)

    def sql(self, sql_statement, table_name):
        """
        Execute a spark-sql command against a XFrame
        """
        self._entry(sql_statement=sql_statement, table_name=table_name)
        self.to_spark_dataframe(table_name, 8)  # registers table for use in query
        sqlc = self.spark_sql_context()
        s_res = sqlc.sql(sql_statement)
        res = XFrameImpl.load_from_spark_dataframe(s_res)
        self._exit()
        return res
