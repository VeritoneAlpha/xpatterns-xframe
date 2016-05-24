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
import datetime
import dateutil
import logging


from xframes.deps import HAS_PANDAS
from xframes.deps import HAS_NUMPY

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField

import xframes.fileio as fileio
from xframes.xobject_impl import XObjectImpl
from xframes.traced_object import TracedObject
from xframes.spark_context import CommonSparkContext
from xframes.util import infer_type_of_rdd
from xframes.util import cache, uncache, persist, unpersist
from xframes.util import is_missing, is_missing_or_empty
from xframes.util import to_ptype, to_schema_type, hint_to_schema_type, pytype_from_dtype, safe_cast_val
from xframes.util import distribute_seed
from xframes.lineage import Lineage
import xframes
from xframes.xarray_impl import XArrayImpl
from xframes.xrdd import XRdd
from xframes.cmp_rows import CmpRows
from xframes.aggregator_impl import aggregator_properties

if HAS_NUMPY:
    import numpy


# Used to save the original line being parsed.
# If there are any errors, then the line is picked up from here.
saved_line = None


# noinspection PyUnresolvedReferences,PyShadowingNames,PyIncorrectDocstring
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


# noinspection PyUnresolvedReferences,PyShadowingNames,PyIncorrectDocstring
class XFrameImpl(XObjectImpl, TracedObject):
    """ Implementation for XFrame. """

    def __init__(self, rdd=None, col_names=None, column_types=None, lineage=None):
        """ Instantiate a XFrame implementation.

        The RDD holds all the data for the XFrame.
        The rows in the rdd are stored as a list.
        Each column must be of uniform type.
        Types permitted include int, long, float, string, list, and dict.
        """
        self._entry(col_names=col_names, column_types=column_types, lineage=lineage)
        super(XFrameImpl, self).__init__(rdd)
        col_names = col_names or []
        column_types = column_types or []
        self.col_names = list(col_names)
        self.column_types = list(column_types)
        self.lineage = lineage or Lineage.init_frame_lineage(Lineage.EMPTY, self.col_names)
        self.iter_pos = None
        self._num_rows = None

        self.materialized = False

    def _rv(self, rdd, col_names=None, column_types=None, lineage=None):
        """
        Return a new XFrameImpl containing the RDD, column names, column types, and lineage.

        Column names and types default to the existing ones.
        This is typically used when a function returns a new XFrame.
        """
        # only use defaults if values are None, not []
        col_names = self.col_names if col_names is None else col_names
        column_types = self.column_types if column_types is None else column_types
        lineage = lineage or self.lineage
        return XFrameImpl(rdd, col_names, column_types, lineage)

    def _reset(self):
        self._rdd = None
        self.col_names = []
        self.column_types = []
        self.table_lineage = Lineage.init_frame_lineage(Lineage.Empty, self.col_names)
        self.materialized = False

    def _replace(self, rdd, col_names=None, column_types=None, lineage=None):
        """
        Replaces the existing RDD, column names, column types, and lineage with new values.

        Column names, types, and lineage default to the existing ones.
        This is typically used when a function modifies the current XFrame.
        """
        self._replace_rdd(rdd)
        if col_names is not None:
            self.col_names = col_names
        if column_types is not None:
            self.column_types = column_types
        if lineage is not None:
            self.lineage = lineage

        self._num_rows = None
        self.materialized = False
        return self

    def _count(self):
        persist(self._rdd)
        count = self._rdd.count()
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
    def load_from_tuple_list(cls, data, column_names, column_types):
        sc = cls.spark_context()
        rdd = sc.parallelize(data)
        lineage = Lineage.init_frame_lineage(Lineage.PROGRAM, column_names)
        return XFrameImpl(rdd, column_names, column_types, lineage)

    @classmethod
    def load_from_pandas_dataframe(cls, data):
        """
        Load from a pandas.DataFrame.
        """
        cls._entry()
        if not HAS_PANDAS:
            raise NotImplementedError('Pandas is required.')
        if not HAS_NUMPY:
            raise NotImplementedError('Numpy is required.')

        # build something we can parallelize
        # list of rows, each row is a tuple
        columns = data.columns
        dtypes = data.dtypes
        column_names = [col for col in columns]
        column_types = [type(numpy.zeros(1, dtype).tolist()[0]) for dtype in dtypes]
        lineage = Lineage.init_frame_lineage(Lineage.PANDAS, column_names)

        res = []
        for row in data.iterrows():
            rowval = row[1]
            cols = [rowval[col] for col in columns]
            res.append(tuple(cols))
        sc = cls.spark_context()
        rdd = sc.parallelize(res)
        return XFrameImpl(rdd, column_names, column_types, lineage)

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
        XFrameImpl.check_input_uri(metadata_path)
        with fileio.open_file(metadata_path) as f:
            names, types = pickle.load(f)
        lineage_path = os.path.join(path, '_lineage')
        if fileio.exists(lineage_path):
            lineage = Lineage.load(lineage_path)
        else:
            lineage = Lineage.init_frame_lineage(path, names)
        return cls(res, names, types, lineage)

    @classmethod
    def load_from_spark_dataframe(cls, rdd):
        """
        Load data from an existing spark dataframe.
        """
        cls._entry()
        schema = rdd.schema
        xf_names = [str(col.name) for col in schema.fields]
        xf_types = [to_ptype(col.dataType) for col in schema.fields]
        lineage = Lineage.init_frame_lineage(Lineage.DATAFRAME, xf_names)

        def row_to_tuple(row):
            return tuple([row[i] for i in range(len(row))])
        xf_rdd = rdd.map(row_to_tuple)
        return cls(xf_rdd, xf_names, xf_types, lineage)

    # noinspection SqlNoDataSourceInspection
    @classmethod
    def load_from_hive(cls, dataset):
        """
        Load data from a hive dataset.  This is normally given as database.table.
        """
        cls._entry()
        hc = cls.hive_context()
        # guard agains SQL injection attack
        if not re.match('^[A-Za-z0-9]+(.[A-Za-z0-9]+)?$', dataset):
            raise ValueError('Hive dataset name must contain only alphanumeric and period.')
        hive_dataframe = hc.sql('SELECT * from {}'.format(dataset))
        xf_names = [str(col) for col in hive_dataframe.columns]
        type_names = [name_type[1] for name_type in hive_dataframe.dtypes]
        xf_types = [pytype_from_dtype(type_name) for type_name in type_names]

        def row_to_tuple(row):
            return tuple([row[i] for i in range(len(row))])
        rdd = hive_dataframe.map(row_to_tuple)
        lineage = Lineage.init_frame_lineage(dataset, xf_names)
        return cls(rdd, xf_names, xf_types, lineage)

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
        lineage = Lineage.init_frame_lineage(Lineage.RDD, names)
        # TODO sniff types using more of the rdd
        return cls(rdd, names, types, lineage)

    @classmethod
    def load_from_csv(cls, path, parsing_config, type_hints):
        """
        Load RDD from a csv file and return a XFrameImpl
        """
        cls._entry(path=path, parsing_config=parsing_config, type_hints=type_hints)

        def get_config(name):
            return parsing_config[name] if name in parsing_config else None
        row_limit = get_config('row_limit')
        use_header = get_config('use_header')
        comment_char = get_config('comment_char')
        store_errors = get_config('store_errors')
        na_values = get_config('na_values')
        if not isinstance(na_values, list):
            na_values = [na_values]

        sc = CommonSparkContext().spark_context()
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

        # read and parse a csv file in a partition
        def read_csv_stream(f, params, num_columns):
            def record_input(_f):
                global saved_line
                for line in _f:
                    saved_line = line
                    yield line

            def strip_utf8_bom(_f):
                for line in _f:
                    if len(line) > 3 and ord(line[0]) == 239 and ord(line[1]) == 187 and ord(line[2]) == 191:
                        yield line[3:]
                    else:
                        yield line

            def handle_unicode(_f):
                # The Python 2 CSV parser can only handle ASCII and UTF-8.
                for line in _f:
                    if isinstance(line, str):
                        yield line
                    else:
                        yield line.encode('utf-8')

            def strip_comments(_f, comment_char):
                # Wraps the file iterator, providing comment stripping
                for line in _f:
                    yield line.partition(comment_char)[0].rstrip()

            def unquote(field):
                if field is None or len(field) == 0:
                    return field
                if field[0] == '"' and field[-1] == '"':
                    return field[1: -1]
                if field[0] == "'" and field[-1] == "'":
                    return field[1: -1]
                return field

            def strip_newline(field):
                return field.replace('\n', '\\n').replace('\r', '\\r')

            f = strip_utf8_bom(handle_unicode(record_input(f)))

            params = copy.copy(params)

            if 'commentchar' in params:
                commentchar = params['commentchar']
                del params['commentchar']
                f = strip_comments(f, commentchar)

            reader = csv.reader(f, **params)

            try:
                for row in reader:
                    # get rid of newline in fields
                    # get rid of quotes around fields
                    row = [unquote(strip_newline(col)) for col in row]
                    if num_columns is not None and len(row) != num_columns:
                        yield 'width', saved_line
                    else:
                        yield 'data', row
            except csv.Error:
                yield 'csv', saved_line
            except SystemError:
                yield 'csv', saved_line

        errs = {}

        # use first row, if available, to make column names
        first_raw = raw.first()
        res = read_csv_stream([first_raw], params, num_columns=None).next()
        if res[0] != 'data':
            errs['header'] = XArrayImpl(rdd=sc.parallelize([res[1]]), elem_type=str)
            return errs, XFrameImpl()
        first = res[1]
        if use_header:
            col_names = [item.strip() for item in first]
        else:
            col_names = ['X.{}'.format(i) for i in range(len(first))]
        col_count = len(col_names)

        parsed = raw.mapPartitions(lambda partition:
                                   read_csv_stream(partition, params, col_count))

        persist(parsed)
        res = parsed.filter(lambda tup: tup[0] == 'data').values()
        if store_errors:
            errs['width'] = XArrayImpl(rdd=parsed.filter(lambda tup: tup[0] == 'width').values(), elem_type=str)
            errs['csv'] = XArrayImpl(rdd=parsed.filter(lambda tup: tup[0] == 'csv').values(), elem_type=str)
        unpersist(parsed)

        # filter out all rows that match header
        # this could potentialy filter data rows, but we will ignore that for now
        if use_header:
            res = res.filter(lambda row: row != first)

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
        if len(na_values) > 0:
            res = res.map(lambda row: apply_na(row, na_values))

        # drop columns with empty header
        remove_cols = [col_index for col_index, col_name in enumerate(col_names) if len(col_name) == 0]

        def remove_columns(row):
            return [val for index, val in enumerate(row) if index not in remove_cols]
        if len(remove_cols) > 0:
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
                if typ is datatime.datetime:
                    return datatime.datetime(1, 1, 1)
                if typ is str:
                    return ''
                if typ is dict:
                    return {}
                if typ is list:
                    return []
            try:
                if typ in (dict, list):
                    return ast.literal_eval(val)
                elif typ is datetime.datetime:
                    return dateutil.parser.parse(val)
                return typ(val)
            except ValueError:
                raise ValueError('Cast failed: ({}) {}  col: {}'.format(typ, val, name))
            except TypeError:
                raise TypeError('Cast failed: ({}) {}  col: {}'.format(typ, val, name))

        # This is where the result is cast as a tuple
        def cast_row(row, types, names):
            return tuple([cast_val(val, typ, name) for val, typ, name in zip(row, types, names)])

        # TODO -- if cast fails, then store None
        if not (len(types) == 0 or all([t == str for t in types])):
            res = res.map(lambda row: cast_row(row, types, col_names))
            if row_limit is None:
                persist(res)

        lineage = Lineage.init_frame_lineage(path, col_names)

        # returns a dict of errors and XFrameImpl
        return errs, XFrameImpl(res, col_names, column_types, lineage)

    # noinspection PyUnusedLocal
    @classmethod
    def read_from_text(cls, path, delimiter, nrows, verbose):
        """
        Load RDD from a text file
        """
        # TODO handle nrows, verbose
        cls._entry(path=path, delimiter=delimiter, nrows=nrows)
        sc = CommonSparkContext.spark_context()
        if delimiter is None:
            rdd = sc.textFile(path)
            res = rdd.map(lambda line: (line.encode('utf-8'), ))
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
        lineage = Lineage.init_frame_lineage(path, col_names)
        return XFrameImpl(res, col_names, col_types, lineage)

    @classmethod
    def load_from_parquet(cls, path):
        """
        Load RDD from a parquet file
        """
        cls._entry(path=path)
        sqlc = CommonSparkContext.spark_sql_context()
        spark_ver = CommonSparkContext.spark_version()
        if spark_ver >= [1, 6, 0]:
            s_rdd = sqlc.read.parquet(path)
        else:
            s_rdd = sqlc.parquetFile(path)
        schema = s_rdd.schema
        col_names = [str(col.name) for col in schema.fields]
        col_types = [to_ptype(col.dataType) for col in schema.fields]
        lineage = Lineage.init_frame_lineage(path, col_names)

        rdd = s_rdd.map(lambda row: tuple(row))
        return XFrameImpl(rdd, col_names, col_types, lineage)

    # Save
    def save(self, path):
        """
        Save to a file.

        Saved in an efficient internal format, intended for reading back into an RDD.

        Note: do not save over the old file name of an XFrame.  The lazy evaluator is
        triggered by the save operation, and still needs to read from the old file to generate
        the data for the file to be saved.  Instead, save to a temp file, then delete the old file
        and rename the temp file.
        """
        self._entry(path=path)
        fileio.delete(path)
        # save rdd
        self._rdd.saveAsPickleFile(path)
        # save metadata in the same directory

        metadata_path = os.path.join(path, '_metadata')
        metadata = [self.col_names, self.column_types]
        with fileio.open_file(metadata_path, 'w') as f:
            # TODO detect filesystem errors
            pickle.dump(metadata, f)

        lineage_path = os.path.join(path, '_lineage')
        self.lineage.save(lineage_path)

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

    def save_as_parquet(self, url, column_names=None, column_type_hints=None, number_of_partitions=None):
        """
        Save to a parquet file.
        """
        column_names = column_names or self.col_names

        self._entry(url=url,
                    column_names=column_names,
                    column_type_hints=column_type_hints,
                    number_of_partitions=number_of_partitions)
        fileio.delete(url)
        table_name = None
        dataframe = self.to_spark_dataframe(table_name,
                                            column_names=column_names,
                                            column_type_hints=column_type_hints,
                                            number_of_partitions=number_of_partitions)
        if dataframe is None:
            logging.warn('Save_as_parquet -- dataframe conversion failed.')
            return
        else:
            spark_ver = CommonSparkContext.spark_version()
        if spark_ver >= [1, 6, 0]:
            dataframe.write.parquet(url)
        else:
            dataframe.saveAsParquetFile(url)

    def to_rdd(self, number_of_partitions=None):
        """
        Returns the underlying RDD.

        Discards the column name and type information.
        """
        self._entry(number_of_partitions=number_of_partitions)
        res = self._rdd.repartition(number_of_partitions) if number_of_partitions is not None else self._rdd
        return res.RDD()

    def to_spark_dataframe(self, table_name,
                           column_names=None,
                           column_type_hints=None,
                           number_of_partitions=None):
        """
        Use column names and hints to convert the XFrame into a DataFrame by generating
        and applying the schema.
        """
        self._entry(table_name=table_name,
                    column_names=column_names,
                    column_type_hints=column_type_hints,
                    number_of_partitions=number_of_partitions)

        column_type_hints = column_type_hints or {}
        column_names = column_names or self.col_names
        if not isinstance(column_names, list):
            raise TypeError('Column names must be a list.')
        if len(column_names) != len(self.col_names):
            raise ValueError('Column names list must match number of columns: actual: {}, expected: {}'
                             .format(len(column_names), len(self.col_names)))

        if isinstance(self._rdd, DataFrame):
            return self._rdd

        def rename_columns(column_names):
            # rename columns to be acceptable to parquet
            return [re.sub(r'[\s,;{}()]', '_', column_name) for column_name in column_names]

        def convert_column_type(column_type, column_name, element):
            if column_name in column_type_hints:
                hint = column_type_hints[column_name]
                return hint_to_schema_type(hint)
            return to_schema_type(column_type, element)

        def convert_column_types(column_types, column_names, first_row):
            return [convert_column_type(column_type, column_name, element)
                    for column_type, column_name, element in
                    zip(column_types, column_names, first_row)]

        head = self.head_as_list(1)
        first_row = [None] * len(self.col_names) if len(head) == 0 else list(head[0])

        parquet_column_names = rename_columns(column_names)
        parquet_column_types = convert_column_types(self.column_types, self.col_names, first_row)

        fields = [StructField(name, typ, True)
                  for name, typ in zip(parquet_column_names, parquet_column_types)]
        schema = StructType(fields)

        rdd = self._rdd if number_of_partitions is None else self._rdd.repartition(number_of_partitions)
        sqlc = self.spark_sql_context()
        res = sqlc.createDataFrame(rdd.RDD(), schema)
        if table_name is not None:
            sqlc.registerDataFrameAsTable(res, table_name)
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
        self._num_rows = self._count()
        return self._num_rows

    def num_columns(self):
        """
        Returns the number of columns in the XFrame.
        """
        self._entry()
        num_cols = len(self.col_names)
        return num_cols

    def column_names(self):
        """
        Returns the column names in the XFrame.
        """
        self._entry()
        col_names = self.col_names
        return col_names

    def dtype(self):
        """
        Returns the column data types in the XFrame.
        """
        self._entry()
        return self.column_types

    def lineage_as_dict(self):
        """
        Returns the lineage.
        """
        self._entry()
        return {'table': self.lineage.table_lineage,
                'column': self.lineage.column_lineage}

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
            return self._rv(res)
        pairs = self._rdd.zipWithIndex()
        cache(pairs)
        filtered_pairs = pairs.filter(lambda x: x[1] < n)
        uncache(pairs)
        res = filtered_pairs.keys()
        self.materialized = True
        return self._rv(res)

    def head_as_list(self, n):
        # Used in xframe when doing dry runs to determine type
        self._entry(n=n)
        lst = self._rdd.take(n)      # action
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
        return self._rv(res)

    # Sampling
    def sample(self, fraction, max_partitions, seed):
        """
        Sample the current RDDs rows as an XFrame.
        """
        self._entry(fraction=fraction, max_partitions=max_partitions, seed=seed)
        res = self._rdd.sample(False, fraction, seed)
        if max_partitions is not None and max_partitions < res.getNumPartitions():
            res = res.coalesce(max_partitions)
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
        return self._rv(rdd1), self._rv(rdd2)

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
        materialized = self.materialized
        return materialized

    def has_size(self):
        """
        Returns whether or not the size of the XFrame is known.
        """
        self._entry()
        materialized = self.materialized
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
        lineage = self.lineage.to_array_lineage(column_name)
        return xframes.xarray_impl.XArrayImpl(res, col_type, lineage)

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
        lineage = self.lineage.select_columns(names)
        return self._rv(res, names, types, lineage)

    def copy(self):
        """
        Creates a copy of the XFrameImpl.

        The underlying RDD is immutale, so we just need to copy the metadata.
        """
        self._entry()
        return self._rv(self._rdd)

    @classmethod
    def from_xarray(cls, arry_impl, name=None):
        cls._entry(name=name)
        name = name or 'X.0'
        col_names = [name]
        col_types = [arry_impl.elem_type]
        rdd = arry_impl.rdd().map(lambda val: (val,))
        lineage = Lineage.from_array_lineage(arry_impl.lineage, name)
        return XFrameImpl(rdd, col_names, col_types, lineage)

    def add_column(self, col, name):
        """
        Create a new xFrame with one additional column (an XArray).

        The number of elements in the data given
        must match the length of every other column of the XFrame. If no
        name is given, a default name is chosen.
        """
        self._entry(name=name)
        col_index = len(self.col_names)
        if name is None or len(name) == 0:
            new_name = 'X.{}'.format(col_index)
            while new_name in self.column_names():
                col_index += 1
                new_name = 'X.{}'.format(col_index)
        elif name in self.column_names():
            new_name = '{}.{}'.format(name, col_index)
            while new_name in self.column_names():
                col_index += 1
                new_name = '{}.{}'.format(name, col_index)
        else:
            new_name = name
        col_names = copy.copy(self.col_names)
        col_names.append(new_name)
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
        lineage = self.lineage.add_column(col, new_name)
        return self._rv(res, col_names, col_types, lineage)

    def add_column_in_place(self, col, name):
        """
        Add a column (as XArray) to this XFrame.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry(name=name)
        col_index = len(self.col_names)
        if name == '':
            name = 'X{}'.format(col_index)
        if name in self.col_names:
            raise ValueError("Column name already exists: '{}'.".format(name))
        self.col_names.append(name)
        self.column_types.append(col.elem_type)
        # zip the data into the rdd, then shift into the tuple
        if self._rdd is None:
            res = col.rdd().map(lambda x: (x, ))
        else:
            res = self._rdd.zip(col.rdd())

            def move_inside(old_val, new_elem):
                return tuple(old_val + (new_elem, ))
            res = res.map(lambda pair: move_inside(pair[0], pair[1]))
        lineage = self.lineage.add_column(col, name)
        return self._replace(res, lineage=lineage)

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
        types = self.column_types + [col.elem_type for col in cols]
        rdd = self._rdd
        for col in cols:
            rdd = rdd.zip(col.rdd())

            def move_inside(old_val, new_elem):
                return tuple(old_val + (new_elem, ))
            rdd = rdd.map(lambda pair: move_inside(pair[0], pair[1]))
        lineage = self.lineage.add_columns(cols, namelist)
        return self._rv(rdd, names, types, lineage)

    def add_columns_array_in_place(self, cols, namelist):
        """
        Adds multiple columns to this XFrame.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry(namelist=namelist)
        names = self.col_names + namelist
        types = self.column_types + [col.elem_type for col in cols]
        rdd = self._rdd
        for col in cols:
            rdd = rdd.zip(col.rdd())

            def move_inside(old_val, new_elem):
                return tuple(old_val + (new_elem, ))
            rdd = rdd.map(lambda pair: move_inside(pair[0], pair[1]))
        lineage = self.lineage.add_columns(cols, namelist)
        return self._replace(rdd, names, types, lineage)

    def add_columns_frame(self, other):
        """
        Adds multiple columns to this XFrame.

        The number of elements in all
        columns must match the length of every other column of the RDD.
        The columns to be added are in an XFrame.

        This operation returns a new XFrame.
        """
        self._entry()
        names = self.col_names + other.col_names
        new_names = []
        name_map = {}
        for name in names:
            old_name = name
            if name in new_names:
                col_index = 1
                name = '{}.{}'.format(name, col_index)
                while name in new_names:
                    col_index += 1
                    name = '{}.{}'.format(name, col_index)
            new_names.append(name)
            name_map[old_name] = name

        types = self.column_types + other.column_types

        def merge(old_cols, new_cols):
            return old_cols + new_cols

        rdd = self._rdd.zip(other.rdd())
        res = rdd.map(lambda pair: merge(pair[0], pair[1]))
        lineage = self.lineage.merge(other.lineage.replace_column_names(name_map))
        return self._rv(res, new_names, types, lineage)

    def add_columns_frame_in_place(self, other):
        """
        Adds multiple columns to this XFrame.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry()
        names = self.col_names + other.col_names
        types = self.column_types + other.column_types

        def merge(old_cols, new_cols):
            return old_cols + new_cols

        rdd = self._rdd.zip(other.rdd())
        res = rdd.map(lambda pair: merge(pair[0], pair[1]))
        lineage = self.lineage.merge(other.lineage)
        return self._replace(res, names, types, lineage)

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
        lineage = self.lineage.remove_column(name)
        return self._rv(res, col_names, col_types, lineage)

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
        lineage = self.lineage.remove_column(name)
        return self._replace(res, lineage=lineage)

    def remove_columns(self, col_names):
        """
        Remove columns from the RDD. 

        This operation creates a new xframe_impl and returns it.
        """
        self._entry(col_names=col_names)
        cols = [self.col_names.index(name) for name in col_names]
        # pop from highest to lowest does not foul up indexes
        cols.sort(reverse=True)
        remaining_col_names = copy.copy(self.col_names)
        remaining_col_types = copy.copy(self.column_types)
        for col in cols:
            remaining_col_names.pop(col)
            remaining_col_types.pop(col)

        def pop_cols(row, cols):
            lst = list(row)
            for col in cols:
                lst.pop(col)
            return tuple(lst)
        res = self._rdd.map(lambda row: pop_cols(row, cols))
        lineage = self.lineage.remove_columns(col_names)
        return self._rv(res, remaining_col_names, remaining_col_types, lineage)

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
                logging.warn('Swap index error {} {} {} {}'.warn(col1, col2, row, len(row)))
        col1 = self.col_names.index(column_1)
        col2 = self.col_names.index(column_2)
        names = swap_list(self.col_names, col1, col2)
        types = swap_list(self.column_types, col1, col2)
        res = self._rdd.map(lambda row: swap_cols(row, col1, col2))
        return self._rv(res, names, types)

    def reorder_columns(self, column_names):
        """
        Return new XFrameImpl, with columns reordered.
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
        return self._rv(res, names, types)

    def replace_column_names(self, new_names):
        """
        Return new XFrameImpl, with column names replaced.
        """
        self._entry(new_names=new_names)
        name_map = {k: v for k, v in zip(self.col_names, new_names)}
        lineage = self.lineage.replace_column_names(name_map)
        return self._rv(self._rdd, new_names, lineage=lineage)

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
        lineage = self.lineage.add_col_const(name)
        return self._replace(res, lineage=lineage)

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
        lineage = self.lineage.add_col_const(name)
        return self._replace(res, lineage=lineage)

    def replace_single_column_in_place(self, col):
        """
        Replace the column in a single-column table with the given one.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry()
        res = col.rdd().map(lambda item: (item, ))
        col_type = infer_type_of_rdd(col.rdd())
        self.column_types[0] = col_type
        lineage = col.lineage
        return self._replace(res, lineage=lineage)

    def replace_selected_column(self, column_name, col):
        """
        Replace the given column  with the given one.

        This operation returns a new XFrame.
        """
        self._entry(column_name=column_name)
        rdd = self._rdd.zip(col.rdd())
        col_num = self.col_names.index(column_name)

        def replace_col(row_col, col_num):
            row = list(row_col[0])
            col = row_col[1]
            row[col_num] = col
            return tuple(row)
        res = rdd.map(lambda row_col: replace_col(row_col, col_num))
        col_names = copy.copy(self.column_names())
        col_names[col_num] = column_name
        col_type = infer_type_of_rdd(col.rdd())
        col_types = copy.copy(self.column_types)
        col_types[col_num] = col_type
        lineage = self.lineage.replace_column(col, column_name)
        return self._rv(res, col_names, col_types, lineage)

    def replace_selected_column_in_place(self, column_name, col):
        """
        Replace the given column  with the given one.

        This operation modifies the current XFrame in place and returns self.
        """
        self._entry(column_name=column_name)
        rdd = self._rdd.zip(col.rdd())
        col_num = self.col_names.index(column_name)

        def replace_col(row_col, col_num):
            row = list(row_col[0])
            col = row_col[1]
            row[col_num] = col
            return tuple(row)
        res = rdd.map(lambda row_col: replace_col(row_col, col_num))
        col_type = infer_type_of_rdd(col.rdd())
        self.column_types[col_num] = col_type
        lineage = self.lineage.replace_column(col, column_name)
        return self._replace(res, lineage=lineage)

    # Row Manipulation
    def flat_map(self, fn, column_names, column_types, use_columns, seed):
        """
        Map each row of the RDD to multiple rows in a new RDD via a
        function.

        The input to `fn` is a dictionary of column/value pairs.
        The output of `fn` must have type List[List[...]].  Each inner list
        will be a single row in the new output, and the collection of these
        rows within the outer list make up the data for the output RDD.
        """
        self._entry(column_names=column_names, column_types=column_types, use_coluns=use_columns, seed=seed)
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)
        names = self.col_names
        use_columns_index = [names.index(col) for col in use_columns]

        # fn needs the row as a dict
        def build_row(names, row):
            if use_columns:
                names = [name for name in names if name in use_columns]
                row = [row[i] for i in use_columns_index]
            return dict(zip(names, row))
        res = self._rdd.flatMap(lambda row: fn(build_row(names, row)))
        res = res.map(tuple)
        lineage = self.lineage.flat_map(column_names, use_columns)
        return self._rv(res, column_names, column_types, lineage)

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
        return self._rv(res)

    def stack_list(self, column_name, new_column_names, new_column_types, drop_na):
        """
        Convert a "wide" list column of an XFrame to one or two "tall" columns by
        stacking all values.

        new_column_names and new_column_types are lists of 1 item
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
        lineage = self.lineage.stack(column_name, [new_name])
        return self._rv(res, column_names, column_types, lineage)

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
        new_names = [new_name_k, new_name_v]
        column_names.insert(col_num + 1, new_name_v)
        column_types = list(self.column_types)
        column_types[col_num] = new_column_types[0]
        column_types.insert(col_num + 1, new_column_types[1])
        lineage = self.lineage.stack(column_name, new_names)
        return self._rv(res, column_names, column_types, lineage)

    def append(self, other):
        """
        Add the rows of an RDD to the end of this RDD.

        Both RDDs must have the same set of columns with the same column
        names and column types.
        """
        self._entry()
        res = self._rdd.union(other.rdd())
        lineage = self.lineage.merge(other.lineage)
        return self._rv(res, lineage=lineage)

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
            return self._rv(res)
        else:
            res1 = self._rdd.filter(lambda row: f(row, cols))
            res2 = self._rdd.filter(lambda row: not f(row, cols))
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
        lineage = self.lineage.add_col_index(column_name)
        return self._rv(res, names, types, lineage)

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

        if dtype is list:
            res = keys.map(lambda row: pack_row_list(row, fill_na))
        elif dtype is array.array:
            typecode = 'd'
            res = keys.map(lambda row: pack_row_array(row, fill_na, typecode))
        elif dtype is dict:
            res = keys.map(lambda row: pack_row_dict(row, dict_keys, fill_na))
        else:
            raise NotImplementedError
        lineage = self.lineage.pack_columns(columns)
        return xframes.xarray_impl.XArrayImpl(res, dtype, lineage)

    def apply(self, fn, dtype, use_columns, seed):
        """
        Transform each row to an XArray according to a
        specified function. Returns a array RDD of ``dtype`` where each element
        in this array RDD is transformed by `fn(x)` where `x` is a single row in
        the xframe represented as a dictionary.  The ``fn`` should return
        exactly one value which is or can be cast into type ``dtype``.
        """
        self._entry(dtype=dtype, use_columns=use_columns, seed=seed)
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)
        names = self.col_names
        use_columns_index = [names.index(col) for col in use_columns]

        # fn needs the row as a dict
        def build_row(names, row):
            if use_columns:
                names = [name for name in names if name in use_columns]
                row = [row[i] for i in use_columns_index]
            return dict(zip(names, row))

        def transformer(row):
            result = fn(build_row(names, row))
            if not isinstance(result, dtype):
                return safe_cast_val(result, dtype)
            return result
        res = self._rdd.map(transformer)
        lineage = self.lineage.apply(use_columns)
        return xframes.xarray_impl.XArrayImpl(res, dtype, lineage)

    def transform_col(self, col, fn, dtype, use_columns, seed):
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
        self._entry(col=col, dtype=dtype, use_columns=use_columns, seed=seed)
        if col not in self.col_names:
            raise ValueError("Column name does not exist: '{}'.".format(col))
        if seed:
            distribute_seed(self._rdd, seed)
            random.seed(seed)
        col_index = self.col_names.index(col)
        names = self.col_names
        use_columns_index = [names.index(col_name) for col_name in use_columns]

        # fn needs the row as a dict
        def build_row(names, row):
            if use_columns:
                names = [name for name in names if name in use_columns]
                row = [row[i] for i in use_columns_index]
            return dict(zip(names, row))

        def transformer(row):
            result = fn(build_row(names, row))
            if not isinstance(result, dtype):
                result = safe_cast_val(result, dtype)
            lst = list(row)
            lst[col_index] = result
            return tuple(lst)

        res = self._rdd.map(transformer)
        new_col_types = list(self.column_types)
        new_col_types[col_index] = dtype
        lineage = self.lineage.transform_col(col, use_columns)
        return self._rv(res, column_types=new_col_types, lineage=lineage)

    def transform_cols(self, cols, fn, dtypes, use_columns, seed):
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
        use_columns_index = [names.index(col) for col in use_columns]

        def build_row(names, row):
            if use_columns:
                names = [name for name in names if name in use_columns]
                row = [row[i] for i in use_columns_index]
            return dict(zip(names, row))

        def transformer(row):
            result = fn(build_row(names, row))
            lst = list(row)
            for dtype_index, col_index in enumerate(col_indexes):
                dtype = dtypes[dtype_index]
                result_item = result[dtype_index]
                if dtype is None:
                    lst[col_index] = None
                elif isinstance(result_item, dtype):
                    lst[col_index] = result_item
                else:
                    lst[col_index] = safe_cast_val(result_item, dtype)
            return tuple(lst)

        res = self._rdd.map(transformer)
        new_col_types = list(self.column_types)
        for dtype_index, col_index in enumerate(col_indexes):
            new_col_types[col_index] = dtypes[dtype_index]
        lineage = self.lineage.transform_cols(cols, use_columns)
        return self._rv(res, column_types=new_col_types, lineage=lineage)

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

    def filter_by_function(self, fn, column_name, exclude):
        """
        Perform filtering on a single column by a function
        """
        col_index = self.col_names.index(column_name)

        def filter_fun(row):
            res = fn(row[col_index])
            return not res if exclude else res

        res = self._rdd.filter(filter_fun)
        return self._rv(res)

    def filter_by_function_row(self, fn, exclude):
        """
        Perform filtering on all columns by a function
        """
        # fn needs the row as a dict
        col_names = self.col_names

        def filter_fun(row):
            res = fn(dict(zip(col_names, row)))
            return not res if exclude else res

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
            return [self.column_types[col] if isinstance(col, int) else None for col in cols]

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
        lineage = self.lineage.groupby(key_columns_array, group_output_columns, group_columns)
        return self._rv(res, new_col_names, new_col_types, lineage)

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
        def process_column_names(right_col_names, right_col_types):
            # make a list of the result column names and types
            # rename duplicate names
            new_col_names = list(self.col_names)
            new_col_types = list(self.column_types)
            name_map = {}
            for col in right_col_names:
                new_name = name_col(new_col_names, col)
                new_col_names.append(new_name)
                name_map[col] = new_name
            right_lineage = right.lineage.replace_column_names(name_map)
            for t in right_col_types:
                new_col_types.append(t)
            left_count = len(self.col_names)
            right_count = len(right.col_names)
            return new_col_names, new_col_types, left_count, right_count, right_lineage

        if how == 'cartesian':
            new_col_names, new_col_types, left_count, right_count, right_lineage = \
                process_column_names(right.col_names, right.column_types)
            # outer join is substantially different
            # so do it separately
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
            new_col_names, new_col_types, left_count, right_count, right_lineage = \
                process_column_names(right_col_names, right_col_types)

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

            def combine_results(left_row, right_row, left_count, right_count):
                if left_row is None:
                    left_row = tuple([None] * left_count)
                if right_row is None:
                    right_row = tuple([None] * right_count)
                return left_row, right_row

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

        lineage = self.lineage.merge(right_lineage)
        return self._rv(res, new_col_names, new_col_types, lineage)

    def unique(self):
        """
        Remove duplicate rows of the XFrame. Will not necessarily preserve the
        order of the given XFrame in the new XFrame.
        """

        self._entry()
        as_json = self._rdd.map(lambda row: json.dumps(row))
        unique_rows = as_json.distinct()
        res = unique_rows.map(lambda s: json.loads(s))
        return self._rv(res)

    def sort(self, sort_column_names, sort_column_orders):
        """
        Sort current XFrame by the given columns, using the given sort order.
        Only columns that are type of str, int and float can be sorted.

        sort_column_orders is an array of boolean; True is ascending
        """
        self._entry(sort_column_names=sort_column_names, sort_column_orders=sort_column_orders)

        sort_column_indexes = [self.col_names.index(name) for name in sort_column_names]

        def key_fn(row):
            return CmpRows(row, sort_column_indexes, sort_column_orders)

        res = self._rdd.sortBy(keyfunc=key_fn)
        return self._rv(res)

    def sql(self, sql_statement, table_name):
        """
        Execute a spark-sql command against a XFrame
        """
        self._entry(sql_statement=sql_statement, table_name=table_name)
        self.to_spark_dataframe(table_name, number_of_partitions=8)  # registers table for use in query
        sqlc = self.spark_sql_context()
        s_res = sqlc.sql(sql_statement)
        res = XFrameImpl.load_from_spark_dataframe(s_res)
        return res
