import copy
import pickle

from xframes import fileio


def merge_column_lineage(lin1, lin2):
    # lin1 and lin2 are column lineages (dict of key: set)
    # returns column lineage, where result has a element for
    #   every key in lin1 or lin2, and values are the set
    #   union of lin1 and lin2
    s = frozenset()
    return {key: lin1.get(key, s) | lin2.get(key, s) for key in set(lin1.keys()) | set(lin2.keys())}

# TODO remove assertions
# TODO write unit tests

class Lineage(object):
    """
    Create a new column lineage object.

    Parameters
    ----------
    table_lineage: frozenset, optional
        The table lineage is a set of filenames.  These are the tables that contributed to the ofbect that this
        lineage describes.  Instead of filenames, the entries can also be special tokens such as RDD, PANDAS,
        and so forth.

    column_lineage: dict(string: frozenset(tuple)}, optional
        The column lineage is a dictionary of column names and the column's lineage.  A column
        lineage is a set of tupples, where the tuple contains a filename and a column names.
        For each column, the contributors to its value are the filename/column name pairs.
        As before, the filename can also be a special token.

    Notes
    -----
    When the object is describing an XArray, where there is no column name, the
    token _XARRAY is used in the column_lineage.
    """

    RDD = 'RDD'
    RANGE = 'RANGE'
    PROGRAM = 'PROGRAM'
    CONST = 'CONST'
    INDEX = 'INDEX'
    PANDAS = 'PANDAS'
    DATAFRAME = 'DATAFRAME'
    EMPTY = 'EMPTY'
    COUNT = 'COUNT'

    XARRAY = '_XARRAY'

    def __init__(self, table_lineage=None, column_lineage=None):
        self.table_lineage = table_lineage or frozenset()
        self.column_lineage = column_lineage or dict()

    @staticmethod
    def copy(lineage):
        """
        Copy an existing lineage.

        Parameters
        ----------
        lineage : Lineage
            The lineage to copy

        Returns
        -------
            A copy of the lineage.
        """
        assert isinstance(lineage, Lineage)
        table_lineage = lineage.table_lineage
        column_lineage = lineage.column_lineage
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    @staticmethod
    def from_array_lineage(xa_lineage, col_name):
        """
        Create a new lineage for a single-column XFrame from the lineage of an XArray.

        Parameters
        ----------
        xa_lineage : Lineage
            The lineage of an XArray.

        col_name : str
            The name of the column in the XFrame.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(xa_lineage, Lineage)
        assert isinstance(col_name, basestring)
        table_lineage = xa_lineage.table_lineage
        column_lineage = {col_name: xa_lineage.column_lineage[Lineage.XARRAY]}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def to_array_lineage(self, col_name):
        """
        Create an XArray lineage from one column of an existing lineage.

        Parameters
        ----------
        col_name : str
            The column name of the lineage.

        Returns
        -------
        out : Lineage
        """
        assert isinstance(col_name, basestring)
        table_lineage = self.table_lineage
        column_lineage = {Lineage.XARRAY: self.column_lineage[col_name]}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    @staticmethod
    def init_array_lineage(origin=None):
        """
        Create a lineage for an XArray.

        Parameters
        ----------
        origin : str, optional
            The parent filename or token.

        Returns
        -------
            out : Lineage
        """
        assert origin is None or isinstance(origin, basestring)
        if origin is None:
            table_lineage = frozenset()
            column_lineage = {Lineage.XARRAY: frozenset()}
        else:
            table_lineage = frozenset([origin])
            column_lineage = {Lineage.XARRAY: frozenset([(origin, Lineage.XARRAY), ])}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    @staticmethod
    def init_frame_lineage(origin, col_names):
        """
        Create a lineage for an XFrame.

        Parameters
        ----------
        origin : str
            The parent filename or token.

        col_names : list[str]
            The column names in the XFrame.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(origin, basestring)
        assert isinstance(col_names, list)
        if origin is None:
            table_lineage = frozenset()
        else:
            table_lineage = frozenset([origin])
        column_lineage = {col_name: frozenset([(name, col_name) for name in table_lineage]) for col_name in col_names}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def add_column(self, col, name):
        """
        Add a column to an XFrame lineage.

        Parameters
        ----------
        col : XArrayImpl
            An existing XArrayImpl, whose lineage is added to the existing lineage.

        name : str
            The name of the new column.

        Returns
        -------
            out : Lineage
        """
        from xframes.xarray_impl import XArrayImpl
        assert isinstance(col, XArrayImpl)
        assert isinstance(name, basestring)
        table_lineage = self.table_lineage | col.lineage.table_lineage
        column_lineage = copy.copy(self.column_lineage)
        column_lineage[name] = col.lineage.column_lineage[Lineage.XARRAY]
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def add_columns(self, cols, names):
        """
        Add a list of columns to an XFrame lineage.

        Parameters
        ----------
        cols : list[XFrameImpl]
            An existing XFrame, whose lineage is added to the existing lineage.

        names : list[str]
            The names of the new column.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(cols, list)
        assert isinstance(names, list)
        from xframes.xarray_impl import XArrayImpl
        for col in cols:
            assert isinstance(col, XArrayImpl)
        for name in names:
            assert isinstance(name, basestring)
        table_lineage = self.table_lineage
        for col in cols:
            table_lineage |= col.lineage.table_lineage
        column_lineage = copy.copy(self.column_lineage)
        for col, name in zip(cols, names):
            column_lineage[name] = col.lineage.column_lineage[Lineage.XARRAY]
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def add_col_const(self, name):
        """
        Add a constant column to an XFrame lineage.

        Parameters
        ----------
        name : str
            The name of the new column.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(name, basestring)
        table_lineage = self.table_lineage | {Lineage.CONST}
        column_lineage = copy.copy(self.column_lineage)
        column_lineage[name] = frozenset([(Lineage.CONST, name)])
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def add_col_index(self, name):
        """
        Add an index column to an XFrame lineage.

        Parameters
        ----------
        name : str
            The name of the new column.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(name, basestring)
        table_lineage = self.table_lineage | {Lineage.INDEX}
        column_lineage = copy.copy(self.column_lineage)
        column_lineage[name] = frozenset([(Lineage.INDEX, name)])
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def select_columns(self, column_names):
        """
        Create a new lineage containing only the given column names.

        Parameters
        ----------
        column_names : list[str]
            Remove all but these column names from the lineage.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(column_names, list)
        table_lineage = self.table_lineage
        column_lineage = {k: v for k, v in self.column_lineage.iteritems() if k in column_names}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def flat_map(self, column_names, use_columns):
        """
        Create a new lineage containing the use columns for every column

        Parameters
        ----------
        column_names : list[str]
            These are the column names in the output

        use_columns: list[str]
            The columns that are passed to the fn.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(column_names, list)
        assert isinstance(use_columns, list)
        table_lineage = self.table_lineage
        col_lineage = frozenset()
        for col in use_columns:
            col_lineage |= self.column_lineage[col]
        column_lineage = {col: col_lineage for col in column_names}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def merge(self, other):
        """
        Merge the lineage of two XFrame lineages.

        The column names should be distinct.  Rename the columns first
        to make sure this is the case.

        Parameters
        ----------
        other : Lineage
            The other lineage to merge into this lineage.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(other, Lineage)
        table_lineage = self.table_lineage | other.table_lineage
        column_lineage = merge_column_lineage(self.column_lineage, other.column_lineage)
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def replace_column_names(self, name_map):
        """
        Rename the columns in an XFrame lineage.

        Parameters
        ----------
        name_map : dict{str: str}
            Each element of the dictionary gives the mapping from the cold column name to the new column name.
            If a column name is not mentioned in the map, it is not renamed.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(name_map, dict)
        def map_name(name):
            return name_map[name] if name in name_map else name
        table_lineage = copy.copy(self.table_lineage)
        column_lineage = {map_name(k): v for k, v in self.column_lineage.iteritems()}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def replace_column(self, col, col_name):
        """
        Replace one column in an XFrame lineage with an XArrayImpl lineage.

        Parameters
        ----------
        col : XArrayImpl
            The column to add to the lineage.

        col_name : str
            The column name.

        Returns
        -------
            out : Lineage
        """
        from xframes.xarray_impl import XArrayImpl
        assert isinstance(col, XArrayImpl)
        assert isinstance(col_name, basestring)
        table_lineage = self.table_lineage | col.lineage.table_lineage
        column_lineage = copy.copy(self.column_lineage)
        column_lineage[col_name] = col.lineage.column_lineage[Lineage.XARRAY]
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def remove_column(self, name):
        """
        Remove the named column from a XFrame lineage.

        Parameters
        ----------
        name : str
            The name of the column to remove.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(name, basestring)
        table_lineage = copy.copy(self.table_lineage)
        column_lineage = {k: v for k, v in self.column_lineage.iteritems() if k != name}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def remove_columns(self, names):
        """
        Remove the named columns from a XFrame lineage.

        Parameters
        ----------
        names : list[str]
            The name of the column to remove.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(names, list)
        table_lineage = copy.copy(self.table_lineage)
        column_lineage = {k: v for k, v in self.column_lineage.iteritems() if k not in names}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def pack_columns(self, columns):
        """
        Pack the given columns into one column, returned as XArray lineage.

        Parameters
        ----------
        columns : list[str]
            The column names to pack.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(columns, list)
        table_lineage = copy.copy(self.table_lineage)
        s = frozenset()
        for col in columns:
            s |= self.column_lineage[col]
        column_lineage = {Lineage.XARRAY: s}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def groupby(self, key_columns, group_columns, group_output_columns):
        """
        Groupby operation creates new columns from existing columns.

        Parameters
        ----------
        key_columns : list[str]
            The grouping columns.  These will be carried forward as is.

        group_columns : list[str]
            The columns that are being aggregated over.

        group_output_columns : list[str]
            The new columns that result from aggregation.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(key_columns, list)
        assert isinstance(group_columns, list)
        assert isinstance(group_output_columns, list)
        table_lineage = self.table_lineage
        # filter out all but key columns
        column_lineage = {k: v for k, v in self.column_lineage.iteritems() if k in key_columns}
        # copy over group columns --> group_output_columns
        for old_list, new_list in zip(group_columns, group_output_columns):
            for old, new in zip(old_list, new_list):
                if old == '':
                    column_lineage[new] = frozenset([(Lineage.COUNT, old)])
                else:
                    column_lineage[new] = self.column_lineage[old]

        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def unpack_array(self, column_names):
        """
        Create a new lineage by unpacking the xarray lineage

        Parameters
        ----------
        column_names : list[str]
            These are the new column names, whose lineage is the same as the existing xarray

        Returns
        -------
            out : Lineage
        """
        assert isinstance(column_names, list)
        table_lineage = self.table_lineage
        col_lineage_value = self.column_lineage[Lineage.XARRAY]
        column_lineage = {k: col_lineage_value for k in column_names}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def stack(self, column_name, new_column_names):
        """
        Create a new lineage by replacing column_name by column_names

        Parameters
        ----------
        column_name: str
            The column name this is being eliminated
        new_column_names : list[str]
            These are the new column names, whose lineage is the same as the existing column_name

        Returns
        -------
            out : Lineage
        """
        assert isinstance(column_name, basestring)
        assert isinstance(new_column_names, list)
        table_lineage = self.table_lineage
        col_lineage_value = self.column_lineage[column_name]
        column_lineage = copy.copy(self.column_lineage)
        del column_lineage[column_name]
        for name in new_column_names:
            column_lineage[name] = col_lineage_value
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def apply(self, use_columns):
        """
        Create a new lineage containing con column formed from all use_columns.

        Parameters
        ----------
        use_columns: list[str]
            The columns that are passed to the fn.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(use_columns, list)
        table_lineage = self.table_lineage
        col_lineage = frozenset()
        for col in use_columns:
            col_lineage |= self.column_lineage[col]
        column_lineage = {self.XARRAY: col_lineage}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    @staticmethod
    def load(path):
        """
        Load lineage from a file.

        Parameters
        ----------
        path : str
            The path to the lineage file.

        Returns
        -------
            out : Lineage
        """
        assert isinstance(path, basestring)
        with fileio.open_file(path) as f:
            table_lineage, column_lineage = pickle.load(f)
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def save(self, path):
        """
        Save lineage to a file.

        Parameters
        ----------
        path : str
            The path to the lineage file.
        """
        assert isinstance(path, basestring)
        with fileio.open_file(path, 'w') as f:
            # TODO detect filesystem errors
            lineage_fields = [self.table_lineage, self.column_lineage]
            pickle.dump(lineage_fields, f)
