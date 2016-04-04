import copy
import pickle

from xframes import fileio


def merge_column_lineage(lin1, lin2):
    # lin1 and lin2 are column lineages (dict of key: set)
    # returns column lineage, where result has a element for
    #   every key in lin1 or lin2, and values are the set
    #   union of lin1 and lin2
    res = {}
    keys = list(set(lin1.keys()) | set(lin2.keys()))
    for key in keys:
        s = set()
        if key in lin1:
            s |= lin1[key]
        if key in lin2:
            s |= lin2[key]
        res[key] = s
    return res


class Lineage(object):
    def __init__(self, table_lineage=None, column_lineage=None):
        self.table_lineage = table_lineage or set()
        self.column_lineage = column_lineage or {}

    @staticmethod
    def copy(lineage):
        table_lineage = lineage.table_lineage
        column_lineage = lineage.column_lineage
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    @staticmethod
    def from_array_lineage(xa_lineage, col_name):
        table_lineage = xa_lineage.table_lineage
        column_lineage = {col_name: xa_lineage.column_lineage['_XARRAY']}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    @staticmethod
    def init_array_lineage(table_lineage):
        column_lineage = {'_XARRAY': (table_lineage, '_XARRAY')}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    @staticmethod
    def init_frame_lineage(table_lineage, col_names):
        column_lineage = {col_name: (table_lineage, col_name) for col_name in col_names}
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def add_column(self, xa_lineage, col_name):
        table_lineage = self.table_lineage | xa_lineage.table_lineage
        column_lineage = copy.copy(self.column_lineage)
        column_lineage[col_name] = xa_lineage.column_lineage['_XARRAY']
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def merge(self, other):
        table_lineage = self.table_lineage | other.table_lineage
        column_lineage = merge_column_lineage(self.column_lineage, other.column_lineage)
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    @staticmethod
    def load(path):
        with fileio.open_file(path) as f:
            table_lineage, column_lineage = pickle.load(f)
        return Lineage(table_lineage=table_lineage, column_lineage=column_lineage)

    def save(self, path):
        with fileio.open_file(path, 'w') as f:
            # TODO detect filesystem errors
            lineage_fields = [self.table_lineage, self.column_lineage]
            pickle.dump(lineage_fields, f)
