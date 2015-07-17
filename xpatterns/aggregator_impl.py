"""
This module provides aggregator properties, used to define aggregators for groupby.
"""

import random
import math

# Each of these functions operates on a pyspark resultIterable
#  produced by groupByKey and directly produces the aggregated result.

def agg_sum(rows, cols): 
    # cols: [src_col]
    vals = [row[cols[0]] for row in rows]
    return sum(vals)

def agg_argmax(rows, cols): 
    # cols: [agg_col, out_col]
    vals = [row[cols[0]] for row in rows]
    row_index = vals.index(max(vals))
    vals = [row[cols[1]] for row in rows]
    return vals[row_index]

def agg_argmin(rows, cols): 
    # cols: [agg_col, out_col]
    vals = [row[cols[0]] for row in rows]
    row_index = vals.index(min(vals))
    vals = [row[cols[1]] for row in rows]
    return vals[row_index]

def agg_max(rows, cols): 
    # cols: [src_col]
    vals = [row[cols[0]] for row in rows]
    return max(vals)

def agg_min(rows, cols): 
    # cols: [src_col]
    vals = [row[cols[0]] for row in rows]
    return min(vals)

def agg_count(rows, cols): 
    # cols: []
    return len(rows)

def agg_avg(rows, cols): 
    # cols: [src_col]
    vals = [row[cols[0]] for row in rows]
    return sum(vals) / float(len(vals))

def agg_var(rows, cols): 
    # cols: [src_col]
    vals = [row[cols[0]] for row in rows]
    avg = sum(vals) / float(len(vals))
    return sum([(avg - val) ** 2 for val in vals]) / float(len(vals))

def agg_std(rows, cols): 
    # cols: [src_col]
    return math.sqrt(agg_var(rows, cols))

def agg_select_one(rows, cols):
    # cols: [src_col, seed]
    num_rows = len(rows)
    seed = cols[1]
    random.seed(seed)
    row_index = random.randint(0, num_rows-1)
    vals = [row[cols[0]] for row in rows]
    val = vals[row_index]
    return val

def agg_concat_list(rows, cols): 
    # cols: [src_col]
    vals = [row[cols[0]] for row in rows]
    return vals

def agg_concat_dict(rows, cols): 
    # cols: [src_col dict_value_column]
    vals = {row[cols[0]]: row[cols[1]] for row in rows}
    return vals

def agg_quantile(rows, cols): 
    # cols: [src_col, quantile]
    # cols: [src_col, [quantile ...]]
    # not imlemented
    return None


class AggregatorPropertySet(object):
    """ Store aggregator properties for one aggregator. """

    def __init__(self, name, agg_function, default_col_name, output_type):
        """ 
        Create a new instance.

        Parameters
        ----------
        name: str
            The aggregator internal name.

        agg_function: func(rows, cols)
            The agregator function.  
            This is given a pyspark resultIterable produced by groupByKey
               and containing the rows matching a single group.
            It's responsibility is to compute and return the aggregate value for thhe group.

        default_col_name: str
            The name of the aggregate column, if not supplied explicitly.
    
        output_type: type or int
            If a type is given, use that type as the output column type.
            If an integer is given, then the output type is the same as the
                input type of the column indexed by the integer.
        """

        self.name = name
        self.agg_function = agg_function
        self.default_col_name = default_col_name
        self.output_type = output_type

    def get_output_type(self, input_type):
        candidate = self.output_type
        if type(candidate) is int: return input_type[candidate]
        return candidate

class AggregatorProperties(object):
    """ Manage aggregator properties for all known aggregators. """
    def __init__(self):
        self.aggregator_properties = {}

    def add(self, aggregator_property_set):
        self.aggregator_properties[aggregator_property_set.name] = aggregator_property_set

    def __getitem__(self, op):
        if not op in self.aggregator_properties:
            raise ValueError('unrecognized aggregation operator: {}'.format(op))
        return self.aggregator_properties[op]

aggregator_properties = AggregatorProperties()

aggregator_properties.add(AggregatorPropertySet('__builtin__sum__', agg_sum, 'sum', int))
aggregator_properties.add(AggregatorPropertySet('__builtin__argmax__', agg_argmax, 'argmax', 1))
aggregator_properties.add(AggregatorPropertySet('__builtin__argmin__', agg_argmin, 'argmin', 1))
aggregator_properties.add(AggregatorPropertySet('__builtin__max__', agg_max, 'max', 0))
aggregator_properties.add(AggregatorPropertySet('__builtin__min__', agg_min, 'min', 0))
aggregator_properties.add(AggregatorPropertySet('__builtin__count__', agg_count, 'count', int))
aggregator_properties.add(AggregatorPropertySet('__builtin__avg__', agg_avg, 'avg', float))
aggregator_properties.add(AggregatorPropertySet('__builtin__mean__', agg_avg, 'mean', float))
aggregator_properties.add(AggregatorPropertySet('__builtin__var__', agg_var, 'var', float))
aggregator_properties.add(AggregatorPropertySet('__builtin__variance__', agg_var, 'variance', float))
aggregator_properties.add(AggregatorPropertySet('__builtin__std__', agg_std, 'std', float))
aggregator_properties.add(AggregatorPropertySet('__builtin__stdv__', agg_std, 'stdv', float))
aggregator_properties.add(AggregatorPropertySet('__builtin__select_one__', agg_select_one, 'select_one', 0))
aggregator_properties.add(AggregatorPropertySet('__builtin__concat__list__', agg_concat_list, 'concat', list))
aggregator_properties.add(AggregatorPropertySet('__builtin__concat__dict__', agg_concat_dict, 'concat', dict))
aggregator_properties.add(AggregatorPropertySet('__builtin__quantile__', agg_quantile, 'quantile', float))


