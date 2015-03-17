import os

# Builtin aggregators for groupby

def SUM(src_column):
  """
  Builtin sum aggregator for groupby

  Example: Get the sum of the rating column for each user.
  >>> xf.groupby("user",
                 {'rating_sum':aggregate.SUM('rating')})

  """
  return ("__builtin__sum__", [src_column])

def ARGMAX(agg_column, out_column):
  """
  Builtin arg maximum aggregator for groupby

  Example: Get the movie with maximum rating per user.

  >>> xf.groupby("user",
                 {'best_movie':aggregate.ARGMAX('rating','movie')})
  """
  return ("__builtin__argmax__", [agg_column, out_column])

def ARGMIN(agg_column, out_column):
  """
  Builtin arg minimum aggregator for groupby

  Example: Get the movie with minimum rating per user.

  >>> xf.groupby("user",
                 {'best_movie':aggregate.ARGMIN('rating','movie')})

  """
  return ("__builtin__argmin__", [agg_column, out_column])

def MAX(src_column):
  """
  Builtin maximum aggregator for groupby

  Example: Get the maximum rating of each user.

  >>> xf.groupby("user",
                 {'rating_max':aggregate.MAX('rating')})

  """
  return ("__builtin__max__", [src_column])

def MIN(src_column):
  """
  Builtin minimum aggregator for groupby

  Example: Get the minimum rating of each user.

  >>> xf.groupby("user",
                 {'rating_min':aggregate.MIN('rating')})

  """
  return ("__builtin__min__", [src_column])


def COUNT(*args):
  """
  Builtin count aggregator for groupby

  Example: Get the number of occurrences of each user.

  >>> xf.groupby("user",
                 {'count':aggregate.COUNT()})

  """
  # arguments if any are ignored
  return ("__builtin__count__", [""])



def AVG(src_column):
  """
  Builtin average aggregator for groupby. Synonym for aggregate.MEAN.

  Example: Get the average rating of each user.

  >>> xf.groupby("user",
                 {'rating_avg':aggregate.AVG('rating')})

  """
  return ("__builtin__avg__", [src_column])



def MEAN(src_column):
  """
  Builtin average aggregator for groupby. Synonym for aggregate.AVG.

  Example: Get the average rating of each user.

  >>> xf.groupby("user",
                 {'rating_mean':aggregate.MEAN('rating')})

  """
  return ("__builtin__avg__", [src_column])


def VAR(src_column):
  """
  Builtin variance aggregator for groupby. Synonym for aggregate.VARIANCE

  Example: Get the rating variance of each user.

  >>> xf.groupby("user",
                 {'rating_var':aggregate.VAR('rating')})

  """
  return ("__builtin__var__", [src_column])


def VARIANCE(src_column):
  """
  Builtin variance aggregator for groupby. Synonym for aggregate.VAR

  Example: Get the rating variance of each user.

  >>> xf.groupby("user",
                 {'rating_var':aggregate.VARIANCE('rating')})

  """
  return ("__builtin__var__", [src_column])


def STD(src_column):
  """
  Builtin standard deviation aggregator for groupby. Synonym for aggregate.STDV

  Example: Get the rating standard deviation of each user.

  >>> xf.groupby("user",
                 {'rating_std':aggregate.STD('rating')})

  """
  return ("__builtin__stdv__", [src_column])


def STDV(src_column):
  """
  Builtin standard deviation aggregator for groupby. Synonym for aggregate.STD

  Example: Get the rating standard deviation of each user.

  >>> xf.groupby("user",
                 {'rating_stdv':aggregate.STDV('rating')})

  """
  return ("__builtin__stdv__", [src_column])


def SELECT_ONE(src_column):
  """
  Builtin aggregator for groupby which selects one row in the group.

  Example: Get one rating row from a user.

  >>> xf.groupby("user",
                 {'rating':aggregate.SELECT_ONE('rating')})

  If multiple columns are selected, they are guaranteed to come from the
  same row. for instance:
  >>> xf.groupby("user",
                 {'rating':aggregate.SELECT_ONE('rating')},
                 {'item':aggregate.SELECT_ONE('item')})

  The selected 'rating' and 'item' value for each user will come from the
  same row in the XFrame.

  """
  # use seed to make selection repeatable
  # it would be more random to use the column name
  seed = src_column
  return ("__builtin__select_one__", [src_column, seed])

def CONCAT(src_column, dict_value_column=None):
  """
  Builtin aggregator that combines values from one or two columns in one group
  into either a dictionary value, list value or array value

  For example, to combine values from two columns that belong to one group into
  one dictionary value:

  >>> xf.groupby(["document"],
       {"word_count": aggregate.CONCAT("word", "count")})

  To combine values from one column that belong to one group into a list value:

  >>> xf.groupby(["user"],
       {"friends": aggregate.CONCAT("friend")})

  """
  if dict_value_column == None:
    return ("__builtin__concat__list__", [src_column])
  else:
    return ("__builtin__concat__dict__", [src_column, dict_value_column])


def QUANTILE(src_column, *args):
    """
    Builtin approximate quantile aggregator for groupby.
    Accepts as an argument, one or more of a list of quantiles to query.
    For instance:

    To extract the median

    >>> xf.groupby("user", {'rating_quantiles':aggregate.QUANTILE('rating', 0.5)})

    To extract a few quantiles

    >>> xf.groupby("user", {'rating_quantiles':aggregate.QUANTILE('rating', [0.25,0.5,0.75])})

    Or equivalently

    >>> xf.groupby("user", {'rating_quantiles':aggregate.QUANTILE('rating', 0.25,0.5,0.75)})

    The returned quantiles are guaranteed to have 0.5% accuracy. That is to say,
    if the requested quantile is 0.50, the resultant quantile value may be
    between 0.495 and 0.505 of the true quantile.
    """
    if len(args) == 1:
        quantiles = args[0]
    else:
        quantiles = list(args)

    if not hasattr(quantiles, '__iter__'):
        quantiles = [quantiles]
    query = ",".join([str(i) for i in quantiles])
    return ("__builtin__quantile__[" + query + "]", [src_column])
