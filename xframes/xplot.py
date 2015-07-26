
import traceback
import operator
import math

from xframes.deps import matplotlib, HAS_MATPLOTLIB
if HAS_MATPLOTLIB:
  import matplotlib.pyplot as plt

import xframes


class XPlot(object):
    """
    Plotting library for xFrames.

    Creates simple data plots.

    Parameters
    ----------
    axes : list, optional
        The size of the axes.  Should be a four-element list.
        [x_origin, y_origin, x_length, y_length]
        Defaults to [0.0, 0.0, 1.5, 1.0]

    alpha : float, optional
        The opacity of the plot.
    """
    def __init__(self, xframe, axes=None, alpha=None):
        """
        Create a plotting object for the given XFrame.  

        Parameters
        ----------
        axes : list, optional
            The size of the axes.  Should be a four-element list.
            [x_origin, y_origin, x_length, y_length]
            Defaults to [0.0, 0.0, 1.5, 1.0]

        alpha : float, optional
            The opacity of the plot.
        """
        self.xframe = xframe
        self.axes = axes if axes else [0.0, 0.0, 1.5, 1.0]
        self.alpha = alpha or 0.5

    def make_barh(self, items, xlabel, ylabel, title=None):
        if items is not None and len(items) > 0:
            try:
                y_pos = range(len(items))
                vals = [int(key[1]) for key in items]
                labels = [str(key[0])[:30] for key in items]
                plt.barh(y_pos, vals, align='center', alpha=self.alpha)
                plt.yticks(y_pos, labels)
                plt.xlabel(xlabel)
                plt.ylabel(ylabel)
                if title:
                    plt.title(title)
                plt.show()
            except Exception as e:
                print "got an exception!"
                print traceback.format_exc()
                print e

    def make_bar(self, items, xlabel, ylabel, title=None):
        if items is not None:
            bins = len(items)
            try:
                counts = [col[1] for col in items]
                vals = [col[0] for col in items]
                y_pos = range(len(counts))
                plt.bar(y_pos, counts, align='center', alpha=self.alpha)
                plt.xlabel(xlabel)
                plt.ylabel(ylabel)
                delta = vals[1] - vals[0]
                min = vals[0]
                max = min + bins * delta
                n_ticks = 8
                tick_delta = (max - min)/float(n_ticks)
                step = int(bins/float(n_ticks))
                if step <= 0: step = 1
                tick_pos = range(0, bins+1, step)
                tick_labels = [ min + i *tick_delta for i in range(n_ticks+1)]
                tick_labels = [str(lab)[:5] for lab in tick_labels]
                plt.xticks(tick_pos, tick_labels)
                if title:
                    plt.title(title)
                plt.show()
            except Exception as e:
                print "got an exception!"
                print traceback.format_exc()
                print e

    def top_values(self, x_col, y_col, k=15, title=None, xlabel=None, ylabel=None):
        """
        Plot the top values of a column of data.

        Parameters
        ----------
        x_col : str
            A column name: the top values in this column are plotted.  These values must be numerical.

        y_col : str
            A column name: the values in this colum will be used to label the corresponding values
            in the x column.

        k : int, optional
            The number of values to plot.  Defaults to 15.

        title : str, optional
            A plot title.

        xlabel : str, optional
            A label for the X axis.

        ylabel : str, optional
            A label for the Y axis.

        Examples
        --------
        (Come up with an example)
        """
        top_rows = self.xframe.topk(x_col, k=k)
        items = [(row[y_col], row[x_col]) for row in top_rows]
        xlabel = xlabel or x_col
        ylabel = ylabel or y_col

        self.make_barh(items, xlabel, ylabel, title=title)

    def frequent_values(self, y_col, k=15, title=None, xlabel=None, ylabel=None):
        """
        Plots the number of occurances of specific values in a column.  

        The most frequent values are plotted.

        Parameters
        ----------
        y_col : str
            A column name: the column to plot.  The number of distinct occurrances of each value is
            calculated and plotted.  

        k : int, optional
            The number of different values to plot.  Defaults to 15.

        title : str, optional
            A plot title.

        xlabel : str, optional
            A label for the X axis.

        ylabel : str, optional
            A label for the Y axis.

        Examples
        --------
        (Need examples)

        """
        column = self.xframe[y_col]
        sk = column.sketch_summary()
        fi = sk.frequent_items()
        if len(fi) > 0:
            sorted_fi = sorted(fi.iteritems(), key=operator.itemgetter(1), reverse=True)
        frequent = [x for x in sorted_fi[:k] if x[1] > 1]
        self.make_barh(frequent, xlabel=xlabel, ylabel=ylabel, title=title)

    @staticmethod
    def create_histogram_buckets(vals, bins, min_val, max_val):
        delta = float(max_val - min_val)
        if delta == 0:
            return None, None
        n_buckets = bins or 50
        delta = float(delta) / n_buckets
        bucket_counts = [0] * n_buckets
        bucket_vals = [0] * n_buckets
        for i in range(0, n_buckets):
            bucket_vals[i] = min_val + (i * delta)

        def iterate_values(value_iterator):
            bucket_counts = [0] * n_buckets
            for val in value_iterator:
                if val is None or math.isnan(val):
                    continue
                b = int((val - min_val)/delta)
                if b >= n_buckets:
                    b = n_buckets - 1
                elif b < 0:
                    b = 0
                bucket_counts[b] += 1
            yield bucket_counts

        def merge_accumulators(acc1, acc2):
            return [ a1 + a2 for a1, a2 in zip(acc1, acc2)]

        accumulators = vals.__impl__._rdd.mapPartitions(iterate_values)
        bucket_counts = accumulators.reduce(merge_accumulators)
        return bucket_vals, bucket_counts

    def histogram(self,
                  column,
                  title=None,
                  bins=None,
                  sketch=None, 
                  xlabel=None, ylabel=None,
                  lower_cutoff=0.0, upper_cutoff=0.99):
        """ 
        Plot a histogram.

        All values greater than the cutoff (given as a quantile) are set equal to the cutoff.

        Parameters
        ----------
        column : XArray
            A column to display.

        title : str, optional
            A plot title.

        bins : int, optional
            The number of bins to use.  Defaults to 50.

        sketch : Sketch, optional
            The column sketch.  If this is available, then it saves time not to recompute it.

        xlabel : str, optional
            A label for the X axis.

        ylabel : str, optional
            A label for the Y axis.

        lower_cutoff : float, optional
            This is a quantile value, between 0 and 1.  
            Values below this cutoff are placed in the first bin.
            Defaults to 0.

        upper_cutoff : float, optional
            This is a quantile value, between 0 and 1.  
            Values above this cutoff are placed in the last bin.
            Defaults to 0.99.

        bins : int, optional
            The number of bins to use.  Defaults to 50.

        Examples
        --------
        (Need examples)
        """
        if lower_cutoff < 0.0 or lower_cutoff > 1.0:
            raise ValueError('lower cutoff must be between 0.0 and 1.0')
        if upper_cutoff < 0.0 or upper_cutoff > 1.0:
            raise ValueError('upper cutoff must be between 0.0 and 1.0')
        if lower_cutoff >= upper_cutoff:
            raise ValueError('lower cutoff must be less than upper cutoff')

        bins = bins or 50
        sk = sketch or column.sketch_summary()
        q_epsilon = 0.01
        q_lower = float(sk.quantile(lower_cutoff)) - q_epsilon
        q_upper = float(sk.quantile(upper_cutoff)) + q_epsilon
        xlabel = xlabel or 'Value'
        ylabel = ylabel or 'Count'
        vals = column.dropna()

        def enforce_cutoff(x):
            if x < q_lower: return q_lower
            if x > q_upper: return q_upper
            return x
        vals = vals.apply(enforce_cutoff)
        bucket_counts, bucket_vals = self.create_histogram_buckets(column, bins, sk.min(), sk.max())
        column = [ (x, y) for x, y in zip(bucket_counts, bucket_vals)]
        self.make_bar(column, xlabel=xlabel, ylabel=ylabel, title=title)

    def col_info(self, col_name, table_name=None, title=None, topk=None, bins=None, cutoff=False):
        """ 
        Print column summary information.

        The number of the most frequent values is shown.
        If the column to summarize is numerical, then a histogram is also shown.
        the most frequent values is shown.

        Parameters
        ----------
        col_name : str
            The column name to summarize.

        table_name : str, optional
            The table name; used to labeling only.  The table that us used for the data
            is given in the constructor.

        title : str, optional
            The plot title.

        topk: int, optional
            The number of frequent items to show.

        bins : int, optional
            The number of bins in a histogram.

        cutoff : float, optional
            The number to use as an upper cutoff, if the plot is a histogram.

        Examples
        --------
        (Need examples)
        """

        title = title or table_name
        table_name = table_name or ''
        print 'Table Name:  ', table_name
        print 'Column Name: ', col_name
        column = self.xframe[col_name]
        print 'Column Type: ', column.dtype().__name__
        sk = column.sketch_summary()
        print 'Rows:        ', sk.size()
        unique_items = sk.num_unique()
        print 'Unique Items:', unique_items
        print 'Approximate Frequent Items:'
        fi = sk.frequent_items()
        topk = topk or 15
        if len(fi) == 0:
            print '    None'
            top = None
        else:
            sorted_fi = sorted(fi.iteritems(), key=operator.itemgetter(1), reverse=True)
            top = [x for x in sorted_fi[:topk] if x[1] > 1]
            for key in top: print '   {:10}  {:10}'.format(key[1], key[0])
        col_type = self.xframe[col_name].dtype()
        if col_type is int or col_type is float:
            # number: show a histogram
            print 'Num Undefined:', sk.num_undefined()
            print 'Min:          ', sk.min()
            print 'Max:          ', sk.max()
            print 'Mean:         ', sk.mean()
            if unique_items > 1:
                print 'StDev:        ', sk.std()
                print 'Distribution Plot'
                upper_cutoff = cutoff or 1.0
                self.histogram(column, title=title, bins=bins, sketch=sk, upper_cutoff=upper_cutoff)

        # ordinal: show a bar chart of frequent values
        # set x_col and y_col
        if top is not None:
            vals = xframes.XArray([key[0] for key in top], dtype=col_type)
            counts = xframes.XArray([key[1] for key in top], dtype=int)
            x_col = 'Count'
            y_col = col_name
            tmp = xframes.XFrame({x_col: counts, y_col: vals})
            tmp.show().top_values(x_col, y_col, title=title, k=topk)

