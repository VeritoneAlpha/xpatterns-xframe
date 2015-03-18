
import traceback
import operator

import numpy as np
import matplotlib.pyplot as plt

from xpatterns.aggregate import COUNT

class XPlot:
    def __init__(self, xframe, axes=None, alpha=None):
        self.xframe = xframe
        self.axes = axes if axes else [0.0, 0.0, 1.5, 1.0]
        self.alpha = alpha or 0.5

    def top_values(self, x_col, y_col, k=15, title=None, xlabel=None, ylabel=None):
        top_rows = self.xframe.topk(x_col, k)
        items = [(row[y_col], row[x_col]) for row in top_rows]
    
        try:
            y_pos = np.arange(len(items))
            vals = [int(key[1]) for key in items]
            labels = [key[0] for key in items]
            fig = plt.figure()
            axes = fig.add_axes(self.axes)
            axes.barh(y_pos, vals, align='center', alpha=self.alpha)
            axes.set_yticks(y_pos)
            axes.set_yticklabels(labels)
            xlabel = xlabel or x_col
            ylabel = ylabel or y_col
            axes.set_xlabel(xlabel)
            axes.set_ylabel(ylabel)
            if title:
                axes.set_title(title)
#            fig.canvas.set_target('ipynb')
#            fig.canvas.draw()
#            fig.show()
#            fig.close()
        except Exception as e:
            print "got an exception!"
            print traceback.format_exc()
            print e


    def frequent_values(self, y_col, k=15, title=None, xlabel=None, ylabel=None):
        count = self.xframe.groupby(y_col, {'Count': COUNT})
        count.show.top_values('Count', y_col, k, title, xlabel, ylabel)

    def histogram(self, col_name, title=None, 
                       lower_cutoff=0.0, upper_cutoff=0.99, 
                       bins=None, xlabel=None, ylabel=None):
        """ 
        Plot histogram.

        All values greater than the cutoff (given as a quantile) are set equal to the cutoff.
        """
        if lower_cutoff < 0.0 or lower_cutoff > 1.0:
            raise ValueError('lower cutoff must be between 0.0 and 1.0')
        if upper_cutoff < 0.0 or upper_cutoff > 1.0:
            raise ValueError('upper cutoff must be between 0.0 and 1.0')
        if lower_cutoff >= upper_cutoff:
            raise ValueError('lower cutoff must be less than upper cutoff')
        if not col_name in self.xframe.column_names():
            raise ValueError('column name {} is not in the XFrame'.format(col_name))

        bins = bins or 50
        sk = self.xframe[col_name].sketch_summary()
        q_epsilon = 0.01
        q_lower = float(sk.quantile(lower_cutoff)) - q_epsilon
        q_upper = float(sk.quantile(upper_cutoff)) + q_epsilon
        try:
            fig = plt.figure()
            axes = fig.add_axes(self.axes)
            xlabel = xlabel or col_name
            ylabel = ylabel or 'Count'
            vals = self.xframe[col_name].dropna()
            def enforce_cutoff(x):
                if x < q_lower: return q_lower
                if x > q_upper: return q_upper
                return x
            vals = vals.apply(enforce_cutoff)
            vals = list(vals)
            axes.hist(vals, bins=bins, alpha=self.alpha)
            axes.set_xlabel(xlabel)
            axes.set_ylabel(ylabel)
            if title:
                axes.set_title(title)
#            fig.canvas.set_target('ipynb')
#            fig.canvas.draw()
#            fig.show()
#            fig.close()
        except Exception as e:
            print "got an exception!"
            print traceback.format_exc()
            print e

    def col_info(self, col_name, table_name=None, title=None, bins=None, cutoff=False):
        """ 
        Print column summary information.
        """

        title = title or table_name
        table_name = table_name or ''
        print 'Name:', table_name, col_name
        sk = self.xframe[col_name].sketch_summary()
        print 'Number:', sk.size()
        unique_items = sk.num_unique()
        print 'Unique Items:', unique_items
        print 'Frequent Items:'
        fi = sk.frequent_items()
        if len(fi) == 0:
            print '    None'
            return
        else:
            sorted_fi = sorted(fi.iteritems(), key=operator.itemgetter(1), reverse=True)
            top = sorted_fi[:10]
            for key in top: print '   {:10}  {:10}'.format(key[1], key[0])
        col_type = self.xframe[col_name].dtype()
        if col_type is int or col_type is float:
            # number: show a histogram
            print 'Num Undefined:', sk.num_undefined()
            print 'Min:', sk.min()
            print 'Max:', sk.max()
            print 'Mean:', sk.mean()
            if unique_items > 1:
                print 'StDev:', sk.std()
                print 'Distribution Plot'
                upper_cutoff = cutoff or 1.0
                self.histogram(col_name, title=title, upper_cutoff=upper_cutoff, bins=bins)
        else:
            # ordinal: show a histogram of frequent values
            # set x_col and y_col     compute y_col ?
            tmp = self.xframe.groupby(col_name, {'Count': COUNT})
            x_col = 'Count'
            y_col = col_name
            tmp.show.top_values(x_col, y_col, title=title)
