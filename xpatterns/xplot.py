
import traceback

import numpy as np
import matplotlib.pyplot as plt

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
            fig.canvas.draw()
        except Exception as e:
            print "got an exception!"
            print traceback.format_exc()
            print e


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
        q_lower = sk.quantile(lower_cutoff)
        q_upper = sk.quantile(upper_cutoff)
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
        except Exception as e:
            print "got an exception!"
            print traceback.format_exc()
            print e
