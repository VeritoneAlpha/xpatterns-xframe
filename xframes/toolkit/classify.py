import os
from abc import ABCMeta, abstractmethod

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors, DenseVector

from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.classification import LogisticRegressionWithLBFGS

from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.tree import DecisionTree

from xframes.deps import matplotlib, HAS_MATPLOTLIB
if HAS_MATPLOTLIB:
    import matplotlib.pyplot as plt

from xframes.spark_context import CommonSparkContext
from xframes.toolkit.model import Model, ModelBuilder
from xframes import XFrame, XArray
from xframes.xarray_impl import XArrayImpl
from xframes.util import delete_file_or_dir

__all__ = ['LogisticRegressionWithSGDBuilder', 
           'LogisticRegressionWithLBFGSBuilder', 
           'SVMWithSGDBuilder', 
           'NaiveBayesBuilder', 
           'DecisionTreeBuilder']

# Models
class ClassificationModel(Model):
    __metaclass__ = ABCMeta

    def __init__(self, model, feature_cols):
        self.model = model
        # need this to handle predict over dict
        self.feature_cols = feature_cols

    def __repr__(self):
        return '{!r}'.format(self.model)

    @staticmethod
    def _file_paths(path):
        """
        Return the file paths for model and metadata.
        """
        model_path = os.path.join(path, 'model')
        metadata_path = os.path.join(path, '_metadata')
        return (model_path, metadata_path)

    def _base_predict(self, data):
        """
        Call the model's predict function.
        
        Data can be a single item or a collection of items.
        """
        
        features = self.make_features(data)
        if isinstance(features, DenseVector): 
            return self.model.predict(features)
        if isinstance(features, XArray) and issubclass(features.dtype(), DenseVector):
            res = self.model.predict(features.to_spark_rdd())
            return XArray.from_rdd(res, float)

        raise TypeError('must pass a DenseVector or XArray of DenseVector')

    def predict(self, data):
        """
        Call the base predictor.
        """
        return self._base_predict(data)

    def _base_evaluate(self, data, labels):
        """
        Evaluate the performance of the classifier.

        Use the data to make predictions, then test the effectiveness of 
        the predictions against the labels.

        The data must be a collection of items (XArray of SenseVector).

        Returns
        -------
        out : A list of:
            - overall correct prediction proportion
            - true positive proportion
            - true negative proportion
            - false positive proportion
            - false negative proportion
        """
        results = XFrame()
        predictions = self._base_predict(data)
        results['predicted'] = predictions
        results['actual'] = labels
#        print results
        def evaluate(row):
            prediction = row['predicted']
            actual = row['actual']
            return {'correct': 1 if prediction == actual else 0,
                    'true_pos': 1 if prediction == 1 and actual == 1 else 0,
                    'true_neg': 1 if prediction == 0 and actual == 0 else 0,
                    'false_pos': 1 if prediction == 1 and actual == 0 else 0,
                    'false_neg': 1 if prediction == 0 and actual == 1 else 0,
                    'positive': 1 if actual == 1 else 0,
                    'negative': 1 if actual == 0 else 0
                    }

        score = results.apply(evaluate)
        def sum_item(item):
            return score.apply(lambda x: x[item]).sum()

        all_scores = float(len(labels))
        correct = float(sum_item('correct'))
        tp = float(sum_item('true_pos'))
        tn = float(sum_item('true_neg'))
        fp = float(sum_item('false_pos'))
        fn = float(sum_item('false_neg'))
        pos = float(sum_item('positive'))
        neg = float(sum_item('negative'))

        # precision = true pos / (true pos + false pos)
        # recall = true pos / (true pos + false neg)
        # true pos rate = true pos / positive
        # false pos rate = false pos / negative
        result = {}
        result['correct'] = correct
        result['true_pos'] = tp
        result['true_neg'] = tn
        result['false_pos'] = fp
        result['false_neg'] = fn
        result['all'] = all_scores
        result['accuracy'] = correct / all_scores if all_scores > 0 else float('nan')
        result['precision'] = tp / (tp + fp) if (tp + fp) > 0 else float('nan')
        result['recall'] = tp / (tp + fn) if (tp + fn) > 0 else float('nan')
        result['tpr'] = tp / pos if pos > 0 else float('nan')
        result['fpr'] = fp / neg if neg > 0 else float('nan')
        return result
        
    def evaluate(self, data, labels):
        return self._base_evaluate(data, labels)

    # Need a function that evaluates at a set of points and does the plots
    # It should return the results.
    def plot_roc(self, metrics=None, xlabel=None, ylabel=None, title=None, **kwargs):
        metrics = metrics or self.metrics
        if metrics is None:
            raise ValueError("metrics should be passed in or computed by calling 'evaluate'")
        fig = plt.figure()
        tpr = [ ev['tpr'] for ev in metrics]
        fpr = [ ev['fpr'] for ev in metrics]
        auc = sum(tpr) / len(tpr)
        ax = [0.0, 0.0, 1.0, 1.0]
        axes = fig.add_axes(ax)
        axes.set_xlabel(xlabel if xlabel else 'False Positive Rate') 
        axes.set_ylabel(ylabel if ylabel else 'True Positive Rate')
        axes.set_title(title if title else 'ROC Curve  AUC: {:5.3f}'.format(auc))

        axes.plot([0, 1], [0, 1], '--') 
        axes.plot(fpr, tpr, **kwargs)
        plt.show()
#        print 'fpr', fpr
#        print
#        print 'tpr', tpr
        
    def plot_pr(self, metrics=None, xlabel=None, ylabel=None, title=None, **kwargs):
        metrics = metrics or self.metrics
        if metrics is None:
            raise ValueError("metrics should be passed in or computed by calling 'evaluate'")
        fig = plt.figure()
        r = [ ev['recall'] for ev in metrics]
        p = [ ev['precision'] for ev in metrics]
        ax = [0.0, 0.0, 1.0, 1.0]
        axes = fig.add_axes(ax)
        axes.set_xlabel(xlabel if xlabel else 'Recall') 
        axes.set_ylabel(ylabel if ylabel else 'Precision')
        axes.set_title(title if title else 'Precision Recall Curve')
        
        axes.plot([0, 1], [1, 0], '--')
        axes.plot(r, p, **kwargs)
        plt.show()
 #       print 'r', r
#        print 'p', p
        
    def eval_and_plot(self, features, labels, num_points=10):
        metrics = [self.evaluate(features, labels, 
                            threshold=float(i)/num_points)
           for i in range(0, num_points + 1)]
        
        self.plot_roc(metrics)
        self.plot_pr(metrics)
        self.metrics = metrics
        return metrics

    def save(self, path):
        """
        Save a model.

        The model can be saved, then reloaded later to provide recommendations.

        Parameters
        ----------
        path : str
            The path where the model will be saved.
            This should refer to a file, not to a directory.
            Three items will be stored here: the underlying model parameters, the original ratings,
            and the column names.  These are stored with suffix '.model', '.ratings', and
            '.metadata'.
        """
        sc = CommonSparkContext.Instance().sc
        delete_file_or_dir(path)
        os.makedirs(path)
        model_path, metadata_path = self._file_paths(path)
        # save model
        self.model.save(sc, model_path)
        # save metadata
        model_type = self.__class__.__name__
        metadata = [model_type, self.feature_cols]
        with open(metadata_path, 'w') as f:
            pickle.dump(metadata, f)

    @classmethod
    def load(cls, path):
        """
        Load a model that was saved previously.

        Parameters
        ----------
        path : str
            The path where the model files are stored.
            This is the same path that was passed to ``save``.
            There are three files/directories based on this path, with
            extensions '.model', '.ratings', and '.metadata'.

        Returns
        -------
        out : A classification model.
            A model that can be used to predict ratings.
        """
        sc = CommonSparkContext.Instance().sc
        model_path, metadata_path = cls._file_paths(path)
        
        # load metadata
        with open(metadata_path) as f:
            model_type, feature_cols = pickle.load(f)
#        print 'model_type', model_type
#        print 'feature_cols', feature_cols
        try:
            klass = getattr(xframes.toolkit.classify, model_type)
        except:
            raise ValueError('model type is not valid: {}'.format(model_type))

        # load model
        # TODO use the class to call constructor
        model = klass.load(sc, model_path)

        return cls(model, feature_cols)


    def make_features(self, data):
        """
        Builds a feature vector from a row or XFrame of input data.

        Parameters
        ----------
        data : dict or XFrame
            Either a single row of data (as a dict) or an XFrame of input data.
            The data should be in the same format as was used in training.

        Returns
        -------
        out : a DenseVector if the input was a dict, or an XArray of DenseVector if the
            inut is an XFrame
        """
        def build_features(row, feature_cols):
            # collect values into dense vector
            features =[row[col] for col in feature_cols]
            return Vectors.dense(features)

        if isinstance(data, dict): 
            return build_features(data, self.feature_cols)
        if isinstance(data, XFrame): 
            # must make copy to avoid pickle errors
            feature_cols = self.feature_cols
            return data.apply(lambda row: build_features(row, feature_cols))
        raise TypeError('must pass a dict (row) or XFrame')


class LinearBinaryClassificationModel(ClassificationModel):
    """ 
    Common model type for LogisticRegressionModel and SVMModel
    """
    def predict(self, data, threshold=0.5):
        if threshold is not None:
            self.model.setThreshold(threshold)
        else:
            self.model.clearThreshold()
        return self._base_predict(data)

    def evaluate(self, data, labels, threshold=0.5):
        if threshold is not None:
            self.model.setThreshold(threshold)
        else:
            self.model.clearThreshold()
        metrics = self._base_evaluate(data, labels)
        metrics['threshold'] = threshold
        return metrics

class LinearRegressionModel(ClassificationModel):
    """
    Linear Regression Model.
    """
    pass

class LogisticRegressionModel(LinearBinaryClassificationModel):
    """
    Logistic Regression Model
    """
    pass

class SVMModel(LinearBinaryClassificationModel):
    """
    SVM Model
    """
    pass

class NaiveBayesModel(ClassificationModel):
    """
    Naive Bayes Model
    """
    def evaluate(self, features, labels, threshold=0.5):
        nb_features = XFrame()
        for col in features.column_names():
            nb_features[col] = features[col].clip(lower=0.0)
        metrics = self._base_evaluate(features, labels)

        metrics['threshold'] = threshold
        return metrics

class DecisionTreeModel(ClassificationModel):
    """
    Decision Tree Model
    """
    pass




# Builders
class ClassificationBuilder(ModelBuilder):
    __metaclass__ = ABCMeta

    @abstractmethod
    # provide standardization as a stand-alone function
    # and remove it from here.
    # The labeled feature vector would have to be re-done after this
    #    so delay doing that also
    def __init__(self, features, labels, standardize=False):
        self.standardize = standardize
        self.means = None
        self.stdevs = None
        if standardize:
            self.features = self._standardize(features)
        else:
            self.features = features
        self.labels = labels
        self.feature_cols = features.column_names()
        labeled_feature_vector = XFrame(features)
        label_col = 'label'     # TODO what if there is a feature with this name ?
        feature_cols = self.feature_cols   # need local reference
        labeled_feature_vector[label_col] = labels
        def build_labeled_features(row):
            label = row[label_col]
            features =[row[col] for col in feature_cols]
            return LabeledPoint(label, features)

        self.labeled_feature_vector = labeled_feature_vector.apply(build_labeled_features)
        

    def _labeled_feature_vector_rdd(self):
        """
        Returns the labeled feature vector rdd.
        """
        return self.labeled_feature_vector.to_spark_rdd()

    @abstractmethod
    def train(self):
        pass

    @staticmethod
    def _make_standard(item, mean, stdev):
        return (item - mean) / float(stdev)
        
    def _standardize(self, features):
        def standardize_col(features, col):
            mean = features[col].mean()
            stdev = features[col].std()
            if stdev == 0: return None
            self.means[col] = mean
            self.stdevs[col] = stdev
            return features[col].apply(lambda item: 
                                       ClassificationBuilder._make_standard(item, mean, stdev))

        std_features = XFrame()
        for col in features.column_names():
            new_col = standardize_col(features, col)
            if new_col is not None: std_features[col] = new_col
        return std_features



class LogisticRegressionWithSGDBuilder(ClassificationBuilder):
    """
    LogisticRegressionWith SGD Builder

    Builds a Logistic Regression model from features and labels.
    
    Parameters
    ----------
    features : XFrame
        The features used to build the model.

    labels : XArray
        The labels.  Each label applies to the corresponding features.
    """
    def __init__(self, features, labels, standardize=False):
        super(LogisticRegressionWithSGDBuilder, self).__init__(features, labels, standardize)

    def train(self, num_iterations=10):
        model = LogisticRegressionWithSGD.train(
            self._labeled_feature_vector_rdd(), 
            num_iterations)
        return LogisticRegressionModel(model, self.feature_cols)

class LogisticRegressionWithLBFGSBuilder(ClassificationBuilder):
    """
    LogisticRegressionWith LBFGS Builder

    Builds a Logistic Regression model from features and labels.
    
    Parameters
    ----------
    features : XFrame
        The features used to build the model.

    labels : XArray
        The labels.  Each label applies to the corresponding features.
    """
    def __init__(self, features, labels, standardize=False):
        super(LogisticRegressionWithLBFGSBuilder, self).__init__(features, labels, standardize)

    def train(self, num_iterations=10):
        model = LogisticRegressionWithLBFGS.train(
            self._labeled_feature_vector_rdd(), 
            num_iterations)
        return LogisticRegressionModel(model, self.feature_cols)

class SVMWithSGDBuilder(ClassificationBuilder):
    """
    SVM SGD Builder

    Builds a SVM model from features and labels.
    
    Parameters
    ----------
    features : XFrame
        The features used to build the model.

    labels : XArray
        The labels.  Each label applies to the corresponding features.
    """
    def __init__(self, features, labels, standardize=False):
        super(SVMWithSGDBuilder, self).__init__(features, labels, standardize)

    def train(self, num_iterations=10):
        # TODO support all the keyword training params
        model = SVMWithSGD.train(self._labeled_feature_vector_rdd(), num_iterations)
        return SVMModel(model, self.feature_cols)

class NaiveBayesBuilder(ClassificationBuilder):
    """
    Naive Bayes Builder

    Builds a Naive Bayes model from features and labels.
    
    Parameters
    ----------
    features : XFrame
        The features used to build the model.

    labels : XArray
        The labels.  Each label applies to the corresponding features.
    """
    def __init__(self, features, labels, standardize=False):
        nb_features = XFrame()
        for col in features.column_names():
            nb_features[col] = features[col].clip(lower=0.0)
        super(NaiveBayesBuilder, self).__init__(nb_features, labels)

    def train(self, lambda_=1.0):
        model = NaiveBayes.train(self._labeled_feature_vector_rdd(), lambda_)
        return NaiveBayesModel(model, self.feature_cols)

class DecisionTreeBuilder(ClassificationBuilder):
    """
    Decision Tree Builder

    Builds a decision tree model from features and labels.
    
    Parameters
    ----------
    features : XFrame
        The features used to build the model.

    labels : XArray
        The labels.  Each label applies to the corresponding features.
    """
    def __init__(self, features, labels, standardize=False):
        super(DecisionTreeBuilder, self).__init__(features, labels)

    def train(self, num_classes=2, categorical_features=None, max_depth=5):
        categorical_features = categorical_features or {}
        model = DecisionTree.trainClassifier(
            self._labeled_feature_vector_rdd(),
            numClasses=num_classes, 
            categoricalFeaturesInfo=categorical_features,
            maxDepth=max_depth)
        return DecisionTreeModel(model, self.feature_cols)

def create(features, labels, classifier_type='DecisionTree', standardize=False, **kwargs):
    """
    Create a classification model.

    Parameters
    ----------

    features : XFrame
        A table containing the training data.

    labels : XArray
        An XArray containing labels.  Each row provides the label for the 
        corresponding row of features.

    classifier_type : string, optional
        The type of classifier.  Optons are:
        * LogisticRegressionWithSGD
        * LogisticRegressionWithLBFGS
        * SVMWithSGD
        * NaiveBayes
        * DecisionTree
        
    standardize : bool, optional
        If set, standardize the features by transforming into mean of zero and standard deviation of 1.
    kwargs : various
        Keyword arguments, passed to the train method.
        See ``pyspark.mllib.classification`` module for details on the train arguments.
    """

    
    if classifier_type == 'LogisticRegressionWithSGD':
        return LogisticRegressionWithSGDBuilder(features, labels, standardize=standardize) \
            .train(**kwargs)
    if classifier_type == 'LogisticRegressionWithLBFGS':
        return LogisticRegressionWithLGFBSBuilder(features, labels, standardize=standardize) \
            .train(*kwargs)
    if classifier_type == 'SVMWithSGD':
        return SVMWithSGDBuilder(features, labels, standardize=standardize) \
            .train(**kwargs)
    if classifier_type == 'NaiveBayes':
        return NaiveBayesBuilder(features, labels, standardize=standardize) \
            .train(**kwargs)
    if classifier_type == 'DecisionTree':
        return DecisionTreeBuilder(features, labels, standardize=standardize) \
            .train(**kwargs)
    raise ValueError('classifier type is not recognized')
