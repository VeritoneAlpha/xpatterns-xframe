from abc import ABCMeta, abstractmethod

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors

from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.classification import LogisticRegressionWithLBFGS

from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.tree import DecisionTree



from xframes.toolkit.model import Model, ModelBuilder

# Models
class RegressionModel(Model):
    __metaclass__ = ABCMeta

    def __init__(self, model):
        self.model = model

    def __repr__(self):
        return '{!r}'.format(self.model)

    def predict(self, features):
        return self.model.predict(features)

class LinearRegressionModel(RegressionModel):
    """
    Linear Regression Model.
    """
    pass

class LinearModel(RegressionModel):
    """
    Linear Model.
    """
    pass

class RidgeRegressionModel(RegressionModel):
    """
    Ridge Regression Model.
    """
    pass

class LassoModel(RegressionModel):
    """
    Lasso Model.
    """
    pass

class RandomForestModel(RegressionModel):
    """
    Random Forest Model
    """
    pass

class GradientBoostedTreesModel(RegressionModel):
    """
    Gradient Boosted Trees Model
    """
    pass

# Builders
def _make_labeled_features(row, label_col, feature_cols):
    label = row[label_col]
    features =[row[col] for col in feature_cols]
    return LabeledPoint(label, Vectors.dense(features))

def _make_features(row, feature_cols):
    features =[row[col] for col in feature_cols]
    return Vectors.dense(features)

#class RegressionBuilder(ModelBuilder):
class RegressionBuilder(object):
    __metaclass__ = ABCMeta
    @abstractmethod
    def __init__(self, xf, label_col, feature_cols):
        self.label_col = label_col
        self.feature_cols = feature_cols
        self.labeled_feature_vector = xf.apply(lambda row: 
                                    _make_labeled_features(row, label_col, feature_cols))

    def _labeled_feature_vector_rdd(self):
        """
        Returns the labeled feature vector rdd.
        """
        return self.labeled_feature_vector._impl.rdd.rdd

    @abstractmethod
    def train(self):
        pass

    def make_features(self, row):
        """
        Builds a feature vector from a row of input.
        """
        return _make_features(row, self.feature_cols)


class LinearRegressionWithSGDBuilder(RegressionBuilder):
    """
    LinearRegressionWithSGD Builder
    """
    def __init__(self):
        raise NotImplementedError()

class RidgeRegressionWithSGDBuilder(RegressionBuilder):
    """
    RidgeRegressionWithSGD Builder
    """
    def __init__(self):
        raise NotImplementedError()

class LassoWithSGDBuilder(RegressionBuilder):
    """
    LassoWithSGD Builder
    """
    def __init__(self):
        raise NotImplementedError()

