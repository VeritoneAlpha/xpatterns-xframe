from abc import ABCMeta

from xpatterns.toolkit.model import Model, ModelBuilder


# Models
class ClassifyModel(Model):
    __metaclass__ = ABCMeta

class LinearRegressionModel(ClassifyModel):
    """
    Linear Regression Model
    """
    pass

class LogisticRegressionModel(ClassifyModel):
    """
    Logistic Regression Model
    """
    pass

class SVMModel(ClassifyModel):
    """
    SVM Model
    """
    pass

class NaiveBayesModel(ClassifyModel):
    """
    Naive Bayes Model
    """
    pass

class DecisionTreeModel(ClassifyModel):
    """
    Decision Tree Model
    """
    pass

class RandomForestModel(ClassifyModel):
    """
    Random Forest Model
    """
    pass

class GradientBoostedTreesModel(ClassifyModel):
    """
    Gradient Boosted Trees Model
    """
    pass


# Builders
class ClassifierBuilder(ModelBuilder):
    __metaclass__ = ABCMeta

class LogisticRegressionWithSGD(ClassifierBuilder):
    """
    LogisticRegressionWith SGD Builder
    """
    pass


class LogisticRegressionWithLBFGS(ClassifierBuilder):
    """
    LogisticRegressionWith BFGS Builder
    """
    pass





