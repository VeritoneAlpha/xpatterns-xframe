from abc import ABCMeta

from xpatterns.toolkit.model import Model, ModelBuilder


# Models
class ClassifyModel(Model):
    __metaclass__ = ABCmeta

class LinearRegressionModel(ClassifyModel):
    pass

class LogisticRegressionModel(ClassifyModel):
    pass

class SVMModel(ClassifyModel):
    pass

class NaiveBayesModel(ClassifyModel):
    pass

class DecisionTreeModel(ClassifyModel):
    pass

class RandomFirestModel(ClassifyModel):
    pass

class GradientBoostedTreesModel(ClassifyModel):
    pass


# Builders
class ClassifierBuilder(ModelBuilder):
    __metaclass__ = ABCmeta

class LogisticRegressionWithSGD(ClassifierBuilder):
    pass


class LogisticRegressionWithLBFGS(ClassifierBuilder):
    pass





