from abc import ABCMeta

from xpatterns.toolkit.model import Model, ModelBuilder

# Models
class RegressionModel(Model):
    __metaclass__ = ABCMeta

class LinearModel(RegressionModel):
    """
    Linear Model.
    """
    pass

class LinearRegressionModel(RegressionModel):
    """
    Linear Regression Model.
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


# Builders
class RegressionBuilder(ModelBuilder):
    __metaclass__ = ABCMeta

class LinearRegressionWithSGD(RegressionBuilder):
    """
    LinearRegressionWithSGD Builder
    """
    pass

class RidgeRegressionWithSGD(RegressionBuilder):
    """
    RidgeRegressionWithSGD Builder
    """
    pass

class LassoWithSGD(RegressionBuilder):
    """
    LassoWithSGD Builder
    """
    pass
