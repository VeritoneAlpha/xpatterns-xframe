from abc import ABCMeta

from xpatterns.toolkit.model import Model, ModelBuilder

# Models
class RegressionModel(Model):
    __metaclass__ = ABCmeta

class LinearModel(RegressionModel):
    pass

class LinearRegressionModel(RegressionModel):
    pass

class RidgeRegressionModel(RegressionModel):
    pass

class LassoModel(RegressionModel):
    pass


# Builders
class RegressionBuilder(ModelBuilder):
    __metaclass__ = ABCmeta

class LinearRegressionWithSGD(RegressionBuilder):
    pass

class RidgeRegressionWithSGD(RegressionBuilder):
    pass

class LassoWithSGD(Regressor):
    pass
