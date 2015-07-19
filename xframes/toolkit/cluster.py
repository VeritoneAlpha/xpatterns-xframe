from abc import ABCMeta

from xframes.toolkit.model import Model, ModelBuilder



# Models
class ClusterModel(Model):
    __metaclass__ = ABCMeta

class KMeansModel(ClusterModel):
    """
    KMeans Model
    """
    pass

class GaussianMixtureModel(ClusterModel):
    """
    Gaussian Mixture Model
    """
    pass


# Builders
class ClusterBuilder(ModelBuilder):
    __metaclass__ = ABCMeta

class KMeans(ClusterBuilder):
    """
    KMeans Builder
    """
    pass

class GaussianMixture(ClusterBuilder):
    """
    Gaussian Moxture Builder
    """
    pass

