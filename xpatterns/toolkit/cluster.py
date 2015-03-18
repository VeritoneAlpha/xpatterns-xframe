from abc import ABCMeta

from xpatterns.toolkit.model import Model, ModelBuilder



# Models
class ClusterModel(Model):
    __metaclass__ = ABCMeta

class KMeansModel(ClusterModel):
    pass

class GaussianMixtureModel(ClusterModel):
    pass


# Builders
class ClusterBuilder(ModelBuilder):
    __metaclass__ = ABCMeta

class KMeans(ClusterBuilder):
    pass

class GaussianMixture(ClusterBuilder):
    pass

