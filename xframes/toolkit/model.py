import abc


class Model:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def predict():
        pass

class ModelBuilder:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def train():
        pass
