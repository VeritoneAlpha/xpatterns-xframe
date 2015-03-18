import abc


class Model:
    __metaclass__ = abc.ABCmeta

    @abc.abstractmethod
    def predict():
        pass

class ModelBuilder:
    __metaclass__ = abc.ABCmeta

    @abc.abstractmethod
    def train():
        pass
