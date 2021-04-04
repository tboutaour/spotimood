import abc
from pandas import DataFrame

class ModelResource(abc.ABC):
    @abc.abstractmethod
    def load_model(self):
        pass

    @abc.abstractmethod
    def predict(self, data):
        pass