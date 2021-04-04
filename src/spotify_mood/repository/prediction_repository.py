import abc
from pandas import DataFrame


class PredictionRepository(abc.ABC):
    @abc.abstractmethod
    def store_prediction_dataframe(self, df, table):
        pass
