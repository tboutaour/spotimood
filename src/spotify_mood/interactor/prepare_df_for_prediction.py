import abc
from pandas import DataFrame


class PrepareDfForPrediction(abc.ABC):
    @abc.abstractmethod
    def apply(self, data: DataFrame) -> DataFrame:
        pass