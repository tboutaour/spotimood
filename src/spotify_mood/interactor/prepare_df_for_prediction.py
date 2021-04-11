import abc
from pyspark.sql import DataFrame


class PrepareDfForPrediction(abc.ABC):
    @abc.abstractmethod
    def apply(self, data: DataFrame) -> DataFrame:
        pass