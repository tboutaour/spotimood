import abc
from pyspark.sql import DataFrame


class ModelRepository(abc.ABC):
    @abc.abstractmethod
    def get_prediction(self, data: DataFrame) -> DataFrame:
        pass
