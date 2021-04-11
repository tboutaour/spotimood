import abc
from pyspark.sql import DataFrame


class FilterByDate(abc.ABC):
    @abc.abstractmethod
    def apply(self, data: DataFrame, start_date, end_date) -> DataFrame:
        pass
