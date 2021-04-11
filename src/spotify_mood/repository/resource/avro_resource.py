import abc
from typing import List

from pyspark.sql import DataFrame


class AvroResource(abc.ABC):

    @abc.abstractmethod
    def read(self, path: List[str]) -> DataFrame:
        pass

    @abc.abstractmethod
    def read_with_schema(self, path: List[str], schema) -> DataFrame:
        pass

    @abc.abstractmethod
    def write_with_partition(self, data, path, partition_by):
        pass
