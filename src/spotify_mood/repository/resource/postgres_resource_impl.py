import abc
from pyspark.sql import DataFrame


class PostgresResource(abc.ABC):

    @abc.abstractmethod
    def get_table(self, db, table) -> DataFrame:
        pass
