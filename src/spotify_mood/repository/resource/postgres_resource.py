import abc
from pyspark.sql import DataFrame


class PostgresResource(abc.ABC):

    @abc.abstractmethod
    def get_many_from_table(self, table, fields, conditions):
        pass

    @abc.abstractmethod
    def get_one_from_table(self, table, fields, conditions):
        pass

    @abc.abstractmethod
    def update_many(self, table, updates, conditions):
        pass

    @abc.abstractmethod
    def set_dataframe_to_table(self, df, table):
        pass
