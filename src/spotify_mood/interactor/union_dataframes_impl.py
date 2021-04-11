from pyspark.sql import DataFrame
from spotify_mood.interactor.union_dataframes import UnionDataframes
from functools import reduce


class UnionDataframesImpl(UnionDataframes):
    def apply(self, data: [DataFrame]) -> DataFrame:
        return reduce(DataFrame.union, data)
