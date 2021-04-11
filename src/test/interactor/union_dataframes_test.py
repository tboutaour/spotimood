from pyspark.sql.types import StructType, StructField, IntegerType

from spotify_mood.interactor.union_dataframes_impl import UnionDataframesImpl
from test.pyspark_test_base import PySparkTestBase


class UnionDataframe(PySparkTestBase):

    def test_union(self):
        schema = StructType([StructField("a", IntegerType(), True),
                             StructField("b", IntegerType(), True)])
        input_data = self.spark.createDataFrame(data=[(1, 3),
                                                      (1, 3),
                                                      (1, 3)], schema=schema)

        input_data.show()
        expected_data = self.spark.createDataFrame(data=[(1, 3)for _ in range(1, 7)], schema=schema)
        union_dataframe = UnionDataframesImpl()
        result_df = union_dataframe.apply([input_data, input_data])

        self.assertDataFrameEquals(expected_data, result_df)