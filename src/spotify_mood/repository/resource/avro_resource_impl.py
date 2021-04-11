from typing import List

from pyspark.sql import DataFrame, SparkSession
from spotify_mood.repository.resource.avro_resource import AvroResource


class AvroResourceImpl(AvroResource):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, path: List[str]) -> DataFrame:
        return self.spark.read.format("avro").load(path)

    def read_with_schema(self, path: List[str], schema) -> DataFrame:
        return self.spark.read.format("avro").schema(schema).load(path)

    def write_with_partition(self, data: DataFrame, path: str, partition_by):
        data.write.mode('append').partitionBy(*partition_by).format("avro").save(path)
