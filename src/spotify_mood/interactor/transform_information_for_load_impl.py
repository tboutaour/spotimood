from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from spotify_mood.interactor.transform_information_for_load import TransformInformationForLoad
import datetime


class TransformInformationForLoadImpl(TransformInformationForLoad):
    def apply(self, data: DataFrame) -> DataFrame:
        return data.withColumn('classified_at', lit(datetime.datetime.utcnow()))
