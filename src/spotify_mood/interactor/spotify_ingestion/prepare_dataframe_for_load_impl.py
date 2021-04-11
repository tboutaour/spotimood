from pyspark.sql import DataFrame
from spotify_mood.interactor.spotify_ingestion.prepare_dataframe_for_load import PrepareDataframeForLoad
from pyspark.sql.functions import year, month, dayofmonth


class PrepareDataframeForLoadImpl(PrepareDataframeForLoad):
    def apply(self, data: DataFrame) -> DataFrame:
        return data.withColumn('year', year('added_at'))\
            .withColumn('month', month('added_at'))\
            .withColumn('day', dayofmonth('added_at'))\
            .withColumnRenamed('track_id__', 'track_id')\
            .withColumnRenamed('track_name__', 'track_name')\
            .withColumnRenamed('artists__', 'artists')
