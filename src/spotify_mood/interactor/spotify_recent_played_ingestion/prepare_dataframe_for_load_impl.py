from pyspark.sql import DataFrame
from spotify_mood.interactor.spotify_ingestion.prepare_dataframe_for_load import PrepareDataframeForLoad
from pyspark.sql.functions import year, month, dayofmonth, hour


class PrepareDataframeForLoadImpl(PrepareDataframeForLoad):
    def apply(self, data: DataFrame) -> DataFrame:
        return data.withColumn('year', year('played_at'))\
            .withColumn('month', month('played_at'))\
            .withColumn('day', dayofmonth('played_at'))\
            .withColumn('hour', hour('played_at'))
