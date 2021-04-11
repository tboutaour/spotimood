from pyspark.sql import DataFrame
from spotify_mood.interactor.spotify_ingestion.filter_by_date import FilterByDate
from pyspark.sql.functions import col, to_timestamp, lit
from datetime import datetime


class FilterByDateImpl(FilterByDate):
    def apply(self, data: DataFrame, start_date, end_date) -> DataFrame:
        return data.withColumn("played_at", to_timestamp(col('played_at'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
            .filter((col('played_at') > lit(start_date.strftime("%Y-%m-%d %H:%M:%S"))) &
                    (col('played_at') < lit(end_date.strftime("%Y-%m-%d %H:%M:%S"))))
