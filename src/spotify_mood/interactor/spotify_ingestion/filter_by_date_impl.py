from pyspark.sql import DataFrame
from spotify_mood.interactor.spotify_ingestion.filter_by_date import FilterByDate
from pyspark.sql.functions import col, to_timestamp


class FilterByDateImpl(FilterByDate):
    def apply(self, data: DataFrame, start_date, end_date) -> DataFrame:
        return data.withColumn("added_at", to_timestamp(col('added_at'), "yyyy-MM-dd'T'HH:mm:ssXXX"))\
                .filter((col('added_at') > start_date.strftime("%Y-%m-%d")) &
                        (col('added_at') < end_date.strftime("%Y-%m-%d")))
