from pyspark.sql import DataFrame
from spotify_mood.interactor.prepare_df_for_prediction import PrepareDfForPrediction
from pyspark.sql.functions import lit, col,to_timestamp


class PrepareDfForPredictionImpl(PrepareDfForPrediction):
    def apply(self, data: DataFrame) -> DataFrame:
        pass_columns = ['user_id', 'playlist_id', 'track_id', 'added_at', 'track_name', 'principal_artist', "duration_ms",
                        "danceability", "acousticness", "energy", "instrumentalness", "liveness",
                        "valence", "loudness", "speechiness", "tempo"]
        return data.withColumn('principal_artist', lit(col('artists')[0]))\
            .withColumn("added_at", to_timestamp(col('added_at'), "yyyy-MM-dd'T'HH:mm:ss'Z'"))\
            .select(pass_columns)
