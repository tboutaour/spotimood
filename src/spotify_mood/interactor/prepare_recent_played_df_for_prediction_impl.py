from pyspark.sql import DataFrame
from spotify_mood.interactor.prepare_df_for_prediction import PrepareDfForPrediction
from pyspark.sql.functions import lit, col, to_timestamp


class PrepareRecentPlayedDfForPredictionImpl(PrepareDfForPrediction):
    def apply(self, data: DataFrame) -> DataFrame:
        pass_columns = ['user_id', 'track_id', 'played_at', 'track_name', 'principal_artist', "duration_ms",
                        "danceability", "acousticness", "energy", "instrumentalness", "liveness",
                        "valence", "loudness", "speechiness", "tempo"]
        return data\
            .withColumn('principal_artist', lit(col('artists')[0]))\
            .withColumn("played_at", to_timestamp(col('played_at'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))\
            .select(pass_columns)
