from pyspark.sql import SparkSession

from spotify_mood.conf.config import *
from spotify_mood.interactor.prepare_recent_played_df_for_prediction_impl import PrepareRecentPlayedDfForPredictionImpl
from spotify_mood.interactor.transform_information_for_load_impl import TransformInformationForLoadImpl
from spotify_mood.main.arguments import Arguments
from spotify_mood.pipeline.playlist_mood_tracker_pipeline import PlaylistMoodTrackerPipeline
from spotify_mood.repository.internal_recent_played_music_repository_impl import InternalRecentPlayedMusicRepositoryImpl
from spotify_mood.repository.model_repository_impl import ModelRepositoryImpl
from spotify_mood.repository.recent_played_prediction_repository_impl import RecentPlayedPredictionRepositoryImpl
from spotify_mood.repository.resource.avro_resource_impl import AvroResourceImpl
from spotify_mood.repository.resource.model_resource_impl import ModelResourceImpl
from spotify_mood.repository.resource.postgres_resource_impl import PostgresResourceImpl
from spotify_mood.repository.resource.spotify_resource_impl import SpotifyResourceImpl
from spotify_mood.repository.user_repository_impl import UserRepositoryImpl


def main():
    args = Arguments()
    spark = SparkSession.builder.appName("hourly-playlist-mood-tracker").getOrCreate()
    spark.sparkContext.setCheckpointDir(LOCAL_TEMP_CHECKPOINT)

    start_date = args.get_start_date()
    end_date = args.get_end_date()

    postgres_resource = PostgresResourceImpl(host=POSTGRES_BACKEND_HOST,
                                             db=POSTGRES_BACKEND_DATABASE,
                                             user=POSTGRES_BACKEND_USER,
                                             password=POSTGRES_BACKEND_PASSWORD)
    spotify_resource = SpotifyResourceImpl(client_id=SPOTIFY_CLIENT_ID,
                                           client_secret=SPOTIFY_CLIENT_SECRET,
                                           backend_user_id=BACKENT_USER_ID,
                                           postgres_resource=postgres_resource)

    avro_resource = AvroResourceImpl(spark=spark)
    model_resource = ModelResourceImpl(path=MODEL_PATH)
    user_repository = UserRepositoryImpl(spotify_resource)
    internal_music_repository = InternalRecentPlayedMusicRepositoryImpl(avro_resource=avro_resource)
    model_repository = ModelRepositoryImpl(model_resource)
    prediction_repository = RecentPlayedPredictionRepositoryImpl(postgres_resource)

    prepare_df_for_prediction = PrepareRecentPlayedDfForPredictionImpl()
    transform_information_for_load = TransformInformationForLoadImpl()

    track_classifier = PlaylistMoodTrackerPipeline(
        user_repository=user_repository,
        playlist_repository=internal_music_repository,
        model_repository=model_repository,
        prediction_repository=prediction_repository,
        prepare_df_for_prediction=prepare_df_for_prediction,
        transform_information_for_load=transform_information_for_load
    )

    track_classifier.run(start_date, end_date)