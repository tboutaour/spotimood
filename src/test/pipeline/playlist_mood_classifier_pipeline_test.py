import datetime

from spotify_mood.conf.config import MODEL_PATH
from spotify_mood.interactor.prepare_df_for_prediction_impl import PrepareDfForPredictionImpl
from spotify_mood.interactor.transform_information_for_load_impl import TransformInformationForLoadImpl
from spotify_mood.pipeline.playlist_mood_tracker_pipeline import PlaylistMoodTrackerPipeline
from spotify_mood.repository.internal_music_repository_impl import InternalMusicRepositoryImpl
from spotify_mood.repository.model_repository_impl import ModelRepositoryImpl
from spotify_mood.repository.prediction_repository_impl import PredictionRepositoryImpl
from spotify_mood.repository.resource.avro_resource_impl import AvroResourceImpl
from spotify_mood.repository.resource.model_resource_impl import ModelResourceImpl
from spotify_mood.repository.resource.postgres_resource_impl import PostgresResourceImpl
from spotify_mood.repository.resource.spotify_resource_impl import SpotifyResourceImpl
from spotify_mood.repository.user_repository_impl import UserRepositoryImpl
from test.pyspark_test_base import PySparkTestBase
from spotify_mood.conf.config import *


class PlaylistMoodeClassifierPipelineTest(PySparkTestBase):
    def test_pipeline(self):
        postgres_resource = PostgresResourceImpl(host=POSTGRES_BACKEND_HOST,
                                                 db=POSTGRES_BACKEND_DATABASE,
                                                 user=POSTGRES_BACKEND_USER,
                                                 password=POSTGRES_BACKEND_PASSWORD)
        spotify_resource = SpotifyResourceImpl(client_id=SPOTIFY_CLIENT_ID,
                                               client_secret=SPOTIFY_CLIENT_SECRET,
                                               backend_user_id=BACKENT_USER_ID,
                                               postgres_resource=postgres_resource)
        avro_resource = AvroResourceImpl(self.spark)
        model_resource = ModelResourceImpl(project=GCS_PROJECT_ID, path=MODEL_PATH)
        user_repository = UserRepositoryImpl(spotify_resource)
        internal_music_repository = InternalMusicRepositoryImpl(avro_resource=avro_resource)
        model_repository = ModelRepositoryImpl(model_resource)
        prediction_repository = PredictionRepositoryImpl(postgres_resource)

        prepare_df_for_prediction = PrepareDfForPredictionImpl()
        transform_information_for_load = TransformInformationForLoadImpl()

        track_classifier = PlaylistMoodTrackerPipeline(
            user_repository=user_repository,
            playlist_repository=internal_music_repository,
            model_repository=model_repository,
            prediction_repository=prediction_repository,
            prepare_df_for_prediction=prepare_df_for_prediction,
            transform_information_for_load=transform_information_for_load
        )

        track_classifier.run(datetime.datetime(2021, 4, 9), datetime.datetime(2021, 4, 9))


