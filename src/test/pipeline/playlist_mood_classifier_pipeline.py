import datetime
import unittest

from pandas import DataFrame
from pandas._libs import json
from pandas.io.json import json_normalize

from spotify_mood.pipeline.playlist_mood_tracker_pipeline import PlaylistMoodTrackerPipeline
from spotify_mood.repository.model_repository_impl import ModelRepositoryImpl
from spotify_mood.repository.music_repository_impl import MusicRepositoryImpl
from spotify_mood.repository.prediction_repository_impl import PredictionRepositoryImpl
from spotify_mood.repository.resource.model_resource_impl import ModelResourceImpl
from spotify_mood.repository.resource.postgres_resource_impl import PostgresResourceImpl
from spotify_mood.repository.resource.spotify_resource_impl import SpotifyResourceImpl
import pprint
import pandas as pd

from spotify_mood.repository.user_repository_impl import UserRepositoryImpl

client_id = '4a3108858bb040cc8988f1214539a0a7'
client_secret = '42a4446973f64c90b30beacc49c3d112'
backend_user_id = 'pqor0kh7srwmehv50gan5saqi19miz8b'


class PlaylistMoodeClassifierPipelineTest(unittest.TestCase):
    def test_pipeline(self):
        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')
        spotify_resource = SpotifyResourceImpl(client_id=client_id,
                                               client_secret=client_secret,
                                               backend_user_id=backend_user_id,
                                               postgres_resource=postgres_resource)
        model_resource = ModelResourceImpl(path='./../assets/ml_model.sav')
        user_repository = UserRepositoryImpl(spotify_resource)
        music_repository = MusicRepositoryImpl(spotify_resource)
        model_repository = ModelRepositoryImpl(model_resource)
        prediction_repository = PredictionRepositoryImpl(postgres_resource)

        track_classifier = PlaylistMoodTrackerPipeline(
            user_repository=user_repository,
            playlist_repository=music_repository,
            model_repository=model_repository,
            prediction_repository=prediction_repository
        )

        track_classifier.run(datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 2))


