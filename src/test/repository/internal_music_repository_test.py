import unittest
from datetime import datetime
from pandas import DataFrame
from pandas.io.json import json_normalize

from spotify_mood.repository.internal_music_repository_impl import InternalMusicRepositoryImpl
from spotify_mood.repository.music_repository_impl import MusicRepositoryImpl
from spotify_mood.repository.resource.avro_resource_impl import AvroResourceImpl
from spotify_mood.repository.resource.postgres_resource_impl import PostgresResourceImpl
from spotify_mood.repository.resource.spotify_resource_impl import SpotifyResourceImpl
import pprint
import pandas as pd

from spotify_mood.repository.user_repository_impl import UserRepositoryImpl
from test.pyspark_test_base import PySparkTestBase

client_id = '4a3108858bb040cc8988f1214539a0a7'
client_secret = '42a4446973f64c90b30beacc49c3d112'
backend_user_id = 'pqor0kh7srwmehv50gan5saqi19miz8b'


class MusicRepositoryTest(PySparkTestBase):

    def test_read_avro_file(self):
        avro_resource = AvroResourceImpl(self.spark)
        internal_music_repository = InternalMusicRepositoryImpl(avro_resource=avro_resource)
        start_date = datetime(2021, 4, 7)
        end_date = datetime(2021, 4, 7)

        df = internal_music_repository.read_track_feature(start_date, end_date)
        df.show()


if __name__ == '__main__':
    unittest.main()
