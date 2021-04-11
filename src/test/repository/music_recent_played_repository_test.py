import unittest

from spotify_mood.conf.config import POSTGRES_BACKEND_PASSWORD, POSTGRES_BACKEND_USER, POSTGRES_BACKEND_DATABASE, \
    POSTGRES_BACKEND_HOST, SPOTIFY_CLIENT_SECRET, SPOTIFY_CLIENT_ID, BACKENT_USER_ID
from spotify_mood.repository.music_recent_played_repository_impl import MusicRecentPlayedRepositoryImpl
from spotify_mood.repository.resource.postgres_resource_impl import PostgresResourceImpl
from spotify_mood.repository.resource.spotify_resource_impl import SpotifyResourceImpl
from test.pyspark_test_base import PySparkTestBase


class MusicRepositoryTest(PySparkTestBase):
    def test_retrieve_recent_played_names(self):
        postgres_resource = PostgresResourceImpl(host=POSTGRES_BACKEND_HOST,
                                                 db=POSTGRES_BACKEND_DATABASE,
                                                 user=POSTGRES_BACKEND_USER,
                                                 password=POSTGRES_BACKEND_PASSWORD)
        spotify_resource = SpotifyResourceImpl(client_id=SPOTIFY_CLIENT_ID,
                                               client_secret=SPOTIFY_CLIENT_SECRET,
                                               backend_user_id=BACKENT_USER_ID,
                                               postgres_resource=postgres_resource)

        playlist_repository = MusicRecentPlayedRepositoryImpl(self.spark, spotify_resource)
        result = playlist_repository.read_track_feature()
        result.select('track_name').show()


if __name__ == '__main__':
    unittest.main()
