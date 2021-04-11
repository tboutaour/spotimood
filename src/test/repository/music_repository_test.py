import unittest

from pandas import DataFrame
from pandas.io.json import json_normalize

from spotify_mood.repository.music_repository_impl import MusicRepositoryImpl
from spotify_mood.repository.resource.postgres_resource_impl import PostgresResourceImpl
from spotify_mood.repository.resource.spotify_resource_impl import SpotifyResourceImpl
import pprint
import pandas as pd

from spotify_mood.repository.user_repository_impl import UserRepositoryImpl

client_id = '4a3108858bb040cc8988f1214539a0a7'
client_secret = '42a4446973f64c90b30beacc49c3d112'
backend_user_id = 'pqor0kh7srwmehv50gan5saqi19miz8b'


class MusicRepositoryTest(unittest.TestCase):
    @unittest.skip("Privated_method")
    def test_retrieve_playlist_names(self):
        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')
        spotify_resource = SpotifyResourceImpl(client_id=client_id,
                                               client_secret=client_secret,
                                               backend_user_id=backend_user_id,
                                               postgres_resource=postgres_resource)
        user_repository = UserRepositoryImpl(spotify_resource)
        playlist_repository = MusicRepositoryImpl(spotify_resource)
        # result = playlist_repository.get_playlists(user_repository.get_current_user_information()['id'])
        # names = [r['name'] for r in result['items']]
        # assert "B R X M" in names
        # assert "TEST" not in names

    @unittest.skip("Implementation skipping")
    def test_dataframe_playlist_features(self):
        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')
        spotify_resource = SpotifyResourceImpl(client_id=client_id,
                                               client_secret=client_secret,
                                               backend_user_id=backend_user_id,
                                               postgres_resource=postgres_resource)
        playlist_repository = MusicRepositoryImpl(spotify_resource)
        user_repository = UserRepositoryImpl(spotify_resource)
        result = playlist_repository.read_track_feature(user_repository.get_current_user_information()['id'])
        result.to_csv('result_features4.csv', header=True)

    @unittest.skip("Deprecated")
    def test_saved_tracks_names(self):
        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')
        spotify_resource = SpotifyResourceImpl(client_id=client_id,
                                               client_secret=client_secret,
                                               backend_user_id=backend_user_id,
                                               postgres_resource=postgres_resource)
        playlist_repository = MusicRepositoryImpl(spotify_resource)
        # result = playlist_repository.get_saved_tracks()
        assert len(result) > 0

    @unittest.skip("Implementation skipping")
    def test_get_features_for_categorized_songs(self):
        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')
        spotify_resource = SpotifyResourceImpl(client_id=client_id,
                                               client_secret=client_secret,
                                               backend_user_id=backend_user_id,
                                               postgres_resource=postgres_resource)
        music_repository = MusicRepositoryImpl(spotify_resource)

        input_df = pd.read_csv('./input.csv')
        input_df['features'] = input_df['track_id'].apply(lambda x: music_repository.get_feature_by_song(x))
        df1 = (pd.concat({i: json_normalize(x) for i, x in input_df.pop('features').items()})
               .reset_index(level=1, drop=True)
               .join(input_df)
               .reset_index(drop=True))
        df1.to_csv('result_features300.csv', header=True)

    @unittest.skip("Privated method")
    def test_retrieve_song_names(self):
        client_id = '4a3108858bb040cc8988f1214539a0a7'
        client_secret = '42a4446973f64c90b30beacc49c3d112'
        backend_user_id = 'pqor0kh7srwmehv50gan5saqi19miz8b'
        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')
        spotify_resource = SpotifyResourceImpl(client_id=client_id,
                                               client_secret=client_secret,
                                               backend_user_id=backend_user_id,
                                               postgres_resource=postgres_resource)
        song_repository = MusicRepositoryImpl(spotify_resource)
        # result = song_repository.get_songs_by_playlist('0VtpMlGXhS35801SNIEcUN')
        # assert len(songs) > 0

    @unittest.skip("Privated method")
    def test_retrieve_song_features(self):

        backend_user_id = 'pqor0kh7srwmehv50gan5saqi19miz8b'
        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')
        spotify_resource = SpotifyResourceImpl(client_id=client_id,
                                               client_secret=client_secret,
                                               backend_user_id=backend_user_id,
                                               postgres_resource=postgres_resource)
        song_repository = MusicRepositoryImpl(spotify_resource)
        # result = song_repository.get_feature_by_song('2tGENA8xef0wABdHvyXwv9')
        # assert len(result) > 0

    @unittest.skip("Implementation skipping")
    def test_retrieve_song_names_for_testing(self):
        client_id = '4a3108858bb040cc8988f1214539a0a7'
        client_secret = '42a4446973f64c90b30beacc49c3d112'
        playlist_id = '6izHwFSnmAJZ7FhpujeOkX'
        backend_user_id = 'pqor0kh7srwmehv50gan5saqi19miz8b'
        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')
        spotify_resource = SpotifyResourceImpl(client_id=client_id,
                                               client_secret=client_secret,
                                               backend_user_id=backend_user_id,
                                               postgres_resource=postgres_resource)
        song_repository = MusicRepositoryImpl(spotify_resource)
        result = song_repository.__get_songs_by_playlist(playlist_id)
        #pprint.pprint(result)
        #songs = [pprint.pprint(r['track_id'] + " => " + r['track_name']) for r in result]
        song_df = DataFrame.from_records(result)
        song_df.to_csv('playlist-6izHwFSnmAJZ7FhpujeOkX-songs.csv', header=True)

if __name__ == '__main__':
    unittest.main()
