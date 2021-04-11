from pyspark.sql import SparkSession

from spotify_mood.repository.resource.spotify_resource import SpotifyResource
from spotify_mood.repository.music_repository import MusicRepository
from pyspark.sql import DataFrame
import pandas as pd


class MusicRecentPlayedRepositoryImpl(MusicRepository):

    def __init__(self,
                 spark: SparkSession,
                 spotify_resource: SpotifyResource):
        self.spark = spark
        self.spotify_connection = spotify_resource.connect()

    def __get_recent_played(self):
        return self.spotify_connection.current_user_recently_played()['items']

    def read_track_feature(self) -> DataFrame:
        tracks = [tr for tr in self.__get_recent_played()]
        songs_features = [self.__get_songs_features(track) for track in tracks]
        return self.spark.createDataFrame(pd.DataFrame.from_records(songs_features))

    def __get_songs_features(self, track):
        feature_result = self.__get_feature_by_song(track['track']['id'])
        feature_result['user_id'] = self.spotify_connection.current_user()['id']
        feature_result['played_at'] = track['played_at']
        return feature_result

    def __get_feature_by_song(self, track_id: str):
        song_info = self.spotify_connection.track(track_id)
        song_ampliated = {
            "track_id": track_id,
            "track_name": song_info['name'],
            "popularity": song_info['popularity'],
            "artists": [artist['name'] for artist in song_info['artists']]
        }
        song_feature = self.spotify_connection.audio_features(track_id)[0]
        a = {key: value for (key, value) in (list(song_ampliated.items()) + list(song_feature.items()))}
        return a

    def store_track_feature(self):
        pass
