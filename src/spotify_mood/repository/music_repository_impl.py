from pyspark.sql import SparkSession

from spotify_mood.repository.resource.spotify_resource import SpotifyResource
from spotify_mood.repository.music_repository import MusicRepository
from pyspark.sql import DataFrame
import pandas as pd


class MusicRepositoryImpl(MusicRepository):

    def __init__(self,
                 spark: SparkSession,
                 spotify_resource: SpotifyResource):
        self.spark = spark
        self.spotify_connection = spotify_resource.connect()

    def __get_playlists(self, user_id):
        return self.spotify_connection.user_playlists(user_id)

    def __get_saved_tracks(self):
        return self.spotify_connection.current_user_saved_tracks(limit=2)

    def read_track_feature(self) -> DataFrame:
        user_id = self.spotify_connection.current_user()['id']
        playlist_ids = [pl['id'] for pl in self.__get_playlists(user_id)['items'] if pl['owner']['id'] == user_id]
        track_ids = [tr for pl_id in playlist_ids for tr in self.__get_songs_by_playlist(pl_id)]
        songs_features = [self.__get_songs_of_playlist(tr_id) for tr_id in track_ids]
        return self.spark.createDataFrame(pd.DataFrame.from_records(songs_features))

    def __get_songs_of_playlist(self, track):

        feature_result = self.__get_feature_by_song(track['track_id'])
        feature_result['playlist_id'] = track['playlist_id']
        feature_result['user_id'] = track['user_id']
        feature_result['added_at'] = track['added_at']
        return feature_result

    def __get_songs_by_playlist(self, pl_id: str):
        playlist_items = self.spotify_connection.playlist_items(pl_id,
                                                                fields='items.added_by.id, items.added_at, items.track.id, items.track.name, items.track.artists.name, items.track.duration_ms')
        return [{"playlist_id": pl_id,
                 "user_id": item['added_by']['id'],
                 "added_at": item['added_at'],
                 "track_id": item['track']['id'],
                 "track_name": item['track']['name'],
                 "artists": item['track']['artists']}
                for item in playlist_items['items']]

    def __get_feature_by_song(self, track_id: str):
        song_info = self.spotify_connection.track(track_id)
        song_ampliated = {
            "track_id__": track_id,
            "track_name__": song_info['name'],
            "popularity": song_info['popularity'],
            "artists__": [artist['name'] for artist in song_info['artists']]
        }
        song_feature = self.spotify_connection.audio_features(track_id)[0]
        a = {key: value for (key, value) in (list(song_ampliated.items()) + list(song_feature.items()))}
        return a

    def store_track_feature(self):
        pass
