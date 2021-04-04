from spotify_mood.repository.resource.spotify_resource import SpotifyResource
from spotify_mood.repository.playlist_repository import MusicRepository
from pandas import DataFrame


class MusicRepositoryImpl(MusicRepository):

    def __init__(self,
                 spotify_resource: SpotifyResource):
        self.spotify_connection = spotify_resource.connect()

    def get_playlists(self, user_id):
        return self.spotify_connection.user_playlists(user_id)

    def get_saved_tracks(self):
        return self.spotify_connection.current_user_saved_tracks(limit=2)

    def get_feature_dataframe_from_playlists(self, user_id) -> DataFrame:
        playlist_ids = [pl['id'] for pl in self.get_playlists(user_id)['items']]
        track_ids = [tr for pl_id in playlist_ids for tr in self.get_songs_by_playlist(pl_id)]
        songs_features = [self.get_songs_of_playlist(tr_id['playlist_id'], tr_id['track_id']) for tr_id in track_ids]
        return DataFrame.from_records(songs_features)

    def get_songs_of_playlist(self, pl_id, track_id):
        feature_result = self.get_feature_by_song(track_id)
        feature_result['playlist_id'] = pl_id
        return feature_result

    def get_songs_by_playlist(self, pl_id: str):
        playlist_items = self.spotify_connection.playlist_items(pl_id,
                                                                fields='items.added_at, '
                                                                       'items.track.id, '
                                                                       'items.track.name, '
                                                                       'items.track.artists.name, '
                                                                       'items.track.duration_ms')
        return [{"playlist_id": pl_id,
                 "added_at": item['added_at'],
                 "track_id": item['track']['id'],
                 "track_name": item['track']['name'],
                 "artists": item['track']['artists']}
                for item in playlist_items['items']]

    def get_feature_by_song(self, track_id: str):
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

    def store_playlist(self):
        pass
