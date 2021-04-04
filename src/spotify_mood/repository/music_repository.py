import abc
from pandas import DataFrame


class MusicRepository(abc.ABC):
    @abc.abstractmethod
    def get_playlists(self, user_id):
        pass

    @abc.abstractmethod
    def get_saved_tracks(self):
        pass

    @abc.abstractmethod
    def get_feature_dataframe_from_playlists(self, user_id) -> DataFrame:
        pass

    @abc.abstractmethod
    def store_playlist(self):
        pass

    @abc.abstractmethod
    def get_songs_by_playlist(self, pl_id: str):
        pass

    @abc.abstractmethod
    def get_feature_by_song(self, track_id: str):
        pass

