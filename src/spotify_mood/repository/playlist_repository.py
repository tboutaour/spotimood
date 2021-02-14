import abc


class PlaylistRepository(abc.ABC):
    @abc.abstractmethod
    def get_self_playlists(self):
        pass

    @abc.abstractmethod
    def get_favourite_playlists(self):
        pass

    @abc.abstractmethod
    def store_playlist(self):
        pass
