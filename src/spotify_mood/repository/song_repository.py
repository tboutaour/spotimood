import abc


class SongRepository(abc.ABC):
    @abc.abstractmethod
    def get_songs_by_playlist(self):
        pass

    @abc.abstractmethod
    def get_random_songs(self):
        pass

    @abc.abstractmethod
    def get_recently_reproduced_songs(self):
        pass

    @abc.abstractmethod
    def store_songs(self):
        pass
