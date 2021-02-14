import abc


class SongFeatureRepository(abc.ABC):
    @abc.abstractmethod
    def get_feature_by_song(self):
        pass

    @abc.abstractmethod
    def store_song_feature(self):
        pass
