import abc
from pandas import DataFrame


class MusicRepository(abc.ABC):
    @abc.abstractmethod
    def read_track_feature(self) -> DataFrame:
        pass

    @abc.abstractmethod
    def store_track_feature(self):
        pass
