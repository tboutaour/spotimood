import abc
from pandas import DataFrame


class InternalMusicRepository(abc.ABC):
    @abc.abstractmethod
    def read_track_feature(self, start_day, end_day) -> DataFrame:
        pass

    @abc.abstractmethod
    def store_track_feature(self, data: DataFrame):
        pass
