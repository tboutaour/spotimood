import abc


class SpotifyResource(abc.ABC):
    @abc.abstractmethod
    def connect(self):
        pass