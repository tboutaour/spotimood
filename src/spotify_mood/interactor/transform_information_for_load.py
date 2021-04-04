import abc
from pandas import DataFrame


class TransformInformationForLoad(abc.ABC):
    @abc.abstractmethod
    def apply(self, data: DataFrame) -> DataFrame:
        pass