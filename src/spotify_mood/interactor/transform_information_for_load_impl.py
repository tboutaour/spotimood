from pandas import DataFrame
from spotify_mood.interactor.transform_information_for_load import TransformInformationForLoad


class TransformInformationForLoadImpl(TransformInformationForLoad):
    def apply(self, data: DataFrame) -> DataFrame:
        pass
        # TODO Transform headers
        # TODO put classification time
        # TODO filter columns
