from pandas import DataFrame
from spotify_mood.interactor.prepare_df_for_prediction import PrepareDfForPrediction


class PrepareDfForPredictionImpl(PrepareDfForPrediction):
    def apply(self, data: DataFrame) -> DataFrame:
        pass
        # TODO Transform headers
        # TODO put classification time
        # TODO filter columns
