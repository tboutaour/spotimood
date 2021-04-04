from spotify_mood.interactor.prepare_df_for_prediction import PrepareDfForPrediction
from spotify_mood.interactor.transform_information_for_load import TransformInformationForLoad
from spotify_mood.repository.model_repository import ModelRepository
from spotify_mood.repository.music_repository import MusicRepository
from spotify_mood.repository.prediction_repository import PredictionRepository
from spotify_mood.repository.user_repository import UserRepository


class PlaylistMoodTrackerPipeline:
    def __init__(self,
                 user_repository: UserRepository,
                 playlist_repository: MusicRepository,
                 model_repository: ModelRepository,
                 prediction_repository: PredictionRepository,
                 prepare_df_for_prediction: PrepareDfForPrediction,
                 tranform_information_for_load: TransformInformationForLoad
                 ):
        self.user_repository = user_repository
        self.playlist_repository = playlist_repository
        self.model_repository = model_repository
        self.prediction_repository = prediction_repository
        self.prepare_df_for_prediction = prepare_df_for_prediction
        self.transform_information_for_load = tranform_information_for_load

    def run(self, start_date, end_date):
        # Get features dataframe
        feature_track_df = self.playlist_repository\
            .get_feature_dataframe_from_playlists(self.user_repository.get_current_user_information()['id'])

        # Prepare Dataframe for prediction (?)
        feature_track_df = self.prepare_df_for_prediction.apply(feature_track_df)

        # Predict dataframe
        predicted_df = self.model_repository.get_prediction(feature_track_df)

        # Transform information for load
        main_df = self.transform_information_for_load.apply(predicted_df)

        # Save prediction
        self.prediction_repository.store_prediction_dataframe(predicted_df, "spotify_user_playlist_songs_classified")
