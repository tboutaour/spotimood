from spotify_mood.conf.config import RECENT_PLAYED_CLASSIFIED_TABLE
from spotify_mood.repository.prediction_repository import PredictionRepository
from spotify_mood.repository.resource.postgres_resource import PostgresResource


class RecentPlayedPredictionRepositoryImpl(PredictionRepository):

    def __init__(self,
                 postgres_resource: PostgresResource):
        self.postgres_resource = postgres_resource

    def store_prediction_dataframe(self, df):
        # set columns to save
        export_columns = ["user_id",
                          "track_id",
                          "track_name",
                          "principal_artist",
                          "track_mood_classification",
                          "played_at",
                          "classified_at"]

        self.postgres_resource.set_dataframe_to_table(df[export_columns], RECENT_PLAYED_CLASSIFIED_TABLE)
        return True
