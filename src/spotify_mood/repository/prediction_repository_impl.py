from spotify_mood.repository.prediction_repository import PredictionRepository
from spotify_mood.repository.resource.postgres_resource import PostgresResource


class PredictionRepositoryImpl(PredictionRepository):

    def __init__(self,
                 postgres_resource: PostgresResource):
        self.postgres_resource = postgres_resource

    def store_prediction_dataframe(self, df, table):
        # set columns to save
        export_columns = ["user",
                          "playlist_id",
                          "playlist_name",
                          "track_id",
                          "track_name",
                          "principal_artist",
                          "track_mood_classification",
                          "added_at",
                          "classified_at"]

        self.postgres_resource.set_dataframe_to_table(df[export_columns], table)
        return True
