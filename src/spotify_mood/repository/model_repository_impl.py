from pandas import DataFrame

from spotify_mood.repository.model_repository import ModelRepository
from spotify_mood.repository.resource.model_resource import ModelResource


class ModelRepositoryImpl(ModelRepository):

    def __init__(self,
                 model_resource: ModelResource):
        self.model_resource = model_resource
        self.target = {
            0: 'Anxious',
            1: 'Contentment',
            2: 'Depression',
            3: 'Exhuberance'
        }

    def get_prediction(self, data: DataFrame) -> DataFrame:
        col_features = ["duration_ms", "danceability", "acousticness", "energy", "instrumentalness", "liveness",
                        "valence", "loudness", "speechiness", "tempo"]
        data['classification'] = self.model_resource.predict(data[col_features])
        data['classification_desc'] = data.apply(lambda x: self.target.get(x.classification), axis=1)
        return data
