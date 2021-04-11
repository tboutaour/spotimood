import unittest

from spotify_mood.repository.model_repository_impl import ModelRepositoryImpl
from spotify_mood.repository.resource.model_resource_impl import ModelResourceImpl
from spotify_mood.conf.config import MODEL_PATH
import pprint
import pandas as pd


class ModelTestCase(unittest.TestCase):

    def test_model(self):
        model_resource = ModelResourceImpl(path=MODEL_PATH)
        model_repository = ModelRepositoryImpl(model_resource)

        input_data = [{
            'energy': 0.148192,
            'duration_ms': 0.133874,
            'valence': 0.131164,
            'acousticness': 0.125107,
            'loudness': 0.103643,
            'tempo': 0.101814,
            'danceability': 0.084321,
            'instrumentalness': 0.067127,
            'speechiness': 0.058096,
            'liveness': 0.046661}]

        data = pd.DataFrame.from_records(input_data)
        result = model_repository.get_prediction(data)
        pprint.pprint(result)


if __name__ == '__main__':
    unittest.main()
