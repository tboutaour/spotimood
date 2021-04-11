from spotify_mood.repository.resource.model_resource import ModelResource
import pickle
from os.path import dirname, abspath


class ModelResourceImpl(ModelResource):

    def __init__(self, path):
        self.model = self.__import_model(path)

    def load_model(self):
        pass

    def predict(self, data) -> int:
        return self.model.predict(data)

    def __import_model(self, path):
        return pickle.load(open(dirname(dirname(dirname(abspath(__file__)))) + '/' + path, "rb"))
