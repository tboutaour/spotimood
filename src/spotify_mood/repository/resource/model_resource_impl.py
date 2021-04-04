from spotify_mood.repository.resource.model_resource import ModelResource
import numpy as np
import pickle
import sklearn
import os

class ModelResourceImpl(ModelResource):

    def __init__(self, path):
        self.model = self.__import_model(path)

    def load_model(self):
        pass

    def predict(self, data):
        return self.model.predict(data)

    def __import_model(self, path):
        return pickle.load(open(os.getcwd() + '/' + path, "rb"))
