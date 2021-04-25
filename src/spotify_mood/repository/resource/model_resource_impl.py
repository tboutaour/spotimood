from spotify_mood.repository.resource.model_resource import ModelResource
import pickle
import gcsfs


class ModelResourceImpl(ModelResource):

    def __init__(self, project, path):
        self.storage_client = gcsfs.GCSFileSystem(project=project)
        self.model = self.__import_model(path)

    def load_model(self):
        pass

    def predict(self, data) -> int:
        return self.model.predict(data)

    def __import_model(self, path):
        with self.storage_client.open(path, 'rb') as file:
            r = pickle.load(file)
        return r
