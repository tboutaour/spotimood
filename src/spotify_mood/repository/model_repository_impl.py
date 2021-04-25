import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, pandas_udf
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, LongType
from spotify_mood.repository.model_repository import ModelRepository
from spotify_mood.repository.resource.model_resource import ModelResource
from pyspark.sql.functions import col, create_map, lit
from itertools import chain


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

        def __predict_udf(*cols):
            X = pd.concat(cols, axis=1)
            d = self.model_resource.predict(X)
            return pd.Series(d)

        pred = pandas_udf(__predict_udf, returnType=LongType())
        features = [col(x) for x in col_features]
        data = data.withColumn("track_mood_classification_cat", lit(pred(*features)))

        mapping_expr = create_map([lit(x) for x in chain(*self.target.items())])

        data = data.withColumn("track_mood_classification", mapping_expr.getItem(col("track_mood_classification_cat")))
        return data
