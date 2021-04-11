from pyspark.sql import SparkSession

from spotify_mood.interactor.spotify_ingestion.filter_by_date_impl import FilterByDateImpl
from spotify_mood.interactor.spotify_ingestion.prepare_dataframe_for_load_impl import PrepareDataframeForLoadImpl
from spotify_mood.interactor.union_dataframes_impl import UnionDataframesImpl
from spotify_mood.main.arguments import Arguments
from spotify_mood.conf.config import *
from spotify_mood.pipeline.spotify_data_ingestion_pipeline import SpotifyDataIngestionPipeline
from spotify_mood.repository.internal_music_repository_impl import InternalMusicRepositoryImpl
from spotify_mood.repository.music_repository_impl import MusicRepositoryImpl
from spotify_mood.repository.resource.avro_resource_impl import AvroResourceImpl
from spotify_mood.repository.resource.postgres_resource_impl import PostgresResourceImpl
from spotify_mood.repository.resource.spotify_resource_impl import SpotifyResourceImpl
from spotify_mood.repository.user_repository_impl import UserRepositoryImpl


def main():
    args = Arguments()
    spark = SparkSession.builder.appName("reporting-online").getOrCreate()
    spark.sparkContext.setCheckpointDir(LOCAL_TEMP_CHECKPOINT)

    start_date = args.get_start_date()
    end_date = args.get_end_date()

    avro_resource = AvroResourceImpl(spark=spark)

    postgres_resource = PostgresResourceImpl(host=POSTGRES_BACKEND_HOST,
                                             db=POSTGRES_BACKEND_DATABASE,
                                             user=POSTGRES_BACKEND_USER,
                                             password=POSTGRES_BACKEND_PASSWORD)

    main_user_repository = UserRepositoryImpl(postgres_resource=postgres_resource)

    backend_users = main_user_repository.get_status_active_users()

    spotify_resources = [SpotifyResourceImpl(client_id=SPOTIFY_CLIENT_ID,
                                             client_secret=SPOTIFY_CLIENT_SECRET,
                                             backend_user_id=backend_user,
                                             postgres_resource=postgres_resource) for backend_user in backend_users]
    internal_music_repository = InternalMusicRepositoryImpl(avro_resource=avro_resource)

    external_music_repositories = [MusicRepositoryImpl(spark=spark,
                                                       spotify_resource=spotify_resource)
                                   for spotify_resource in spotify_resources]

    union_dataframes = UnionDataframesImpl()
    filter_by_date = FilterByDateImpl()
    prepare_dataframe_for_load = PrepareDataframeForLoadImpl()

    # Pipeline
    data_ingestion_pipeline = SpotifyDataIngestionPipeline(external_music_repositories=external_music_repositories,
                                                           internal_music_repository=internal_music_repository,
                                                           union_dataframes=union_dataframes,
                                                           filter_by_date=filter_by_date,
                                                           prepare_dataframe_for_load=prepare_dataframe_for_load)

    data_ingestion_pipeline.run(start_date, end_date)
