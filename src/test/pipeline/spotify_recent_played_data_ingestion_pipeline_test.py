import datetime

from spotify_mood.conf.config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
from spotify_mood.interactor.spotify_recent_played_ingestion.filter_by_date_impl import FilterByDateImpl
from spotify_mood.interactor.spotify_recent_played_ingestion.prepare_dataframe_for_load_impl import PrepareDataframeForLoadImpl
from spotify_mood.interactor.union_dataframes_impl import UnionDataframesImpl
from spotify_mood.pipeline.spotify_data_ingestion_pipeline import SpotifyDataIngestionPipeline
from spotify_mood.repository.internal_music_repository_impl import InternalMusicRepositoryImpl
from spotify_mood.repository.internal_recent_played_music_repository_impl import InternalRecentPlayedMusicRepositoryImpl
from spotify_mood.repository.music_recent_played_repository_impl import MusicRecentPlayedRepositoryImpl
from spotify_mood.repository.music_repository_impl import MusicRepositoryImpl
from spotify_mood.repository.resource.avro_resource_impl import AvroResourceImpl
from spotify_mood.repository.resource.postgres_resource_impl import PostgresResourceImpl
from spotify_mood.repository.resource.spotify_resource_impl import SpotifyResourceImpl
from spotify_mood.repository.user_repository_impl import UserRepositoryImpl
from test.pyspark_test_base import PySparkTestBase

client_id = '4a3108858bb040cc8988f1214539a0a7'
client_secret = '42a4446973f64c90b30beacc49c3d112'
backend_user_id = 'pqor0kh7srwmehv50gan5saqi19miz8b'


class SpotifyDataIngestionPipelineTest(PySparkTestBase):
    def test_pipeline(self):
        avro_resource = AvroResourceImpl(spark=self.spark)

        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')

        main_user_repository = UserRepositoryImpl(postgres_resource=postgres_resource)

        backend_users = main_user_repository.get_status_active_users()

        spotify_resources = [SpotifyResourceImpl(client_id=SPOTIFY_CLIENT_ID,
                                                 client_secret=SPOTIFY_CLIENT_SECRET,
                                                 backend_user_id=backend_user,
                                                 postgres_resource=postgres_resource) for backend_user in backend_users]
        internal_music_repository = InternalRecentPlayedMusicRepositoryImpl(avro_resource=avro_resource)

        external_music_repositories = [MusicRecentPlayedRepositoryImpl(spark=self.spark,
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

        data_ingestion_pipeline.run(datetime.datetime(2021, 4, 11, 15), datetime.datetime(2021, 4, 11, 16))
