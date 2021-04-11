from spotify_mood.interactor.spotify_ingestion.filter_by_date import FilterByDate
from spotify_mood.interactor.spotify_ingestion.prepare_dataframe_for_load import PrepareDataframeForLoad
from spotify_mood.interactor.union_dataframes import UnionDataframes
from spotify_mood.repository.internal_music_repository import InternalMusicRepository
from spotify_mood.repository.music_repository import MusicRepository


class SpotifyDataIngestionPipeline:
    def __init__(self,
                 external_music_repositories: [MusicRepository],
                 internal_music_repository: InternalMusicRepository,
                 union_dataframes: UnionDataframes,
                 filter_by_date: FilterByDate,
                 prepare_dataframe_for_load: PrepareDataframeForLoad
                 ):
        self.internal_music_repository = internal_music_repository
        self.external_music_repositories = external_music_repositories
        self.union_dataframes = union_dataframes
        self.filter_by_date = filter_by_date
        self.prepare_dataframe_for_load = prepare_dataframe_for_load

    def run(self, start_date, end_date):
        # Get features dataframe
        feature_track_df = [external_music_repository.read_track_feature()
                            for external_music_repository in self.external_music_repositories]

        united_df = self.union_dataframes.apply(feature_track_df)
        united_df = self.filter_by_date.apply(united_df, start_date, end_date)
        united_df = self.prepare_dataframe_for_load.apply(united_df)

        # Save feature dataframe to avro
        united_df.show()
        self.internal_music_repository.store_track_feature(united_df)
