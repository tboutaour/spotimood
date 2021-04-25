from pandas import DataFrame
from spotify_mood.repository.internal_music_repository import InternalMusicRepository
from spotify_mood.repository.resource.avro_resource import AvroResource
from spotify_mood.conf.config import DATA_LANDING_RECENT_PLAYED
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit
import os


class InternalRecentPlayedMusicRepositoryImpl(InternalMusicRepository):

    def __init__(self, avro_resource: AvroResource):
        self.avro_resource = avro_resource

    def read_track_feature(self, start_day, end_day) -> DataFrame:
        df = self.avro_resource.read(DATA_LANDING_RECENT_PLAYED)
        return df.filter(
            (col("played_at") >= lit(start_day.strftime("%Y-%m-%d %H:00:00"))) & (
                    col("played_at") < lit(end_day.strftime("%Y-%m-%d %H:00:00"))))

    def _get_read_path(self, start_date: datetime, end_date: datetime):
        for date in self._daterange(start_date, end_date + timedelta(hours=1)):
            yield DATA_LANDING_RECENT_PLAYED + f"/year={date.strftime('%Y')}/month={date.month}/day={date.day}/hour={date.hour}/*"


    def store_track_feature(self, data: DataFrame):
        partition_columns = ['year', 'month', 'day', 'hour', 'user_id']
        self.avro_resource.write_with_partition(data=data, path=DATA_LANDING_RECENT_PLAYED,
                                                partition_by=partition_columns)
    @staticmethod
    def _check_if_exists(path):
        return os.path.exists(path)

    @staticmethod
    def _daterange(start_date: datetime, end_date: datetime):
        for n in range(int((end_date - start_date).seconds / 3600)):
            yield start_date + timedelta(hours=n)
