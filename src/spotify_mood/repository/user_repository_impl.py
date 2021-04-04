from typing import Tuple

from spotify_mood.repository.resource.postgres_resource import PostgresResource
from spotify_mood.repository.resource.spotify_resource import SpotifyResource
from spotify_mood.repository.user_repository import UserRepository
import pytz
from datetime import timedelta, datetime

USER_STATUS_TABLE_NAME = 'spotify_web_userstatus'
SPOTIFY_EXPIRE_TIME = 3600


class UserRepositoryImpl(UserRepository):

    def __init__(self,
                 spotify_resource: SpotifyResource = None,
                 postgres_resource: PostgresResource = None):
        self.spotify_resource = spotify_resource
        self.postgres_resource = postgres_resource

    def get_current_user_information(self):
        return self.spotify_resource.connect().current_user()

    def store_user_information(self):
        pass

    def get_status_active_users(self):
        users = self.postgres_resource.get_many_from_table(USER_STATUS_TABLE_NAME,
                                                           f"""{USER_STATUS_TABLE_NAME}.user""",
                                                           f"""analysis_status_active = true""")
        return [user[0] for user in users]

