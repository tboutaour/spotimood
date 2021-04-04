from spotify_mood.repository.resource.postgres_resource import PostgresResource
from spotify_mood.repository.resource.spotify_resource import SpotifyResource
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pytz
from datetime import timedelta, datetime

scope = 'user-library-modify'
USER_TOKEN_TABLE_NAME = 'spotify_web_spotifytoken'
SPOTIFY_EXPIRE_TIME = 3600


class SpotifyResourceImpl(SpotifyResource):

    def __init__(self,
                 client_id: str = None,
                 client_secret: str = None,
                 backend_user_id: str = None,
                 postgres_resource: PostgresResource = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.postgres_resource = postgres_resource
        self.backend_user_id = backend_user_id
        self.redirect_uri = "http://127.0.0.1:8000/spotify_web/redirect"
        self.scope = 'playlist-read-private user-library-read '
        self.oauth = SpotifyOAuth(client_id=self.client_id,
                                  client_secret=self.client_secret,
                                  redirect_uri=self.redirect_uri,
                                  scope=self.scope)

    def __spotify_connect(self, auth):
        return spotipy.Spotify(auth=auth,
                               auth_manager=SpotifyOAuth)

    def connect(self):
        token, expires_in = self.__get_user_token(self.backend_user_id)
        now = pytz.utc.localize(datetime.now())
        if expires_in < now:
            refresh_token = self.__get_refresh_token(self.backend_user_id)
            token = self.__refresh_access_token(refresh_token=refresh_token)
            self.__refresh_user_token(token)
        return self.__spotify_connect(token)

    def __refresh_access_token(self, refresh_token):
        token = self.oauth.refresh_access_token(refresh_token=refresh_token)
        return token['access_token']

    def __get_user_token(self, user_id: str):
        token = self.postgres_resource.get_one_from_table(
            USER_TOKEN_TABLE_NAME,
            "access_token, expires_in",
            f"""{USER_TOKEN_TABLE_NAME}.user = '{user_id}'"""
        )
        return token[0], token[1]

    def __get_refresh_token(self, user_id: str) -> str:
        token = self.postgres_resource.get_one_from_table(
            USER_TOKEN_TABLE_NAME,
            "refresh_token, expires_in",
            f"""{USER_TOKEN_TABLE_NAME}.user = '{user_id}'"""
        )
        return token[0]

    def __refresh_user_token(self, token):
        expires_in = datetime.now() + timedelta(seconds=SPOTIFY_EXPIRE_TIME)
        self.postgres_resource.update_many(USER_TOKEN_TABLE_NAME,
                                           f"""access_token = '{token}', expires_in = '{expires_in}'""",
                                           f"""{USER_TOKEN_TABLE_NAME}.user = '{self.backend_user_id}'""")
        return True
