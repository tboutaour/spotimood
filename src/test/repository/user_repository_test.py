import unittest

from spotify_mood.repository.resource.postgres_resource_impl import PostgresResourceImpl
from spotify_mood.repository.user_repository_impl import UserRepositoryImpl
from spotify_mood.repository.resource.spotify_resource_impl import SpotifyResourceImpl
import pprint


class MyTestCase(unittest.TestCase):

    def test_get_current_user_information(self):
        client_id = '4a3108858bb040cc8988f1214539a0a7'
        client_secret = '42a4446973f64c90b30beacc49c3d112'
        backend_user_id = 'pqor0kh7srwmehv50gan5saqi19miz8b'
        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')
        spotify_resource = SpotifyResourceImpl(client_id=client_id,
                                               client_secret=client_secret,
                                               backend_user_id=backend_user_id,
                                               postgres_resource=postgres_resource)
        user_repository = UserRepositoryImpl(spotify_resource=spotify_resource)
        result = user_repository.get_current_user_information()
        pprint.pprint(result)

    def test_get_user_active(self):
        postgres_resource = PostgresResourceImpl(host='localhost',
                                                 db='postgres',
                                                 user='postgres',
                                                 password='postgres')
        user_repository = UserRepositoryImpl(postgres_resource=postgres_resource)
        result = user_repository.get_status_active_users()
        pprint.pprint(result)


if __name__ == '__main__':
    unittest.main()
