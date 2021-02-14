import abc


class UserRepository(abc.ABC):
    @abc.abstractmethod
    def get_user_information(self):
        pass

    def store_user_information(self):
        pass
