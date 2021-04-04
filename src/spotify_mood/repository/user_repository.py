import abc


class UserRepository(abc.ABC):
    @abc.abstractmethod
    def get_current_user_information(self):
        pass

    @abc.abstractmethod
    def store_user_information(self):
        pass

    @abc.abstractmethod
    def get_status_active_users(self):
        pass
