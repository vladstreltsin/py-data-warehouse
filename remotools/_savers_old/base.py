from abc import ABC, abstractmethod


class BaseSaver(ABC):
    """
    The base
    """
    def __init__(self, remote, default_save_key=None):
        self.remote = remote
        self.default_save_key = default_save_key

    @abstractmethod
    def save(self, obj, key=None, check_exists=True, *args, **kwargs):
        pass

    @abstractmethod
    def load(self, key, search_cache=True, *args, **kwargs):
        pass

