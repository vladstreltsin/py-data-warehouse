from abc import ABC, abstractmethod
from remotools.remotes.base import BaseRemote
import typing as tp
from remotools.concurrent.saver import ConcurrentSaver


class BaseSaver(ABC):
    """
    The base class from which all Savers must inherit.

    A saver allows for easy saving and loading of object of different kinds, locally or
    remotely, through the use of a Remote. For example, it permits loading an image from a remotely stored
    object directly using some image IO function without the need to store it first in a local file.

    Attributes
    ----------
    remote
        The Remote backend

    Methods
    -------
    concurrent(**kwargs) -> ConcurrentSaver
        Converts the saver into a concurrent saver that utilizes threads to speed up the operations.
        Returns an instance of a ConcurrentSaver.

    Abstract Methods
    ----------------
    save(obj, key, ...) -> remote_key
        Save a given object in the remote with the given key. Returns the actual key the object was saved under.

    load(key) -> obj
        Load an object from the given key. Returns the loaded object.

    """

    def __init__(self, remote: BaseRemote):
        assert isinstance(remote, BaseRemote), f"remote must be an derived from {BaseRemote.__name__}, (got {remote})"
        self.remote = remote

    @abstractmethod
    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs) -> str:
        pass

    @abstractmethod
    def load(self, key: str, download_params=None, progress=True, **kwargs) -> tp.Any:
        pass

    def concurrent(self, **kwargs) -> ConcurrentSaver:
        return ConcurrentSaver(saver=self, **kwargs)
