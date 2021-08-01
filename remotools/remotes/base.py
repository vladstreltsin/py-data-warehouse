from __future__ import annotations
import tqdm
from abc import ABC, abstractmethod
import typing as tp
from remotools.utils import keep_position
from remotools.parallel.remote import ConcurrentRemote
import io


class BaseRemote(ABC):
    """
    The base class from which all Remotes must inherit.

    A Remote is a object that represents a configured connector to any target object storage. It is designed
    to perform the following three actions:
    * Download - download an object to a stream
    * Upload - upload an object from a stream
    * Contains - check whether an object exists

    Each object in the target storage is uniquely identified by a string called a 'key'. The keys are unique
    only in the context of each Remote, that is for a given remote R1 the keys 'abc' and 'def' represent
    different objects. However, for two remotes R1 and R2 the key 'abc' might represent the same object or
    two different objects.

    The uploading and downloading are performed over binary streams. The download operation will
    copy the byte contents of the remote object into a given stream, while the upload operation will
    copy the contents of a local stream to the remote.

    Attributes
    ----------
    name:
        The name of the current remote. Used mostly for logging and debug.
        Defaults to the class name.


    Methods
    -------
    download(f, key, ...)
        Copies a remote object identified by key to the stream f

    upload(f, key, ...) -> modified_key
        Copies the contents of the stream f to a remote object identified by key.
        Returns the a string that represents the actual key to which the stream was copied.

    contains(key)
        Checks whether a given key exists in on the target storage

    concurrent(**kwargs) -> ConcurrentRemote
        Converts the remote into a concurrent remote that utilizes threads to speed up the operations.
        Returns an instance of a ConcurrentRemote.

    Abstract Methods
    ----------------
    The following methods must be implemented by a subclass:
    _download(f, key, ...)
        The actual implementation of download

    _upload(f, key, ...) -> modified_key: str
        The actual implementation of upload

    _contains(key) -> bool
        The actual implementation of existence check

    """

    def __init__(self, name=None):
        """
        Parameters
        ----------
        name
            The name of the current remote. Used mostly for logging and debug.
            Defaults to the class name.
        """

        self.name = name or self.__class__.__name__

    def download(self, f, key: str, progress=True, keep_stream_position=False, params: tp.Optional[dict]=None):
        """
        Download a key to the provided stream.

        Parameters
        ----------
        f
            A stream (file-like) object

        key
            Remote object identifier string

        progress
            Show progress bar

        keep_stream_position
            Whether to revert to the current position in the stream after downloading

        params
            Extra parameter dictionary passed to _download(...) as keyword arguments


        Raises
        ------
        KeyNotFoundError
            When the key doesn't exist on the storage

        NonDownloadableKeyError
            When the key can't be downloaded

        StorageConnectionError
            When the connection to the remote storage was interrupted

        UnknownError
            Other implementation specific errors

        Returns
        -------
        None


        """
        if params is None:
            params = {}

        with self._download_progress_bar(f, key, progress=progress) as fp:
            if keep_stream_position:
                with keep_position(fp):
                    self._download(fp, key, **params)
            else:
                self._download(fp, key, **params)

    def upload(self, f, key: str, progress=True, keep_stream_position=False, params: tp.Optional[dict]=None) -> str:
        """
        Upload a stream to the provided key.

        Since some implementations may modify the key value, the method returns the actual key that was used.

        Parameters
        ----------
        f
            A stream (file-like) object

        key
            Remote object identifier string

        progress
            Show progress bar

        keep_stream_position
            Whether to revert to the current position in the stream after uploading

        params
            Extra parameter dictionary passed to _upload(...) as keyword arguments

        Raises
        ------
        NonUploadableKeyError
            When the key can't be uploaded

        StorageConnectionError
            When the connection to the remote storage was interrupted

        UnknownError
            Other implementation specific errors

        Returns
        -------
        modified_key
            The actual key value used for upload

        """
        if params is None:
            params = {}

        with self._upload_progress_bar(f, key, progress=progress) as fp:
            if keep_stream_position:
                with keep_position(fp):
                    return self._upload(fp, key, **params)
            else:
                return self._upload(fp, key, **params)

    def contains(self, key: str) -> bool:
        """
        Check whether the given key exists on the storage.

        Parameters
        ----------
        key
            Remote object identifier string

        Returns
        -------
        True if the given key exists and False otherwise.

        """
        return self._contains(key)

    def _upload_progress_bar(self, f, key: str, progress: bool = True):
        return tqdm.tqdm.wrapattr(f, "read", desc=f"[{self.name} UPLOAD] {key}", disable=not progress)

    def _download_progress_bar(self, f, key: str, progress: bool = True):
        return tqdm.tqdm.wrapattr(f, "write", desc=f"[{self.name} DOWNLOAD] {key}", disable=not progress)

    @abstractmethod
    def _download(self, f, key: str, **kwargs):
        pass

    @abstractmethod
    def _upload(self, f, key: str, **kwargs) -> str:
        pass

    @abstractmethod
    def _contains(self, key: str) -> bool:
        pass

    def concurrent(self, **kwargs) -> ConcurrentRemote:
        return ConcurrentRemote(remote=self, **kwargs)

    def copy(self, src_key, dst_key, progress=True,
             download_params: tp.Optional[dict]=None,
             upload_params: tp.Optional[dict]=None) -> str:

        f = io.BytesIO()
        self.download(f, src_key, progress=progress, keep_stream_position=True, params=download_params)
        return self.upload(f, dst_key, progress=progress, params=upload_params)
