import os
from shutil import copyfileobj
from remotools.remotes.base import BaseRemote
from remotools.remotes.exceptions import KeyNotFoundError, NonDownloadableKeyError, \
    NonUploadableKeyError, UnknownError


class LocalRemote(BaseRemote):
    """
    A Remote object based on the current file system.

    This class treats the current file system as a 'remote' storage. Uploads and downloads are implemented
    as simple file copies using the shutil module. An optional prefix may be provided such that all
    object keys will be interpreted as relative paths to it.

    Attributes
    ----------
    prefix
        The root directory to which keys are appended to create the actual file paths.


    Examples
    --------
    >> remote = LocalRemote(prefix='/home/<user>/remote')
    >> f = io.BytesIO(initial_bytes=bytes([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 9]))
    >> remote.upload(f, 'pi')       # Upload the buffer
    >> g = io.BytesIO()
    >> remote.download(g, 'pi')     # Download the buffer
    >> 'pi' in remote               # Returns True
    >> 'e' in remote                # Returns False
    """

    def __init__(self, prefix=None, *args, **kwargs):
        super(LocalRemote, self).__init__(*args, **kwargs)
        self.prefix = os.path.realpath(os.path.expandvars(prefix)) if prefix is not None else os.path.sep
        assert isinstance(self.prefix, str)

    def _full_path(self, key):
        return os.path.join(self.prefix, key)

    def _download(self, f, key: str, **kwargs):

        path = self._full_path(key)
        try:
            with open(path, 'rb') as f_key:
                copyfileobj(f_key, f)

        except FileNotFoundError as e:
            raise KeyNotFoundError from e

        except (IsADirectoryError, PermissionError) as e:
            raise NonDownloadableKeyError from e

        except Exception as e:
            raise UnknownError from e

    def _upload(self, f, key: str, exists_ok=True, **kwargs) -> str:

        path = self._full_path(key)
        if os.path.exists(path) and not exists_ok:
            raise NonUploadableKeyError(f'Path {path} exists and exists_ok={exists_ok}')

        try:
            # Create the parent directories
            directory, _ = os.path.split(path)
            if directory:
                os.makedirs(directory, exist_ok=True)

            with open(path, 'wb') as f_key:
                copyfileobj(f, f_key)

        except (IsADirectoryError, PermissionError) as e:
            raise NonUploadableKeyError from e

        except Exception as e:
            raise UnknownError from e

        return key

    def _contains(self, key: str):
        path = self._full_path(key)
        return os.path.isfile(path)

