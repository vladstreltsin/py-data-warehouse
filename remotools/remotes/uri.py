from remotools.remotes.base import BaseRemote
from remotools.remotes.local import LocalRemote
from remotools.remotes.web import WebRemote
from remotools.utils import DictProxy
from remotools.remotes.exceptions import KeyNotFoundError
import typing as tp
import re

# A bunch of utility constants
REMOTE_NAME_RE = re.compile("^[a-zA-Z0-9-]+$")
REMOTE_NAME_SEPARATOR = '://'
WEB_REMOTE_NAMES = ['http', 'https']


class URIRemote(BaseRemote):
    """
    A remote whose keys that follow a uri-like syntax.

    This type of remote functions as an 'umbrella' class for other remotes. It allows for the composition
    of several remotes into a single one. The various remotes are provided in a dictionary whose keys are
    the remotes' names and the values are the remote instances themselves. If a certain object is identified
    by the key 'abc' by a remote whose dictionary-key is 'rem1', that object will be identified by the
    key 'rem1://abc' by the URIRemote.  In general, the syntax is key = <remote_name>://<remote_key>.

    There are several reserved remote names with the following meaning:
        file - the key is a file in the local file system (Realised with LocalRemote(prefix=None)
        https, http - used to designate a web URL. Available for download only.

    Attributes
    ----------
    remotes
        A dictionary like object mapping remote names to remotes

    Examples
    --------
    >> import io
    >> f = io.BytesIO()
    >> remote = URIRemote()
    >> remote.download(f, 'https://cdn.wallpapersafari.com/81/51/1Bx4Pg.jpg', keep_stream_position=True)
    >> remote.upload(f, 'file://home/<user>/jungle.jpg')
    """

    def __init__(self, remotes: tp.Optional[dict]=None, *args, **kwargs):
        super(URIRemote, self).__init__(*args, **kwargs)
        self.remotes = RemotesDict({'file': LocalRemote()})
        for name in WEB_REMOTE_NAMES:
            self.remotes[name] = WebRemote()
        self.remotes.update(remotes or {})

    @staticmethod
    def parse_key(key):
        assert REMOTE_NAME_SEPARATOR in key, f'A key must contain the separator {REMOTE_NAME_SEPARATOR} (given: {key})'
        remote_name, remote_key = key.split('://', maxsplit=1)

        # In case we deal with a web remote, we'll need the remote name back in the remote key
        if remote_name in WEB_REMOTE_NAMES:
            remote_key = key

        return remote_name, remote_key

    def _download(self, f, key: str, **kwargs):
        remote_name, remote_key = self.parse_key(key)
        if remote_name not in self.remotes:
            raise KeyNotFoundError(f'No such remote {remote_name}')

        remote = self.remotes[remote_name]
        remote.download(f, remote_key, progress=False, params=kwargs)

    def _upload(self, f, key: str, **kwargs):
        remote_name, remote_key = self.parse_key(key)
        if remote_name not in self.remotes:
            raise KeyNotFoundError(f'No such remote {remote_name}')

        remote = self.remotes[remote_name]
        remote.upload(f, remote_key, progress=False, params=kwargs)

    def _contains(self, key: str):
        remote_name, remote_key = self.parse_key(key)
        if remote_name not in self.remotes:
            return False

        remote = self.remotes[remote_name]
        return remote.contains(key)


class RemotesDict(DictProxy):
    """
    A utility class to hold remotes in a dictionary like structure.
    """

    def __setitem__(self, key, value):
        assert isinstance(key, str), f"Remote name must be a string (given: {key})"
        assert isinstance(value, BaseRemote), f"A remote must be a subclass of {BaseRemote.__name__} (given: {value}"
        assert REMOTE_NAME_RE.match(key), f"Remote name must only contain alphanumeric " \
                                          f"characters and/or a hyphen (-) (given: {key})"

        self._data[key] = value

