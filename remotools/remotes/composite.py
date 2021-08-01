from __future__ import annotations
from remotools.remotes.base import BaseRemote
from remotools.remotes.local import LocalRemote
from remotools.remotes.web import WebRemote
from collections import UserDict
from remotools.remotes.exceptions import KeyNotFoundError
import typing as tp
import re


class CompositeRemote(BaseRemote):
    """
    A remote whose keys that follow a uri-like syntax.

    This type of remote functions as an 'umbrella' class for other remotes. It allows for the composition
    of several remotes into a single one. The various remotes are provided in a dictionary whose keys are
    the remotes' names and the values are the remote instances themselves. If a certain object is identified
    by the key 'abc' by a remote whose dictionary-key is 'rem1', that object will be identified by the
    key 'rem1/abc' by the URIRemote.  In general, the syntax is key = <remote_name>/<remote_key>.

    Attributes
    ----------
    remotes
        A dictionary like object mapping remote names to remotes

    """

    SEPARATOR = '/'

    def __init__(self, remotes: tp.Optional[dict]=None, *args, **kwargs):
        super(CompositeRemote, self).__init__(*args, **kwargs)
        self.remotes = _RemotesDict(dict={'%': LocalRemote()})
        self.remotes.update(remotes or {})

    @staticmethod
    def parse_key(key):
        assert CompositeRemote.SEPARATOR in key, \
            f'A key must contain the separator {CompositeRemote.SEPARATOR} (given: {key})'
        remote_name, remote_key = key.split(CompositeRemote.SEPARATOR, maxsplit=1)

        return remote_name, remote_key

    def _download(self, f, key: str, **kwargs):
        remote_name, remote_key = self.parse_key(key)
        if remote_name not in self.remotes:
            raise KeyNotFoundError(f'No such remote {remote_name}')

        remote = self.remotes[remote_name]
        remote.download(f, remote_key, progress=False, params=kwargs)

    def _upload(self, f, key: str, **kwargs) -> str:
        remote_name, remote_key = self.parse_key(key)
        if remote_name not in self.remotes:
            raise KeyNotFoundError(f'No such remote {remote_name}')

        remote = self.remotes[remote_name]
        return f'{remote_name}{CompositeRemote.SEPARATOR}{remote.upload(f, remote_key, progress=False, params=kwargs)}'

    def _contains(self, key: str):
        remote_name, remote_key = self.parse_key(key)

        if remote_name not in self.remotes:
            return False

        remote = self.remotes[remote_name]
        return remote.contains(remote_key)


class _RemotesDict(UserDict):
    """
    A utility class to hold remotes in a dictionary like structure.
    """

    def __setitem__(self, key, value):
        assert isinstance(key, str), f"Remote name must be a string (given: {key})"
        assert isinstance(value, BaseRemote), f"A remote must be a subclass of {BaseRemote.__name__} (given: {value}"
        super(_RemotesDict, self).__setitem__(key, value)


