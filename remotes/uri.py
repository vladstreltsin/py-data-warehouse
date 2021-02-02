from remotes.base import BaseRemote
from remotes.local import LocalRemote
from remotes.gs import GSRemote
from remotes.s3 import S3Remote
from remotes.hfs import HFSRemote
from exceptions import RemoteError
from utils import join, keep_position
import requests
import os
import os.path as osp
import typing as T
from abc import ABC, abstractmethod
from sqlitedict import SqliteDict
from concurrent.futures import ThreadPoolExecutor
import io
from functools import partial


def parse_params(s: str) -> T.Dict[str, str]:
    """ parse uri parameter string given as: s = 'p1=v1;p2=v2;...'"""
    if len(s) == 0:
        return {}

    params = {}
    for x in s.split(';'):
        key, value = x.split('=', maxsplit=1)
        params[key] = value

    return params


def parse_uri(uri):
    scheme, netloc, url, params, query, fragment = requests.utils.urlparse(uri)
    key = join(netloc, url)
    params = parse_params(params)
    return scheme, key, params


class URIRemote(BaseRemote):
    """Remote whose keys are given in URI format"""

    def __init__(self, *args, remotes=None, **kwargs):
        super(URIRemote, self).__init__(*args, **kwargs)
        self.remotes = remotes or {}       # Refactor this to a separate class

    def download(self, f, key: str, chunk_size=8192, *args, **kwargs):

        scheme, path, params = parse_uri(key)

        # Download from the local file system
        if scheme == 'file':
            LocalRemote(progress=False).download(f, path)

        # Try getting the result from the specified remotes
        elif scheme in self.remotes:
            self.remotes[scheme].download(f, path)

        # The default gs remote
        elif scheme == 'gs':
            GSRemote(**params).download(f, path)

        # The default s3 remote
        elif scheme == 's3':
            S3Remote(**params).download(f, path)

        # The overall default - fallback to general requests get()
        else:
            # https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
            with self.download_progress_bar(f, key) as fp:
                with requests.get(key, stream=True) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        fp.write(chunk)

    def upload(self, f, key: str=None, *args, **kwargs):

        scheme, path, params = parse_uri(key)

        # Download from the local file system
        if scheme == 'file':
            new_key = LocalRemote(progress=False).upload(f, path)

        # Try getting the result from the specified remotes
        elif scheme in self.remotes:
            new_key = self.remotes[scheme].upload(f, path)

        # The default gs remote
        elif scheme == 'gs':
            new_key = GSRemote(**params).upload(f, path)

        # The default s3 remote
        elif scheme == 's3':
            new_key = S3Remote(**params).upload(f, path)

        else:
            raise RemoteError(f"Unknown scheme {scheme}")

        return f'{scheme}://{new_key}'

    def contains(self, key: str):

        scheme, path, params = parse_uri(key)

        # Download from the local file system
        if scheme == 'file':
            return LocalRemote(progress=False).contains(path)

        # Try getting the result from the specified remotes
        if scheme in self.remotes:
            return self.remotes[scheme].contains(path)

        # The default gs remote
        if scheme == 'gs':
            return GSRemote(**params).contains(path)

        # The default s3 remote
        if scheme == 's3':
            return S3Remote(**params).contains(path)

        return False


class CacheDB(ABC):
    """ A base class from """
    @abstractmethod
    def __getitem__(self, item):
        pass

    @abstractmethod
    def __setitem__(self, key, value):
        pass

    @abstractmethod
    def __contains__(self, item):
        pass


class SqliteDictCacheDB:

    def __init__(self, path):
        os.makedirs(osp.dirname(path), exist_ok=True)
        self.db_path = path

    def __getitem__(self, item):
        with SqliteDict(self.db_path) as dct:
            return dct[item]

    def __setitem__(self, key, value):
        with SqliteDict(self.db_path) as dct:
            dct[key] = value
            dct.commit()

    def __contains__(self, item):
        with SqliteDict(self.db_path) as dct:
            return item in dct


class CachingURIRemote(URIRemote):
    """Same as URIRemote but with a local cache support"""

    def __init__(self, prefix, hfs_width=2, hfs_depth=4, hfs_algorithm='xxh128', remotes=None, *args, **kwargs):
        super(URIRemote, self).__init__(*args, **kwargs)
        self.cache = HFSRemote(remote=LocalRemote(prefix=prefix, progress=False),
                               width=hfs_width, depth=hfs_depth, algorithm=hfs_algorithm)

        self.cache_db = SqliteDictCacheDB(osp.join(prefix, '.cache'))
        self.remotes = remotes or {}       # Refactor this to a separate class

    def download(self, f, key: str, search_cache=True, update_cache=True, *args, **kwargs):
        scheme, path, _ = parse_uri(key)

        # This means a direct hash lookup
        if scheme == '':
            self.cache.download(f, path)
            return

        # Check whether the specified key is in the cache
        if search_cache and key in self.cache_db:
            path = self.cache_db[key]
            self.cache.download(f, path)
            return

        # Download from the remote
        if update_cache:
            with keep_position(f):
                super(CachingURIRemote, self).download(f, key, *args, **kwargs)
            hid = self.cache.upload(f)
            self.cache_db[key] = hid
            return

        super(CachingURIRemote, self).download(f, key, *args, **kwargs)

    def upload(self, f, key: str=None, update_cache=True, check_exists=False, *args, **kwargs):
        key = key or ''
        scheme, path, _ = parse_uri(key)

        # This means a direct upload to the cache
        if scheme == '':
            if path != '':
                raise RemoteError("Key cannot be specified when uploading to cache directly")
            return self.cache.upload(f)

        if update_cache:
            with keep_position(f):
                key = super(CachingURIRemote, self).upload(f, key, *args, check_exists=check_exists, **kwargs)
            hid = self.cache.upload(f, check_exists=check_exists)
            self.cache_db[key] = hid
            return key

        return super(CachingURIRemote, self).upload(f, key, *args, check_exists=check_exists, **kwargs)

    def contains(self, key: str):
        scheme, path, _ = parse_uri(key)

        # This means a direct upload to the cache
        if scheme == '':
            if key != '':
                raise RemoteError("Key cannot be specified when uploading to cache directly")
            return self.cache.contains(key)

        return super(CachingURIRemote, self).contains(key)

    def fetch(self, *keys, search_cache=True, max_workers=None):
        """ Pre-fetch all keys into the cache """

        def download(k):
            f = io.BytesIO()
            self.download(f, k, search_cache=search_cache)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for key in keys:
                pool.submit(partial(download, key))
