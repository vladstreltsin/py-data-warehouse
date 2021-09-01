from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, Future
from remotools.savers import BaseSaver
from remotools.savers import JSONPickleSaver
from remotools.remotes import BaseRemote
import typing as tp
from functools import partial
import logging
from cachetools import LRUCache
from collections import UserDict
from remotools.remotes.exceptions import KeyNotFoundError
from scalpl import Cut
from typing import Type
from contextlib import contextmanager
from operator import attrgetter


logger = logging.getLogger(__name__)


class RemoteDict(UserDict):
    """
    This class implements a key-value store backed by a remote. It provides the commit and fetch
    methods to upload and download its contents using a JSONPickleSever over the provided remote.
    """

    SEP = '/'

    def __init__(self, remote: tp.Optional[BaseRemote]=None, prefix=None, timeout=None, **kwargs):
        super(RemoteDict, self).__init__()
        self.prefix = prefix
        self.timeout = timeout

        self._remote = remote
        self._parent = None
        self._extra_prefix = None
        self._pool = None
        self._atomic = True

    @property
    def root(self):
        if self._parent is None:
            return self
        else:
            return self._parent.root

    def _check_key_value(self, key: str, value: tp.Any):
        if not isinstance(key, str):
            raise ValueError(f"Keys can only be strings (given type: {type(key)})")

        if self._atomic and isinstance(value, RemoteDict):
            raise ValueError(f"Atomic remote dicts can't contain other remote dicts")

        if not self._atomic and not isinstance(value, RemoteDict):
            raise ValueError(f"Non-Atomic remote dicts can only contain other remote dicts")

    def __setitem__(self, key: str, value: tp.Any):
        self._check_key_value(key, value)
        super(RemoteDict, self).__setitem__(key, value)

    @property
    def remote(self):
        if self._remote is None:
            if self._parent is None:
                raise ValueError('No remote was set and RemoteDict has no parent')
            else:
                return self._parent.remote
        else:
            return self._remote

    @property
    def full_prefix(self):
        if self._parent is None:
            return self.prefix
        else:
            return self._join(self._parent.full_prefix, self._extra_prefix, self.prefix)

    @property
    def pool(self):
        if self._pool is not None:
            return self._pool
        elif self._parent is not None:
            return self._parent.pool
        else:
            return None

    @pool.setter
    def pool(self, value: ThreadPoolExecutor):
        self._pool = value

    def remote_key(self, key: tp.Optional[str]):
        key = key or '.state'
        return self._join(self.full_prefix, key)

    # TODO what about nested calls here?
    @contextmanager
    def parallel(self, **kwargs):

        # In case no pool is available, start one
        if self.pool is None:
            self.pool = ThreadPoolExecutor(**kwargs)
            try:
                yield self

            finally:
                self.pool.shutdown(wait=True)
                self.pool = None

        # If a pool is available, then use it
        else:
            yield self

    @property
    def atomic(self):
        return self._atomic

    @property
    def parent(self):
        return self._parent

    def commit(self, key: tp.Optional[str]=None, upload_params=None, progress=True, **kwargs) -> tp.Optional[str]:
        """
        Save the self.data attribute in a state file using a JSONPickleSaver over the remote.
        Return the actual key that the result was saved with.
        """

        # Attach remote key prefix
        remote_key = key or self.remote_key(key)
        saved_key = JSONPickleSaver(self.remote).save(obj=self.data,
                                                      key=remote_key,
                                                      upload_params=upload_params,
                                                      progress=progress,
                                                      **kwargs)
        if saved_key != remote_key:
            logger.warning(f'Remote key was set to : {saved_key} by the saver during commit')
            return saved_key

        return None

    def fetch(self, key: tp.Optional[str]=None, download_params=None, progress=True, **kwargs):

        # Attach remote key prefix
        remote_key = key or self.remote_key(key)

        try:
            self.data.update(JSONPickleSaver(self.remote).load(remote_key,
                                                               download_params=download_params,
                                                               progress=progress,
                                                               **kwargs))
        except KeyNotFoundError:
            logging.warning('No state file was found during fetch')

    def dump(self):
        """ Returns constructor arguments required to later re-create the given object """
        state = self.__dict__.copy()
        state.pop('data')
        state.pop('_remote')
        state.pop('_parent')
        state.pop('_extra_prefix')
        state.pop('_pool')
        return state

    def _get_future_result(self, future: Future):
        """ Obtains the result of a future object with exception checking """
        exc = future.exception(timeout=self.timeout)
        if exc:
            raise exc
        return future.result(timeout=self.timeout)

    def _join(self, *args):
        return self.SEP.join(filter(lambda x: x, list(args)))


class RemoteBlobDict(RemoteDict):
    """
    This class extends the RemoteDict class. Instead of storing the whole
    dictionary in memory, it stores each of its objects with the provided saver in a parallel fashion using
    ambient threads.
    """

    def __init__(self,
                 saver_cls: tp.Type[BaseSaver],
                 **kwargs):

        super(RemoteBlobDict, self).__init__(**kwargs)
        self.saver_cls = saver_cls

    @property
    def saver(self):
        return self.saver_cls(self.remote)

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs):
        """ Stores the object using the provided saver in a parallel fashion """

        self._check_key_value(key, obj)

        # If for some reason the stored object is a future, then we have to gets its result to proceed
        if isinstance(obj, Future):
            obj = self._get_future_result(obj)

        if self.pool is not None:
            # Submit the save task to the Executor
            future: Future = self.pool.submit(self.saver.save,
                                              obj=obj,
                                              key=self.remote_key(key),
                                              upload_params=upload_params,
                                              progress=progress,
                                              **kwargs)

            # Keep the Future object in the executor. When the future finishes, that is,
            # when the object finishes uploading, or an error occurs, a callback is called to
            # keep to object key in self.data
            self.data[key] = future
            future.add_done_callback(partial(self._finalize_upload, key))

        else:
            self.data[key] = self.saver.save(obj=obj,
                                             key=self.remote_key(key),
                                             upload_params=upload_params,
                                             progress=progress, **kwargs)

    def load(self, key: str, blocking=True, download_params=None, progress=True, **kwargs):
        """ Loads the object attached to the given key """

        if key not in self.data:
            raise KeyError(f'No such key: {key}')

        obj = self.data[key]

        # Check if the object is currently being uploaded. If it is, wait until done
        if isinstance(obj, Future):
            obj = self._get_future_result(obj)

        if not isinstance(obj, str):
            raise TypeError(f'Stored values must be strings')

        if self.pool is not None:
            # Download the object in the Executor. When done, update the cache using the callback
            future: Future = self.pool.submit(self.saver.load,
                                              key=obj,
                                              download_params=download_params,
                                              progress=progress,
                                              **kwargs)
            future.add_done_callback(partial(self._finalize_download, key))

            # Wait until download completes in the blocking mode. Otherwise, return the future
            if blocking:
                return self._get_future_result(future)
            else:
                return future

        else:
            return self.saver.load(key=obj, download_params=download_params, progress=progress, **kwargs)

    def _finalize_upload(self, key: str, future: Future):
        result = self._get_future_result(future)
        self.data[key] = result

    def _finalize_download(self, key: str, future: Future):
        return self._get_future_result(future)

    def __setitem__(self, key: str, value: tp.Any):
        self.save(key=key, obj=value)

    def __getitem__(self, key):
        return self.load(key=key)

    def keys(self):
        yield from self.data.keys()

    def values(self):
        for key in self.keys():
            yield self[key]

    def items(self):
        for key in self.keys():
            yield key, self[key]

    def commit(self, key: tp.Optional[str]=None, upload_params=None, progress=True, **kwargs):

        # Make sure that everything was uploaded before committing
        for k in self.data.keys():
            obj = self.data[k]
            if isinstance(obj, Future):
                self._get_future_result(future=obj)

        return super(RemoteBlobDict, self).commit(key=key,
                                                  upload_params=upload_params,
                                                  progress=progress,
                                                  **kwargs)


class RemoteBlobDictWithLRUCache(RemoteBlobDict):
    """ Extends RemoteBlobDict by adding it an LRU Cache to hold the results in memory """

    def __init__(self,
                 saver_cls: tp.Type[BaseSaver],
                 maxsize=1,
                 getsizeof=None,
                 **kwargs):

        super(RemoteBlobDictWithLRUCache, self).__init__(saver_cls=saver_cls, **kwargs)
        self.maxsize = maxsize
        self.getsizeof = getsizeof
        self._cache = LRUCache(maxsize=maxsize, getsizeof=getsizeof)

    @property
    def cache(self):
        return self._cache

    def _finalize_download(self, key: str, future: Future):
        result = super(RemoteBlobDictWithLRUCache, self)._finalize_download(key=key, future=future)
        self._cache[key] = result

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs):
        super(RemoteBlobDictWithLRUCache, self).save(obj=obj, key=key,
                                                     upload_params=upload_params, progress=progress, **kwargs)

        self._cache[key] = obj

    def load(self, key: str, blocking=True, download_params=None, progress=True, **kwargs):
        if key in self._cache:
            return self._cache[key]
        obj = super(RemoteBlobDictWithLRUCache, self).load(key=key,
                                                           blocking=blocking,
                                                           download_params=download_params,
                                                           progress=progress, **kwargs)
        if not isinstance(obj, Future):
            self._cache[key] = obj

        return obj

    def dump(self):
        state = super(RemoteBlobDictWithLRUCache, self).dump()
        state.pop('_cache')
        return state


class CompositeRemoteDict(RemoteDict):

    def __init__(self, *, state_remote: tp.Optional[BaseRemote] = None, **kwargs):
        super(CompositeRemoteDict, self).__init__(**kwargs)
        self._atomic = False
        self.state_remote = state_remote

    def __setitem__(self, name: str, dct: RemoteDict):

        self._check_key_value(name, dct)

        if dct._parent is self:
            raise ValueError('Dict is already a child')

        self.data[name] = dct
        dct._parent = self
        dct._extra_prefix = name

    def __delitem__(self, name: str):
        dct = self.data.pop(name)
        dct._parent = None
        dct._extra_prefix = None

    def commit(self, key: tp.Optional[str]=None, upload_params=None, progress=True, **kwargs):

        # Commit all children
        if self.pool is not None:
            # Fetch all remote dicts using ambient threads
            futures = {name: self.pool.submit(remote_dict.commit,
                                              key=key,
                                              upload_params=upload_params,
                                              progress=progress, **kwargs)
                       for name, remote_dict in self.data.items()}

            # Make sure all futures terminate
            key_dict = {name: self._get_future_result(future) for name, future in futures.items()}

        else:
            key_dict = {name: remote_dict.commit(key=key, upload_params=upload_params, progress=progress, **kwargs)
                        for name, remote_dict in self.data.items()}

        # Store the dump parameters of the children and the keys they were stored with
        state = {name: dict(dump=child.dump(),
                            cls=type(child),
                            key=key_dict[name])
                 for name, child in self.data.items()}

        remote_key = key or self.remote_key(key)
        saved_key = JSONPickleSaver(self.state_remote or self.remote).save(obj=state,
                                                                           key=remote_key,
                                                                           upload_params=upload_params,
                                                                           progress=progress)

        if saved_key != remote_key:
            logger.warning(f'Remote key was set to : {saved_key} by the saver during commit')
            return saved_key

        return None

    def fetch(self, key: tp.Optional[str]=None, download_params=None, progress=True, **kwargs):

        try:
            remote_key = key or self.remote_key(key)
            state = JSONPickleSaver(self.state_remote or self.remote).load(key=remote_key,
                                                                           download_params=download_params,
                                                                           progress=progress)
        except KeyNotFoundError:
            logging.warning('No state file was found during fetch')
            return

        # Construct the children objects
        for name, dct in state.items():
            self[name] = dct['cls'](**dct['dump'])

        # Fetch all remote dicts using ambient threads
        if self.pool is not None:

            futures = [self.pool.submit(remote_dict.fetch,
                                        key=state[name]['key'] if name in state else key,
                                        download_params=download_params,
                                        progress=progress, **kwargs)
                       for name, remote_dict in self.data.items()]

            # Make sure all futures terminate
            for future in futures:
                self._get_future_result(future)

        else:
            for name, remote_dict in self.data.items():
                remote_dict.fetch(key=state[name]['key'] if name in state else key,
                                  download_params=download_params,
                                  progress=progress, **kwargs)


# class XPathCompositeRemoteDict(CompositeRemoteDict):
#     """ Same as parent class, but allows x-path syntax """
#
#     def __setitem__(self, key: str, value: tp.Any):
#         if self.SEP in key:
#             name, key = key.split(self.SEP, maxsplit=1)
#             self.data[name][key] = value
#         else:
#             super(XPathCompositeRemoteDict, self).__setitem__(key, value)
#
#     def __getitem__(self, key: str):
#         if self.SEP in key:
#             name, key = key.split(self.SEP, maxsplit=1)
#             return self.data[name][key]
#         else:
#             return super(XPathCompositeRemoteDict, self).__getitem__(key)
#
#     def __delitem__(self, key: str):
#         if self.SEP in key:
#             name, key = key.split(self.SEP, maxsplit=1)
#             del self[name][key]
#         else:
#             super(XPathCompositeRemoteDict, self).__delitem__(key)


