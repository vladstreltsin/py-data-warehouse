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


logger = logging.getLogger(__name__)
#
# SEP = '/'
#
#
# class ObjectStore:
#
#     def __init__(self, library_id: str, remote: BaseRemote, max_threads=None, timeout=None, autocommit=False):
#         self._pool = None
#         self._max_threads = max_threads
#         self._sections = {}
#         self._library_id = library_id
#         self._remote = remote
#
#         self.autocommit = autocommit
#         self.timeout = timeout
#
#     def __enter__(self):
#         self._pool = ThreadPoolExecutor(max_workers=self._max_threads)
#         self._pool.__enter__()
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#
#         # Commit everything before exiting
#         if self.autocommit:
#             self.commit()
#
#         self._pool.__exit__(exc_type, exc_val, exc_tb)
#         self._pool = None
#
#     def add_section(self, name, saver_cls, cache_maxsize: int = 0, **kwargs):
#         if self._pool is None:
#             raise ValueError("Thread-Pool Executor is not running. Use the class through a context manager.")
#
#         self._sections[name] = _Section(section_id=f'{self._library_id}/{name}',
#                                         remote=self._remote,
#                                         saver_cls=saver_cls,
#                                         pool=self._pool,
#                                         cache_maxsize=cache_maxsize,
#                                         **kwargs)
#
#     def add_in_memory_section(self, name):
#         self._sections[name] = _InMemorySection(section_id=f'{self._library_id}/{name}', remote=self._remote)
#
#     def __getitem__(self, key: str):
#
#         if SEP not in key:
#             store_id, key = key, None
#         else:
#             store_id, key = key.split(SEP, maxsplit=1)
#
#         if store_id not in self._sections:
#             raise KeyError(f'No such store_id: {store_id}')
#
#         store = self._sections[store_id]
#
#         return store if key is None else store[key]
#
#     def __setitem__(self, key, value):
#
#         if SEP not in key:
#             raise ValueError("Key must be of the form <store_id>/<asset_name>")
#
#         store_id, key = key.split(SEP, maxsplit=1)
#
#         if store_id not in self._sections:
#             raise KeyError(f'No such store_id: {store_id}')
#
#         store = self._sections[store_id]
#         store[key] = value
#
#     def commit(self, key: str='.stores', upload_params=None, progress=True, **kwargs):
#
#         key = self.remote_key(key)
#         store_info_dict = {name: store.info for name, store in self._sections.items()}
#
#         # Commit self
#         self_future = self._pool.submit(JSONPickleSaver(self._remote).save,
#                                         obj=store_info_dict,
#                                         key=key,
#                                         upload_params=upload_params,
#                                         progress=progress,
#                                         **kwargs)
#
#         # Commit all stores
#         store_futures = {name: self._pool.submit(store.commit,
#                                                  upload_params=upload_params,
#                                                  progress=progress,
#                                                  **kwargs) for name, store in self._sections.items()}
#
#         store_saved_keys = {name: self._get_future_result(future) for name, future in store_futures.items()}
#
#         self_saved_key = self._get_future_result(self_future)
#         if self_saved_key != key:
#             logger.warning(f'Remote key was set to : {self_saved_key} by the saver '
#                            f'for ObjectLibrary {self._library_id}')
#
#         return self_saved_key, store_saved_keys
#
#     def fetch(self, key: str='.stores', download_params=None, progress=True, **kwargs):
#
#         key = self.remote_key(key)
#
#         # Fetch stores info
#         store_info_dict = JSONPickleSaver(self._remote).load(key=key,
#                                                              download_params=download_params,
#                                                              progress=progress,
#                                                              **kwargs)
#
#         # Init stores:
#         for store_id, store_info in store_info_dict.items():
#             self._sections.update({store_id: _Section(pool=self._pool,
#                                                       remote=self._remote,
#                                                       **store_info)})
#
#         # Fetch sections
#         futures = [self._pool.submit(store.fetch,
#                                      download_params=download_params,
#                                      progress=progress,
#                                      **kwargs) for store in self._sections.values()]
#         for future in futures:
#             self._get_future_result(future)
#
#     def remote_key(self, key):
#         """ Attach store_id to the key """
#         return f'{self._library_id}{SEP}{key}'
#
#     def _get_future_result(self, future: Future):
#         exc = future.exception(timeout=self.timeout)
#         if exc:
#             raise exc
#         return future.result(timeout=self.timeout)
#
#
# class _BaseSection(ABC):
#
#     @abstractmethod
#     def __getitem__(self, item):
#         pass
#
#     @abstractmethod
#     def __setitem__(self, key, value):
#         pass
#
#     @abstractmethod
#     def __contains__(self, item):
#         pass
#
#     @abstractmethod
#     def keys(self):
#         pass
#
#     @abstractmethod
#     def values(self):
#         pass
#
#     @abstractmethod
#     def items(self):
#         pass
#
#     @abstractmethod
#     def commit(self, key: str='.state', upload_params=None, progress=True, **kwargs):
#         pass
#
#     @abstractmethod
#     def fetch(self, key: str = '.state', download_params=None, progress=True, **kwargs):
#         pass
#
#     @property
#     @abstractmethod
#     def info(self):
#         pass
#
#
# class _Section(_BaseSection):
#
#     def __init__(self,
#                  section_id: str,
#                  remote: BaseRemote,
#                  saver_cls,
#                  pool: ThreadPoolExecutor,
#                  cache_maxsize: int = 0,
#                  timeout=None):
#
#         self.section_id = section_id
#         self.cache_maxsize = cache_maxsize
#         self.timeout = timeout
#         self._saver_cls = saver_cls
#
#         self._objects = {}
#         self._cache = LRUCache(maxsize=cache_maxsize)
#         self._remote = remote
#         self._saver = saver_cls(self._remote)
#         self._pool = pool
#
#     def keys(self):
#         for key in self._objects.keys():
#             yield key
#
#     def values(self):
#         for key in self._objects.keys():
#             yield self[key]
#
#     def items(self):
#         for key in self._objects.keys():
#             yield key, self[key]
#
#     def commit(self, key: str='.state', upload_params=None, progress=True, **kwargs):
#         """ Save objects in a state file on the remote """
#
#         # Collect object identifiers. Make sure that all uploads are finished.
#         objects = {}
#         for name, obj in self._objects.items():
#             if isinstance(obj, Future):
#                 obj = self._get_future_result(obj)
#
#             if isinstance(obj, str):
#                 objects[name] = obj
#             else:
#                 raise TypeError(f'Stored object has illegal type {type(obj)}')
#
#         key = self.remote_key(key)
#         saved_key = JSONPickleSaver(self._remote).save(obj=objects,
#                                                        key=key,
#                                                        upload_params=upload_params,
#                                                        progress=progress,
#                                                        **kwargs)
#
#         if saved_key != key:
#             logger.warning(f'Remote key was set to : {saved_key} by the saver for Object Store {self.section_id}')
#
#         return saved_key
#
#     def fetch(self, key: str='.state', download_params=None, progress=True, **kwargs):
#         """ Fetch objects from the remote """
#         key = self.remote_key(key)
#         self._objects.update(JSONPickleSaver(self._remote).load(key,
#                                                                 download_params=download_params,
#                                                                 progress=progress,
#                                                                 **kwargs))
#
#     def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs):
#         """ Stores the object using the provided saver in a parallel fashion """
#
#         # Submit the save task to the Executor
#         future: Future = self._pool.submit(self._saver.save,
#                                            obj=obj,
#                                            key=self.remote_key(key),
#                                            upload_params=upload_params,
#                                            progress=progress,
#                                            **kwargs)
#
#         # Keep the Future object in the executor. When the future finishes, that is,
#         # when the object finishes uploading or an error occurs, a callback is called to
#         # keep to object URI in self._objects
#         future.add_done_callback(partial(self._finalize_upload, key))
#         self._objects[key] = future
#
#         if self.use_cache:
#             self._cache[key] = obj
#
#     def load(self, key: str, blocking=True, download_params=None, progress=True, **kwargs):
#         """ Loads the object attached to the given key """
#
#         # Fetch object from the cache if available
#         if self.use_cache and key in self._cache:
#             return self._cache[key]
#
#         if key not in self._objects:
#             raise KeyError(f'No such key: {key}')
#         obj = self._objects[key]
#
#         # Check if the object is currently being uploaded. If it is, wait until done
#         if isinstance(obj, Future):
#             obj = self._get_future_result(obj)
#
#         if isinstance(obj, str):
#
#             # Download the object in the Executor. When done, update the cache using the callback
#             future: Future = self._pool.submit(self._saver.load,
#                                                key=self.remote_key(key),
#                                                download_params=download_params,
#                                                progress=progress,
#                                                **kwargs)
#             future.add_done_callback(partial(self._finalize_download, key))
#
#             # Wait until download completes in the blocking mode. Otherwise, return the future
#             if blocking:
#                 return self._get_future_result(future)
#             else:
#                 return future
#
#         else:
#             raise TypeError(f'Stored object has illegal type {type(obj)}')
#
#     def __setitem__(self, key: str, value: tp.Any):
#         self.save(key=key, obj=value)
#
#     def __getitem__(self, key):
#         return self.load(key=key)
#
#     def __contains__(self, key):
#         return key in self._objects
#
#     def remote_key(self, key):
#         """ Attach store_id to the key """
#         return f'{self.section_id}{SEP}{key}'
#
#     def _finalize_upload(self, key: str, future: Future):
#         result = self._get_future_result(future)
#         self._objects[key] = result
#
#     def _finalize_download(self, key: str, future: Future):
#         result = self._get_future_result(future)
#
#         # Update cache if needed
#         if self.use_cache:
#             self._cache[key] = result
#
#     def _get_future_result(self, future: Future):
#         exc = future.exception(timeout=self.timeout)
#         if exc:
#             raise exc
#         return future.result(timeout=self.timeout)
#
#     @property
#     def use_cache(self):
#         return self.cache_maxsize > 0
#
#     @property
#     def info(self):
#         return {'cache_maxsize': self.cache_maxsize,
#                 'section_id': self.section_id,
#                 'timeout': self.timeout,
#                 'saver_cls': self._saver_cls}
#
#     def clear_cache(self):
#         self._cache.clear()
#
#
# class _InMemorySection(UserDict, _BaseSection):
#
#     def __init__(self, section_id, remote):
#         # UserDict.__init__(self)
#         super(_InMemorySection, self).__init__()
#         self.section_id = section_id
#         self._remote = remote
#
#     @property
#     def info(self):
#         return {'section_id': self.section_id}
#
#     def commit(self, key: str='.state', upload_params=None, progress=True, **kwargs):
#         key = self.remote_key(key)
#         saved_key = JSONPickleSaver(self._remote).save(obj=self.data,
#                                                        key=key,
#                                                        upload_params=upload_params,
#                                                        progress=progress,
#                                                        **kwargs)
#
#         if saved_key != key:
#             logger.warning(f'Remote key was set to : {saved_key} by the saver for Object Store {self.section_id}')
#
#         return saved_key
#
#     def fetch(self, key: str = '.state', download_params=None, progress=True, **kwargs):
#         """ Fetch objects from the remote """
#         key = self.remote_key(key)
#         self.data.update(JSONPickleSaver(self._remote).load(key,
#                                                             download_params=download_params,
#                                                             progress=progress,
#                                                             **kwargs))
#
#     def remote_key(self, key):
#         """ Attach store_id to the key """
#         return f'{self.section_id}{SEP}{key}'
#


class RemoteDict(UserDict):
    """
    This class implements a key-value store backed by a remote. It provides the commit and fetch
    methods to upload and download its contents using a JSONPickleSever over the provided remote.
    """

    def __init__(self, remote: BaseRemote, **kwargs):
        super(RemoteDict, self).__init__()
        self.remote = remote

    def commit(self, key: str = '.state', upload_params=None, progress=True, **kwargs) -> str:
        """ Save the self.data attribute in a state file using a JSONPickleSaver over the remote.
        Return the actual key that the result was saved with.
        """

        saved_key = JSONPickleSaver(self.remote).save(obj=self.data,
                                                      key=key,
                                                      upload_params=upload_params,
                                                      progress=progress,
                                                      **kwargs)
        if saved_key != key:
            logger.warning(f'Remote key was set to : {saved_key} by the saver during commit')

        return saved_key

    def fetch(self, key: str = '.state', download_params=None, progress=True, **kwargs):

        try:
            self.data.update(JSONPickleSaver(self.remote).load(key,
                                                               download_params=download_params,
                                                               progress=progress,
                                                               **kwargs))
        except KeyNotFoundError:
            logging.warning('No state file was found during fetch')


class RemoteBlobDict(RemoteDict):
    """ This class extends the RemoteDict class. Instead of storing the whole
    dictionary in memory, it stores each of its objects with the provided saver in a parallel fashion using
    ambient threads.
    """

    def __init__(self, remote: BaseRemote, saver: BaseSaver, pool: ThreadPoolExecutor, timeout=None, **kwargs):
        super(RemoteBlobDict, self).__init__(remote=remote)
        self.saver = saver
        self.pool = pool
        self.timeout = timeout

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs):
        """ Stores the object using the provided saver in a parallel fashion """

        # Submit the save task to the Executor
        future: Future = self.pool.submit(self.saver.save,
                                          obj=obj,
                                          key=key,
                                          upload_params=upload_params,
                                          progress=progress,
                                          **kwargs)

        # Keep the Future object in the executor. When the future finishes, that is,
        # when the object finishes uploading, or an error occurs, a callback is called to
        # keep to object key in self.data
        self.data[key] = future
        future.add_done_callback(partial(self._finalize_upload, key))

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

    def _finalize_upload(self, key: str, future: Future):
        result = self._get_future_result(future)
        self.data[key] = result

    def _finalize_download(self, key: str, future: Future):
        return self._get_future_result(future)

    def _get_future_result(self, future: Future):
        """ Obtains the result of a future object with exception checking """
        exc = future.exception(timeout=self.timeout)
        if exc:
            raise exc
        return future.result(timeout=self.timeout)

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

    def commit(self, key: str = '.state', upload_params=None, progress=True, **kwargs):

        # Make sure that everything was uploaded before committing
        for k in self.data.keys():
            obj = self.data[k]
            if isinstance(obj, Future):
                self._get_future_result(future=obj)

        return super(RemoteBlobDict, self).commit(key=key, upload_params=upload_params, progress=progress, **kwargs)


class RemoteBlobDictWithLRUCache(RemoteBlobDict):
    """ Extends RemoteBlobDict by adding it an LRU Cache to hold the results in memory """

    def __init__(self, remote: BaseRemote, saver: BaseSaver, pool: ThreadPoolExecutor, timeout=None,
                 maxsize=1, getsizeof=None, **kwargs):
        super(RemoteBlobDictWithLRUCache, self).__init__(remote=remote, saver=saver, pool=pool, timeout=timeout)
        self.cache = LRUCache(maxsize=maxsize, getsizeof=getsizeof)

    def _finalize_download(self, key: str, future: Future):
        result = super(RemoteBlobDictWithLRUCache, self)._finalize_download(key=key, future=future)
        self.cache[key] = result

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs):
        super(RemoteBlobDictWithLRUCache, self).save(obj=obj, key=key,
                                                     upload_params=upload_params, progress=progress, **kwargs)
        self.cache[key] = obj

    def load(self, key: str, blocking=True, download_params=None, progress=True, **kwargs):
        if key in self.cache:
            return self.cache[key]
        return super(RemoteBlobDictWithLRUCache, self).load(key=key, blocking=blocking,
                                                            download_params=download_params, progress=progress, **kwargs)


class DummyRemote(BaseRemote):

    def _download(self, f, key: str, **kwargs):
        raise NotImplementedError

    def _upload(self, f, key: str, **kwargs):
        raise NotImplementedError

    def _contains(self, key: str):
        raise NotImplementedError


class RemoteDictCollection(UserDict):
    """ A collection of remote dicts that share the same ThreadPoolExecutor """

    SEPARATOR = '/'

    def __init__(self, pool: ThreadPoolExecutor, timeout=None):
        super(RemoteDictCollection, self).__init__()
        self.data = Cut(sep=self.SEPARATOR)
        self.pool = pool
        self.timeout = timeout

    def add_remote_dict(self, name, cls: Type[RemoteDict], remote: BaseRemote, **kwargs):
        self.data[name] = cls(pool=self.pool, remote=remote, **kwargs)

    def __setitem__(self, key: str, value: tp.Any):
        if self.SEPARATOR not in key:
            raise ValueError(f'key must contain the separator: {self.SEPARATOR}')
        super(RemoteDictCollection, self).__setitem__(key, value)

    def fetch(self, key: str='.state', download_params=None, progress=True,
              options: tp.Optional[dict]=None, **kwargs):

        # Initialize the default options
        opts = {name: dict(key=key, download_params=download_params, progress=progress, **kwargs)
                for name in self.data}

        # Update the options per member remote dict
        if options is not None:
            for name in set(self.data.keys()) & set(options.keys()):
                opts[name].update(options[name])

        # Fetch all remote dicts using ambient threads
        futures = [self.pool.submit(remote_dict.fetch, **opts[name])
                   for name, remote_dict in self.data.items()]

        # Make sure all futures terminate
        for future in futures:
            self._get_future_result(future)

    def commit(self, key: str='.state', upload_params=None, progress=True,
               options: tp.Optional[dict] = None, **kwargs):

        # Initialize the default options
        opts = {name: dict(key=key, upload_params=upload_params, progress=progress, **kwargs)
                for name in self.data}

        # Update the options per member remote dict
        if options is not None:
            for name in set(self.data.keys()) & set(options.keys()):
                opts[name].update(options[name])

        # Fetch all remote dicts using ambient threads
        futures = {name: self.pool.submit(remote_dict.commit, **opts[name])
                   for name, remote_dict in self.data.items()}

        # Make sure all futures terminate
        futures = {name: self._get_future_result(future) for name, future in futures.items()}

        return futures

    def _get_future_result(self, future: Future):
        """ Obtains the result of a future object with exception checking """
        exc = future.exception(timeout=self.timeout)
        if exc:
            raise exc
        return future.result(timeout=self.timeout)



