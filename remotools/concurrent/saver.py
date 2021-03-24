from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import typing as tp
import tqdm
from operator import setitem
from functools import partial
from remotools.exceptions import SaverError

if tp.TYPE_CHECKING:
    from remotools.savers import BaseSaver


class ConcurrentSaver:

    def __init__(self, saver: BaseSaver, **kwargs):
        self.saver = saver
        self._pool = ThreadPoolExecutor(**kwargs)

    def __enter__(self):
        self._pool.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pool.__exit__(exc_type, exc_val, exc_tb)

    def async_save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs) -> Future:
        return self._pool.submit(self.saver.save, obj=obj, key=key,
                                 upload_params=upload_params, progress=progress, **kwargs)

    def async_load(self, key: str, download_params=None, progress=True, **kwargs) -> Future:
        return self._pool.submit(self.saver.load, key=key,
                                 download_params=download_params, progress=progress, **kwargs)

    def concurrent_save(self,
                        objs: tp.Iterable[tp.Any],
                        keys: tp.Iterable[str],
                        upload_params=None, progress=True, **kwargs) -> tp.List[str]:
        """ Save multiple objects in parallel. Return the list of final keys. """

        objs = list(objs)
        keys = list(keys)

        if len(objs) != len(keys):
            raise SaverError(f'Number of objects ({len(objs)}) != number of keys ({len(keys)})')

        futures = []
        for key, obj in zip(keys, objs):
            future = self.async_save(obj=obj, key=key, upload_params=upload_params, progress=False, **kwargs)
            futures.append(future)

        # TODO add retries handling
        for future in tqdm.tqdm(as_completed(futures),
                                total=len(futures), desc=f"Concurrent save  by {self.saver.__class__.__name__} "
                                                         f"over {self.saver.remote.name}"):
            if future.exception() is not None:
                raise future.exception()

        return [x.result() for x in futures]

    def concurrent_load(self,
                        keys: tp.Iterable[str],
                        download_params=None, progress=True, **kwargs) -> tp.List[tp.Any]:
        """ Load multiple objects in parallel. Return the list of objects. """

        keys = list(keys)

        futures = []
        for key in keys:
            future = self.async_load(key=key, download_params=download_params, progress=False, **kwargs)
            futures.append(future)

        # TODO add retries handling
        for future in tqdm.tqdm(as_completed(futures),
                                total=len(futures), desc=f"Concurrent load  by {self.saver.__class__.__name__} "
                                                         f"over {self.saver.remote.name}"):
            if future.exception() is not None:
                raise future.exception()

        return [x.result() for x in futures]

