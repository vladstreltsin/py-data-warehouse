from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, Future
import typing as tp

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

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs) -> Future:
        return self._pool.submit(self.saver.save, obj=obj, key=key,
                                 upload_params=upload_params, progress=progress, **kwargs)

    def load(self, key: str, download_params=None, progress=True, **kwargs) -> Future:
        return self._pool.submit(self.saver.load, key=key,
                                 download_params=download_params, progress=progress, **kwargs)
