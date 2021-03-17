from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, Future
import typing as tp

if tp.TYPE_CHECKING:
    from remotools.remotes.base import BaseRemote


class ConcurrentRemote:

    def __init__(self, remote: BaseRemote, **kwargs):
        self.remote = remote
        self._pool = ThreadPoolExecutor(**kwargs)

    def __enter__(self):
        self._pool.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pool.__exit__(exc_type, exc_val, exc_tb)

    def download(self, f, key: str, progress=True,
                 keep_stream_position=False, params: tp.Optional[dict]=None) -> Future:

        return self._pool.submit(self.remote.download,
                                 f=f,
                                 key=key,
                                 progress=progress,
                                 keep_stream_position=keep_stream_position,
                                 params=params)

    def upload(self, f, key: str, progress=True,
               keep_stream_position=False, params: tp.Optional[dict] = None) -> Future:

        return self._pool.submit(self.remote.upload,
                                 f=f,
                                 key=key,
                                 progress=progress,
                                 keep_stream_position=keep_stream_position,
                                 params=params)

    def contains(self, key) -> Future:

        return self._pool.submit(self.remote.contains, key=key)
