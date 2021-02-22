import io
import pickle
from remotools.savers.base import BaseSaver
from remotools.utils import keep_position
import typing as tp


class PickleSaver(BaseSaver):

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs) -> str:

        f = io.BytesIO()
        with keep_position(f):
            pickle.dump(obj, f, **kwargs)

        return self.remote.upload(f, key, progress=progress, params=upload_params)

    def load(self, key: str, download_params=None, progress=True, **kwargs):

        f = io.BytesIO()
        self.remote.download(f, key, progress=progress, keep_stream_position=True, params=download_params)

        return pickle.load(f, **kwargs)
