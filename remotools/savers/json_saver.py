import io
import json
from remotools.savers.base import BaseSaver
from remotools.utils import keep_position
import typing as tp


class JSONSaver(BaseSaver):

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs):
        f = io.BytesIO()
        with keep_position(f):
            f.write(json.dumps(obj, **kwargs).encode('utf-8'))
        return self.remote.upload(f, key, params=upload_params, progress=progress)

    def load(self, key: str, download_params=None, progress=True, **kwargs):
        f = io.BytesIO()
        self.remote.download(f, key, params=download_params, progress=progress, keep_stream_position=True)
        return json.load(io.TextIOWrapper(f, encoding='utf-8'), **kwargs)
