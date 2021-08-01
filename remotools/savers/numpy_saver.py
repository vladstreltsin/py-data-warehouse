import io
import json
from remotools.savers.base import BaseSaver
from remotools.utils import keep_position
import typing as tp


class NumpySaver(BaseSaver):

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs):
        import numpy as np
        f = io.BytesIO()
        with keep_position(f):
            np.save(f, obj, **kwargs)
        return self.remote.upload(f, key, params=upload_params, progress=progress)

    def load(self, key: str, download_params=None, progress=True, **kwargs):
        import numpy as np
        f = io.BytesIO()
        self.remote.download(f, key, params=download_params, progress=progress, keep_stream_position=True)
        return np.load(f, **kwargs)
