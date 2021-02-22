import io

from remotools.savers.base import BaseSaver
from remotools.utils import keep_position
import typing as tp


class CSVPandasSaver(BaseSaver):

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, index=False, **kwargs):
        import pandas as pd
        assert isinstance(obj, pd.DataFrame)

        f = io.BytesIO()
        with keep_position(f):
            f.write(obj.to_csv(index=index, **kwargs).encode())
        return self.remote.upload(f, key, params=upload_params, progress=progress)

    def load(self, key: str, download_params=None, progress=True, **kwargs):
        import pandas as pd
        f = io.BytesIO()

        self.remote.download(f, key, params=download_params, progress=progress, keep_stream_position=True)
        return pd.read_csv(f, **kwargs)
