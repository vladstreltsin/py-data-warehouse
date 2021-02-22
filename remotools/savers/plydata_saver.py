import io
from remotools.savers.base import BaseSaver
from remotools.utils import keep_position
import typing as tp


class PlyDataSaver(BaseSaver):

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs) -> str:
        from plyfile import PlyData
        assert isinstance(obj, PlyData)

        f = io.BytesIO()
        with keep_position(f):
            obj.write(f)
        return self.remote.upload(f, key, params=upload_params, progress=progress)

    def load(self, key: str, download_params=None, progress=True, **kwargs):
        from plyfile import PlyData
        f = io.BytesIO()
        self.remote.download(f, key, params=download_params, progress=progress, keep_stream_position=True)
        return PlyData.read(f)

