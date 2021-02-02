import io

from plyfile import PlyData

from savers.base import BaseSaver
from utils import keep_position


class PlyDataSaver(BaseSaver):

    def save(self, obj: PlyData, key=None, check_exists=True, *args, **kwargs):
        key = key or self.default_save_key
        f = io.BytesIO()
        with keep_position(f):
            obj.write(f)
        return self.remote.upload(f, key, check_exists=check_exists)

    def load(self, key, *args, **kwargs):
        f = io.BytesIO()
        with keep_position(f):
            self.remote.download(f, key)
        return PlyData.read(f)