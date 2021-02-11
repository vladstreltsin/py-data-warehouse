import io
import json

from remotools.savers import BaseSaver
from remotools.utils import keep_position


class JSONSaver(BaseSaver):

    def save(self, obj, key=None, check_exists=False, *args, **kwargs):
        key = key or self.default_save_key
        f = io.BytesIO()
        with keep_position(f):
            f.write(json.dumps(obj, *args, **kwargs).encode('utf-8'))
        return self.remote.upload(f, key, check_exists=check_exists)

    def load(self, key, search_cache=True, *args, **kwargs):
        f = io.BytesIO()
        with keep_position(f):
            self.remote.download(f, key, search_cache=search_cache)
        return json.load(io.TextIOWrapper(f, encoding='utf-8'), *args, **kwargs)
