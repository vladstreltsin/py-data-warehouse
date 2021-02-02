import io

import jsonpickle

from savers.base import BaseSaver
from utils import keep_position

# Add support for numpy arrays
import jsonpickle.ext.numpy as jsonpickle_numpy
jsonpickle_numpy.register_handlers()


class JSONPickleSaver(BaseSaver):

    def save(self, obj, key=None, check_exists=True, warn=True, *args, **kwargs):
        key = key or self.default_save_key
        f = io.BytesIO()
        with keep_position(f):
            f.write(jsonpickle.encode(obj, *args, warn=warn, **kwargs).encode('utf-8'))
        return self.remote.upload(f, key, check_exists=check_exists)

    def load(self, key, *args, **kwargs):
        f = io.BytesIO()
        with keep_position(f):
            self.remote.download(f, key)
        return jsonpickle.decode(io.TextIOWrapper(f, encoding='utf-8').read(), *args, **kwargs)
