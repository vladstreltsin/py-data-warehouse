import io
import typing as tp
from remotools.savers.base import BaseSaver
from remotools.utils import keep_position


class JSONPickleSaver(BaseSaver):

    def __init__(self, *args, **kwargs):
        super(JSONPickleSaver, self).__init__(*args, **kwargs)

        # Add support for numpy arrays
        import jsonpickle.ext.numpy
        jsonpickle.ext.numpy.register_handlers()

        # Add support from pandas dataframes
        import jsonpickle.ext.pandas
        jsonpickle.ext.pandas.register_handlers()

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs) -> str:
        import jsonpickle
        f = io.BytesIO()
        with keep_position(f):
            f.write(jsonpickle.encode(obj, **kwargs).encode('utf-8'))

        return self.remote.upload(f, key, params=upload_params, progress=progress)

    def load(self, key: str, download_params=None, progress=True, **kwargs):
        import jsonpickle
        f = io.BytesIO()
        self.remote.download(f, key, keep_stream_position=True, params=download_params, progress=progress)
        return jsonpickle.decode(io.TextIOWrapper(f, encoding='utf-8').read(), **kwargs)
