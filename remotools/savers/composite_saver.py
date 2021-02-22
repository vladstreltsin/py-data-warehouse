from remotools.savers.base import BaseSaver
from remotools.utils import DictProxy
from remotools.remotes.exceptions import IllegalKeyError
import re
import typing as tp

SAVER_NAME_RE = re.compile("^[a-zA-Z0-9-]+$")
SAVER_NAME_SEPARATOR = '@'


class CompositeSaver(BaseSaver):

    def __init__(self, remote, saver_builders=None):
        super(CompositeSaver, self).__init__(remote)

        self.savers = SaversDict()
        for saver_name, saver_builder in (saver_builders or {}).items():
            self.savers[saver_name] = saver_builder(remote)

    @staticmethod
    def _parse_key(key):
        result = key.split(SAVER_NAME_SEPARATOR, maxsplit=1)
        if len(result) < 2:
            raise IllegalKeyError(f'key must contain {SAVER_NAME_SEPARATOR}')

        saver_name, remote_key = result
        return saver_name, remote_key

    def save(self, obj: tp.Any, key: str, upload_params=None, progress=True, **kwargs):
        saver_name, remote_key = self._parse_key(key)
        if saver_name not in self.savers:
            raise IllegalKeyError(f'Unknown saver {saver_name}')

        saver = self.savers[saver_name]
        return saver.save(obj=obj, key=remote_key, upload_params=upload_params, progress=progress, **kwargs)

    def load(self, key: str, download_params=None, progress=True, **kwargs):
        saver_name, remote_key = self._parse_key(key)
        if saver_name not in self.savers:
            raise IllegalKeyError(f'Unknown saver {saver_name}')

        saver = self.savers[saver_name]
        return saver.load(key=remote_key, download_params=download_params, progress=progress, **kwargs)


class SaversDict(DictProxy):
    """
    A dictionary like object to store all supported saver types
    """

    def __setitem__(self, key, value):
        assert isinstance(key, str), f"Saver name must be a string (given: {key})"
        assert isinstance(value,
                          BaseSaver), f"A saver must be a subclass of {BaseSaver.__name__} (given: {value}"
        assert SAVER_NAME_RE.match(key), f"Saver name must only contain alphanumeric " \
                                         f"characters and/or a hyphen (-) (given: {key})"

        self._data[key] = value
