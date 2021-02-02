import io
import ruamel.yaml
from savers.base import BaseSaver
from utils import keep_position


class YAMLSaver(BaseSaver):

    def save(self, obj, key=None, check_exists=True, *args, **kwargs):
        key = key or self.default_save_key
        yaml = ruamel.yaml.YAML()
        f = io.BytesIO()
        with keep_position(f):
            yaml.dump(obj, f, *args, **kwargs)
        return self.remote.upload(f, key, check_exists=check_exists)

    def load(self, key, *args, **kwargs):
        yaml = ruamel.yaml.YAML()
        f = io.BytesIO()
        with keep_position(f):
            self.remote.download(f, key)

        return yaml.load(f)
