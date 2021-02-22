import io
import ruamel.yaml
from remotools._savers_old import BaseSaver
from remotools.utils import keep_position


class YAMLSaver(BaseSaver):

    def save(self, obj, key=None, check_exists=False, *args, **kwargs):
        key = key or self.default_save_key
        yaml = ruamel.yaml.YAML()
        f = io.BytesIO()
        with keep_position(f):
            yaml.dump(obj, f, *args, **kwargs)
        return self.remote.upload(f, key, check_exists=check_exists)

    def load(self, key, search_cache=True, *args, **kwargs):
        yaml = ruamel.yaml.YAML()
        f = io.BytesIO()
        with keep_position(f):
            self.remote.download(f, key, search_cache=search_cache)

        return yaml.load(f)
