from enum import Enum

from exceptions import SaverError
from savers.base import BaseSaver
from savers.image_saver import ImageSaver
from savers.json_saver import JSONSaver
from savers.jsonpickle_saver import JSONPickleSaver
from savers.pickle_saver import PickleSaver
from savers.plydata_saver import PlyDataSaver
from savers.yaml_saver import YAMLSaver


class SaverType(Enum):
    JSONPICKLE = 0,
    IMAGE = 1,
    PICKLE = 2,
    JSON = 3,
    YAML = 4,
    PLYDATA = 5,


class Saver(BaseSaver):

    def save(self, obj, key=None, check_exists=True, *args, **kwargs):
        save_as, key = key.split('@', maxsplit=1)
        key = key or self.default_save_key
        save_type = SaverType[save_as.upper()]

        if save_type is SaverType.JSONPICKLE:
            key = JSONPickleSaver(self.remote).save(obj=obj, key=key, check_exists=check_exists, *args, **kwargs)

        elif save_type is SaverType.IMAGE:
            key = ImageSaver(self.remote).save(obj=obj, key=key, check_exists=check_exists, *args, **kwargs)

        elif save_type is SaverType.PICKLE:
            key = PickleSaver(self.remote).save(obj=obj, key=key, check_exists=check_exists, *args, **kwargs)

        elif save_type is SaverType.JSON:
            key = JSONSaver(self.remote).save(obj=obj, key=key, check_exists=check_exists, *args, **kwargs)

        elif save_type is SaverType.YAML:
            key = YAMLSaver(self.remote).save(obj=obj, key=key, check_exists=check_exists, *args, **kwargs)

        elif save_type is SaverType.PLYDATA:
            key = PlyDataSaver(self.remote).save(obj=obj, key=key, check_exists=check_exists, *args, **kwargs)

        else:
            raise SaverError(f'Unsupported value for save_as ({save_as})')

        return f'{save_as}@{str(key)}'

    def load(self, key, *args, **kwargs):

        save_as, key = key.split('@', maxsplit=1)
        save_type = SaverType[save_as.upper()]

        if save_type is SaverType.JSONPICKLE:
            return JSONPickleSaver(self.remote).load(key=key, *args, **kwargs)

        elif save_type is SaverType.IMAGE:
            return ImageSaver(self.remote).load(key=key, *args, **kwargs)

        elif save_type is SaverType.PICKLE:
            return PickleSaver(self.remote).load(key=key, *args, **kwargs)

        elif save_type is SaverType.JSON:
            return JSONSaver(self.remote).load(key=key, *args, **kwargs)

        elif save_type is SaverType.YAML:
            return YAMLSaver(self.remote).load(key=key, *args, **kwargs)

        elif save_type is SaverType.PLYDATA:
            return PlyDataSaver(self.remote).load(key=key, *args, **kwargs)

        else:
            raise SaverError(f'Unsupported type ({save_as})')
