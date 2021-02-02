import os
from shutil import copyfileobj
from remotes.base import BaseRemote


class LocalRemote(BaseRemote):

    def __init__(self, prefix=None, *args, **kwargs):
        super(LocalRemote, self).__init__(*args, **kwargs)
        self.prefix = prefix or os.path.sep

    def download(self, f, key: str, *args, **kwargs):
        path = os.path.join(self.prefix, key)

        with self.download_progress_bar(f, key) as fp:
            with open(path, 'rb') as f_key:
                copyfileobj(f_key, fp)

    def upload(self, f, key: str, *args, check_exists=False, **kwargs) -> str:
        if check_exists and self.contains(key):
            return key

        path = os.path.join(self.prefix, key)

        # Create the parent directories
        directory, _ = os.path.split(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with self.upload_progress_bar(f, key) as fp:
            with open(path, 'wb') as f_key:
                copyfileobj(fp, f_key)

        return key

    def contains(self, key: str) -> bool:
        path = os.path.join(self.prefix, key)
        return os.path.isfile(path)
