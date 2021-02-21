from google.cloud import storage
from google.cloud.exceptions import NotFound
from remotools.remotes_old.base import BaseRemote
from remotools.utils import join
from remotools.exceptions import RemoteError


class GSRemote(BaseRemote):

    def __init__(self, prefix=None, credentials=None, *args, **kwargs):
        super(GSRemote, self).__init__(*args, **kwargs)

        # TODO credentials require a particular format, not the file that can be set in os.environ
        self.prefix = prefix or ''
        self.credentials = credentials

    def download(self, f, key: str, *args, **kwargs):
        path = join(self.prefix, key)
        project, bucket, blob = path.split(sep='/', maxsplit=2)

        with self.download_progress_bar(f, key) as fp:
            try:
                storage.Client(project=project,
                               credentials=self.credentials).bucket(bucket).blob(blob).download_to_file(fp)
            except NotFound as exc:
                raise RemoteError(f"Key {key} not found") from exc

    def upload(self, f, key: str, check_exists=False, *args, **kwargs) -> str:
        if check_exists and self.contains(key):
            return key

        path = join(self.prefix, key)
        project, bucket, blob = path.split(sep='/', maxsplit=2)

        with self.upload_progress_bar(f, key) as fp:
            storage.Client(project=project,
                           credentials=self.credentials).bucket(bucket).blob(blob).upload_from_file(fp)
        return key

    def contains(self, key: str) -> bool:
        path = join(self.prefix, key)
        project, bucket, key = path.split(sep='/', maxsplit=2)
        return storage.Client(project=project,
                              credentials=self.credentials).bucket(bucket).blob(key).exists()
