from remotools.remotes.base import BaseRemote
from remotools.utils import join
from remotools.remotes.exceptions import KeyNotFoundError, IllegalKeyError

KEY_SEPARATOR = '/'


class GSRemote(BaseRemote):
    """
    A remote whose target is Google Cloud Storage.

    This remote uses the google-cloud-storage library to communicate with the remote storage.
    If prefix=None, the keys are given in the form of project_name/bucket_name/path/to/blob. If
    a prefix is specified, it is appended to each given key.
    For example, if:
        project = 'MyProject'
        bucket = 'MyBucket'
        blob resides = 'top_dir/middle_dir/object' (with respect to the bucket)
    then, the corresponding key is:
        key = 'MyProject/MyBucket/top_dir/middle_dir/object'

    If the prefix is set to be:
        prefix = 'MyProject/MyBucket/top_dir'
    the key will be:
        key = 'middle_dir/object'

    Attributes
    ----------
    prefix
        A path string appended to each key
    credentials
        TBD

    """
    def __init__(self, prefix=None, credentials=None, **kwargs):
        super(GSRemote, self).__init__(**kwargs)

        # TODO credentials require a particular format, not the file that can be set in os.environ
        self.prefix = prefix or ''
        self.credentials = credentials

    def _download(self, f, key: str, **kwargs):

        from google.cloud import storage
        from google.cloud.exceptions import NotFound

        path = join(self.prefix, key)

        result = path.split(sep=KEY_SEPARATOR, maxsplit=2)
        if len(result) < 3:
            raise IllegalKeyError(f'Full path {path} is too short (must contain at least 2 separators)')
        project, bucket, blob = result

        try:
            storage.Client(project=project,
                           credentials=self.credentials).bucket(bucket).blob(blob).download_to_file(f)
        except NotFound as e:
            raise KeyNotFoundError(f"Key {key} not found") from e

    def _upload(self, f, key: str, **kwargs) -> str:

        from google.cloud import storage

        path = join(self.prefix, key)

        result = path.split(sep=KEY_SEPARATOR, maxsplit=2)
        if len(result) < 3:
            raise IllegalKeyError(f'Full path {path} is too short (must contain at least 2 separators)')
        project, bucket, blob = result

        storage.Client(project=project,
                       credentials=self.credentials).bucket(bucket).blob(blob).upload_from_file(f)
        return key

    def _contains(self, key: str) -> bool:

        from google.cloud import storage

        path = join(self.prefix, key)

        result = path.split(sep=KEY_SEPARATOR, maxsplit=2)
        if len(result) < 3:
            return False
        project, bucket, blob = result

        return storage.Client(project=project,
                              credentials=self.credentials).bucket(bucket).blob(key).exists()

