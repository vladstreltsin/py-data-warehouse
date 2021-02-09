from remotools.utils import compute_hash, to_path, keep_position
from remotools.exceptions import HFSError
from remotools.remotes.base import BaseRemote


class HFSRemote(BaseRemote):
    """ Wraps a given remote to use hashes of the provided streams as keys """

    def __init__(self, remote: BaseRemote, width=2, depth=4, algorithm='md5'):
        super(HFSRemote, self).__init__(name=None, progress=False)
        self.remote = remote
        self.width = width
        self.depth = depth
        self.algorithm = algorithm

    def upload(self, f, key=None, check_exists=True, *args, **kwargs):

        # Figure out the hash of the object to upload
        with keep_position(f):
            key = compute_hash(f, algorithm=self.algorithm)

        if check_exists and self.contains(key):
            return key

        # Break it according to the desired directory structure
        path = to_path(key, width=self.width, depth=self.depth)
        self.remote.upload(f, path, check_exists=False)

        # The hid is the key to lookup the object
        return key

    def download(self, f, key: str, *args, **kwargs):

        # Convert to the desired key according to the directory structure
        path = to_path(key, width=self.width, depth=self.depth)
        with keep_position(f):
            self.remote.download(f, path)

        # Make sure that the hash matches
        recv_key = compute_hash(f, algorithm=self.algorithm)
        if recv_key != key:
            raise HFSError(f"Hash check for key {key} failed (expected: {key} got: {recv_key}")

    def contains(self, key: str):
        try:
            path = to_path(key, width=self.width, depth=self.depth)
        except ValueError:
            return False

        return self.remote.contains(path)


