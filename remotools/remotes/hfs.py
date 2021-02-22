from remotools.utils import compute_hash, to_path, keep_position
from remotools.remotes.base import BaseRemote
from remotools.remotes.exceptions import CorruptedKeyError, KeyNotFoundError


class HFSRemote(BaseRemote):
    """
    Implements a remote based on a Hash File System.

    A Hash File System (HFS) is an a Content Addressable Storage (see
    https://en.wikipedia.org/wiki/Content-addressable_storage). Each file is uniquely identified by its
    content hash with very high probability (e.g. p = 1 - 2^(-64) for 128-bit hashes).
    Upon uploading, a hash function is applied to the input stream and the resulting hex string is
    considered as the corresponding object key. When downloading, the remote content is copied the the buffer
    and a hash check is performed on the contents. If the hash check fails a CorruptedKeyError is raised.

    The file structure derived from the keys is characterised by two parameters: width and depth.
    The key itself is divided into chunks each of size <width> characters and the total number of chunks is <depth>.
    For example, given that key = 0123456789abcdef, width = 2 and depth = 3, the actual file on the remote
    storage will be <storage root>/01/23/45/6789abcdef. This is done to avoid having excessively large
    directories. Note that the key itself that is used as input to download(...) and contains(...) is the
    original key, that is '0123456789abcdef' in this case.

    Since the key is computed from the file contents there is no need to provide it to the upload(...). It
    still has a key parameter to comply with the interface but it is effectively ignored.

    This class serves as a wrapper around an existing Remote object.

    Attributes
    ----------
    remote
        The wrapped remote backend (a class derived from BaseRemote)

    width
        The number of hex string characters belonging to each level in the directory tree. Defaults to 2

    depth
        The number of levels in the directory tree. Defaults to 4

    algorithm
        The hashing algorithm used. Must be an attribute of either the hashlib or the xxhash modules.
        Defaults to 'md5'
    """

    def __init__(self, remote: BaseRemote, width=2, depth=4, algorithm='md5'):
        super(HFSRemote, self).__init__(name=f'{self.__class__.__name__}<{remote.name}>')
        self.remote = remote
        self.width = width
        self.depth = depth
        self.algorithm = algorithm

    def _upload(self, f, key=None, **kwargs) -> str:

        # Figure out the hash of the object to upload
        key = compute_hash(f, algorithm=self.algorithm)

        # Break it according to the desired directory structure
        path = to_path(key, width=self.width, depth=self.depth)
        self.remote.upload(f, path, progress=False, keep_stream_position=False, params=kwargs)

        # The hid is the key to lookup the object
        return key

    def _download(self, f, key: str, **kwargs):
        # Convert to the desired key according to the directory structure
        try:
            path = to_path(key, width=self.width, depth=self.depth)
        except ValueError as e:
            raise KeyNotFoundError from e

        with keep_position(f):
            self.remote.download(f, path, progress=False)

        # Make sure that the hash matches
        recv_key = compute_hash(f, algorithm=self.algorithm, keep_stream_position=False)
        if recv_key != key:
            raise CorruptedKeyError(f"Hash check for key {key} failed (expected: {key} got: {recv_key}")

    def _contains(self, key: str):
        try:
            path = to_path(key, width=self.width, depth=self.depth)

        # If the key is too short to_path may throw a ValueError
        except ValueError:
            return False

        return self.remote.contains(path)

