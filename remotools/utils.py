import hashlib
import io
import os
from contextlib import contextmanager
from itertools import chain


def join(*paths, sep='/'):
    """ Cleanly concatenates paths without repeated separators """
    return sep.join(list(filter(lambda x: x, chain.from_iterable(map(lambda path: path.split(sep), paths)))))


def is_binary(f):
    return not isinstance(f, io.TextIOBase)


@contextmanager
def keep_position(f, enabled=True):
    """Stores the current position in the file handle and restores it in the end"""
    position = f.tell()
    try:
        yield f
    finally:
        if enabled:
            f.seek(position)


def compute_hash(f, algorithm='md5', buffer_size=8192, keep_stream_position=True):
    """Compute hash of file using :attr:`algorithm`."""

    with keep_position(f, enabled=keep_stream_position):
        # Allow for XXH algorithms
        if algorithm.startswith('xxh'):
            try:
                import xxhash
                hash_fn = getattr(xxhash, algorithm)()
            except ImportError as e:
                raise ImportError("It appears that the xxhash package is not installed. Reinstall the package with "
                                  "xxhash as an extra option.") from e
        else:
            hash_fn = hashlib.new(algorithm)

        # Compute the hash over the object
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            hash_fn.update(data)

        return hash_fn.hexdigest()


def to_path(hid: str, width: int, depth: int):
    w = width
    d = depth

    if len(hid[d*w:]) == 0:
        raise ValueError(f"Hash length (in hex) "
                         f"is too short. Must be at least {w*d + 1} for the chosen width and depth. "
                         f"Given: {len(hid)}")

    return os.path.join(*[hid[i * w: (i + 1) * w] for i in range(d)], hid[d*w:])


class DictProxy:
    """
    Wraps a dictionary and allows for its methods specializations.
    """

    def __init__(self, data):
        self._data = {}
        self.update(data)

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __getitem__(self, item):
        return self._data[item]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]

    def __contains__(self, item):
        return item in self._data

    def keys(self):
        return self._data.keys()

    def values(self):
        for key in self.keys():
            yield self[key]

    def items(self):
        for key in self.keys():
            yield key, self[key]

    def update(self, dct):
        for key in dct:
            self[key] = dct[key]

