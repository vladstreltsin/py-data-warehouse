import hashlib
import io
import os
from contextlib import contextmanager
from itertools import chain
import xxhash


def join(*paths, sep='/'):
    """ Cleanly concatenates paths without repeated separators """
    return sep.join(list(filter(lambda x: x, chain.from_iterable(map(lambda path: path.split(sep), paths)))))


def is_binary(f):
    return not isinstance(f, io.TextIOBase)


@contextmanager
def keep_position(f):
    """Stores the current position in the file handle and restores it in the end"""
    position = f.tell()
    try:
        yield f
    finally:
        f.seek(position)


def compute_hash(f, algorithm='md5', buffer_size=8192):
    """Compute hash of file using :attr:`algorithm`."""

    # Allow for XXH algorithms
    if algorithm.startswith('xxh'):
        hash_fn = getattr(xxhash, algorithm)()
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
