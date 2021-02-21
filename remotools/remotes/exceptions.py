class KeyNotFoundError(Exception):
    """
    This exception is raised when a key doesn't exist on the remote
    """
    pass


class NonDownloadableKeyError(Exception):
    """
    This exception is raised when a key is found but it can't be downloaded
    """


class NonUploadableKeyError(Exception):
    """
    This exception is raised when a key cannot be uploaded
    """


class CorruptedKeyError(Exception):
    """
    This exception is raised when the remote object is found to be corrupt.
    """


class StorageConnectionError(Exception):
    """
    This exception is raised when there is a connection problem with the target storage
    """


class IllegalKeyError(Exception):
    """
    Raise when a given string cannot be used as a key
    """


class UnknownError(Exception):
    """
    The default exception class
    """


