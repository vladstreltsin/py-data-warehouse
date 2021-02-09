class RemoteError(Exception):
    pass


class HFSError(RemoteError):
    pass


class SaverError(RemoteError):
    pass
