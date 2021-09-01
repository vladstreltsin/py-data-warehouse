import typing as tp
from remotools.remote_dict import RemoteDict, RemoteBlobDict, RemoteBlobDictWithLRUCache, CompositeRemoteDict
from remotools.savers import BaseSaver
from remotools.remotes import BaseRemote


class RemoteFSError(Exception):
    pass


class RemoteFS(CompositeRemoteDict):

    def _split_key(self, key):
        if self.SEP not in key:
            name, key = key, None
        else:
            name, key = key.split(self.SEP, maxsplit=1)

        return name, key

    def _get_parent(self):
        if self._parent is None:
            raise RemoteFSError('Top level reached')
        return self._parent

    def mkdir(self, key: tp.Optional[str]):
        if key is None:
            return self

        else:

            # Split key by separator
            name, key = self._split_key(key)

            # Stay at current level
            if name == '.':
                return self.mkdir(key)

            # Go up one level
            elif name == '..':
                return self._get_parent().mkdir(key)

            else:

                # Create a RemoteFS at current level
                if name not in self:
                    self[name] = self.__class__()

                # Error if current level already contains a non-RemoteFS object
                elif not isinstance(self[name], RemoteFS):
                    raise RemoteFSError("Path Already contains a non RemoteFS element")

                # Go to to next level
                return self[name].mkdir(key)

    def __call__(self, key: tp.Optional[str]):
        # TODO make it function either as cd or as open (for files)
        return self.cd(key=key)

    def cd(self, key: tp.Optional[str]):

        if key is None:
            return self

        name, key = self._split_key(key)

        if name == '.':
            return self.cd(key)

        elif name == '..':
            return self._get_parent().cd(key)

        else:
            if name not in self:
                raise RemoteFSError(f'No such RemoteFS {name}')

            elif not isinstance(self[name], RemoteFS):
                raise RemoteFSError(f"Path {name} is not a RemoteFS")

            return self[name].cd(key)

    def exists(self, key: str):

        name, key = self._split_key(key)

        if name == '.':
            if key is None:
                return True
            else:
                return self.exists(key)

        if name == '..':
            if self._parent is None:
                return False
            if key is None:
                return True
            return self._parent.exists(key)

        if name not in self:
            return False

        if key is None:
            return True

        if not isinstance(self[name], RemoteFS):
            return False

        return self[name].exists(key)

    def isfile(self, key: str):

        name, key = self._split_key(key)

        if name == '.':
            if key is None:
                return False
            else:
                return self.isfile(key)

        if name == '..':
            if self._parent is None:
                return False

            if key is None:
                return False

            return self._parent.isfile(key)

        if name not in self:
            return False

        if key is None:
            return not isinstance(self[name], RemoteFS)

        if not isinstance(self[name], RemoteFS):
            return False

        return self[name].isfile(key)

    def isdir(self, key: str):

        name, key = self._split_key(key)

        if name == '.':
            if key is None:
                return True
            else:
                return self.isdir(key)

        if name == '..':
            if self._parent is None:
                return False

            if key is None:
                return True

            return self._parent.isdir(key)

        if name not in self:
            return False

        if key is None:
            return isinstance(self[name], RemoteFS)

        if not isinstance(self[name], RemoteFS):
            return False

        return self[name].isdir(key)

    def touch(self, key: str,
              saver_cls: tp.Optional[tp.Type[BaseSaver]]=None,
              cache=True,
              ignore_errors=False,
              **kwargs):

        if self.SEP in key:
            path, key = key.rsplit(sep=self.SEP, maxsplit=1)
            if key == '.' or key == '..':
                raise RemoteFSError(f'Illegal file name {key}')
            fs = self.cd(path)
        else:
            fs = self

        if fs.exists(key):
            if ignore_errors:
                if not isinstance(fs[key], RemoteFS):
                    return fs[key]
                else:
                    return None
            else:
                raise RemoteFSError(f'File exists: {key}')

        # Create the appropriate RemoteDict
        if saver_cls is None:
            fs[key] = RemoteDict(**kwargs)

        elif not cache:
            fs[key] = RemoteBlobDict(saver_cls=saver_cls, **kwargs)

        else:
            fs[key] = RemoteBlobDictWithLRUCache(saver_cls=saver_cls, **kwargs)

        return fs[key]

    def open(self, key: str):

        if self.SEP in key:
            path, key = key.rsplit(sep=self.SEP, maxsplit=1)
            if key == '.' or key == '..':
                raise RemoteFSError(f'Illegal file name {key}')
            fs = self.cd(path)
        else:
            fs = self

        if not fs.isfile(key):
            raise RemoteFSError(f'No such file {key}')

        return fs[key]

    def ls(self, key: tp.Optional[str]=None):
        if key is None:
            return list(self.keys())
        else:
            fs = self.cd(key)
            return fs.ls(key=None)

