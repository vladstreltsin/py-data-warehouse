from remotools.remotes.base import BaseRemote


class CachingRemote(BaseRemote):
    """
    A remote accompanied by an HFS-based cache.

    This type of remote provides a caching mechanism for the incoming/outgoing data. The cache
    is realized by an instance of HFSRemote on the local file system. Any download/upload can be memoized
    for faster retrieval with the same key. A key-value store is used as a backend to map remote keys to
    local HFS keys.

    Attributes
    ----------
    remote
        The actual remote being wrapped

    cache
        The caching remote. Usually an HFS on a LocalRemote

    keystore
        Dictionary like object storing the mapping between remote keys and cache keys. For example SqliteDict

    Examples
    --------
    >> with SqliteDict(filename='/home/<user>/store.db', tablename='keystore', autocommit=True) as keystore:
    >>     remote = CachingRemote(URIRemote(),
    >>                            cache=HFSRemote(LocalRemote('/home/<user>/hfs')),
    >>                            keystore=keystore)
    >>     remote.download(f, 'https://cdn.wallpapersafari.com/81/51/1Bx4Pg.jpg')

    The next time the code above is called the object will be taken from the local cache
    """

    def __init__(self, remote: BaseRemote, cache: BaseRemote, keystore):
        super(CachingRemote, self).__init__(name=f'{self.__class__.__name__}<{remote.name}, {cache.name}>')
        self.remote = remote
        self.cache = cache
        self.keystore = keystore

    def _download(self, f, key: str, override_cache=False, **kwargs):
        # Check if exists locally
        if (not override_cache) and key in self.keystore:
            # If the key is found in the keystore, look for it in the cache, and get it if possible
            cache_key = self.keystore[key]

            if self.cache.contains(cache_key):
                self.cache.download(f, cache_key, progress=False, params=kwargs)
                return

        # If we got here that means the key doesn't exist either in the keystore or in the cache
        # Lets download it and update the cache and the keystore
        self.remote.download(f, key, progress=False, params=kwargs, keep_stream_position=True)
        cache_key = self.cache.upload(f, key, progress=False)

        if key in self.keystore:
            del self.keystore[key]

        self.keystore[key] = cache_key

    def _upload(self, f, key: str, **kwargs):
        # Upload to remote and get the new key
        key = self.remote.upload(f, key, progress=False, params=kwargs, keep_stream_position=True)

        # Update the cache and the key store
        cache_key = self.cache.upload(f, key, progress=False)
        self.keystore[key] = cache_key

        return key

    def _contains(self, key: str):
        # Check local cache
        if key in self.keystore:
            cache_key = self.keystore[key]
            if self.cache.contains(cache_key):
                return True

        # Check remote if key wasn't found in the cache
        return self.remote.contains(key)

