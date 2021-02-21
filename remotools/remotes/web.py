from remotools.remotes.base import BaseRemote
import requests


class WebRemote(BaseRemote):
    """
    A remote used for downloading files from web URLs.

    The keys here are simply the urls. A download is attempted as-is using the requests library.
    """

    def _download(self, f, key: str, chunk_size=8192, **kwargs):
        with requests.get(key, stream=True) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=chunk_size):
                f.write(chunk)

    def _upload(self, f, key: str, **kwargs):
        raise NotImplementedError(f"Uploads are not supported for {self.__class__.__name__}")

    def _contains(self, key: str):
        return False
        # raise NotImplementedError(f"Existence checks are not supported for {self.__class__.__name__}")
