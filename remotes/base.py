from __future__ import annotations
import tqdm
from abc import ABC, abstractmethod


class BaseRemote(ABC):

    def __init__(self, name=None, progress=True):

        self.progress = progress
        self.name = name or self.__class__.__name__

    def upload_progress_bar(self, f, key):
        return tqdm.tqdm.wrapattr(f,
                                  "read", desc=f"[{self.name} UPLOAD] {key}",
                                  disable=not self.progress)

    def download_progress_bar(self, f, key):
        return tqdm.tqdm.wrapattr(f,
                                  "write", desc=f"[{self.name} DOWNLOAD] {key}",
                                  disable=not self.progress)

    @abstractmethod
    def download(self, f, key: str, *args, **kwargs):
        pass

    @abstractmethod
    def upload(self, f, key: str, check_exists=True, *args, **kwargs) -> str:
        pass

    @abstractmethod
    def contains(self, key: str) -> bool:
        pass


