from .base import BaseRemote
from .caching import CachingRemote, HFSLocalCachingRemote
from .hfs import HFSRemote
from .local import LocalRemote
from .web import WebRemote
from .uri import URIRemote
from .composite import CompositeRemote

# Dependent on extra packages
from .extras.gs import GSRemote
from .extras.s3 import S3Remote
