from remotools.remotes.local import LocalRemote
from remotools.remotes.hfs import HFSRemote
from remotools.remotes.uri import URIRemote
from remotools.remotes.caching import CachingRemote
from remotools.remotes.extras.gs import GSRemote
from sqlitedict import SqliteDict
import os
# import numpy as np
import io

CREDENTIALS = '/mnt/storage/PycharmProjects/validation/resources/dag/credentials/vlad@supersmart.co.il.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS

f = io.BytesIO()
# remote = URIRemote()

# remote.upload(f, 'file://home/vlad/jungle.jpg')
os.makedirs('/home/vlad/supersmart/crap/hfs/', exist_ok=True)
with SqliteDict(filename='/home/vlad/supersmart/crap/hfs/store.db',
                tablename='keystore', autocommit=True) as keystore:

    remote = CachingRemote(URIRemote(remotes={'dvc-datasets':
                                              HFSRemote(GSRemote(prefix='Osher-Ad Production/images_datasets/dvc/datasets'),
                                                        depth=1, width=2, algorithm='md5')}),
                           cache=HFSRemote(LocalRemote('/home/vlad/supersmart/crap/hfs')),
                           keystore=keystore)
    remote.download(f, 'dvc-datasets://1675de425761c1dfd4a7475e25b4817b')
    # remote.download(f, 'https://cdn.wallpapersafari.com/81/51/1Bx4Pg.jpg', keep_stream_position=True)

# remote = HFSRemote(base)
# f = io.BytesIO(initial_bytes=bytes([10, 20, 10, 20, 1, 1, 1]))
# print(remote.upload(f, ''))
# print(remote.download(f, '6cd057a53573edd7dec24ed91f81d4ca'))
# print('6cd057a53573edd7dec24ed91f81d4ca' in remote)
# g = io.BytesIO()
# remote.download(g, 'pi')  # Download the buffer
# print('pi' in remote)  # Returns True
# print('e' in remote)  # Returns False
# # x = np.array('')


