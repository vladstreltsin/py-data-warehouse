from remotools.remotes.local import LocalRemote
from remotools.remotes.hfs import HFSRemote
from remotools.remotes.uri import URIRemote
from remotools.remotes.caching import CachingRemote
from remotools.remotes.extras.gs import GSRemote
from remotools.savers.pil_image_saver import PILImageSaver
from remotools.savers.composite_saver import CompositeSaver
from remotools.savers.jsonpickle_saver import JSONPickleSaver
from remotools.savers.pickle_saver import PickleSaver
from remotools.savers.yaml_saver import YAMLSaver
from remotools.savers.json_saver import JSONSaver
from remotools.savers.csvpandas_saver import CSVPandasSaver
import pandas as pd

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
with SqliteDict(filename='/home/vlad/supersmart/crap/hfs/store4.db',
                tablename='keystore', autocommit=True) as keystore:

    remote = CachingRemote(
                remote=URIRemote(
                        remotes=
                        {'dvc-datasets': HFSRemote(
                                            remote=GSRemote(prefix='Osher-Ad Production/images_datasets/dvc/datasets'),
                                            depth=1,
                                            width=2,
                                            algorithm='md5')
                         }),
                cache=HFSRemote(LocalRemote('/home/vlad/supersmart/crap/hfs')),
                keystore=keystore)

    saver = CompositeSaver(remote=remote,
                           saver_builders={'image': PILImageSaver,
                                           'jsonpickle': JSONPickleSaver,
                                           'pickle': PickleSaver,
                                           'yaml': YAMLSaver,
                                           'json': JSONSaver,
                                           'dataframe': CSVPandasSaver})

    df = pd.DataFrame(data={'A': [1, 2, 3, 1], 'B': [3, 4, 5, 1], 'C': [1, 1, 1, 1]})
    saver.save(df, key='dataframe@file://home/vlad/supersmart/crap/obj.csv')
    print(saver.load('dataframe@file://home/vlad/supersmart/crap/obj.csv'))
    # x = ['a', 'b', 10]
    # saver.save(x, key='json@file://home/vlad/supersmart/crap/obj.json')
    # print(saver.load(key='json@file://home/vlad/supersmart/crap/obj.json'))

    # x = saver.load('jsonpickle@dvc-datasets://5769913244134444aeb280bef42f1c08')
    # print(x)
    # x = saver.load('image@dvc-datasets://cb6287f1bc026abff769a185a43500a5')
    # saver.save(x, key='image@file://home/vlad/supersmart/crap/crap_image.jpg')

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


