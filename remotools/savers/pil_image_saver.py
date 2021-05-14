import io
from os import path as osp
from remotools.savers.base import BaseSaver
from remotools.utils import keep_position


class PILImageSaver(BaseSaver):

    def save(self, obj, key: str, ext=None, upload_params=None, progress=True, **kwargs) -> str:
        """
        Save a given image, given as a numpy array, under the provided key.

        Parameters
        ----------
        obj
             A numpy.ndarray representing the given image

        key
            Remote key to use

        ext
            Which extension to use when saving. Legal values are the keys of PIL.Image.EXTENSION

        progress
            Show a progress bar for the upload

        upload_params
            Parameters passed to the remote's upload method

        kwargs
            Extra arguments passed to PIL.Image.save(...)

        Returns
        -------
            Remote key used to save the image

        """

        Image = self._import_pil_image()

        # Try figuring out the format from the file extension
        if not ext and key:
            ext = osp.splitext(key)[1].lower()

        # In case format comes out '' or stays None - use the default which is JPEG
        if not ext:
            ext = ext or '.jpg'

        f = io.BytesIO()
        with keep_position(f):
            Image.fromarray(obj).save(f, format=Image.EXTENSION[ext], **kwargs)

        return self.remote.upload(f, key, progress=progress, params=upload_params or {})

    def load(self, key: str, download_params=None, progress=True, **kwargs):

        Image = self._import_pil_image()
        import numpy as np

        f = io.BytesIO()
        self.remote.download(f, key, params=download_params, progress=progress, keep_stream_position=True)

        return np.asarray(Image.open(f, **kwargs))

    def shape(self, key, download_params=None, progress=True, **kwargs):
        Image = self._import_pil_image()

        f = io.BytesIO()
        self.remote.download(f, key, params=download_params, progress=progress, keep_stream_position=True)

        # Use PIL's lazy loading to get only the image parameters
        image = Image.open(f, **kwargs)
        width, height = image.size
        return height, width

    def _import_pil_image(self):
        # The way PIL.Image works is mega weird. Sometimes it fails to initialize
        from PIL import Image
        if not Image._initialized:
            Image.preinit()
            Image.init()
        return Image
