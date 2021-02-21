import boto3
from botocore.exceptions import ClientError
from remotools.remotes_old.base import BaseRemote
from remotools.utils import join
from io import BufferedReader


# https://github.com/boto/s3transfer/issues/80
class NonCloseableBufferedReader(BufferedReader):
    def close(self):
        self.flush()


class S3Remote(BaseRemote):

    def __init__(self, prefix=None, region_name=None, aws_access_key_id=None, aws_secret_access_key=None,
                 *args, **kwargs):
        super(S3Remote, self).__init__(*args, **kwargs)
        self.prefix = prefix or ''
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def download(self, f, key: str, *args, **kwargs):
        path = join(self.prefix, key)
        bucket, blob = path.split(sep='/', maxsplit=1)

        with self.download_progress_bar(f, key) as fp:
            boto3.client('s3', region_name=self.region_name,
                         aws_access_key_id=self.aws_access_key_id,
                         aws_secret_access_key=self.aws_secret_access_key
                         ).download_fileobj(bucket, blob, fp)

    def upload(self, f, key: str, check_exists=False, *args, **kwargs) -> str:
        if check_exists and self.contains(key):
            return key

        path = join(self.prefix, key)
        bucket, blob = path.split(sep='/', maxsplit=1)

        # A hack around boto3 closing the file upon uploading
        # https://github.com/boto/s3transfer/issues/80
        f = NonCloseableBufferedReader(f)

        with self.upload_progress_bar(f, key) as fp:
            boto3.client('s3', region_name=self.region_name,
                         aws_access_key_id=self.aws_access_key_id,
                         aws_secret_access_key=self.aws_secret_access_key,
                         ).upload_fileobj(fp, bucket, blob)

        # https://github.com/boto/s3transfer/issues/80
        f.detach()

        return key

    def contains(self, key: str) -> bool:
        path = join(self.prefix, key)
        bucket, blob = path.split(sep='/', maxsplit=1)

        # https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
        try:
            boto3.resource('s3', region_name=self.region_name,
                           aws_access_key_id=self.aws_access_key_id,
                           aws_secret_access_key=self.aws_secret_access_key,
                           ).Object(bucket, blob).load()

        except ClientError as exception:
            if exception.response['Error']['Code'] == "404":
                # The object does not exist.
                return False

            else:
                # Something else has gone wrong.
                raise exception
        else:
            return True
