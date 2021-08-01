from remotools.remotes.base import BaseRemote
from remotools.utils import join
from io import BufferedReader
from remotools.remotes.exceptions import UnknownError, KeyNotFoundError

# TODO wrap boto3 commands with standardized exceptions
# TODO add documentation to S3Remote class

KEY_SEPARATOR = '/'


# https://github.com/boto/s3transfer/issues/80
class NonCloseableBufferedReader(BufferedReader):
    def close(self):
        self.flush()


class S3Remote(BaseRemote):

    def __init__(self, prefix=None, region_name=None, aws_access_key_id=None, aws_secret_access_key=None,
                 **kwargs):
        super(S3Remote, self).__init__(**kwargs)
        self.prefix = prefix or ''
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        assert isinstance(self.prefix, str)

    def _download(self, f, key: str, **kwargs):
        import boto3
        from botocore.exceptions import ClientError

        path = join(self.prefix, key)
        result = path.split(sep=KEY_SEPARATOR, maxsplit=1)
        if len(result) < 2:
            raise KeyNotFoundError(f'No key corresponding to {path} (must contain at least one separator)')
        bucket, blob = result

        try:
            session = boto3.session.Session()
            session.client('s3', region_name=self.region_name,
                           aws_access_key_id=self.aws_access_key_id,
                           aws_secret_access_key=self.aws_secret_access_key,
                           ).download_fileobj(bucket, blob, f)

            # boto3.client('s3', region_name=self.region_name,
            #              aws_access_key_id=self.aws_access_key_id,
            #              aws_secret_access_key=self.aws_secret_access_key
            #              ).download_fileobj(bucket, blob, f)

        except ClientError as e:
            raise KeyNotFoundError from e

    def _upload(self, f, key: str, **kwargs) -> str:
        import boto3

        path = join(self.prefix, key)
        result = path.split(sep=KEY_SEPARATOR, maxsplit=1)
        if len(result) < 2:
            raise KeyNotFoundError(f'No key corresponding to {path} (must contain at least one separator)')
        bucket, blob = result

        # A hack around boto3 closing the file upon uploading
        # https://github.com/boto/s3transfer/issues/80
        f = NonCloseableBufferedReader(f)

        session = boto3.session.Session()
        session.client('s3', region_name=self.region_name,
                       aws_access_key_id=self.aws_access_key_id,
                       aws_secret_access_key=self.aws_secret_access_key,
                       ).upload_fileobj(f, bucket, blob)

        # This doesn't seem to handle multi-threading
        # boto3.client('s3', region_name=self.region_name,
        #              aws_access_key_id=self.aws_access_key_id,
        #              aws_secret_access_key=self.aws_secret_access_key,
        #              ).upload_fileobj(f, bucket, blob)


        # https://github.com/boto/s3transfer/issues/80
        f.detach()

        return key

    def _contains(self, key: str) -> bool:
        import boto3
        from botocore.exceptions import ClientError

        path = join(self.prefix, key)
        result = path.split(sep=KEY_SEPARATOR, maxsplit=1)
        if len(result) < 2:
            return False
        bucket, blob = result

        # https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
        try:
            session = boto3.session.Session()
            session.resource('s3', region_name=self.region_name,
                             aws_access_key_id=self.aws_access_key_id,
                             aws_secret_access_key=self.aws_secret_access_key,
                            ).Object(bucket, blob).load()
            # boto3.resource('s3', region_name=self.region_name,
            #                aws_access_key_id=self.aws_access_key_id,
            #                aws_secret_access_key=self.aws_secret_access_key,
            #                ).Object(bucket, blob).load()

        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                return False

            else:
                # Something else has gone wrong.
                raise UnknownError from e
        else:
            return True
