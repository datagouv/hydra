import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from udata_hydra import config

# Match datagouvfr_data_pipelines S3 defaults for slow networks / large uploads.
_DEFAULT_BOTO_CONFIG = {"connect_timeout": 3600, "read_timeout": 3600}


class S3Client:
    def __init__(self, bucket: str, folder: str):
        self.user = config.S3_ACCESS_KEY_ID
        self.password = config.S3_SECRET_ACCESS_KEY
        self.bucket = bucket
        self.folder = folder
        host = config.S3_ENDPOINT or "test"
        self._resource = boto3.resource(
            "s3",
            endpoint_url=f"https://{host}",
            aws_access_key_id=self.user or "test",
            aws_secret_access_key=self.password or "test",
            config=Config(**_DEFAULT_BOTO_CONFIG),
        )
        self._client = self._resource.meta.client
        if self.bucket:
            self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchBucket", "NotFound"):
                raise ValueError(f"Bucket '{self.bucket}' does not exist.") from e
            raise

    def send_file(
        self,
        file_path: str,
        delete_source: bool = True,
    ) -> str:
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        if os.path.isfile(file_path):
            file_name = os.path.basename(file_path)
            key = f"{self.folder}/{file_name}"
            self._resource.Bucket(self.bucket).upload_file(file_path, key)
            if delete_source:
                os.remove(file_path)
            return f"https://{config.S3_ENDPOINT}/{self.bucket}/{self.folder}/{file_name}"
        else:
            raise Exception(f"file '{file_path}' does not exists")
