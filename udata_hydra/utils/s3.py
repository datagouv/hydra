from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from udata_hydra import config

# Match datagouvfr_data_pipelines S3 defaults for slow networks / large uploads.
_DEFAULT_BOTO_CONFIG = {"connect_timeout": 3600, "read_timeout": 3600}
CONTENT_TYPES = {
    "parquet": "application/vnd.apache.parquet",
    "geojson": "application/geo+json",
    "pmtiles": "application/vnd.pmtiles",
}


class S3Client:
    def __init__(self, bucket: str):
        self.user = config.S3_ACCESS_KEY_ID
        self.password = config.S3_SECRET_ACCESS_KEY
        self.bucket = bucket
        self._resource = boto3.resource(
            "s3",
            endpoint_url=f"https://{config.S3_ENDPOINT or 'test'}",
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
        file_path: str | Path,
        delete_source: bool = True,
    ) -> str:
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        path = Path(file_path)
        if path.is_file():
            object_key = f"{path.suffix[1:]}/{path.name}"
            self._resource.Bucket(self.bucket).upload_file(
                str(path),
                object_key,
                ExtraArgs={
                    "ContentType": CONTENT_TYPES[path.suffix[1:]],
                    "ACL": "public-read",
                },
            )
            if delete_source:
                path.unlink()
            return config.S3_URL_PATTERN.format(
                endpoint=config.S3_ENDPOINT,
                bucket=self.bucket,
                key=object_key,
            )
        else:
            raise Exception(f"file '{path}' does not exists")
