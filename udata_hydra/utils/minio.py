import logging
import os

from minio import Minio

from udata_hydra import config

log = logging.getLogger("udata-hydra")


class MinIOClient:
    def __init__(self, bucket=config.MINIO_BUCKET):
        self.user = config.MINIO_USER
        self.password = config.MINIO_PWD
        self.bucket = bucket
        self.client = Minio(
            config.MINIO_URL or "test",
            access_key=self.user or "test",
            secret_key=self.password or "test",
            secure=True,
        )
        if self.bucket:
            self.bucket_exists = self.client.bucket_exists(self.bucket)
            if not self.bucket_exists:
                raise ValueError(f"Bucket '{self.bucket}' does not exist.")

    def send_file(
        self,
        file_name,
        delete_source=True,
    ) -> str:
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        if os.path.isfile(file_name):
            self.client.fput_object(
                self.bucket,
                f"{config.MINIO_FOLDER}/{file_name}",
                file_name,
            )
            if delete_source:
                os.remove(file_name)
            return f"https://{config.MINIO_URL}/{self.bucket}/{config.MINIO_FOLDER}/{file_name}"
        else:
            raise Exception(f"file '{file_name}' does not exists")
