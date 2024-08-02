import logging
import os

from minio import Minio

from udata_hydra import config

log = logging.getLogger("udata-hydra")


class MinIOClient:
    def __init__(self, bucket=config.MINIO_BUCKET):
        self.url = config.MINIO_URL
        self.user = config.MINIO_USER
        self.password = config.MINIO_PWD
        self.bucket = bucket
        self.client = Minio(
            self.url or "test",
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
        file_path,
        delete_source=True,
    ):
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        is_file = os.path.isfile(os.path.join(file_path))
        if is_file:
            dest_path = f"{config.MINIO_FOLDER}{file_path.split('/')[-1]}"
            self.client.fput_object(
                self.bucket,
                dest_path,
                file_path,
            )
            if delete_source:
                os.remove(file_path)
        else:
            raise Exception(f"file '{file_path}' does not exists")
