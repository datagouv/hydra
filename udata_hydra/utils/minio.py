import logging
import os

from minio import Minio

from udata_hydra import config

log = logging.getLogger("udata-hydra")


class MinIOClient:
    def __init__(self, bucket: str, folder: str):
        self.user = config.MINIO_USER
        self.password = config.MINIO_PWD
        self.bucket = bucket
        self.folder = folder
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
        file_path: str,
        delete_source: bool = True,
    ) -> str:
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        if os.path.isfile(file_path):
            file_name = os.path.basename(file_path)
            self.client.fput_object(
                self.bucket,
                f"{self.folder}/{file_name}",
                file_path,
            )
            if delete_source:
                os.remove(file_path)
            return f"https://{config.MINIO_URL}/{self.bucket}/{self.folder}/{file_name}"
        else:
            raise Exception(f"file '{file_path}' does not exists")
