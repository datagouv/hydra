import logging
import os
from typing import BinaryIO

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("udata-hydra")

MINIO_FOLDER = os.environ.get("MINIO_FOLDER", "folder")


def get_resource_minio_url(key: str, resource_id: str) -> str:
    """Returns location of given resource in minio once it is saved"""
    return (
        os.getenv("MINIO_URL")
        + "/"
        + os.getenv("MINIO_BUCKET")
        + "/"
        + MINIO_FOLDER
        + "/"
        + key
        + "/"
        + resource_id
    )


def save_resource_to_minio(resource_file: BinaryIO, key: str, resource_id: str) -> None:
    log.info("Saving to minio")
    log.debug(f'Bucket is {os.getenv("MINIO_BUCKET")}')
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_URL"),
        aws_access_key_id=os.getenv("MINIO_USER"),
        aws_secret_access_key=os.getenv("MINIO_PWD"),
        config=Config(signature_version="s3v4"),
    )
    try:
        with open(resource_file.name, "rb") as f:
            s3.upload_fileobj(
                f,
                os.getenv("MINIO_BUCKET"),
                MINIO_FOLDER + "/" + key + "/" + resource_id,
            )
        log.info(
            f"Resource saved into minio at {get_resource_minio_url(key, resource_id)}"
        )
    except ClientError as e:
        log.error(e)


def delete_resource_from_minio(key: str, resource_id: str) -> None:
    log.info("Deleting from minio")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_URL"),
        aws_access_key_id=os.getenv("MINIO_USER"),
        aws_secret_access_key=os.getenv("MINIO_PWD"),
        config=Config(signature_version="s3v4"),
    )
    try:
        s3.delete_object(
            Bucket=os.getenv("MINIO_BUCKET"),
            Key=MINIO_FOLDER + "/" + key + "/" + resource_id,
        )
        log.info(
            f"Resource deleted from minio at {get_resource_minio_url(key, resource_id)}"
        )
    except ClientError as e:
        log.error(e)
