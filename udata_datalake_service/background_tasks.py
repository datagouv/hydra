import logging
import os
import tempfile
from typing import BinaryIO

import agate
import boto3
from celery import Celery
import magic
import requests
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# from udata_datalake_service.producer import produce
from udata_event_service.producer import produce

load_dotenv()

BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6380/0")
KAFKA_URI = f'{os.environ.get("KAFKA_HOST", "localhost")}:{os.environ.get("KAFKA_PORT", "9092")}'
MINIO_FOLDER = os.environ.get("MINIO_FOLDER", "folder")
MAX_FILESIZE_ALLOWED = os.environ.get("MAX_FILESIZE_ALLOWED", 1000)
celery = Celery("tasks", broker=BROKER_URL)


def download_resource(url: str) -> BinaryIO:
    """
    Attempts downloading a resource from a given url.
    Returns the downloaded file object.
    Raises IOError if the resource is too large.
    """
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        if float(r.headers.get("content-length", -1)) > float(
            MAX_FILESIZE_ALLOWED
        ):
            raise IOError("File too large to download")

        chunck_size = 1024
        for i, chunk in enumerate(r.iter_content(chunk_size=chunck_size)):
            if i * chunck_size < float(MAX_FILESIZE_ALLOWED):
                tmp_file.write(chunk)
            else:
                tmp_file.close()
                logging.error(f"File {url} is too big, skipping")
                raise IOError("File too large to download")
    tmp_file.close()
    return tmp_file


def get_resource_minio_url(key, resource):
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
        + resource["id"]
    )


def save_resource_to_minio(resource_file, key, resource):
    logging.info("Saving to minio")
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
                MINIO_FOLDER + "/" + key + "/" + resource["id"],
            )
        logging.info(
            f"Resource saved into minio at {get_resource_minio_url(key, resource)}"
        )
    except ClientError as e:
        logging.error(e)


@celery.task
def manage_resource(dataset_id: str, resource: dict):
    logging.info(
        "Processing task for resource {} in dataset {}".format(
            resource["id"], dataset_id
        )
    )
    try:
        tmp_file = download_resource(resource["url"])

        # Get file size
        filesize = os.path.getsize(tmp_file.name)

        # Check resource MIME type
        mime_type = magic.from_file(tmp_file.name, mime=True)
        if mime_type in ["text/plain", "text/csv"]:
            # Save resource only if CSV
            try:
                # Raise ValueError if file is not a CSV
                agate.Table.from_csv(
                    tmp_file.name, sniff_limit=4096, row_limit=40
                )
                save_resource_to_minio(tmp_file, dataset_id, resource)
                storage_location = {
                    "netloc": os.getenv("MINIO_URL"),
                    "bucket": os.getenv("MINIO_BUCKET"),
                    "key": MINIO_FOLDER
                    + "/"
                    + dataset_id
                    + "/"
                    + resource["id"],
                }
                logging.info(
                    f"Sending kafka message for resource stored {resource['id']} in dataset {dataset_id}"
                )
                produce(
                    KAFKA_URI,
                    "resource.stored",
                    "datalake",
                    resource["id"],
                    {
                        "location": storage_location,
                        "mime_type": mime_type,
                        "filesize": filesize,
                    },
                    meta={"dataset_id": dataset_id},
                )
            except ValueError:
                logging.info(
                    f"Resource {resource['id']} in dataset {dataset_id} is not a CSV"
                )

        # Send a Kafka message for both CSV and non CSV resources
        logging.info(
            f"Sending kafka message for resource analysed {resource['id']} in dataset {dataset_id}"
        )
        message = {
            "filesize": filesize,
            "mime": mime_type,
            "resource_url": resource["url"],
        }
        produce(
            KAFKA_URI,
            "resource.analysed",
            "datalake",
            resource["id"],
            message,
            meta={"dataset_id": dataset_id},
        )
    except IOError:
        produce(
            KAFKA_URI,
            "resource.analysed",
            "datalake",
            resource["id"],
            {
                "resource_url": resource["url"],
                "error": "File too large to download",
                "filesize": None,
            },
            meta={"dataset_id": dataset_id},
        )
    finally:
        os.unlink(tmp_file.name)
