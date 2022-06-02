import logging
import os
import tempfile
from typing import BinaryIO

import agate
from aiohttp import ClientResponse
import boto3
import magic
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from udata_event_service.producer import produce

load_dotenv()

BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6380/0")
KAFKA_URI = f'{os.environ.get("KAFKA_HOST", "localhost")}:{os.environ.get("KAFKA_PORT", "9092")}'
MINIO_FOLDER = os.environ.get("MINIO_FOLDER", "folder")
MAX_FILESIZE_ALLOWED = os.environ.get("MAX_FILESIZE_ALLOWED", 1000)


async def download_resource(url: str, response: ClientResponse) -> BinaryIO:
    """
    Attempts downloading a resource from a given url.
    Returns the downloaded file object.
    Raises IOError if the resource is too large.
    """
    tmp_file = tempfile.NamedTemporaryFile(delete=False)

    if float(response.headers.get("content-length", -1)) > float(
        MAX_FILESIZE_ALLOWED
    ):
        raise IOError("File too large to download")

    chunk_size = 1024
    i = 0
    async for chunk in response.content.iter_chunked(chunk_size):
        if i * chunk_size < float(MAX_FILESIZE_ALLOWED):
            tmp_file.write(chunk)
        else:
            tmp_file.close()
            logging.error(f"File {url} is too big, skipping")
            raise IOError("File too large to download")
        i += 1
    tmp_file.close()
    return tmp_file


def get_resource_minio_url(key, resource_id):
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


def save_resource_to_minio(resource_file, key, resource_id):
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
                MINIO_FOLDER + "/" + key + "/" + resource_id,
            )
        logging.info(
            f"Resource saved into minio at {get_resource_minio_url(key, resource_id)}"
        )
    except ClientError as e:
        logging.error(e)


async def process_resource(url: str, dataset_id: str, resource_id: str, response: ClientResponse) -> None:
    logging.info(
        "Processing task for resource {} in dataset {}".format(
            resource_id, dataset_id
        )
    )
    try:
        tmp_file = await download_resource(url, response)

        # Get file size
        filesize = os.path.getsize(tmp_file.name)

        # Check resource MIME type
        mime_type = magic.from_file(tmp_file.name, mime=True)
        if mime_type in ["text/plain", "text/csv", "application/csv"]:
            # Save resource only if CSV
            try:
                # Raise ValueError if file is not a CSV
                # TODO: Ensure JSON files are not accepted
                agate.Table.from_csv(
                    tmp_file.name, sniff_limit=4096, row_limit=40
                )
                save_resource_to_minio(tmp_file, dataset_id, resource_id)
                storage_location = {
                    "netloc": os.getenv("MINIO_URL"),
                    "bucket": os.getenv("MINIO_BUCKET"),
                    "key": MINIO_FOLDER
                    + "/"
                    + dataset_id
                    + "/"
                    + resource_id,
                }
                logging.info(
                    f"Sending kafka message for resource stored {resource_id} in dataset {dataset_id}"
                )
                produce(
                    KAFKA_URI,
                    "resource.stored",
                    "datalake",
                    resource_id,
                    {
                        "location": storage_location,
                        "mime_type": mime_type,
                        "filesize": filesize,
                    },
                    meta={"dataset_id": dataset_id},
                )
            except ValueError:
                logging.info(
                    f"Resource {resource_id} in dataset {dataset_id} is not a CSV"
                )

        # Send a Kafka message for both CSV and non CSV resources
        logging.info(
            f"Sending kafka message for resource analysed {resource_id} in dataset {dataset_id}"
        )
        message = {
            "filesize": filesize,
            "mime": mime_type,
            "resource_url": url,
        }
        produce(
            KAFKA_URI,
            "resource.analysed",
            "datalake",
            resource_id,
            message,
            meta={"dataset_id": dataset_id},
        )
    except IOError:
        produce(
            KAFKA_URI,
            "resource.analysed",
            "datalake",
            resource_id,
            {
                "resource_url": url,
                "error": "File too large to download",
                "filesize": None,
            },
            meta={"dataset_id": dataset_id},
        )
    finally:
        os.unlink(tmp_file.name)
