import logging
import os
import tempfile
from typing import BinaryIO

import pandas as pd
from aiohttp import ClientResponse
import magic
from dotenv import load_dotenv

from udata_event_service.producer import produce

from udata_hydra.utils.json import is_json_file
from udata_hydra.utils.kafka import get_topic
from udata_hydra.utils.minio import save_resource_to_minio
from udata_hydra.utils.csv import detect_encoding, find_delimiter

load_dotenv()

log = logging.getLogger("udata-hydra")

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
            log.error(f"File {url} is too big, skipping")
            raise IOError("File too large to download")
        i += 1
    tmp_file.close()
    return tmp_file


async def process_resource(url: str, dataset_id: str, resource_id: str, response: ClientResponse) -> None:
    log.debug(
        "Processing task for resource {} in dataset {}".format(
            resource_id, dataset_id
        )
    )
    tmp_file = None
    try:
        tmp_file = await download_resource(url, response)

        # Get file size
        filesize = os.path.getsize(tmp_file.name)

        # Check resource MIME type
        mime_type = magic.from_file(tmp_file.name, mime=True)
        if mime_type in ["text/plain", "text/csv", "application/csv"] and not is_json_file(tmp_file.name):
            # Save resource only if CSV
            try:
                # Try to detect encoding from suspected csv file. If fail, set up to utf8 (most common)
                with open(tmp_file.name, mode='rb') as f:
                    try:
                        encoding = detect_encoding(f)
                    except:
                        encoding = 'utf-8'
                # Try to detect delimiter from suspected csv file. If fail, set up to None (pandas will use python engine and try to guess separator itself)
                try:
                    delimiter = find_delimiter(tmp_file.name)
                except:
                    delimiter = None
                # Try to read first 1000 rows with pandas
                df = pd.read_csv(tmp_file.name, sep=delimiter, encoding=encoding, nrows=1000)

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
                log.debug(
                    f"Sending kafka message for resource stored {resource_id} in dataset {dataset_id}"
                )
                produce(
                    KAFKA_URI,
                    get_topic("resource.stored"),
                    "datalake",
                    resource_id,
                    {
                        "location": storage_location,
                        "mime_type": mime_type,
                        "filesize": filesize,
                        "delimiter": delimiter,
                        "encoding": encoding,
                    },
                    meta={"dataset_id": dataset_id, "message_type": "resource.stored"},
                )
            except ValueError:
                log.debug(
                    f"Resource {resource_id} in dataset {dataset_id} is not a CSV"
                )


        # Send a Kafka message for both CSV and non CSV resources
        log.debug(
            f"Sending kafka message for resource analysed {resource_id} in dataset {dataset_id}"
        )
        message = {
            "error": None,
            "filesize": filesize,
            "mime": mime_type,
            "resource_url": url,
        }
        produce(
            KAFKA_URI,
            get_topic("resource.analysed"),
            "datalake",
            resource_id,
            message,
            meta={"dataset_id": dataset_id, "message_type": "resource.analysed"},
        )
    except IOError:
        produce(
            KAFKA_URI,
            get_topic("resource.analysed"),
            "datalake",
            resource_id,
            {
                "resource_url": url,
                "error": "File too large to download",
                "filesize": None,
            },
            meta={"dataset_id": dataset_id, "message_type": "resource.analysed"},
        )
    finally:
        if tmp_file:
            os.remove(tmp_file.name)
