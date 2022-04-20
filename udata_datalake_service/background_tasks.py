import logging
from multiprocessing.sharedctypes import Value
import os
import tempfile

import agate
import boto3
from celery import Celery
import magic
import requests
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from udata_datalake_service.producer import produce

load_dotenv()

BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6380/0")
MINIO_FOLDER = os.environ.get("MINIO_FOLDER", "folder")
celery = Celery('tasks', broker=BROKER_URL)


def download_resource(url):
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1024):
            tmp_file.write(chunk)
    tmp_file.close()
    return tmp_file


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
            s3.upload_fileobj(f, os.getenv("MINIO_BUCKET"), MINIO_FOLDER + key + "/" + resource["id"])
        logging.info(
            "Resource saved into minio at {}".format(
                os.getenv("MINIO_URL")
                + os.getenv("MINIO_BUCKET")
                + MINIO_FOLDER
                + key
                + "/"
                + resource["id"]
            )
        )
    except ClientError as e:
        logging.error(e)


@celery.task
def manage_resource(key, resource):
    logging.info(
        "Processing task for resource {} in dataset {}".format(resource["id"], key)
    )
    try:
        print(resource["url"])
        tmp_file = download_resource(resource["url"])
        
        # Check resource MIME type
        mime_type = magic.from_file(tmp_file.name, mime=True)
        # TODO: Decide if we also want to leverage the URL extension
        if mime_type in ['text/plain', 'text/csv']:
            # Save resource only if CSV
            try:
                # Raise ValueError if file is not a CSV
                agate.Table.from_csv(tmp_file.name, sniff_limit=4096, row_limit=40)
                save_resource_to_minio(tmp_file, key, resource)
                logging.info(
                    "Sending kafka message for resource {} in dataset {}".format(resource["id"], key)
                )
                # TODO: Add MIME type to Kafka message
                produce(key, resource)
            except ValueError:
                # TODO: Check if we need to send a Kafka message for non CSV resources
                logging.info(
                    "Resource {} in dataset {} is not a CSV".format(resource["id"], key)
                )
        return "Resource processed {} - END".format(resource["id"])
    finally:
        os.unlink(tmp_file.name)
