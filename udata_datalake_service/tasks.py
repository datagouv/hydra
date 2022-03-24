import logging
import os
import tempfile

import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from udata_datalake_service.producer import produce

load_dotenv()


def download_resource(url):
    tmp = tempfile.NamedTemporaryFile(delete=False)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_length = r.headers.get("content-length")
        for chunk in r.iter_content(chunk_size=1024):
            tmp.write(chunk)
    return tmp


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
        s3.upload_fileobj(
            resource_file,
            os.getenv("MINIO_BUCKET"),
            "stan/" + key + "/" + resource["id"],
        )
    except ClientError as e:
        logging.error(e)
    logging.info(
        "Resource saved into minio at {}".format(
            os.getenv("MINIO_URL")
            + os.getenv("MINIO_BUCKET")
            + "/stan/"
            + key
            + "/"
            + resource["id"]
        )
    )


def manage_resource(key, resource):
    logging.info(
        "Processing task for resource {} in dataset {}".format(resource["id"], key)
    )
    tmp_resource_file = download_resource(resource["url"])
    save_resource_to_minio(tmp_resource_file, key, resource)
    produce(key, resource)
    return "Resource processed {} - END".format(resource["id"])
