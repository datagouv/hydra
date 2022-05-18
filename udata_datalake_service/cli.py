import logging
import os
import click
from dotenv import load_dotenv
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from udata_datalake_service.background_tasks import celery
from udata_datalake_service.consumer import process_message
from udata_event_service.consumer import consume_kafka


@click.group()
@click.version_option()
def cli() -> None:
    """
    udata-datalake-service
    """


@cli.command()
def consume() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    # Create bucket if it doesn't exist
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_URL"),
        aws_access_key_id=os.getenv("MINIO_USER"),
        aws_secret_access_key=os.getenv("MINIO_PWD"),
        config=Config(signature_version="s3v4"),
    )
    try:
        client.head_bucket(Bucket=os.getenv("MINIO_BUCKET"))
    except ClientError:
        client.create_bucket(Bucket=os.getenv("MINIO_BUCKET"))
    consume_kafka(
        group_id="datalake",
        topics=os.environ.get(
            "TOPICS", ["resource.created", "resource.modified"]
        ),
        message_processing_func=process_message,
    )


@cli.command()
def work() -> None:
    """Starts a worker"""
    worker = celery.Worker()
    worker.start()
    return worker.exitcode
