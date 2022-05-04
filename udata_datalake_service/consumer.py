import json
import logging
import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from kafka import KafkaConsumer
from udata_datalake_service.background_tasks import manage_resource

KAFKA_HOST = os.environ.get("KAFKA_HOST", "localhost")
KAFKA_PORT = os.environ.get("KAFKA_PORT", "9092")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "2.5.0")
TOPICS = os.environ.get("TOPICS", ["resource.created", "resource.modified"])


def create_kafka_consumer() -> KafkaConsumer:
    logging.info("Creating Kafka Consumer")
    consumer = KafkaConsumer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        group_id="datalake",
        reconnect_backoff_max_ms=100000,  # TODO: what value to set here?
        # API Version is needed in order to prevent api version guessing leading to an error
        # on startup if Kafka Broker isn't ready yet
        api_version=tuple(
            [int(value) for value in KAFKA_API_VERSION.split(".")]
        ),
    )
    consumer.subscribe(TOPICS)
    logging.info("Kafka Consumer created")
    return consumer


def consume_kafka() -> None:
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

    consumer = create_kafka_consumer()
    logging.info("Ready to consume message")
    for message in consumer:
        val_utf8 = message.value.decode("utf-8").replace("NaN", "null")
        data = json.loads(val_utf8)
        resource_id = data["value"]["resource"]["id"]
        logging.info(f"New message detected, checking resource {resource_id}")

        dataset_id = data["meta"]["dataset_id"]
        manage_resource.delay(dataset_id, data["value"]["resource"])
