import json
import logging
import os

from udata_datalake_service.background_tasks import manage_resource
from kafka import KafkaConsumer

KAFKA_HOST = os.environ.get("KAFKA_HOST", "localhost")
KAFKA_PORT = os.environ.get("KAFKA_PORT", "9092")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "2.5.0")
TOPICS = os.environ.get("TOPICS", ["resource.created", "resource.modified"])


def create_kafka_consumer():
    logging.info("Creating Kafka Consumer")
    consumer = KafkaConsumer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        group_id=None,
        reconnect_backoff_max_ms=100000,  # TODO: what value to set here?
        # API Version is needed in order to prevent api version guessing leading to an error
        # on startup if Kafka Broker isn't ready yet
        api_version=tuple([int(value) for value in KAFKA_API_VERSION.split(".")]),
    )
    consumer.subscribe(TOPICS)
    logging.info("Kafka Consumer created")
    return consumer


def consume_kafka():
    consumer = create_kafka_consumer()
    logging.info("Ready to consume message")
    for message in consumer:
        val_utf8 = message.value.decode("utf-8").replace("NaN", "null")
        data = json.loads(val_utf8)
        resource_id = data["value"]["resource"]["id"]
        logging.info(f"New message detected, checking resource {resource_id}")

        dataset_id = data["meta"]["dataset_id"]
        manage_resource.delay(dataset_id, data["value"]["resource"])

