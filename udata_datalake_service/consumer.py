import json
import logging
import os

from udata_datalake_service.background_tasks import manage_resource
from kafka import KafkaConsumer

KAFKA_HOST = os.environ.get("KAFKA_HOST", "localhost")
KAFKA_PORT = os.environ.get("KAFKA_PORT", "9092")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "2.5.0")
TOPICS = os.environ.get("TOPICS", ["dataset"])


def create_kafka_consumer():
    logging.info("Creating Kafka Consumer")
    consumer = KafkaConsumer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        group_id="datalake",
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
        value = message.value
        val_utf8 = value.decode("utf-8").replace("NaN", "null")
        key = message.key
        data = json.loads(val_utf8)
        logging.info("New message detected, checking dataset {}".format(key))
        if data:
            for r in data["data"].get('resources', []):
                logging.info("checking resource {}".format(r["id"]))
                manage_resource.delay(key.decode("utf-8"), r)
        else:
            logging.info("Message empty, do not process anything - END")
