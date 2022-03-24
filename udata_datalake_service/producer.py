import json
import os

from kafka import KafkaProducer

KAFKA_HOST = os.environ.get("KAFKA_HOST", "localhost")
KAFKA_PORT = os.environ.get("KAFKA_PORT", "9092")


class KafkaProducerSingleton:
    __instance = None

    @staticmethod
    def get_instance() -> KafkaProducer:
        if KafkaProducerSingleton.__instance is None:
            KafkaProducerSingleton.__instance = KafkaProducer(
                bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        return KafkaProducerSingleton.__instance


def produce(key_id, document=None):
    producer = KafkaProducerSingleton.get_instance()
    key = key_id.encode("utf-8")

    value = {"service": "datalake", "data": document, "meta": {}}

    producer.send("datalake", value=value, key=key)
    producer.flush()
