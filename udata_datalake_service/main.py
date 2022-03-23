import click
import logging
from dotenv import load_dotenv

from consumer import consume_kafka, create_kafka_consumer


@click.command()
def consume():
    consumer = create_kafka_consumer()
    consume_kafka(consumer)


if __name__ == '__main__':
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    consume()
