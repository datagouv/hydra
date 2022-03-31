import logging
import click
from dotenv import load_dotenv

from udata_datalake_service.consumer import consume_kafka


@click.group()
@click.version_option()
def cli():
    """
    udata-datalake-service
    """

@cli.command()
def consume():
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    consume_kafka()
