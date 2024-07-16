from udata_hydra import config
from udata_hydra.logger import setup_logging

setup_logging()

REDIS_URL = config.REDIS_URL

QUEUES = ["high", "default", "low"]
