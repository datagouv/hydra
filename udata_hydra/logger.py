import logging

import coloredlogs

from udata_hydra import config

log = logging.getLogger("udata-hydra")


def setup_logging():
    coloredlogs.install(level=config.LOG_LEVEL)
    return log
