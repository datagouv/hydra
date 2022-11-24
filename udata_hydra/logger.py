import logging

import coloredlogs
import sentry_sdk

from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from udata_hydra import config

log = logging.getLogger("udata-hydra")


def setup_logging():
    if config.SENTRY_DSN:
        sentry_sdk.init(
            dsn=config.SENTRY_DSN,
            integrations=[
                AioHttpIntegration(),
            ],
        )
    coloredlogs.install(level=config.LOG_LEVEL)
    # silence urllib3 a bit
    logging.getLogger("urllib3").setLevel("INFO")
    return log
