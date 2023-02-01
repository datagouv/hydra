import logging

import coloredlogs
import sentry_sdk

from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.rq import RqIntegration

from udata_hydra import config

log = logging.getLogger("udata-hydra")
context = {"inited": False}


def setup_logging():
    if context.get("inited"):
        return log
    if config.SENTRY_DSN:
        sentry_sdk.init(
            dsn=config.SENTRY_DSN,
            integrations=[
                AioHttpIntegration(),
                RqIntegration(),
            ],
        )
    coloredlogs.install(level=config.LOG_LEVEL)
    # silence urllib3 a bit
    logging.getLogger("urllib3").setLevel("INFO")
    context["inited"] = True
    return log


def stop_sentry():
    """Stop sentry collection programatically"""
    if sentry_sdk.Hub.current.client:
        sentry_sdk.Hub.current.client.close()
