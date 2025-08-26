import logging

import coloredlogs
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.rq import RqIntegration

from udata_hydra import config

log = logging.getLogger("udata-hydra")
context = {"inited": False}


def setup_logging() -> logging.Logger:
    if context.get("inited"):
        return log
    if config.SENTRY_DSN:
        sentry_sdk.init(
            dsn=config.SENTRY_DSN,
            integrations=[
                AioHttpIntegration(),
                RqIntegration(),
            ],
            release=f"{config.APP_NAME}@{config.APP_VERSION}",
            environment=config.ENVIRONMENT or "unknown",
            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for performance monitoring.
            # Sentry recommends adjusting this value in production.
            traces_sample_rate=config.SENTRY_SAMPLE_RATE,
            profiles_sample_rate=config.SENTRY_SAMPLE_RATE,
        )

    coloredlogs.install(level=config.LOG_LEVEL)
    # silence urllib3 a bit
    logging.getLogger("urllib3").setLevel("INFO")
    logging.getLogger("asyncio").setLevel("INFO")
    context["inited"] = True
    return log


def stop_sentry() -> None:
    """Stop sentry collection programatically"""
    client = sentry_sdk.get_client()
    if client:
        client.close()
