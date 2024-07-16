import logging
import os
from typing import Union

import coloredlogs
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.rq import RqIntegration

from udata_hydra import config
from udata_hydra.utils.app_version import get_app_version

log = logging.getLogger("udata-hydra")
context = {"inited": False}


def setup_logging():
    if context.get("inited"):
        return log
    release = "hydra@unknown"
    app_version: Union[str, None] = get_app_version()
    if app_version:
        release = f"hydra@{app_version}"
    if config.SENTRY_DSN:
        sentry_sdk.init(
            dsn=config.SENTRY_DSN,
            integrations=[
                AioHttpIntegration(),
                RqIntegration(),
            ],
            release=release,
            environment=os.getenv("HYDRA_ENV", "unknown"),
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


def stop_sentry():
    """Stop sentry collection programatically"""
    if sentry_sdk.Hub.current.client:
        sentry_sdk.Hub.current.client.close()
