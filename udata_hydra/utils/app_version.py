import logging
from typing import Optional

import toml

log = logging.getLogger("udata-hydra")


def get_app_version() -> Optional[str]:
    """Get the app version from pyproject.toml"""
    try:
        pyproject: dict = toml.load("pyproject.toml")
        app_version: str = pyproject["tool"]["poetry"]["version"]
        return app_version
    except Exception as e:
        log.error(f"Error while getting app version: {e}")
        return None
