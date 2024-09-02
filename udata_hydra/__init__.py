import logging
import os
from pathlib import Path

import toml

log = logging.getLogger("udata-hydra")


class Configurator:
    """Loads a dict of config from TOML file(s) and behaves like an object, ie config.VALUE"""

    configuration: dict = {}

    def __init__(self):
        if not self.configuration:
            self.configure()
            self.load_pyproject_info()

    def configure(self) -> None:
        # load default settings
        configuration: dict = toml.load(Path(__file__).parent / "config_default.toml")

        # override with local settings
        local_settings = os.environ.get("HYDRA_SETTINGS", Path.cwd() / "config.toml")
        if Path(local_settings).exists():
            configuration.update(toml.load(local_settings))

        self.configuration = configuration
        self.check()

    def load_pyproject_info(self) -> None:
        """Get more info about the app from pyproject.toml"""
        project_info: dict = {}
        try:
            project_info = toml.load("pyproject.toml")["project"]
        except Exception as e:
            log.error(f"Error while getting pyproject.toml info: {str(e)}")
        self.configuration["APP_NAME"] = project_info.get("name", "udata-hydra")
        self.configuration["APP_VERSION"] = project_info.get("version", "unknown")

    def override(self, **kwargs) -> None:
        self.configuration.update(kwargs)
        self.check()

    def check(self) -> None:
        """Sanity check on config"""
        assert self.MAX_POOL_SIZE >= self.BATCH_SIZE, "BATCH_SIZE cannot exceed MAX_POOL_SIZE"

    def __getattr__(self, __name):
        return self.configuration.get(__name)

    @property
    def __dict__(self):
        return self.configuration


config = Configurator()
