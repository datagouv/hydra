import os

from pathlib import Path

import toml


class Configurator:
    """Loads a dict of config from TOML file(s) and behaves like an object, ie config.VALUE"""
    configuration = None

    def __init__(self):
        if not self.configuration:
            self.configure()

    def configure(self):
        # load default settings
        configuration = toml.load(Path(__file__).parent / "config_default.toml")

        # override with local settings
        local_settings = os.environ.get("HYDRA_SETTINGS", Path.cwd() / "config.toml")
        if Path(local_settings).exists():
            configuration.update(toml.load(local_settings))

        self.configuration = configuration
        self.check()

    def override(self, **kwargs):
        self.configuration.update(kwargs)
        self.check()

    def check(self):
        """Sanity check on config"""
        assert self.MAX_POOL_SIZE >= self.BATCH_SIZE, "BATCH_SIZE cannot exceed MAX_POOL_SIZE"

    def __getattr__(self, __name):
        return self.configuration.get(__name)

    @property
    def __dict__(self):
        return self.configuration


config = Configurator()
