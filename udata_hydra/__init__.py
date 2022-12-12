import os

from pathlib import Path

import toml


class Configurator:
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

    def override(self, **kwargs):
        self.configuration.update(kwargs)

    def __getattr__(self, __name):
        return self.configuration.get(__name)

    @property
    def __dict__(self):
        return self.configuration


config = Configurator()
