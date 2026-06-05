from udata_hydra.data_formats.data_format import DataFormat


class Table(DataFormat):
    @classmethod
    def detect_from_check(cls, **kwargs):
        raise NotImplementedError

    @classmethod
    def detect_from_catalog_format(cls, **kwargs):
        raise NotImplementedError

    def analyse(self, **kwargs):
        raise NotImplementedError
