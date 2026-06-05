from udata_hydra import config
from udata_hydra.data_formats.data_format import DataFormat


class PMTiles(DataFormat):
    standard_mime_type = "application/vnd.pmtiles"
    valid_mime_types = {standard_mime_type}

    def analyse(self, **kwargs):
        raise NotImplementedError
