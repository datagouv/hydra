from udata_hydra import config
from udata_hydra.data_formats.data_format import DataFormat


class Geojson(DataFormat):
    standard_mime_type = "application/vnd.geo+json"
    valid_mime_types = {standard_mime_type}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["geojson"])
    check_url = "geojson"
    further_analysis = True

    def analyse(self, **kwargs):
        raise NotImplementedError
