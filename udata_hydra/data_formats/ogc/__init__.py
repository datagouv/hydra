import re
from typing import Literal
from urllib.parse import parse_qs, urlparse

from udata_hydra import config
from udata_hydra.data_formats.data_format import DataFormat

VALID_LAYER_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_\-.:]{1,100}$")
OgcFormatLiteral = Literal["wfs", "wms"]


class Ogc(DataFormat):
    @classmethod
    def detect_from_check(cls, check: dict, resource_format: str | None) -> bool:
        ogc_format = cls.__name__.lower()
        if not config.OGC_ANALYSIS_ENABLED or ogc_format not in config.OGC_FORMATS:
            return False

        # Check resource format from catalog
        if resource_format:
            normalized = resource_format.lower().replace("ogc:", "")
            if normalized == ogc_format:
                return True

        url = check.get("url", "")
        if not url:
            return False

        parsed = urlparse(url)
        query_params = parse_qs(parsed.query.lower())
        path_segments = parsed.path.lower().rstrip("/").split("/")

        return (
            # Check for SERVICE={fmt} query parameter (case-insensitive)
            ogc_format in query_params.get("service", [])
            # Check for "/{fmt}" as a path segment (e.g. /geoserver/wfs)
            or ogc_format in path_segments
        )

    @classmethod
    def detect_from_catalog_format(cls, format: str) -> bool:
        return False

    @classmethod
    async def analyse(cls, check: dict):
        from udata_hydra.data_formats.ogc.analyse import analyse_ogc

        return await analyse_ogc(data_format=cls, check=check)

    @classmethod
    def is_valid_layer_name(cls, name: str) -> bool:
        """Check if a string looks like a valid OGC layer name."""
        return bool(VALID_LAYER_NAME_PATTERN.match(name))

    @classmethod
    def detect_layer_name(cls, url: str, resource_title: str | None = None) -> str | None:
        """Detect layer name from URL params (typeName/typeNames/typename) or resource title."""
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        # Case-insensitive param lookup for typename/typeName/typeNames
        for key, values in query_params.items():
            if key.lower() in ("typename", "typenames"):
                if values and cls.is_valid_layer_name(values[0]):
                    return values[0]
        # Fallback: resource title
        if resource_title and cls.is_valid_layer_name(resource_title):
            return resource_title
        return None


class Wfs(Ogc):
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["wfs"])


class Wms(Ogc):
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["wms"])
