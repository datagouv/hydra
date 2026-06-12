import logging
from datetime import datetime, timezone
import re
from typing import Literal, TypedDict
from urllib.parse import parse_qs, urlparse

from asyncpg import Record
from owslib.crs import Crs
from owslib.wfs import WebFeatureService
from owslib.wms import WebMapService


from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import ParseException, handle_parse_exception

log = logging.getLogger("udata-hydra")

VALID_LAYER_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_\-.:]{1,100}$")
SERVICE_MAPPING = {
    "wfs": {
        "service": WebFeatureService,
        "versions": ["2.0.0", "1.1.0", "1.0.0"],
    },
    "wms": {
        "service": WebMapService,
        "versions": ["1.3.0", "1.1.1"],
    },
}
OgcFormatLiteral = Literal["wfs", "wms"]


class OgcLayer(TypedDict):
    name: str
    default_crs: str | None


class OgcMetadata(TypedDict):
    format: str
    version: str
    output_formats: list[str]
    detected_layer: OgcLayer | None


class Ogc(DataFormat):
    @classmethod
    def detect_from_check(cls, check: dict, resource_format: str | None) -> bool:  # ty: ignore[invalid-method-override]
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
    def detect_from_catalog_format(cls, format: str | None) -> bool:
        return False

    @classmethod
    async def analyse(cls, check: dict):
        """Analyse an OGC endpoint and extract metadata.

        Currently supports WFS and WMS. Connects to the service, retrieves GetCapabilities,
        and extracts:
        - Service format and version
        - Detected layer with its CRS options if any
        - Supported output formats for WFS

        Args:
            check: Dictionary containing at least "url" key. "id" and "resource_id" are optional
                (for CLI usage without database).

        Returns:
            The extracted metadata dictionary, or None if analysis is disabled or fails
        """
        if not config.OGC_ANALYSIS_ENABLED:
            log.debug("OGC_ANALYSIS_ENABLED turned off, skipping.")
            return None

        format = cls.__name__.lower()
        if format not in config.OGC_FORMATS:
            log.debug(
                f"Only OGC service formats activated in config are : OGC_FORMATS={config.OGC_FORMATS}"
            )
            return None

        url = check["url"]
        resource_id = check.get("resource_id")
        check_id = check.get("id")

        log.debug(f"Starting OGC analysis for {url}")

        resource: Record | None = None
        if resource_id:
            resource = await Resource.update(
                str(resource_id), {"status": f"ANALYSING_{format.upper()}"}
            )

        metadata: OgcMetadata | None = None
        try:
            if check_id:
                check = await Check.update(check_id, {"parsing_started_at": datetime.now(timezone.utc)})  # type: ignore

            # Try connecting with version fallback
            web_service = None
            version = None
            connection_error = None
            for v in SERVICE_MAPPING[format]["versions"]:  # type: ignore[not-iterable]
                try:
                    web_service = SERVICE_MAPPING[format]["service"](  # type: ignore[call-non-callable]
                        url, version=v, timeout=config.OGC_GETCAPABILITIES_TIMEOUT
                    )
                    version = v
                    break
                except Exception as e:
                    connection_error = e
                    continue

            if web_service is None or version is None:
                raise ParseException(
                    message=f"Could not connect to {format} service with any supported version. "
                    f"Latest error was: {connection_error}",
                    step="ogc_service_connection",
                    resource_id=str(resource_id) if resource_id else None,
                    url=url,
                    check_id=check_id,
                ) from connection_error

            # Extract service metadata
            try:
                layers = []
                metadata = {
                    "format": format,
                    "version": version,
                    "output_formats": [],
                    "detected_layer": None,
                }

                if format == "wfs":
                    # Get global output formats from GetFeature operation parameters
                    get_feature_op = web_service.getOperationByName("GetFeature")
                    if get_feature_op and (
                        output_formats := get_feature_op.parameters.get("outputFormat")
                    ):
                        metadata["output_formats"] = list(output_formats.get("values") or [])

                # Extract layer information
                for name, layer in web_service.contents.items():
                    ogc_layer: OgcLayer = {
                        "name": name,
                        "default_crs": None,
                    }

                    # Extract default CRS (ie. the first CRS in the list)
                    crs_options = getattr(layer, "crsOptions", []) or []
                    if crs_options:
                        ogc_layer["default_crs"] = (
                            crs_options[0].getcode()
                            if isinstance(crs_options[0], Crs)
                            else crs_options[0]  # crs_options[0] is already a str in the case of WMS
                        )

                    layers.append(ogc_layer)

                # Detect layer name from URL params or resource title
                resource_title = None
                if resource_id:
                    resource_record = await Resource.get(str(resource_id), "title")
                    if resource_record:
                        resource_title = resource_record["title"]
                candidate = Ogc.detect_layer_name(url, resource_title)
                # Only keep the candidate if it matches one of the layer names
                if candidate and layers:
                    # Exact match (including namespace)
                    exact = next((layer for layer in layers if layer["name"] == candidate), None)
                    if exact:
                        metadata["detected_layer"] = exact
                    else:
                        # Try matching against local name (without namespace prefix),
                        # but only if there's exactly one match to avoid ambiguity
                        matches = [
                            layer for layer in layers if layer["name"].split(":")[-1] == candidate
                        ]
                        if len(matches) == 1:
                            metadata["detected_layer"] = matches[0]
            except Exception as e:
                raise ParseException(
                    message=str(e),
                    step="ogc_service_parsing",
                    resource_id=str(resource_id) if resource_id else None,
                    url=url,
                    check_id=check_id,
                ) from e

            if check_id:
                check = await Check.update(
                    check_id,
                    {
                        "parsing_finished_at": datetime.now(timezone.utc),
                        "ogc_metadata": metadata,
                    },
                )  # type: ignore

            log.debug(
                f"OGC analysis complete for {url}: {len(layers)} layers found"
                + f"; detected layer: {metadata['detected_layer']['name']}"
                if metadata["detected_layer"]
                else ""
            )

            return metadata

        except ParseException as e:
            if check_id:
                check = await handle_parse_exception(e, None, check)  # type: ignore
            else:
                log.error(f"OGC analysis failed for {url}: {e}")
            return None

        finally:
            if resource and check_id:
                await helpers.notify_udata(resource, check)
            if resource_id:
                await Resource.update(str(resource_id), {"status": None})

    @staticmethod
    def is_valid_layer_name(name: str) -> bool:
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
