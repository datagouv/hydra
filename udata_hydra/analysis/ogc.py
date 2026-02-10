import logging
from datetime import datetime, timezone
from typing import TypedDict

from asyncpg import Record
from owslib.wfs import WebFeatureService

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import ParseException, handle_parse_exception
from udata_hydra.utils.ogc import detect_layer_name

log = logging.getLogger("udata-hydra")


class OgcLayer(TypedDict):
    name: str
    default_crs: str | None


class OgcMetadata(TypedDict):
    format: str
    version: str
    layers: list[OgcLayer]
    output_formats: list[str]
    detected_layer_name: str | None


async def analyse_ogc(check: dict) -> OgcMetadata | None:
    """
    Analyse an OGC endpoint and extract metadata.

    Currently supports WFS. Connects to the service, retrieves GetCapabilities,
    and extracts:
    - Service format and version
    - Available layers with their CRS options
    - Supported output formats

    Args:
        check: Dictionary containing at least "url" key. "id" and "resource_id" are optional
               (for CLI usage without database).

    Returns:
        The extracted metadata dictionary, or None if analysis is disabled or fails
    """
    if not config.OGC_ANALYSIS_ENABLED:
        log.debug("OGC_ANALYSIS_ENABLED turned off, skipping.")
        return None

    url = check["url"]
    resource_id = check.get("resource_id")
    check_id = check.get("id")

    log.debug(f"Starting OGC analysis for {url}")

    resource: Record | None = None
    if resource_id:
        resource = await Resource.update(str(resource_id), {"status": "ANALYSING_OGC"})

    metadata: OgcMetadata | None = None
    try:
        if check_id:
            check = await Check.update(check_id, {"parsing_started_at": datetime.now(timezone.utc)})

        # Try connecting with version fallback
        wfs = None
        version = None
        connection_error = None
        for v in ["2.0.0", "1.1.0", "1.0.0"]:
            try:
                wfs = WebFeatureService(url, version=v, timeout=config.OGC_GETCAPABILITIES_TIMEOUT)
                version = v
                break
            except Exception as e:
                connection_error = e
                continue

        if wfs is None or version is None:
            raise ParseException(
                message=f"Could not connect to WFS service with any supported version. "
                f"Latest error was: {connection_error}",
                step="wfs_connection",
                resource_id=str(resource_id) if resource_id else None,
                url=url,
                check_id=check_id,
            ) from connection_error

        # Extract service metadata
        try:
            metadata = {
                "format": "wfs",
                "version": version,
                "layers": [],
                "output_formats": [],
                "detected_layer_name": None,
            }

            # Get global output formats from GetFeature operation parameters
            get_feature_op = wfs.getOperationByName("GetFeature")
            if get_feature_op and (output_formats := get_feature_op.parameters.get("outputFormat")):
                metadata["output_formats"] = list(output_formats.get("values") or [])

            # Extract layer information
            for name, layer in wfs.contents.items():
                ogc_layer: OgcLayer = {
                    "name": name,
                    "default_crs": None,
                }

                # Extract default CRS
                crs_options = getattr(layer, "crsOptions", []) or []
                if crs_options:
                    ogc_layer["default_crs"] = crs_options[0].getcode()

                metadata["layers"].append(ogc_layer)

            # Detect layer name from URL params or resource title
            resource_title = None
            if resource_id:
                resource_record = await Resource.get(str(resource_id), "title")
                if resource_record:
                    resource_title = resource_record["title"]
            candidate = detect_layer_name(url, resource_title)
            # Only keep the candidate if it matches one of the layer names
            if candidate and metadata["layers"]:
                if candidate in [layer["name"] for layer in metadata["layers"]]:
                    metadata["detected_layer_name"] = candidate
        except Exception as e:
            raise ParseException(
                message=str(e),
                step="wfs_parsing",
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
            )

        log.debug(
            f"OGC analysis complete for {url}: "
            f"{len(metadata['layers'])} layers found"
        )

        return metadata

    except ParseException as e:
        if check_id:
            check = await handle_parse_exception(e, None, check)
        else:
            log.error(f"OGC analysis failed for {url}: {e}")
        return None

    finally:
        if resource and check_id:
            await helpers.notify_udata(resource, check)
        if resource_id:
            await Resource.update(str(resource_id), {"status": None})
