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

log = logging.getLogger("udata-hydra")


class WfsFeatureType(TypedDict):
    name: str
    default_crs: str | None
    other_crs: list[str]


class WfsMetadata(TypedDict):
    version: str
    feature_types: list[WfsFeatureType]
    output_formats: list[str]


async def analyse_wfs(check: dict) -> WfsMetadata | None:
    """
    Analyse a WFS endpoint and extract metadata.

    Connects to the WFS service, retrieves GetCapabilities, and extracts:
    - Service version
    - Available feature types with their CRS options
    - Supported output formats

    Args:
        check: Dictionary containing at least "url" key. "id" and "resource_id" are optional
               (for CLI usage without database).

    Returns:
        The extracted metadata dictionary, or None if analysis is disabled or fails
    """
    if not config.WFS_ANALYSIS_ENABLED:
        log.debug("WFS_ANALYSIS_ENABLED turned off, skipping.")
        return None

    url = check["url"]
    resource_id = check.get("resource_id")
    check_id = check.get("id")

    log.debug(f"Starting WFS analysis for {url}")

    # Update resource status to ANALYSING_WFS
    resource: Record | None = None
    if resource_id:
        resource = await Resource.update(str(resource_id), {"status": "ANALYSING_WFS"})

    metadata: WfsMetadata | None = None
    try:
        if check_id:
            check = await Check.update(check_id, {"parsing_started_at": datetime.now(timezone.utc)})

        # Try connecting with version fallback
        wfs = None
        version = None
        connection_error = None
        for v in ["2.0.0", "1.1.0", "1.0.0"]:
            try:
                wfs = WebFeatureService(url, version=v, timeout=config.WFS_GETCAPABILITIES_TIMEOUT)
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
                "version": version,
                "feature_types": [],
                "output_formats": [],
            }

            # Get global output formats from GetFeature operation parameters
            get_feature_op = wfs.getOperationByName("GetFeature")
            if get_feature_op and (output_formats := get_feature_op.parameters.get("outputFormat")):
                metadata["output_formats"] = list(output_formats.get("values") or [])

            # Extract feature type information
            for name, layer in wfs.contents.items():
                feature_type: WfsFeatureType = {
                    "name": name,
                    "default_crs": None,
                    "other_crs": [],
                }

                # Extract CRS options
                crs_options = getattr(layer, "crsOptions", []) or []
                if crs_options:
                    crs_strings = [crs.getcode() for crs in crs_options]
                    # owslib merges DefaultCRS (first position) and OtherCRS in the same list
                    feature_type["default_crs"] = crs_strings[0] if crs_strings else None
                    feature_type["other_crs"] = crs_strings[1:] if len(crs_strings) > 1 else []

                metadata["feature_types"].append(feature_type)
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
                    "wfs_metadata": metadata,
                },
            )

        log.debug(
            f"WFS analysis complete for {url}: "
            f"{len(metadata['feature_types'])} feature types found"
        )

        return metadata

    except ParseException as e:
        if check_id:
            check = await handle_parse_exception(e, None, check)
        else:
            log.error(f"WFS analysis failed for {url}: {e}")
        return None

    finally:
        if resource and check_id:
            await helpers.notify_udata(resource, check)
        # Reset resource status to None
        if resource_id:
            await Resource.update(str(resource_id), {"status": None})
