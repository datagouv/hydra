import re
from urllib.parse import parse_qs, urlparse

from udata_hydra import config

VALID_LAYER_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_\-.:]{1,100}$")


def detect_ogc(check: dict, resource_format: str | None = None) -> bool:
    """
    Detect if a resource is an OGC service based on resource format and URL patterns.

    Supported formats are configured via OGC_FORMATS (e.g. ["wfs"]).
    For each format, checks:
    - Resource format matching the format or "ogc:{format}" (case-insensitive)
    - SERVICE={format} query parameter (case-insensitive)
    - "/{format}" in the URL path

    Args:
        check: Dictionary containing at least a "url" key
        resource_format: Optional format string from the catalog

    Returns:
        True if the resource appears to be an OGC service, False otherwise
    """
    ogc_formats: list[str] = config.OGC_FORMATS

    # Check resource format from catalog
    if resource_format:
        normalized = resource_format.lower().replace("ogc:", "")
        if normalized in ogc_formats:
            return True

    url = check.get("url", "")
    if not url:
        return False

    parsed = urlparse(url)
    query_params = parse_qs(parsed.query.lower())
    path_segments = parsed.path.lower().rstrip("/").split("/")

    for fmt in ogc_formats:
        # Check for SERVICE={fmt} query parameter (case-insensitive)
        if fmt in query_params.get("service", []):
            return True

        # Check for "/{fmt}" as a path segment (e.g. /geoserver/wfs)
        if fmt in path_segments:
            return True

    return False


def is_valid_layer_name(name: str) -> bool:
    """Check if a string looks like a valid OGC layer name."""
    return bool(VALID_LAYER_NAME_PATTERN.match(name))


def detect_layer_name(url: str, resource_title: str | None = None) -> str | None:
    """Detect layer name from URL params (typeName/typeNames/typename) or resource title."""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    # Case-insensitive param lookup for typename/typeName/typeNames
    for key, values in query_params.items():
        if key.lower() in ("typename", "typenames"):
            if values and is_valid_layer_name(values[0]):
                return values[0]
    # Fallback: resource title
    if resource_title and is_valid_layer_name(resource_title):
        return resource_title
    return None
