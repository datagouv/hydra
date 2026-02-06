from urllib.parse import parse_qs, urlparse

from udata_hydra import config


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
