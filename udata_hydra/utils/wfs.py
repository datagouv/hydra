from urllib.parse import parse_qs, urlparse


def detect_wfs(check: dict, resource_format: str | None = None) -> bool:
    """
    Detect if a resource is a WFS endpoint based on resource format and URL patterns.

    Checks for:
    - Resource format matching "wfs" or "ogc:wfs" (case-insensitive)
    - SERVICE=WFS query parameter (case-insensitive)
    - "wfs" in the URL path

    Args:
        check: Dictionary containing at least a "url" key
        resource_format: Optional format string from the catalog

    Returns:
        True if the resource appears to be a WFS endpoint, False otherwise
    """
    if resource_format and resource_format.lower().replace("ogc:", "") == "wfs":
        return True

    url = check.get("url", "")
    if not url:
        return False

    parsed = urlparse(url)

    # Check for SERVICE=WFS parameter (case-insensitive)
    query_params = parse_qs(parsed.query.lower())
    if query_params.get("service") == ["wfs"]:
        return True

    # Check for "/wfs" as a path segment (e.g. /geoserver/wfs or /wfs/layer)
    path_segments = parsed.path.lower().rstrip("/").split("/")
    if "wfs" in path_segments:
        return True

    return False
