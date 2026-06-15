import json


async def detect_geojson_from_headers(check: dict) -> bool:
    headers: dict = json.loads(check["headers"] or "{}")
    # in some cases geojson files have the content-type `application/json`
    # but adding this in the list would not have been a restrictive enough condition
    # so we check the URL
    # (still not good enough for static resources ending with .json only)
    return any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in ["application/vnd.geo+json"]
    ) or "geojson" in check.get("url", "")
