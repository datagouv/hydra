import json


async def detect_geojson_from_headers(check: dict) -> bool:
    headers: dict = json.loads(check["headers"] or "{}")
    if any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in ["application/vnd.geo+json"]
    ):
        return True
    return False
