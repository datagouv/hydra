import json


async def detect_csv_from_headers(check) -> bool:
    """
    Determine if content-type header looks like a csv's one
    or if it's csv.gz.
    For compressed csv we have two checks:
    1. is the file's content binary?
    2. does the URL contain "csv.gz"?
    """
    headers = json.loads(check["headers"] or "{}")
    return any(
        headers.get("content-type", "").lower().startswith(ct) for ct in [
            "application/csv", "text/plain", "text/csv"
        ]
    ) or (any([
        headers.get("content-type", "").lower().startswith(ct) for ct in [
            "application/octet-stream", "application/x-gzip"
        ]
    ]) and "csv.gz" in check.get("url", []))
