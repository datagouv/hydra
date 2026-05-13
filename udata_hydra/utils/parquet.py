import json


async def detect_parquet_from_headers(check: dict) -> bool:
    headers: dict = json.loads(check["headers"] or "{}")
    # most parquet files are exposed with "application/octet-stream"
    # which combined with "parquet" in the url is a good hint
    # the ideal case is "application/vnd.apache.parquet"
    return any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in ["application/vnd.apache.parquet"]
    ) or "parquet" in check.get("url", "")
