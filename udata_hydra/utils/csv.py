import json


async def detect_csv_from_headers(check) -> bool:
    """Determine if content-type header looks like a csv's one"""
    headers = json.loads(check["headers"] or {})
    return any(
        headers.get("content-type", "").lower().startswith(ct) for ct in [
            "application/csv", "text/plain", "text/csv"
        ]
    )
