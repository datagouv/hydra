import json


def detect_tabular_from_headers(check: dict) -> tuple[bool, str]:
    """
    Determine from content-type header if file looks like:
        - a csv
        - a csv.gz (1. is the file's content binary?, 2. does the URL contain "csv.gz"?)
        - a xls(x)
    """
    headers: dict = json.loads(check["headers"] or "{}")

    if any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in ["application/csv", "text/plain", "text/csv"]
    ):
        return True, "csv"

    if any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in ["application/octet-stream", "application/x-gzip", "application/gzip"]
    ) and "csv.gz" in check.get("url", ""):
        return True, "csvgz"

    if any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in [
            "application/vnd.ms-excel",
        ]
    ):
        # and "xls" in check.get("url", "")
        return True, "xls"

    if any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in [
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ]
    ):
        # and "xlsx" in check.get("url", "")
        return True, "xlsx"

    return False, "csv"
