from io import BytesIO
import json

import pandas as pd


def save_as_parquet(
    df: pd.DataFrame,
    output_filename: str | None = None,
) -> tuple[str, BytesIO | None]:
    bytes = df.to_parquet(
        f"{output_filename}.parquet" if output_filename else None,
        compression="zstd",  # best compression to date
    )
    return f"{output_filename}.parquet", bytes


async def detect_parquet_from_headers(check: dict) -> bool:
    headers: dict = json.loads(check["headers"] or "{}")
    # most parquet files are exposed with "application/octet-stream"
    # which combined with "parquet" in the url is a good hint
    # the ideal case is "application/vnd.apache.parquet"
    return any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in ["application/vnd.apache.parquet"]
    ) or "parquet" in check.get("url", "")
