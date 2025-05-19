import json

from asyncpg import Record

from udata_hydra import context


async def detect_geojson_from_headers_or_catalog(check: dict) -> bool:
    headers: dict = json.loads(check["headers"] or "{}")
    # in some cases geojson files have the content-type `application/json`
    # but adding this in the list would not have been a restrictive enough condition
    # so we check the URL, and also the format in the catalog
    # (still not good enough for static resources ending with .json only)
    if any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in ["application/vnd.geo+json"]
    ) or "geojson" in check.get("url", ""):
        return True
    pool = await context.pool()
    async with pool.acquire() as connection:
        row: Record = await connection.fetchrow(
            "SELECT format FROM catalog WHERE resource_id = $1", f"{check['resource_id']}"
        )
    return row["format"] == "geojson"
