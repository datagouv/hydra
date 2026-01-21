"""Sample random catalog resources and summarize observed CORS headers."""

import argparse
import asyncio
import logging
from collections import Counter

import aiohttp

from udata_hydra import config, context
from udata_hydra.crawl.helpers import convert_headers
from udata_hydra.utils.http import CORS_HEADER_FIELDS, CORS_HEADER_PREFIX

log = logging.getLogger("udata-hydra")


async def fetch_random_resources(limit: int) -> list[tuple[str, str]]:
    pool = await context.pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT resource_id, url
            FROM catalog
            WHERE deleted = FALSE AND url IS NOT NULL
            ORDER BY random()
            LIMIT $1;
            """,
            limit,
        )
    return [(str(row["resource_id"]), row["url"]) for row in rows]


async def probe_headers(session: aiohttp.ClientSession, url: str) -> tuple[int | None, dict]:
    origin: str | None = getattr(config, "CORS_PROBE_ORIGIN", None)
    if not origin:
        raise RuntimeError("CORS_PROBE_ORIGIN is not configured")
    headers: dict[str, str] = {
        "Origin": origin,
        "Access-Control-Request-Method": "GET",
    }
    request_header_names: list[str] | None = getattr(config, "CORS_PROBE_REQUEST_HEADERS", None)
    if request_header_names:
        headers["Access-Control-Request-Headers"] = ",".join(request_header_names)

    timeout_seconds = int(getattr(config, "CORS_PROBE_TIMEOUT_SECONDS", 5) or 5)
    probe_timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    try:
        async with session.options(
            url,
            headers=headers,
            timeout=probe_timeout,
            allow_redirects=True,
        ) as cors_resp:
            cors_headers = convert_headers(cors_resp.headers)
            return cors_resp.status, cors_headers
    except Exception as exc:
        return None, {"error": str(exc)}


async def sample_cors_headers(limit: int) -> None:
    resources = await fetch_random_resources(limit)
    log.info("Fetched %s random resources", len(resources))
    if not resources:
        print("Catalog is empty; aborting.")
        return

    origin = getattr(config, "CORS_PROBE_ORIGIN", None)
    if not origin:
        print("CORS_PROBE_ORIGIN is not configured; aborting.")
        return

    async with aiohttp.ClientSession(timeout=None) as session:
        key_counter: Counter[str] = Counter()
        samples: list[tuple[str, str, int | None, list[str], str | None]] = []
        for resource_id, url in resources:
            status, headers = await probe_headers(session, url)
            if headers is None:
                continue
            ac_keys = [key for key in headers if key.startswith("access-control-")]
            key_counter.update(ac_keys)
            samples.append((resource_id, url, status, ac_keys, headers.get("error")))

    print("\nSampled resources and Access-Control-* keys:")
    for resource_id, url, status, keys, error in samples:
        line = f"- {resource_id} :: status={status} :: keys={keys or '∅'}"
        if error:
            line += f" :: error={error}"
        print(line)

    print("\nAggregated Access-Control-* key counts:")
    for key, count in key_counter.most_common():
        print(f"  {key}: {count}")

    observed = set(key_counter)
    tracked = {f"{CORS_HEADER_PREFIX}{field}" for field in CORS_HEADER_FIELDS}
    missing = sorted(observed - tracked)
    if missing:
        print("\nAccess-Control-* keys observed but not in CORS_HEADER_FIELDS:")
        for key in missing:
            print(f"  - {key}")
    else:
        print("\nNo additional Access-Control-* keys beyond current CORS_HEADER_FIELDS.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of random resources to sample (default: 50)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(sample_cors_headers(limit=args.limit))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
