import json
import time

from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse

import aiohttp
import asyncio

from humanfriendly import parse_timespan

from udata_hydra import config, context
from udata_hydra.datalake_service import process_resource
from udata_hydra.logger import setup_logging
from udata_hydra.utils import queue
from udata_hydra.utils.db import insert_check
from udata_hydra.utils.http import send

results = defaultdict(int)

STATUS_OK = "ok"
STATUS_TIMEOUT = "timeout"
STATUS_ERROR = "error"

log = setup_logging()


def is_valid_status(status):
    if not status:
        return False
    status = int(status)
    return status >= 200 and status < 400


async def compute_check_has_changed(check_data, last_check) -> bool:
    is_first_check = not last_check
    status_has_changed = last_check and check_data.get("status") != last_check.get("status")
    status_no_longer_available = (
        last_check
        and is_valid_status(last_check.get("status"))
        and not is_valid_status(check_data.get("status"))
    )
    timeout_has_changed = last_check and check_data.get("timeout") != last_check.get("timeout")

    criterions = {
        "is_first_check": is_first_check,
        "status_has_changed": status_has_changed,
        "status_no_longer_available": status_no_longer_available,
        "timeout_has_changed": timeout_has_changed,
    }
    log.debug("crawl.py::update_checks_and_catalog:::criterions %s", json.dumps(criterions, indent=4))

    has_changed = any(criterions.values())
    if has_changed:
        document = {
            "check:valid-status": is_valid_status(check_data.get("status")),
            "check:status": check_data.get("status"),
            "check:timeout": check_data["timeout"],
            "check:check-date": datetime.utcnow().isoformat(),
            "check:error": check_data.get("error"),
        }
        pool = await context.pool()
        async with pool.acquire() as conn:
            q = "SELECT dataset_id FROM CATALOG where resource_id = $1"
            dataset = await conn.fetchrow(q, check_data["resource_id"])
        queue.enqueue(
            send,
            dataset_id=dataset["dataset_id"],
            resource_id=check_data["resource_id"],
            document=document
        )

    return has_changed


async def process_check_data(check_data: dict) -> int:
    """Preprocess a check before saving it"""
    context.monitor().set_status("Updating checks and catalog...")
    check_data["resource_id"] = str(check_data["resource_id"])

    pool = await context.pool()
    async with pool.acquire() as connection:
        q = f"""
            SELECT * FROM catalog JOIN checks
            ON catalog.last_check = checks.id
            WHERE catalog.resource_id = '{check_data["resource_id"]}'
            AND catalog.deleted = FALSE;
        """
        last_check = await connection.fetchrow(q)

        await compute_check_has_changed(check_data, dict(last_check) if last_check else None)

        await connection.execute(
            "UPDATE catalog SET priority = FALSE WHERE resource_id = $1",
            check_data["resource_id"]
        )

    return await insert_check(check_data)


async def is_backoff(domain):
    no_backoff = [f"'{d}'" for d in config.NO_BACKOFF_DOMAINS]
    no_backoff = f"({','.join(no_backoff)})"
    since = datetime.utcnow() - timedelta(seconds=config.BACKOFF_PERIOD)
    pool = await context.pool()
    async with pool.acquire() as connection:
        res = await connection.fetchrow(
            f"""
            SELECT COUNT(*) FROM checks
            WHERE domain = $1
            AND created_at >= $2
            AND domain NOT IN {no_backoff}
        """,
            domain,
            since,
        )
        return res["count"] >= config.BACKOFF_NB_REQ, res["count"]


def fix_surrogates(value):
    """FIX Unicode low surrogate must follow a high surrogate.
    eg in 'TREMI_2017-R\xe9sultats enqu\xeate bruts.csv'
    """
    if not type(value) == str:
        value = str(value)
    return value.encode("utf-8", "surrogateescape").decode("utf-8", "replace")


def convert_headers(headers):
    """
    Convert headers from CIMultiDict to dict

    :warning: this will only take the first value for a given header
    key but multidict is not json serializable
    """
    if not headers:
        return {}
    _headers = {}
    for k in headers.keys():
        value = fix_surrogates(headers[k])
        _headers[k.lower()] = value
    return _headers


def has_nice_head(resp):
    """Check if a HEAD response looks useful to us"""
    if not is_valid_status(resp.status):
        return False
    if not any([k in resp.headers for k in ("content-length", "last-modified")]):
        return False
    return True


async def check_url(row, session, sleep=0, method="head"):
    log.debug(f"check {row}, sleep {sleep}, method {method}")

    if sleep:
        await asyncio.sleep(sleep)

    url_parsed = urlparse(row["url"])
    domain = url_parsed.netloc
    if not domain:
        log.warning(f"[warning] not netloc in url, skipping {row['url']}")
        await process_check_data(
            {
                "resource_id": row["resource_id"],
                "url": row["url"],
                "error": "Not netloc in url",
                "timeout": False,
            }
        )
        return STATUS_ERROR

    should_backoff, nb_req = await is_backoff(domain)
    if should_backoff:
        log.info(f"backoff {domain} ({nb_req})")
        context.monitor().add_backoff(domain, nb_req)
        # TODO: maybe just skip this url, it should come back in the next batch anyway
        # but won't it accumulate too many backoffs in the end? is this a problem?
        return await check_url(
            row, session, sleep=config.BACKOFF_PERIOD / config.BACKOFF_NB_REQ
        )
    else:
        context.monitor().remove_backoff(domain)

    try:
        start = time.time()
        timeout = aiohttp.ClientTimeout(total=5)
        _method = getattr(session, method)
        async with _method(
            row["url"], timeout=timeout, allow_redirects=True
        ) as resp:
            end = time.time()
            if method != "get" and not has_nice_head(resp):
                return await check_url(row, session, method="get")
            resp.raise_for_status()

            check_id = await process_check_data(
                {
                    "resource_id": row["resource_id"],
                    "url": row["url"],
                    "domain": domain,
                    "status": resp.status,
                    "headers": convert_headers(resp.headers),
                    "timeout": False,
                    "response_time": end - start,
                }
            )

            queue.enqueue(process_resource, check_id)

            return STATUS_OK
    except asyncio.exceptions.TimeoutError:
        await process_check_data(
            {
                "resource_id": row["resource_id"],
                "url": row["url"],
                "domain": domain,
                "timeout": True,
            }
        )
        return STATUS_TIMEOUT
    # TODO: debug AssertionError, should be caught in DB now
    # File "[...]aiohttp/connector.py", line 991, in _create_direct_connection
    # assert port is not None
    # UnicodeError: encoding with 'idna' codec failed (UnicodeError: label too long)
    # eg http://%20Localisation%20des%20acc%C3%A8s%20des%20offices%20de%20tourisme
    except (
        aiohttp.client_exceptions.ClientError,
        AssertionError,
        UnicodeError,
    ) as e:
        error = getattr(e, "message", None) or str(e)
        await process_check_data(
            {
                "resource_id": row["resource_id"],
                "url": row["url"],
                "domain": domain,
                "timeout": False,
                "error": fix_surrogates(error),
                "headers": convert_headers(getattr(e, "headers", {})),
                "status": getattr(e, "status", None),
            }
        )
        log.error(f"Crawling error for url {row['url']}", exc_info=e)
        return STATUS_ERROR


async def crawl_urls(to_parse):
    context.monitor().set_status("Crawling urls...")
    tasks = []
    async with aiohttp.ClientSession(timeout=None) as session:
        for row in to_parse:
            tasks.append(check_url(row, session))
        for task in asyncio.as_completed(tasks):
            result = await task
            results[result] += 1
            context.monitor().refresh(results)


def get_excluded_clause():
    return " AND ".join(
        [f"catalog.url NOT LIKE '{p}'" for p in config.EXCLUDED_PATTERNS]
    )


async def crawl_batch():
    """Crawl a batch from the catalog"""
    context.monitor().set_status("Getting a batch from catalog...")
    pool = await context.pool()
    async with pool.acquire() as connection:
        excluded = get_excluded_clause()
        # first urls that are prioritised
        q = f"""
            SELECT * FROM (
                SELECT catalog.url, dataset_id, resource_id
                FROM catalog
                WHERE {excluded}
                AND deleted = False
                AND priority = True
            ) s
            ORDER BY random() LIMIT {config.BATCH_SIZE};
        """
        to_check = await connection.fetch(q)
        # then urls without checks
        if len(to_check) < config.BATCH_SIZE:
            q = f"""
                SELECT * FROM (
                    SELECT catalog.url, dataset_id, resource_id
                    FROM catalog
                    WHERE catalog.last_check IS NULL
                    AND {excluded}
                    AND deleted = False
                    AND priority = False
                ) s
                ORDER BY random() LIMIT {config.BATCH_SIZE};
            """
            to_check += await connection.fetch(q)
        # if not enough for our batch size, handle outdated checks
        if len(to_check) < config.BATCH_SIZE:
            since = parse_timespan(config.SINCE)  # in seconds
            since = datetime.utcnow() - timedelta(seconds=since)
            limit = config.BATCH_SIZE - len(to_check)
            q = f"""
            SELECT * FROM (
                SELECT catalog.url, dataset_id, catalog.resource_id
                FROM catalog, checks
                WHERE catalog.last_check IS NOT NULL
                AND {excluded}
                AND catalog.last_check = checks.id
                AND checks.created_at <= $1
                AND catalog.deleted = False
                AND catalog.priority = False
            ) s
            ORDER BY random() LIMIT {limit};
            """
            to_check += await connection.fetch(q, since)

    if len(to_check):
        await crawl_urls(to_check)
    else:
        context.monitor().set_status("Nothing to crawl for now.")
        await asyncio.sleep(config.SLEEP_BETWEEN_BATCHES)


async def crawl(iterations=-1):
    """Launch crawl batches

    :iterations: for testing purposes (break infinite loop)
    """
    try:
        context.monitor().init(
            SINCE=config.SINCE,
            BATCH_SIZE=config.BATCH_SIZE,
            BACKOFF_NB_REQ=config.BACKOFF_NB_REQ,
            BACKOFF_PERIOD=config.BACKOFF_PERIOD,
        )
        while iterations != 0:
            await crawl_batch()
            iterations -= 1
    finally:
        pool = await context.pool()
        await pool.close()


def run():
    """Main function

    :iterations: for testing purposes (break infinite loop)
    """
    try:
        asyncio.get_event_loop().run_until_complete(crawl())
    except KeyboardInterrupt:
        pass
    finally:
        context.monitor().teardown()


if __name__ == "__main__":
    run()
