import json
import logging
import os
import sys
import time

from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse

import aiohttp
import asyncio

from humanfriendly import parse_timespan
from udata_event_service.producer import produce

from udata_hydra import config, context
from udata_hydra.datalake_service import process_resource
from udata_hydra.utils.kafka import get_topic

log = logging.getLogger("udata-hydra")

results = defaultdict(int)

STATUS_OK = "ok"
STATUS_TIMEOUT = "timeout"
STATUS_ERROR = "error"


async def insert_check(data: dict):
    if "headers" in data:
        data["headers"] = json.dumps(data["headers"])
    columns = ",".join(data.keys())
    # $1, $2...
    placeholders = ",".join([f"${x + 1}" for x in range(len(data.values()))])
    q = f"""
        INSERT INTO checks ({columns})
        VALUES ({placeholders})
        RETURNING id
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        last_check = await connection.fetchrow(q, *data.values())
        q = """UPDATE catalog SET last_check = $1 WHERE url = $2"""
        await connection.execute(q, last_check["id"], data["url"])
    return last_check["id"]


async def update_check_and_catalog(check_data: dict) -> None:
    """Update the catalog and checks tables"""
    context.monitor().set_status("Updating checks and catalog...")
    pool = await context.pool()
    async with pool.acquire() as connection:
        q = f"""
            SELECT * FROM catalog JOIN checks
            ON catalog.last_check = checks.id
            WHERE catalog.url = '{check_data['url']}';
        """
        last_checks = await connection.fetch(q)

        if len(last_checks) == 0:
            # In case we are doing our first check for given URL
            rows = await connection.fetch(
                f"""
                SELECT resource_id, dataset_id, priority, initialization FROM catalog WHERE url = '{check_data['url']}';
            """
            )
            last_checks = [
                {
                    "resource_id": row[0],
                    "dataset_id": row[1],
                    "priority": row[2],
                    "initialization": row[3],
                    "status": None,
                    "timeout": None,
                }
                for row in rows
            ]

        # There could be multiple resources pointing to the same URL
        for last_check in last_checks:
            if config.ENABLE_KAFKA:
                is_first_check = last_check is None
                status_has_changed = (
                    "status" in check_data
                    and check_data["status"] != last_check["status"]
                )
                status_no_longer_available = (
                    "status" not in check_data
                    and last_check["status"] is not None
                )
                timeout_has_changed = (
                    check_data["timeout"] != last_check["timeout"]
                )

                if (
                    is_first_check
                    or status_has_changed
                    or status_no_longer_available
                    or timeout_has_changed
                ):
                    log.debug("Sending message to Kafka...")
                    message_type = (
                        "event-update"
                        if last_check["priority"]
                        else "initialization"
                        if last_check["initialization"]
                        else "regular-update"
                    )
                    meta = {
                        "dataset_id": last_check["dataset_id"],
                        "message_type": message_type,
                        "check_date": str(datetime.now()),
                    }
                    produce(
                        kafka_uri=config.KAFKA_URI,
                        topic=get_topic("resource.checked"),
                        service="udata-hydra",
                        key_id=str(last_check["resource_id"]),
                        document=check_data,
                        meta=meta,
                    )

        log.debug("Updating priority...")
        await connection.execute(
            f"""
            UPDATE catalog SET priority = FALSE, initialization = FALSE WHERE url = '{check_data['url']}';
        """
        )

    await insert_check(check_data)


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


async def check_url(row, session, sleep=0, method="get"):
    log.debug(f"check {row}, sleep {sleep}")

    if sleep:
        await asyncio.sleep(sleep)

    url_parsed = urlparse(row["url"])
    domain = url_parsed.netloc
    if not domain:
        log.warning(f"[warning] not netloc in url, skipping {row['url']}")
        await update_check_and_catalog(
            {
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
            resp.raise_for_status()

            # Download resource, store on Minio if CSV and produce resource.analysed message
            await process_resource(row["url"], row["dataset_id"], str(row["resource_id"]), resp)

            await update_check_and_catalog(
                {
                    "url": row["url"],
                    "domain": domain,
                    "status": resp.status,
                    "headers": convert_headers(resp.headers),
                    "timeout": False,
                    "response_time": end - start,
                }
            )
            return STATUS_OK
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
        await update_check_and_catalog(
            {
                "url": row["url"],
                "domain": domain,
                "timeout": False,
                "error": fix_surrogates(error),
                "headers": convert_headers(getattr(e, "headers", {})),
                "status": getattr(e, "status", None),
            }
        )
        log.error(f"{row['url']}, {e}")
        return STATUS_ERROR
    except asyncio.exceptions.TimeoutError:
        await update_check_and_catalog(
            {
                "url": row["url"],
                "domain": domain,
                "timeout": True,
            }
        )
        return STATUS_TIMEOUT


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
                SELECT DISTINCT(catalog.url), dataset_id, resource_id
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
                    SELECT DISTINCT(catalog.url), dataset_id, resource_id
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
                SELECT DISTINCT(catalog.url), dataset_id, resource_id
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


def setup_logging():
    file_handler = os.getenv("HYDRA_CURSES_ENABLED", False) == "True"
    if file_handler:
        handler = logging.FileHandler("crawl.log")
    else:
        handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)


def run():
    """Main function

    :iterations: for testing purposes (break infinite loop)
    """
    setup_logging()
    try:
        asyncio.get_event_loop().run_until_complete(crawl())
    except KeyboardInterrupt:
        pass
    finally:
        context.monitor().teardown()


if __name__ == "__main__":
    run()
