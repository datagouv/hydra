import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Tuple, Union
from urllib.parse import urlparse

import aiohttp
from humanfriendly import parse_timespan

from udata_hydra import config, context
from udata_hydra.analysis.resource import process_resource
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.logger import setup_logging
from udata_hydra.utils import queue, send

results: defaultdict = defaultdict(int)

RESOURCE_RESPONSE_STATUSES = {
    "OK": "ok",
    "TIMEOUT": "timeout",
    "ERROR": "error",
    "BACKOFF": "backoff",
}

log = setup_logging()


def is_valid_status(status: str) -> Union[bool, None]:
    if not status:
        return False
    status_nb = int(status)
    if status_nb == 429:
        # We can't say the status since it's our client's fault
        return None
    return status_nb >= 200 and status_nb < 400


async def get_content_type_from_header(headers: dict) -> str:
    """
    Parse content-type header to retrieve only the mime type
    """
    content_type = headers.get("content-type")
    if not content_type or ";" not in content_type:
        return content_type
    try:
        content_type, _ = content_type.split(";")
    except ValueError:
        # Weird e.g.: text/html;h5ai=0.20;charset=UTF-8
        content_type, _, _ = content_type.split(";")
    return content_type


async def compute_check_has_changed(check_data: dict, last_check: dict) -> bool:
    is_first_check = not last_check
    status_has_changed = last_check and check_data.get("status") != last_check.get("status")
    status_no_longer_available = (
        last_check
        and is_valid_status(last_check.get("status"))
        and not is_valid_status(check_data.get("status"))
    )
    timeout_has_changed = last_check and check_data.get("timeout") != last_check.get("timeout")
    current_headers = check_data.get("headers", {})
    last_check_headers = (
        json.loads(last_check.get("headers")) if last_check and last_check.get("headers") else {}
    )
    content_has_changed = last_check and (
        current_headers.get("content-length") != last_check_headers.get("content-length")
        or current_headers.get("content-type") != last_check_headers.get("content-type")
    )

    # TODO: Instead of computing criterions here, store payload and compare with previous one.
    # It would make debugging easier.
    criterions = {
        "is_first_check": is_first_check,
        "status_has_changed": status_has_changed,
        "status_no_longer_available": status_no_longer_available,
        "timeout_has_changed": timeout_has_changed,
        "content_has_changed": content_has_changed,
    }

    has_changed = any(criterions.values())
    if has_changed:
        document = {
            "check:available": is_valid_status(check_data.get("status")),
            "check:status": check_data.get("status"),
            "check:timeout": check_data["timeout"],
            "check:date": datetime.now(timezone.utc).isoformat(),
            "check:error": check_data.get("error"),
            "check:headers:content-type": await get_content_type_from_header(
                check_data.get("headers", {})
            ),
            "check:headers:content-length": int(
                check_data.get("headers", {}).get("content-length", 0)
            )
            or None,
        }
        res = await Resource.get(resource_id=check_data["resource_id"], column_name="dataset_id")
        queue.enqueue(
            send,
            dataset_id=res["dataset_id"],
            resource_id=check_data["resource_id"],
            document=document,
            _priority="high",
        )

    return has_changed


async def process_check_data(check_data: dict, priority: bool = False) -> Tuple[int, bool]:
    """Preprocess a check before saving it"""
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

    await Resource.update(
        resource_id=check_data["resource_id"], data={"status": None, "priority": priority}
    )

    is_first_check = last_check is None
    return await Check.insert(check_data), is_first_check


async def is_backoff(domain: str) -> Tuple[bool, str]:
    """Check if we should not crawl on this domain
    Returns a tuple with if it should backoff or not (boolean) and the reason why (string)
    """
    backoff = False, ""
    no_backoff = [f"'{d}'" for d in config.NO_BACKOFF_DOMAINS]
    no_backoff = f"({','.join(no_backoff)})"
    since_backoff_period = datetime.now(timezone.utc) - timedelta(seconds=config.BACKOFF_PERIOD)
    pool = await context.pool()
    async with pool.acquire() as connection:
        # check if we trigger BACKOFF_NB_REQ for BACKOFF_PERIOD on this domain
        res = await connection.fetchrow(
            f"""
            SELECT COUNT(*) FROM checks
            WHERE domain = $1
            AND created_at >= $2
            AND domain NOT IN {no_backoff}
        """,
            domain,
            since_backoff_period,
        )
        backoff = (
            res["count"] >= config.BACKOFF_NB_REQ,
            f"Too many requests: {res['count']}",
        )

        if not backoff[0]:
            # check if we hit a ratelimit or received a 429 on this domain since COOL_OFF_PERIOD
            since_cool_off_period = datetime.now(timezone.utc) - timedelta(
                seconds=config.COOL_OFF_PERIOD
            )
            q = f"""
                SELECT
                    headers->>'x-ratelimit-remaining' as ratelimit_remaining,
                    headers->>'x-ratelimit-limit' as ratelimit_limit,
                    status,
                    created_at
                FROM checks
                WHERE domain = $1 AND domain NOT IN {no_backoff}
                AND created_at >= $2
                ORDER BY created_at DESC
                LIMIT 1
            """
            res = await connection.fetchrow(q, domain, since_cool_off_period)
            if res:
                if res["status"] == 429:
                    # we have made too many requests already and haven't cooled off yet
                    # TODO: we could also user Retry-after, but it isn't returned correctly on 429 we're getting
                    return True, "429 status code has been returned on the latest call"
                try:
                    remain, limit = (
                        float(res["ratelimit_remaining"]),
                        float(res["ratelimit_limit"]),
                    )
                except (ValueError, TypeError):
                    pass
                else:
                    if limit == -1:
                        return False, ""
                    if remain == 0 or limit == 0:
                        # we have really messed up
                        backoff = True, "X-ratelimit reached"
                    elif remain / limit <= 0.1 and res["created_at"] > since_backoff_period:
                        # less than 10% left from our quota, we're backing off until backoff period
                        backoff = True, "X-ratelimit reached"

    return backoff


def fix_surrogates(value):
    """FIX Unicode low surrogate must follow a high surrogate.
    eg in 'TREMI_2017-R\xe9sultats enqu\xeate bruts.csv'
    """
    if not isinstance(value, str):
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


def has_nice_head(resp) -> bool:
    """Check if a HEAD response looks useful to us"""
    if not is_valid_status(resp.status):
        return False
    if not any([k in resp.headers for k in ("content-length", "last-modified")]):
        return False
    return True


async def check_url(
    url: str,
    resource_id: str,
    session,
    sleep: float = 0,
    method: str = "head",
    priority: bool = False,
) -> str:
    log.debug(f"check {url}, sleep {sleep}, method {method}")

    if sleep:
        await asyncio.sleep(sleep)

    url_parsed = urlparse(url)
    domain = url_parsed.netloc
    if not domain:
        log.warning(f"[warning] not netloc in url, skipping {url}")
        await process_check_data(
            check_data={
                "resource_id": resource_id,
                "url": url,
                "error": "Not netloc in url",
                "timeout": False,
            },
            priority=priority,
        )
        return RESOURCE_RESPONSE_STATUSES["ERROR"]

    should_backoff, reason = await is_backoff(domain)
    if should_backoff:
        log.info(f"backoff {domain} ({reason})")
        # skip this URL, it will come back in a next batch
        await Resource.update(resource_id=resource_id, data={"status": None, "priority": False})
        return RESOURCE_RESPONSE_STATUSES["BACKOFF"]

    try:
        start = time.time()
        timeout = aiohttp.ClientTimeout(total=5)
        _method = getattr(session, method)
        async with _method(url, timeout=timeout, allow_redirects=True) as resp:
            end = time.time()
            if method != "get" and not has_nice_head(resp):
                return await check_url(url, resource_id, session, method="get")
            resp.raise_for_status()

            check_id, is_first_check = await process_check_data(
                check_data={
                    "resource_id": resource_id,
                    "url": url,
                    "domain": domain,
                    "status": resp.status,
                    "headers": convert_headers(resp.headers),
                    "timeout": False,
                    "response_time": end - start,
                },
                priority=priority,
            )

            _priority = "low"
            if priority:
                _priority = "high"

            queue.enqueue(process_resource, check_id, is_first_check, _priority=_priority)

            return RESOURCE_RESPONSE_STATUSES["OK"]
    except asyncio.exceptions.TimeoutError:
        await process_check_data(
            check_data={
                "resource_id": resource_id,
                "url": url,
                "domain": domain,
                "timeout": True,
            },
            priority=priority,
        )
        return RESOURCE_RESPONSE_STATUSES["TIMEOUT"]
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
                "resource_id": resource_id,
                "url": url,
                "domain": domain,
                "timeout": False,
                "error": fix_surrogates(error),
                "headers": convert_headers(getattr(e, "headers", {})),
                "status": getattr(e, "status", None),
            },
            priority=priority,
        )
        log.warning(f"Crawling error for url {url}", exc_info=e)
        return RESOURCE_RESPONSE_STATUSES["ERROR"]


async def crawl_urls(to_parse: list[str]) -> None:
    context.monitor().set_status("Crawling urls...")
    tasks: list = []
    async with aiohttp.ClientSession(
        timeout=None, headers={"user-agent": config.USER_AGENT}
    ) as session:
        for row in to_parse:
            tasks.append(check_url(url=row["url"], resource_id=row["resource_id"], session=session))
        for task in asyncio.as_completed(tasks):
            result = await task
            results[result] += 1
            context.monitor().refresh(results)


def get_excluded_clause() -> str:
    return " AND ".join(
        [f"catalog.url NOT LIKE '{p}'" for p in config.EXCLUDED_PATTERNS]
        + [
            "catalog.deleted = False",
            "(catalog.status != 'crawling' OR catalog.status IS NULL)",
        ]
    )


async def select_rows_based_on_query(connection, q, *args):
    """
    A transaction wrapper around a select query q pass as param with *args.
    It first creates a temporary table based on this query,
    then updates the status values from the rows selected,
    fetches the selected rows and drops the temporary table.
    It finally returns the selected (and updated) rows.
    """
    temporary_table = "crawl_urls"
    create_temp_select_table_query = f"""
        CREATE TEMPORARY TABLE {temporary_table} AS
            {q} FOR UPDATE;
    """
    update_select_catalog_query = f"""
        UPDATE catalog SET status = 'crawling' WHERE resource_id in (select resource_id from {temporary_table});
    """
    async with connection.transaction():
        await connection.execute("BEGIN;")
        await connection.execute(create_temp_select_table_query, *args)
        await connection.execute(update_select_catalog_query)
        to_check = await connection.fetch(f"SELECT * FROM {temporary_table};")
        await connection.execute("COMMIT;")
    await connection.execute(f"DROP TABLE {temporary_table};")
    return to_check


async def crawl_batch() -> None:
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
                AND priority = True
            ) s
            ORDER BY random() LIMIT {config.BATCH_SIZE}
        """
        to_check = await select_rows_based_on_query(connection, q)
        # then urls without checks
        if len(to_check) < config.BATCH_SIZE:
            q = f"""
                SELECT * FROM (
                    SELECT catalog.url, dataset_id, resource_id
                    FROM catalog
                    WHERE catalog.last_check IS NULL
                    AND {excluded}
                    AND priority = False
                ) s
                ORDER BY random() LIMIT {config.BATCH_SIZE}
            """
            to_check += await select_rows_based_on_query(connection, q)
        # if not enough for our batch size, handle outdated checks
        if len(to_check) < config.BATCH_SIZE:
            since = parse_timespan(config.SINCE)  # in seconds
            since = datetime.now(timezone.utc) - timedelta(seconds=since)
            limit = config.BATCH_SIZE - len(to_check)
            q = f"""
            SELECT * FROM (
                SELECT catalog.url, dataset_id, catalog.resource_id
                FROM catalog, checks
                WHERE catalog.last_check IS NOT NULL
                AND {excluded}
                AND catalog.last_check = checks.id
                AND checks.created_at <= $1
                AND catalog.priority = False
            ) s
            ORDER BY random() LIMIT {limit}
            """
            to_check += await select_rows_based_on_query(connection, q, since)

    if len(to_check):
        await crawl_urls(to_check)
    else:
        context.monitor().set_status("Nothing to crawl for now.")
    await asyncio.sleep(config.SLEEP_BETWEEN_BATCHES)


async def crawl(iterations: int = -1) -> None:
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


def run() -> None:
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
