import json
import logging
from urllib.parse import urlparse

import aiohttp
from aiohttp import web

from udata_hydra import config
from udata_hydra.utils import IOException

log = logging.getLogger("udata-hydra")

_http_client: aiohttp.ClientSession | None = None


class UdataPayload:
    HYDRA_UDATA_METADATA = {
        "check": ["available", "date", "error", "id", "status", "timeout"],
        "check:headers": ["content-type", "content-length"],
        "analysis": [
            "checksum",
            "content-length",
            "error",
            "check_id",
            "last-modified-at",
            "last-modified-detection",
            "mime-type",
        ],
        "analysis:parsing": [
            "error",
            "started_at",
            "finished_at",
            "parsing_table",
            "parquet_size",
            "parquet_url",
            "pmtiles_size",
            "pmtiles_url",
            "geojson_size",
            "geojson_url",
        ],
    }

    def __init__(self, payload: dict):
        # if we update one element of a udata metadata category, we reset the others to None
        payload_categories = set([":".join(k.split(":")[:-1]) for k in payload.keys()])
        for cat in payload_categories:
            for field in self.HYDRA_UDATA_METADATA[cat]:
                if f"{cat}:{field}" not in payload:
                    payload[f"{cat}:{field}"] = None
        self.payload = payload


def is_valid_uri(uri: str) -> bool:
    try:
        result = urlparse(uri)
        return all([result.scheme, result.netloc])
    except AttributeError:
        return False


def get_request_params(request, params_names: list[str]) -> list:
    """Get GET parameters from request"""
    data = [request.query.get(p) for p in params_names]
    if not any(data):
        raise web.HTTPBadRequest()
    return data


async def send(dataset_id: str, resource_id: str, document: UdataPayload) -> None:
    log.debug(
        f"Sending payload to udata {dataset_id}/{resource_id}: {json.dumps(document.payload, indent=4)}"
    )

    if not config.WEBHOOK_ENABLED:
        log.debug("Webhook disabled, skipping send")
        return

    if not config.UDATA_URI or not config.UDATA_URI_API_KEY:
        log.error("UDATA_* config is not set, not sending callbacks")
        return

    uri = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    headers = {
        "user-agent": config.USER_AGENT_FULL,
        "content-type": "application/json",
        "X-API-KEY": config.UDATA_URI_API_KEY,
    }

    session = await get_http_client()
    async with session.put(uri, json=document.payload, headers=headers) as resp:
        # we're raising since we should be in a worker thread
        if resp.status == 404:
            pass
        elif resp.status == 410:
            raise IOException(
                "Resource has been deleted on udata", resource_id=resource_id, url=uri
            )
        if resp.status == 502:
            raise IOException("Udata is unreachable", resource_id=resource_id, url=uri)
        else:
            resp.raise_for_status()


async def get_http_client(
    follow_redirects: bool = True, timeout: float | None = None
) -> aiohttp.ClientSession:
    """Get a shared aiohttp ClientSession instance for performance optimization.

    Args:
        follow_redirects: Whether to follow redirects
        timeout: Request timeout in seconds

    Returns:
        Shared aiohttp ClientSession instance
    """
    global _http_client

    if _http_client is None or _http_client.closed:
        # Create a new client session
        timeout_obj = aiohttp.ClientTimeout(total=timeout) if timeout else None

        # Prepare headers
        headers = {}
        if config.USER_AGENT_FULL:
            headers["User-Agent"] = config.USER_AGENT_FULL

        _http_client = aiohttp.ClientSession(
            timeout=timeout_obj,
            headers=headers,
        )
        log.debug("Created new aiohttp ClientSession")

    return _http_client


async def close_http_client():
    """Close the shared aiohttp ClientSession instance."""
    global _http_client

    if _http_client and not _http_client.closed:
        await _http_client.close()
        _http_client = None
        log.debug("Closed aiohttp ClientSession")
