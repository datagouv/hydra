import json
import logging
from urllib.parse import urlparse

import aiohttp
from aiohttp import web

from udata_hydra import config
from udata_hydra.utils import IOException

log = logging.getLogger("udata-hydra")


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
        "content-type": "application/json",
        "X-API-KEY": config.UDATA_URI_API_KEY,
    }

    async with aiohttp.ClientSession() as session:
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
