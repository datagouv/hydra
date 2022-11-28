import json
import logging

import aiohttp

from udata_hydra import config

log = logging.getLogger("udata-hydra")


async def send(dataset_id: str, resource_id: str, document: dict) -> None:
    # Extras in udata can't be None
    document = {k: document[k] for k in document if document[k] is not None}
    log.debug(f"Sending payload to udata {dataset_id}/{resource_id}: {json.dumps(document, indent=4)}")

    if not config.WEBHOOK_ENABLED:
        log.debug("Webhook disabled, skipping send")
        return

    if not config.UDATA_URI or not config.UDATA_URI_API_KEY:
        log.error("UDATA_* config is not set, not sending callbacks")
        return

    uri = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    headers = {"content-type": "application/json", "X-API-KEY": config.UDATA_URI_API_KEY}

    async with aiohttp.ClientSession() as session:
        async with session.put(uri, json=document, headers=headers) as resp:
            body = await resp.text()
            if not resp.status == 200:
                log.error(f"udata responded with a {resp.status} and content: {body}")
