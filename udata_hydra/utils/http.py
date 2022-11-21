import aiohttp
import logging

from udata_hydra import config

log = logging.getLogger("udata-hydra")


async def send(dataset_id: str, resource_id: str, document: dict) -> None:
    if not config.UDATA_URI or not config.UDATA_URI_API_KEY:
        log.error("Missing udata URI and API key to send http query")
        return
    uri = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    headers = {"content-type": "application/json", "X-API-KEY": config.UDATA_URI_API_KEY}

    # Extras in udata can't be None
    document = {key: document[key] for key in document if document[key] is not None}

    async with aiohttp.ClientSession() as session:
        async with session.put(uri, json=document, headers=headers) as resp:
            body = await resp.text()
            if not resp.status == 200:
                log.error(f"udata reponded with a {resp.status} and content: {body}")
