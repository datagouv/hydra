import requests

from udata_hydra import config


def send(dataset_id, resource_id, document):
    uri = f'{config.UDATA_URI}/api/1/{dataset_id}/resources/{resource_id}/extras/'
    headers = {'content-type': 'application/json', 'X-API-KEY': config.UDATA_URI_API_KEY}
    r = requests.put(uri, headers=headers, json=document)
    r.raise_for_status()
