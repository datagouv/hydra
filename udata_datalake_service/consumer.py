import logging

from udata_datalake_service.background_tasks import manage_resource


def process_message(key: str, message: dict) -> None:
    logging.info(f"New message detected, checking resource {key}")
    dataset_id = message["meta"]["dataset_id"]
    manage_resource.delay(dataset_id, message["value"]["resource"])
