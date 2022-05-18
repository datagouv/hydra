import logging

from udata_datalake_service.background_tasks import manage_resource


def process_message(message: dict) -> None:
    resource_id = message["value"]["resource"]["id"]
    logging.info(f"New message detected, checking resource {resource_id}")
    dataset_id = message["meta"]["dataset_id"]
    manage_resource.delay(dataset_id, message["value"]["resource"])
