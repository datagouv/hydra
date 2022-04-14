import pytest
from udata_datalake_service.background_tasks import celery


@pytest.fixture(autouse=True)
def celery_config():
    celery.conf.update(task_always_eager=True)
