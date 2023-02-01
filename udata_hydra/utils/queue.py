from udata_hydra import context
from udata_hydra.logger import setup_logging

log = setup_logging()


def enqueue(fn, *args, **kwargs):
    """
    Enqueue a task
    Simple helper useful to facilitate mock in testing
    Should be used like this (import matters):
        from udata_hydra.utils import queue
        queue.enqueue(fn, a, b=b, _priority="low")
    """
    priority = kwargs.pop("_priority", "default")
    return context.queue(priority).enqueue(fn, *args, **kwargs)
