from udata_hydra import config, context
from udata_hydra.logger import setup_logging

log = setup_logging()


def enqueue(fn, *args, **kwargs):
    """
    Enqueue a task
    Simple helper useful to facilitate mock in testing and pass common params
    Should be used like this (import matters):
        from udata_hydra.utils import queue
        queue.enqueue(fn, a, b=b, _priority="low")
    """
    priority = kwargs.pop("_priority", "default")
    exception = kwargs.pop("_exception", False)
    failure_ttl = config.RQ_DEFAULT_FAILURE_TTL
    # set on each job, not on the queue: queues are cached by name and shared by all jobs
    job_timeout = config.RQ_DEFAULT_TIMEOUT * 5 if exception else config.RQ_DEFAULT_TIMEOUT
    return context.queue(priority).enqueue(  # type: ignore[union-attr]
        fn, *args, **kwargs, failure_ttl=failure_ttl, job_timeout=job_timeout
    )
