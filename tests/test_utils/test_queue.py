from unittest.mock import MagicMock

from udata_hydra import config
from udata_hydra.utils.queue import enqueue


def test_enqueue_forwards_priority_exception_and_failure_ttl(mocker) -> None:
    """Check that enqueue passes priority, exception flag, and TTL to the RQ queue.
    Hydra uses custom _priority/_exception kwargs; this test ensures they reach Redis/RQ."""
    rq_queue = MagicMock()
    context_queue = mocker.patch("udata_hydra.utils.queue.context.queue", return_value=rq_queue)

    enqueue(lambda: None, key="val", _priority="low", _exception=True)

    context_queue.assert_called_once_with("low")
    rq_queue.enqueue.assert_called_once_with(
        mocker.ANY,
        key="val",
        failure_ttl=config.RQ_DEFAULT_FAILURE_TTL,
        job_timeout=config.RQ_DEFAULT_TIMEOUT * 5,
    )


def test_enqueue_timeout_does_not_depend_on_queue_cache(mocker) -> None:
    """Queues are cached by name: the exception timeout must apply whatever job came first."""
    rq_queue = MagicMock()
    mocker.patch("udata_hydra.utils.queue.context.queue", return_value=rq_queue)

    enqueue(lambda: None, _priority="default")
    enqueue(lambda: None, _priority="default", _exception=True)

    timeouts = [c.kwargs["job_timeout"] for c in rq_queue.enqueue.call_args_list]
    assert timeouts == [config.RQ_DEFAULT_TIMEOUT, config.RQ_DEFAULT_TIMEOUT * 5]
