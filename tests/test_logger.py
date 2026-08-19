import logging

from udata_hydra.logger import quiet_logs


def test_quiet_logs_disables_below_error_for_third_party_loggers():
    urllib_logger = logging.getLogger("urllib3")
    botocore_logger = logging.getLogger("botocore")

    with quiet_logs():
        assert not urllib_logger.isEnabledFor(logging.INFO)
        assert not botocore_logger.isEnabledFor(logging.INFO)
        assert not urllib_logger.isEnabledFor(logging.WARNING)
        assert urllib_logger.isEnabledFor(logging.ERROR)
        assert botocore_logger.isEnabledFor(logging.CRITICAL)

    assert urllib_logger.isEnabledFor(logging.INFO)
    assert botocore_logger.isEnabledFor(logging.INFO)


def test_quiet_logs_restores_global_disable_after_context():
    urllib_logger = logging.getLogger("urllib3")

    with quiet_logs():
        urllib_logger.info("would be suppressed at runtime")

    assert urllib_logger.isEnabledFor(logging.INFO)


def test_quiet_logs_disabled_is_noop(caplog):
    caplog.set_level(logging.DEBUG)
    logging.getLogger("urllib3").info("visible when disabled")

    with quiet_logs(enabled=False):
        logging.getLogger("botocore").info("also visible")

    assert "visible when disabled" in caplog.text
    assert "also visible" in caplog.text
