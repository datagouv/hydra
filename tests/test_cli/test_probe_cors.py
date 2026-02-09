import logging

import nest_asyncio2 as nest_asyncio
import pytest

from udata_hydra.cli import probe_cors_cli

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def test_probe_cors_cli_logs_result(monkeypatch, caplog):
    url = "https://example.com/data"
    expected = {"status": 204, "allow-origin": "*"}

    async def fake_probe(session, target):
        assert target == url
        return expected

    monkeypatch.setattr("udata_hydra.cli.probe_cors", fake_probe)
    caplog.set_level(logging.INFO)

    await probe_cors_cli(url=url, resource_id=None)

    assert f"CORS probe result: {expected}" in caplog.text
