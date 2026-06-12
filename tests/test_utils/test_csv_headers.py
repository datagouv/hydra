import json

import pytest

from udata_hydra.data_formats import (
    Csvgz,
    Xls,
    Xlsx,
    detect_data_format_from_check_or_catalog,
)

from tests.conftest import RESOURCE_ID


@pytest.mark.parametrize(
    "headers,url,expected",
    (
        (
            {"content-type": "application/gzip"},
            "https://example.com/data.csv.gz",
            Csvgz,
        ),
        (
            {"content-type": "application/vnd.ms-excel"},
            "https://example.com/data.xls",
            Xls,
        ),
        (
            {"content-type": ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            "https://example.com/data.xlsx",
            Xlsx,
        ),
    ),
)
async def test_detect_tabular_from_headers(headers, url, expected):
    check = {"headers": json.dumps(headers), "url": url, "resource_id": RESOURCE_ID}
    assert await detect_data_format_from_check_or_catalog(check) == expected
