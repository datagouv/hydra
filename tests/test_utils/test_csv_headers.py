import json

import pytest

from udata_hydra.utils.csv import detect_tabular_from_headers


@pytest.mark.parametrize(
    "headers,url,expected",
    (
        (
            {"content-type": "application/gzip"},
            "https://example.com/data.csv.gz",
            (True, "csvgz"),
        ),
        (
            {"content-type": "application/vnd.ms-excel"},
            "https://example.com/data.xls",
            (True, "xls"),
        ),
        (
            {"content-type": ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            "https://example.com/data.xlsx",
            (True, "xlsx"),
        ),
    ),
)
def test_detect_tabular_from_headers(headers, url, expected):
    check = {"headers": json.dumps(headers), "url": url}
    assert detect_tabular_from_headers(check) == expected
