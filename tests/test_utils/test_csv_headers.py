import json

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.data_formats import (
    Csv,
    Geojson,
    Gz,
    Xls,
    Xlsx,
)
from udata_hydra.data_formats.detect import (
    detect_data_format_from_check_or_catalog,
    detect_format_from_payload,
)


@pytest.mark.parametrize(
    "headers,url,expected",
    (
        (
            {"content-type": "application/gzip"},
            "https://example.com/data.csv.gz",
            Gz,
        ),
        (
            {"content-type": "application/octet-stream"},
            "https://example.com/data.csv.gz",
            Gz,
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
async def test_detect_tabular_from_headers(clean_db, headers, url, expected):
    check = {"headers": json.dumps(headers), "url": url, "resource_id": RESOURCE_ID}
    assert await detect_data_format_from_check_or_catalog(check) == expected


@pytest.mark.parametrize(
    "resource_format",
    ("json.gz", "xml.gz", "geojson.gz", "csv.gz", "tsv.gz", "gzip"),
)
async def test_gzip_catalog_format_is_detected_as_gz(
    clean_db, insert_fake_resource, resource_format
):
    await insert_fake_resource(format=resource_format)
    check = {
        "headers": json.dumps({"content-type": "application/gzip"}),
        "url": f"https://example.com/data.{resource_format}",
        "resource_id": RESOURCE_ID,
    }
    assert await detect_data_format_from_check_or_catalog(check) == Gz


@pytest.mark.parametrize(
    "payload_mime,url,resource_format,expected",
    (
        ("text/csv", "https://example.com/data.csv.gz", "csv.gz", Csv),
        ("text/plain", "https://example.com/data.csv.gz", None, Csv),
        ("application/json", "https://example.com/data.json.gz", "json.gz", None),
        ("application/json", "https://example.com/data.geojson.gz", "geojson.gz", Geojson),
        ("application/xml", "https://example.com/data.xml.gz", "xml.gz", None),
    ),
)
def test_detect_format_from_payload(payload_mime, url, resource_format, expected):
    check = {"url": url, "resource_id": RESOURCE_ID, "headers": "{}"}
    assert detect_format_from_payload(check, payload_mime, resource_format) == expected
