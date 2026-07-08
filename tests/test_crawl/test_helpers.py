from types import SimpleNamespace

import pytest
from multidict import CIMultiDict

from udata_hydra.crawl.helpers import (
    SUSPICIOUS_HTML_HEAD_MAX_BYTES,
    convert_headers,
    fix_surrogates,
    get_content_type_from_header,
    has_nice_head,
    is_valid_status,
)


def _resp(status, headers):
    return SimpleNamespace(status=status, headers=headers)


_INVALID_UTF8_FILENAME = b"file-\xff.csv".decode("utf-8", "surrogateescape")


@pytest.mark.parametrize(
    "status,headers,expected",
    [
        pytest.param(200, {"content-length": "10"}, True, id="valid_content_length"),
        pytest.param(
            200,
            {"last-modified": "Thu, 09 Jan 2020 09:33:37 GMT"},
            True,
            id="valid_last_modified_only",
        ),
        pytest.param(
            200,
            {
                "content-type": "application/octet-stream",
                "content-length": "38848259",
            },
            True,
            id="valid_binary_large",
        ),
        pytest.param(200, {}, False, id="missing_size_headers"),
        pytest.param(501, {"content-length": "10"}, False, id="invalid_status"),
        pytest.param(
            200,
            {"content-type": "text/html", "content-length": "247"},
            False,
            id="waf_html_small",
        ),
        pytest.param(
            200,
            {
                "content-type": "text/html",
                "content-length": str(SUSPICIOUS_HTML_HEAD_MAX_BYTES),
            },
            True,
            id="html_at_threshold",
        ),
        pytest.param(
            200,
            {"content-type": "text/html", "content-length": "not-a-number"},
            False,
            id="html_invalid_length",
        ),
    ],
)
def test_has_nice_head(status, headers, expected):
    assert has_nice_head(_resp(status, headers)) is expected


@pytest.mark.parametrize(
    "status,expected",
    [
        pytest.param(None, False, id="none"),
        pytest.param("", False, id="empty"),
        pytest.param(200, True, id="ok_200"),
        pytest.param(399, True, id="ok_399"),
        pytest.param(400, False, id="client_400"),
        pytest.param(500, False, id="server_500"),
        pytest.param(429, None, id="rate_limit_429"),
        pytest.param("200", True, id="string_status"),
    ],
)
def test_is_valid_status(status, expected):
    assert is_valid_status(status) is expected


@pytest.mark.parametrize(
    "headers,expected",
    [
        pytest.param({"content-type": "application/json"}, "application/json", id="plain"),
        pytest.param(
            {"content-type": "text/html; charset=utf-8"},
            "text/html",
            id="with_charset",
        ),
        pytest.param(
            {"content-type": "text/html;h5ai=0.20;charset=UTF-8"},
            "text/html",
            id="weird_h5ai",
        ),
        pytest.param({}, "", id="no_header"),
        pytest.param({"content-type": "text/csv"}, "text/csv", id="no_semicolon"),
    ],
)
async def test_get_content_type_from_header(headers, expected):
    assert await get_content_type_from_header(headers) == expected


@pytest.mark.parametrize(
    "headers,expected",
    [
        pytest.param({}, {}, id="empty_dict"),
        pytest.param(None, {}, id="none"),
        pytest.param(
            {"Content-LENGTH": "10", "X-Do": "you"},
            {"content-length": "10", "x-do": "you"},
            id="lowercase_keys",
        ),
        pytest.param(
            CIMultiDict([("Content-Type", "a"), ("Content-Type", "b")]),
            {"content-type": "a"},
            id="cimultidict_first_value",
        ),
        pytest.param(
            {"X-Filename": _INVALID_UTF8_FILENAME},
            {"x-filename": "file-\ufffd.csv"},
            id="surrogate_in_value",
        ),
    ],
)
def test_convert_headers(headers, expected):
    assert convert_headers(headers) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        pytest.param("client error", "client error", id="plain_ascii"),
        pytest.param(42, "42", id="non_str"),
        pytest.param(_INVALID_UTF8_FILENAME, "file-\ufffd.csv", id="surrogate"),
    ],
)
def test_fix_surrogates(value, expected):
    assert fix_surrogates(value) == expected
