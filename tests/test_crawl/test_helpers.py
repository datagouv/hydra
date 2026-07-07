from types import SimpleNamespace

import pytest

from udata_hydra.crawl.helpers import SUSPICIOUS_HTML_HEAD_MAX_BYTES, has_nice_head


def _resp(status, headers):
    return SimpleNamespace(status=status, headers=headers)


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
        pytest.param(429, {"content-length": "10"}, False, id="status_429"),
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
