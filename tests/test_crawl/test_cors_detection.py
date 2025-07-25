from udata_hydra.crawl.helpers import has_cors_headers


def test_has_cors_headers():
    """Test CORS header detection from response headers"""

    # Test with CORS headers
    headers_with_cors = {
        "content-type": "application/json",
        "content-length": "1024",
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET, POST, OPTIONS",
        "access-control-allow-headers": "Content-Type",
    }

    has_cors = has_cors_headers(headers_with_cors)
    assert has_cors is True

    # Test with no CORS headers
    headers_without_cors = {
        "content-type": "application/json",
        "content-length": "1024",
    }

    has_cors = has_cors_headers(headers_without_cors)
    assert has_cors is False

    # Test with empty headers
    has_cors = has_cors_headers({})
    assert has_cors is False

    # Test with just one CORS header
    headers_single_cors = {
        "content-type": "application/json",
        "access-control-allow-origin": "*",
    }

    has_cors = has_cors_headers(headers_single_cors)
    assert has_cors is True
