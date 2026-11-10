"""Minimal replacement for aioresponses, covering only what our tests use.

It patches `ClientSession._request` to return duck-typed responses instead of building real
`aiohttp.ClientResponse` objects, so that it doesn't depend on aiohttp internals.
"""

import io
import json
from collections import namedtuple
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any
from unittest.mock import patch

from aiohttp import ClientConnectionError, ClientResponseError, ClientSession, RequestInfo
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

RequestCall = namedtuple("RequestCall", ["kwargs"])


@dataclass
class CallbackResult:
    status: int = 200
    body: str | bytes = ""
    payload: Any = None
    headers: dict | None = None


class MockedContent:
    def __init__(self, body: bytes):
        self._buffer = io.BytesIO(body)

    async def read(self, n: int = -1) -> bytes:
        return self._buffer.read(n)

    async def iter_chunked(self, n: int):
        while chunk := self._buffer.read(n):
            yield chunk


class MockedResponse:
    def __init__(self, method: str, url: URL, result: CallbackResult):
        body = json.dumps(result.payload) if result.payload is not None else result.body
        self._body = body.encode() if isinstance(body, str) else body
        self.status = result.status
        self.reason = HTTPStatus(result.status).phrase
        headers = CIMultiDict({"Content-Type": "application/json"})
        headers.update(result.headers or {})
        self.headers = CIMultiDictProxy(headers)
        self.content = MockedContent(self._body)
        self.request_info = RequestInfo(url, method, CIMultiDictProxy(CIMultiDict()), url)

    async def json(self) -> Any:
        return json.loads(self._body)

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise ClientResponseError(
                self.request_info, (), status=self.status, message=self.reason
            )

    async def __aenter__(self) -> "MockedResponse":
        return self

    async def __aexit__(self, *args) -> None:
        pass


@dataclass
class Matcher:
    method: str
    url: URL
    exception: BaseException | type[BaseException] | None
    repeat: bool
    callback: Any
    result: CallbackResult


class aiohttp_mock:
    """Context manager mocking aiohttp requests, with an aioresponses-like API"""

    def __init__(self, passthrough: list[str] | None = None):
        self.passthrough = passthrough or []
        self.matchers: list[Matcher] = []
        self.requests: dict[tuple[str, URL], list[RequestCall]] = {}
        self._original_request = ClientSession._request

        async def _request(session, method, str_or_url, **kwargs):
            return await self._request(session, method, str_or_url, **kwargs)

        self._patcher = patch.object(ClientSession, "_request", _request)

    def __enter__(self) -> "aiohttp_mock":
        self._patcher.start()
        return self

    def __exit__(self, *args) -> None:
        self._patcher.stop()

    def add(
        self,
        url: str,
        method: str,
        *,
        exception: BaseException | type[BaseException] | None = None,
        repeat: bool = False,
        callback: Any = None,
        **kwargs,
    ) -> None:
        self.matchers.append(
            Matcher(method, URL(url), exception, repeat, callback, CallbackResult(**kwargs))
        )

    def get(self, url: str, **kwargs) -> None:
        self.add(url, "GET", **kwargs)

    def head(self, url: str, **kwargs) -> None:
        self.add(url, "HEAD", **kwargs)

    def options(self, url: str, **kwargs) -> None:
        self.add(url, "OPTIONS", **kwargs)

    def put(self, url: str, **kwargs) -> None:
        self.add(url, "PUT", **kwargs)

    async def _request(self, session, method, str_or_url, **kwargs):
        if any(str(str_or_url).startswith(prefix) for prefix in self.passthrough):
            return await self._original_request(session, method, str_or_url, **kwargs)

        url = URL(str_or_url)
        self.requests.setdefault((method, url), []).append(RequestCall(kwargs))

        matcher = next((m for m in self.matchers if (m.method, m.url) == (method, url)), None)
        if matcher is None:
            raise ClientConnectionError(f"Connection refused: {method} {url}")
        if not matcher.repeat:
            self.matchers.remove(matcher)
        if matcher.exception is not None:
            raise matcher.exception

        result = matcher.callback(url, **kwargs) if matcher.callback else matcher.result
        response = MockedResponse(method, url, result)
        if session._raise_for_status:
            response.raise_for_status()
        return response
