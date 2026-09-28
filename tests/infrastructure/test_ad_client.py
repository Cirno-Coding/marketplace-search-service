import httpx

from src.application.tracing import reset_trace_id, set_trace_id
from src.infrastructure.http.ad_client import AdServiceAdSource
from tests.conftest import make_snapshot

AD_JSON = {
    "id": 5,
    "title": "MacBook Pro",
    "description": "Отличный ноутбук",
    "price": 180000,
    "category": "Электроника",
    "city": "Москва",
    "status": "active",
}


def make_source(
    requests: list[httpx.Request], status_code: int = 200
) -> AdServiceAdSource:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code, json=AD_JSON)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return AdServiceAdSource(client, "http://ad")


async def test_sends_current_trace_id() -> None:
    requests: list[httpx.Request] = []
    source = make_source(requests)

    token = set_trace_id("trace-ad-1")
    try:
        snapshot = await source.get(5)
    finally:
        reset_trace_id(token)

    assert snapshot == make_snapshot(ad_id=5)
    assert requests[0].url == "http://ad/internal/ads/5"
    assert requests[0].headers["X-Trace-Id"] == "trace-ad-1"


async def test_omits_header_without_trace_id() -> None:
    requests: list[httpx.Request] = []
    source = make_source(requests)

    await source.get(5)

    assert "X-Trace-Id" not in requests[0].headers


async def test_returns_none_on_not_found() -> None:
    requests: list[httpx.Request] = []
    source = make_source(requests, status_code=404)

    assert await source.get(5) is None


async def test_returns_none_on_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("ad is down", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    source = AdServiceAdSource(client, "http://ad")

    assert await source.get(5) is None
