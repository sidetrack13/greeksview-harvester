"""Unit tests for resilient HTTP client."""

import httpx
import pytest
import respx

from harvester.core.http_client import ResilientHttpClient, resilient_http_client


@pytest.mark.asyncio
async def test_resilient_http_client_basic_get() -> None:
    async with ResilientHttpClient(timeout_seconds=5.0) as client:
        with respx.mock(assert_all_called=True) as respx_mock:
            # 1. Both ETag and Last-Modified
            respx_mock.get("https://example.com/test").respond(
                200, text="hello world", headers={"ETag": '"123"', "Last-Modified": "Wed, 21 Oct 2024 07:28:00 GMT"}
            )
            # 2. Only ETag
            respx_mock.get("https://example.com/etag-single").respond(
                200, text="etag only", headers={"ETag": '"etag-only"'}
            )
            # 3. Only Last-Modified
            respx_mock.get("https://example.com/mod-single").respond(
                200, text="mod only", headers={"Last-Modified": "Wed, 21 Oct 2024 07:28:00 GMT"}
            )
            # 4. Neither header
            respx_mock.get("https://example.com/no-headers").respond(200, text="no cache headers")

            resp = await client.get("https://example.com/test")
            assert resp.status_code == 200
            assert "https://example.com/test" in client.cache_meta

            resp_etag = await client.get("https://example.com/etag-single")
            assert resp_etag.status_code == 200
            assert "etag" in client.cache_meta["https://example.com/etag-single"]
            assert "last_modified" not in client.cache_meta["https://example.com/etag-single"]

            resp_mod = await client.get("https://example.com/mod-single")
            assert resp_mod.status_code == 200
            assert "last_modified" in client.cache_meta["https://example.com/mod-single"]
            assert "etag" not in client.cache_meta["https://example.com/mod-single"]

            resp_none = await client.get("https://example.com/no-headers")
            assert resp_none.status_code == 200
            assert "https://example.com/no-headers" not in client.cache_meta


@pytest.mark.asyncio
async def test_resilient_http_client_conditional_caching_304() -> None:
    async with ResilientHttpClient(timeout_seconds=5.0) as client:
        client.cache_meta["https://example.com/cached"] = {"etag": '"etag123"', "last_modified": "some-date"}

        with respx.mock(assert_all_called=True) as respx_mock:
            route = respx_mock.get("https://example.com/cached").respond(304)
            resp = await client.get("https://example.com/cached", use_cache=True)
            assert resp.status_code == 304
            assert route.calls.last.request.headers.get("If-None-Match") == '"etag123"'
            assert route.calls.last.request.headers.get("If-Modified-Since") == "some-date"


@pytest.mark.asyncio
async def test_resilient_http_client_single_cache_headers() -> None:
    async with ResilientHttpClient(timeout_seconds=5.0) as client:
        # Cache with only etag
        client.cache_meta["https://example.com/etag-only"] = {"etag": '"etag-only"'}
        with respx.mock() as respx_mock:
            respx_mock.get("https://example.com/etag-only").respond(200, text="ok")
            resp = await client.get("https://example.com/etag-only", use_cache=True)
            assert resp.status_code == 200

        # Cache with only last_modified
        client.cache_meta["https://example.com/mod-only"] = {"last_modified": "some-date"}
        with respx.mock() as respx_mock:
            respx_mock.get("https://example.com/mod-only").respond(200, text="ok")
            resp = await client.get("https://example.com/mod-only", use_cache=True)
            assert resp.status_code == 200


@pytest.mark.asyncio
async def test_resilient_http_client_retry_on_429_and_503() -> None:
    client = ResilientHttpClient(timeout_seconds=5.0, max_retries=2, backoff_factor=0.01)
    with respx.mock() as respx_mock:
        respx_mock.get("https://example.com/flaky").mock(
            side_effect=[
                httpx.Response(429, text="Too Many Requests"),
                httpx.Response(200, text="Success after retry"),
            ]
        )
        resp = await client.get("https://example.com/flaky")
        assert resp.status_code == 200
        assert resp.text == "Success after retry"
    await client.close()


@pytest.mark.asyncio
async def test_resilient_http_client_exceed_retries() -> None:
    client = ResilientHttpClient(timeout_seconds=5.0, max_retries=1, backoff_factor=0.01)
    with respx.mock() as respx_mock:
        respx_mock.get("https://example.com/broken").mock(
            side_effect=[
                httpx.Response(500, text="Server Error"),
                httpx.Response(500, text="Server Error"),
            ]
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.get("https://example.com/broken")
    await client.close()


@pytest.mark.asyncio
async def test_resilient_http_client_request_error_retry() -> None:
    client = ResilientHttpClient(timeout_seconds=5.0, max_retries=2, backoff_factor=0.01)
    with respx.mock() as respx_mock:
        respx_mock.get("https://example.com/conn-err").mock(
            side_effect=[
                httpx.ConnectError("Connection refused"),
                httpx.Response(200, text="Connected on retry"),
            ]
        )
        resp = await client.get("https://example.com/conn-err")
        assert resp.status_code == 200
        assert resp.text == "Connected on retry"
    await client.close()


@pytest.mark.asyncio
async def test_resilient_http_client_external_client() -> None:
    async with httpx.AsyncClient() as ext:
        client = ResilientHttpClient(client=ext)
        c = await client.get_client()
        assert c is ext
        await client.close()


@pytest.mark.asyncio
async def test_resilient_http_client_context_manager() -> None:
    async with resilient_http_client(timeout_seconds=2.0) as client:
        assert isinstance(client, ResilientHttpClient)


@pytest.mark.asyncio
async def test_resilient_http_client_post_success() -> None:
    async with ResilientHttpClient(timeout_seconds=5.0) as client:
        with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.post("https://example.com/api/post").respond(200, json={"status": "ok"})
            resp = await client.post(
                "https://example.com/api/post",
                data={"key": "val"},
                json=None,
                extra_headers={"X-Test": "123"},
            )
            assert resp.status_code == 200
            assert resp.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_resilient_http_client_post_retry_on_429() -> None:
    client = ResilientHttpClient(timeout_seconds=5.0, max_retries=2, backoff_factor=0.01)
    with respx.mock() as respx_mock:
        respx_mock.post("https://example.com/api/post-flaky").mock(
            side_effect=[
                httpx.Response(429, text="Rate limit"),
                httpx.Response(200, json={"success": True}),
            ]
        )
        resp = await client.post("https://example.com/api/post-flaky", data={"foo": "bar"})
        assert resp.status_code == 200
        assert resp.json() == {"success": True}
    await client.close()


@pytest.mark.asyncio
async def test_resilient_http_client_post_exceed_retries() -> None:
    client = ResilientHttpClient(timeout_seconds=5.0, max_retries=1, backoff_factor=0.01)
    with respx.mock() as respx_mock:
        respx_mock.post("https://example.com/api/post-broken").mock(
            side_effect=[
                httpx.Response(500, text="Server Error"),
                httpx.Response(500, text="Server Error"),
            ]
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.post("https://example.com/api/post-broken")
    await client.close()


@pytest.mark.asyncio
async def test_resilient_http_client_post_request_error_retry_and_exceed() -> None:
    client = ResilientHttpClient(timeout_seconds=5.0, max_retries=1, backoff_factor=0.01)
    with respx.mock() as respx_mock:
        # Success on retry
        respx_mock.post("https://example.com/api/post-conn").mock(
            side_effect=[
                httpx.ConnectError("Network timeout"),
                httpx.Response(200, text="Recovered"),
            ]
        )
        resp = await client.post("https://example.com/api/post-conn")
        assert resp.status_code == 200
        assert resp.text == "Recovered"

        # Exceed retries
        respx_mock.post("https://example.com/api/post-fatal").mock(
            side_effect=[
                httpx.ConnectError("Down"),
                httpx.ConnectError("Down again"),
            ]
        )
        with pytest.raises(httpx.RequestError):
            await client.post("https://example.com/api/post-fatal")
    await client.close()
