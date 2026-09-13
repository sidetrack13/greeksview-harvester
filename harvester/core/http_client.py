"""Resilient asynchronous HTTP client with conditional caching, retries, and rate-limiting."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class ResilientHttpClient:
    """Async HTTP client with automatic retries, backoff, and caching headers."""

    DEFAULT_USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )

    def __init__(
        self,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        max_concurrency: int = 5,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._external_client = client
        self._internal_client: httpx.AsyncClient | None = None
        # In-memory conditional caching metadata: url -> {"etag": ..., "last_modified": ...}
        self.cache_meta: dict[str, dict[str, str]] = {}

    async def get_client(self) -> httpx.AsyncClient:
        """Return the active HTTPX async client instance."""
        if self._external_client is not None:
            return self._external_client
        if self._internal_client is None or self._internal_client.is_closed:
            self._internal_client = httpx.AsyncClient(
                http2=True,
                timeout=httpx.Timeout(self.timeout_seconds),
                headers={"User-Agent": self.DEFAULT_USER_AGENT},
                follow_redirects=True,
            )
        return self._internal_client

    async def __aenter__(self) -> "ResilientHttpClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the internal HTTP client if initialized."""
        if self._internal_client is not None and not self._internal_client.is_closed:
            await self._internal_client.aclose()
            self._internal_client = None

    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        use_cache: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Perform an async GET request with retries, backoff, and conditional caching."""
        headers = dict(extra_headers or {})
        if use_cache and url in self.cache_meta:
            meta = self.cache_meta[url]
            if "etag" in meta:
                headers["If-None-Match"] = meta["etag"]
            if "last_modified" in meta:
                headers["If-Modified-Since"] = meta["last_modified"]

        async with self.semaphore:
            client = await self.get_client()
            attempt = 0
            while True:
                attempt += 1
                try:
                    response = await client.get(url, params=params, headers=headers)
                    # 304 Not Modified: caller should handle caching
                    if response.status_code == 304:
                        return response

                    if response.status_code in (429, 500, 502, 503, 504) and attempt <= self.max_retries:
                        delay = self.backoff_factor * (2 ** (attempt - 1))
                        logger.warning(
                            "HTTP %s for %s. Retrying attempt %s/%s in %.2fs",
                            response.status_code,
                            url,
                            attempt,
                            self.max_retries,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    response.raise_for_status()

                    # Record caching metadata
                    etag = response.headers.get("ETag")
                    last_mod = response.headers.get("Last-Modified")
                    if etag or last_mod:
                        self.cache_meta[url] = {}
                        if etag:
                            self.cache_meta[url]["etag"] = etag
                        if last_mod:
                            self.cache_meta[url]["last_modified"] = last_mod

                    return response
                except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                    if attempt > self.max_retries:
                        logger.error("HTTP request failed after %s attempts: %s (%s)", attempt, url, exc)
                        raise
                    delay = self.backoff_factor * (2 ** (attempt - 1))
                    logger.warning("Error fetching %s: %s. Retrying in %.2fs", url, exc, delay)
                    await asyncio.sleep(delay)

    async def post(
        self,
        url: str,
        data: dict[str, Any] | None = None,
        json: Any | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Perform an async POST request with retries and backoff."""
        headers = dict(extra_headers or {})
        async with self.semaphore:
            client = await self.get_client()
            attempt = 0
            while True:
                attempt += 1
                try:
                    response = await client.post(url, data=data, json=json, headers=headers)
                    if response.status_code in (429, 500, 502, 503, 504) and attempt <= self.max_retries:
                        delay = self.backoff_factor * (2 ** (attempt - 1))
                        logger.warning(
                            "HTTP %s for POST %s. Retrying attempt %s/%s in %.2fs",
                            response.status_code,
                            url,
                            attempt,
                            self.max_retries,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    response.raise_for_status()
                    return response
                except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                    if attempt > self.max_retries:
                        logger.error("HTTP POST failed after %s attempts: %s (%s)", attempt, url, exc)
                        raise
                    delay = self.backoff_factor * (2 ** (attempt - 1))
                    logger.warning("Error POSTing %s: %s. Retrying in %.2fs", url, exc, delay)
                    await asyncio.sleep(delay)


@asynccontextmanager
async def resilient_http_client(**kwargs: Any) -> AsyncIterator[ResilientHttpClient]:
    """Context manager for ResilientHttpClient lifecycle management."""
    client = ResilientHttpClient(**kwargs)
    try:
        yield client
    finally:
        await client.close()
