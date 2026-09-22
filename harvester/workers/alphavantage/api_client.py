"""
GreeksView Harvester — Resilient Alpha Vantage HTTP Client
==========================================================
Python 3.12 port of GreeksView's production worker/alphavantage-api.js.

Key Responsibilities:
  1. Credential Redaction: Scrub apikey query parameters from every log, exception, and URI.
  2. Status Envelope Inspection: Alpha Vantage returns HTTP 200 for almost all errors (throttles,
     burst notices, missing data, invalid symbols). detect_failure() extracts real semantic statuses.
  3. Pacing & Circuit Breaking: All requests flow through AlphaVantagePacer (30 req/sec ceiling),
     automatically pausing 2000ms upon burst notice detection.
  4. Dual Formats: Handles JSON endpoints (quotes, options, fundamentals) and CSV endpoints
     (LISTING_STATUS, EARNINGS_CALENDAR).
"""

import csv
import io
import logging
import re
from typing import Any

import httpx

from harvester.workers.alphavantage.pacer import AlphaVantagePacer, is_burst_notice

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://www.alphavantage.co/query"
DEFAULT_TIMEOUT_SECONDS = 90.0
MAX_THROTTLE_RETRIES = 3
MAX_TRANSIENT_RETRIES = 2

API_KEY_REGEX = re.compile(r"apikey=[^&\s]+", re.IGNORECASE)


def scrub(text: str, api_key: str = "") -> str:
    """Scrub raw API key from URLs, logs, and error strings."""
    if not isinstance(text, str):
        return ""
    clean = text
    if api_key and len(api_key) >= 4:
        clean = clean.replace(api_key, "[REDACTED]")
    return API_KEY_REGEX.sub("apikey=[REDACTED]", clean)


def detect_failure(data: Any, function_name: str = "") -> dict[str, Any] | None:
    """Detect upstream failure envelopes returned inside HTTP 200 responses."""
    if data is None or not isinstance(data, dict):
        return {"status": 502, "label": "unparseable", "detail": "upstream did not return a JSON object"}

    if "Note" in data:
        note = str(data["Note"])
        if is_burst_notice(note):
            return {"status": 429, "label": "burst", "detail": note}
        return {"status": 429, "label": "throttle", "detail": note}

    if "Information" in data:
        info = str(data["Information"])
        return {"status": 429, "label": "throttle", "detail": info}

    if "Error Message" in data:
        msg = str(data["Error Message"])
        return {"status": 404, "label": "invalid_symbol", "detail": msg}

    if not data:
        # OVERVIEW replies with {} for non-companies (ETFs, unknown tickers)
        if function_name.upper() == "OVERVIEW":
            return {"status": 404, "label": "empty", "detail": "empty object for overview"}
        return {"status": 502, "label": "empty", "detail": "empty response object"}

    return None


class AlphaVantageError(Exception):
    """Structured error representing an Alpha Vantage failure."""

    def __init__(self, message: str, status: int = 500, label: str = "error", detail: str = ""):
        super().__init__(message)
        self.status = status
        self.label = label
        self.detail = detail


class AlphaVantageClient:
    """Resilient asynchronous Alpha Vantage API client with pacing and secret scrubbing."""

    def __init__(
        self,
        api_key: str,
        pacer: AlphaVantagePacer | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.pacer = pacer or AlphaVantagePacer()
        self._external_client = client
        self._internal_client: httpx.AsyncClient | None = None

    async def get_client(self) -> httpx.AsyncClient:
        """Return active httpx client."""
        if self._external_client is not None:
            return self._external_client
        if self._internal_client is None or self._internal_client.is_closed:
            self._internal_client = httpx.AsyncClient(
                http2=True,
                timeout=httpx.Timeout(self.timeout_seconds),
                headers={"User-Agent": "GreeksView-Harvester/0.2.0"},
                follow_redirects=True,
            )
        return self._internal_client

    async def close(self) -> None:
        """Close internal HTTP client if initialized."""
        if self._internal_client is not None and not self._internal_client.is_closed:
            await self._internal_client.aclose()
            self._internal_client = None

    async def fetch_json(
        self,
        function: str,
        params: dict[str, Any] | None = None,
        is_background: bool = True,
    ) -> dict[str, Any]:
        """Fetch and validate a JSON endpoint from Alpha Vantage.

        Args:
            function: Alpha Vantage API function (e.g. TIME_SERIES_DAILY_ADJUSTED, OVERVIEW).
            params: Query parameters (symbol, outputsize, datatype, etc.).
            is_background: True if background batch job, False if interactive reader query.

        Returns:
            Parsed JSON dictionary payload.

        Raises:
            AlphaVantageError: If request fails after allowed retries or returns unrecoverable error.
        """
        req_params = dict(params or {})
        req_params["function"] = function
        req_params["apikey"] = self.api_key

        throttles = 0
        transients = 0

        while True:
            # 1. Acquire pacing slot
            granted, reason, retry_after_ms = await self.pacer.acquire(is_background=is_background)
            if not granted:
                raise AlphaVantageError(
                    f"Alpha Vantage pacer refused slot ({reason}). Retry after {retry_after_ms:.0f}ms",
                    status=429,
                    label="rate_limited",
                )

            client = await self.get_client()
            try:
                response = await client.get(self.base_url, params=req_params)
            except (httpx.RequestError, TimeoutError) as exc:
                if transients < MAX_TRANSIENT_RETRIES:
                    transients += 1
                    logger.warning(
                        "Network error querying Alpha Vantage %s. Retrying (%s/%s)...",
                        function,
                        transients,
                        MAX_TRANSIENT_RETRIES,
                    )
                    continue
                clean_err = scrub(str(exc), self.api_key)
                raise AlphaVantageError(
                    f"Alpha Vantage request failed: {clean_err}", status=502, label="network_error"
                ) from exc

            # 2. Check HTTP status code
            if response.status_code == 429:
                if throttles < MAX_THROTTLE_RETRIES:
                    throttles += 1
                    self.pacer.burst_refused(fn_name=function)
                    continue
                raise AlphaVantageError("Alpha Vantage HTTP 429 Too Many Requests", status=429, label="throttle")

            if response.status_code >= 400:
                raise AlphaVantageError(
                    f"Alpha Vantage HTTP {response.status_code}",
                    status=response.status_code,
                    label="http_error",
                )

            # 3. Parse JSON body
            try:
                data = response.json()
            except Exception as exc:
                if transients < MAX_TRANSIENT_RETRIES:
                    transients += 1
                    continue
                body_peek = scrub(response.text[:120], self.api_key)
                raise AlphaVantageError(
                    f"Alpha Vantage unparseable JSON response: {body_peek}",
                    status=502,
                    label="unparseable",
                ) from exc

            # 4. Inspect for upstream error envelopes
            failure = detect_failure(data, function_name=function)
            if failure:
                status = failure["status"]
                label = failure["label"]
                detail = failure["detail"]

                if label in ("burst", "throttle") and throttles < MAX_THROTTLE_RETRIES:
                    throttles += 1
                    self.pacer.burst_refused(fn_name=function)
                    continue

                if label == "empty" and function.upper() == "OVERVIEW":
                    # Known legitimate response for non-company symbols
                    return {}

                if label in ("unparseable", "empty") and transients < MAX_TRANSIENT_RETRIES:
                    transients += 1
                    continue

                clean_detail = scrub(detail, self.api_key)
                raise AlphaVantageError(
                    f"Alpha Vantage {label}: {clean_detail}",
                    status=status,
                    label=label,
                    detail=clean_detail,
                )

            return data

    async def fetch_csv(
        self,
        function: str,
        params: dict[str, Any] | None = None,
        is_background: bool = True,
    ) -> list[dict[str, str]]:
        """Fetch and parse a CSV endpoint (e.g. LISTING_STATUS, EARNINGS_CALENDAR)."""
        req_params = dict(params or {})
        req_params["function"] = function
        req_params["apikey"] = self.api_key

        throttles = 0
        transients = 0

        while True:
            granted, reason, retry_after_ms = await self.pacer.acquire(is_background=is_background)
            if not granted:
                raise AlphaVantageError(
                    f"Alpha Vantage pacer refused slot ({reason}). Retry after {retry_after_ms:.0f}ms",
                    status=429,
                    label="rate_limited",
                )

            client = await self.get_client()
            try:
                response = await client.get(self.base_url, params=req_params)
            except (httpx.RequestError, TimeoutError) as exc:
                if transients < MAX_TRANSIENT_RETRIES:
                    transients += 1
                    continue
                clean_err = scrub(str(exc), self.api_key)
                raise AlphaVantageError(
                    f"Alpha Vantage CSV request failed: {clean_err}", status=502, label="network_error"
                ) from exc

            if response.status_code == 429:
                if throttles < MAX_THROTTLE_RETRIES:
                    throttles += 1
                    self.pacer.burst_refused(fn_name=function)
                    continue
                raise AlphaVantageError("Alpha Vantage HTTP 429 Too Many Requests", status=429, label="throttle")

            text = response.text
            if is_burst_notice(text) or "standard API call frequency" in text:
                if throttles < MAX_THROTTLE_RETRIES:
                    throttles += 1
                    self.pacer.burst_refused(fn_name=function)
                    continue
                raise AlphaVantageError("Alpha Vantage throttled CSV request", status=429, label="throttle")

            try:
                reader = csv.DictReader(io.StringIO(text))
                return [row for row in reader if row]
            except Exception as exc:
                if transients < MAX_TRANSIENT_RETRIES:
                    transients += 1
                    continue
                raise AlphaVantageError(
                    f"Alpha Vantage CSV parsing error: {exc}", status=502, label="unparseable"
                ) from exc
