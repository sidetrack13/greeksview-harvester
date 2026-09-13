"""Lightweight Prometheus metrics collector and HTTP healthcheck server for the crawler daemon."""

import asyncio
import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Thread-safe and async-safe in-memory metrics registry for crawler events."""

    def __init__(self) -> None:
        self.start_time = time.time()
        self.sync_runs: dict[str, int] = {"house": 0, "senate": 0, "all": 0}
        self.trades_extracted: dict[str, int] = {"house": 0, "senate": 0}
        self.filings_parsed: dict[str, int] = {"house": 0, "senate": 0}
        self.errors_count = 0
        self.last_sync_time: dict[str, float] = {"house": 0.0, "senate": 0.0}

    @property
    def uptime_seconds(self) -> float:
        """Return elapsed uptime in seconds."""
        return round(time.time() - self.start_time, 2)

    def record_sync(
        self,
        chamber: str,
        filings_count: int = 0,
        trades_count: int = 0,
        errors: int = 0,
    ) -> None:
        """Record results from a completed crawl synchronization."""
        ch = chamber.lower()
        if ch in self.sync_runs:
            self.sync_runs[ch] += 1
        else:
            self.sync_runs[ch] = 1

        if ch in self.trades_extracted:
            self.trades_extracted[ch] += trades_count
        else:
            self.trades_extracted[ch] = trades_count

        if ch in self.filings_parsed:
            self.filings_parsed[ch] += filings_count
        else:
            self.filings_parsed[ch] = filings_count

        self.errors_count += errors
        self.last_sync_time[ch] = time.time()

    def generate_prometheus_metrics(self) -> str:
        """Render metrics in standard Prometheus exposition format."""
        lines = [
            "# HELP crawler_uptime_seconds Total crawler daemon uptime in seconds",
            "# TYPE crawler_uptime_seconds gauge",
            f"crawler_uptime_seconds {self.uptime_seconds}",
            "# HELP crawler_sync_runs_total Total crawl sync runs per chamber",
            "# TYPE crawler_sync_runs_total counter",
        ]
        for ch, count in sorted(self.sync_runs.items()):
            lines.append(f'crawler_sync_runs_total{{chamber="{ch}"}} {count}')

        lines.extend(
            [
                "# HELP crawler_trades_extracted_total Total congressional trades parsed into DB",
                "# TYPE crawler_trades_extracted_total counter",
            ]
        )
        for ch, count in sorted(self.trades_extracted.items()):
            lines.append(f'crawler_trades_extracted_total{{chamber="{ch}"}} {count}')

        lines.extend(
            [
                "# HELP crawler_filings_parsed_total Total disclosure filings successfully parsed",
                "# TYPE crawler_filings_parsed_total counter",
            ]
        )
        for ch, count in sorted(self.filings_parsed.items()):
            lines.append(f'crawler_filings_parsed_total{{chamber="{ch}"}} {count}')

        lines.extend(
            [
                "# HELP crawler_errors_total Total errors encountered during crawls",
                "# TYPE crawler_errors_total counter",
                f"crawler_errors_total {self.errors_count}",
                "",
            ]
        )
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize metrics to JSON-compatible dictionary."""
        return {
            "uptime_seconds": self.uptime_seconds,
            "sync_runs": self.sync_runs,
            "trades_extracted": self.trades_extracted,
            "filings_parsed": self.filings_parsed,
            "errors_count": self.errors_count,
            "last_sync_time": self.last_sync_time,
        }


class HealthcheckServer:
    """Minimal async HTTP server handling /healthz, /metrics, and /status."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        collector: MetricsCollector | None = None,
        db_check_cb: Any | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.collector = collector or MetricsCollector()
        self.db_check_cb = db_check_cb
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        """Start async TCP server."""
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        logger.info("Healthcheck and Prometheus metrics server listening on http://%s:%s", self.host, self.port)

    async def stop(self) -> None:
        """Gracefully close async server."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            logger.info("Healthcheck server stopped")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Process incoming HTTP GET request."""
        try:
            line = await reader.readline()
            if not line:
                writer.close()
                await writer.wait_closed()
                return

            request_line = line.decode("utf-8", errors="ignore").strip()
            parts = request_line.split()
            if len(parts) < 2:
                self._send_response(writer, 400, "text/plain", "Bad Request")
                return

            method, path = parts[0], parts[1]
            if method != "GET":
                self._send_response(writer, 405, "text/plain", "Method Not Allowed")
                return

            if path in ("/healthz", "/health"):
                db_healthy = True
                if self.db_check_cb is not None:
                    try:
                        db_healthy = bool(await self.db_check_cb())
                    except Exception:
                        db_healthy = False

                body = json.dumps(
                    {
                        "status": "healthy" if db_healthy else "degraded",
                        "version": "0.1.0",
                        "uptime_seconds": self.collector.uptime_seconds,
                        "db_connected": db_healthy,
                    }
                )
                self._send_response(writer, 200 if db_healthy else 503, "application/json", body)

            elif path in ("/metrics", "/prometheus"):
                metrics_text = self.collector.generate_prometheus_metrics()
                self._send_response(writer, 200, "text/plain; version=0.0.4", metrics_text)

            elif path in ("/status", "/stats"):
                body = json.dumps(
                    {
                        "status": "running",
                        "metrics": self.collector.to_dict(),
                    },
                    indent=2,
                )
                self._send_response(writer, 200, "application/json", body)

            else:
                self._send_response(writer, 404, "text/plain", "Not Found")

        except Exception as exc:
            logger.warning("Error processing HTTP client request: %s", exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _send_response(self, writer: asyncio.StreamWriter, status_code: int, content_type: str, body: str) -> None:
        """Send standard HTTP 1.1 response with headers."""
        reason = {
            200: "OK",
            400: "Bad Request",
            404: "Not Found",
            405: "Method Not Allowed",
            503: "Service Unavailable",
        }.get(status_code, "OK")
        body_bytes = body.encode("utf-8")
        headers = [
            f"HTTP/1.1 {status_code} {reason}",
            f"Content-Type: {content_type}",
            f"Content-Length: {len(body_bytes)}",
            "Connection: close",
            "",
            "",
        ]
        writer.write("\r\n".join(headers).encode("utf-8") + body_bytes)
