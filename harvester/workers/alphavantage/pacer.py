"""
GreeksView Harvester — Alpha Vantage Token-Bucket Pacer
======================================================
Python 3.12 port of GreeksView's production lib/av-pacer.js.

Alpha Vantage enforces dual rate limits on ALPHAVANTAGE_API_KEY:
  - Per MINUTE: 1,200 requests/min (§3(b) commercial plan).
  - Per SECOND: 30 requests/sec commercial ceiling (10 req/sec on trial key).

Exceeding the per-second limit triggers an HTTP 200 refusal containing:
  "Burst pattern detected. Please consider spreading out your API requests
   more evenly across a 1-minute window and query no more than 30 requests per second."

This pacer spaces requests evenly across time slots (spacing_ms) with:
  - Strict 30 req/sec pacing with guard window.
  - Automatic detection of burst notices with 2000ms global cooldown pause.
  - Dual-queue anti-starvation scheduling: 3 reader slots per 1 background slot.
"""

import asyncio
import logging
import math
import re
import time
from typing import Any

logger = logging.getLogger(__name__)

TRIAL_PER_SECOND = 10
COMMERCIAL_PER_SECOND = 30
DEFAULT_MAX_PER_SECOND = 30
WINDOW_MS = 1000.0
GUARD_MS = 8.0
COOLDOWN_MS = 2000.0
READERS_PER_BACKGROUND = 3

BURST_PATTERN = re.compile(r"burst pattern|requests per second", re.IGNORECASE)


def is_burst_notice(text: str) -> bool:
    """Check if upstream response body text contains an Alpha Vantage burst warning."""
    if not isinstance(text, str):
        return False
    return bool(BURST_PATTERN.search(text))


class _Waiter:
    """Internal waiter tracking future, deadline, and queue priority."""

    def __init__(self, future: asyncio.Future[tuple[bool, str, float]], deadline_ms: float, is_background: bool):
        self.future = future
        self.deadline_ms = deadline_ms
        self.is_background = is_background

    @property
    def is_cancelled(self) -> bool:
        return self.future.cancelled()


class AlphaVantagePacer:
    """Resilient Token-Bucket Pacer enforcing 30 req/sec Alpha Vantage commercial SLA."""

    def __init__(
        self,
        max_per_second: int = DEFAULT_MAX_PER_SECOND,
        cooldown_ms: float = COOLDOWN_MS,
    ) -> None:
        if not (1 <= max_per_second <= COMMERCIAL_PER_SECOND):
            logger.warning(
                "Invalid max_per_second=%s. Defaulting to %s",
                max_per_second,
                DEFAULT_MAX_PER_SECOND,
            )
            max_per_second = DEFAULT_MAX_PER_SECOND

        self.max_per_second = max_per_second
        self.cooldown_ms = float(cooldown_ms)
        self.spacing_ms = math.ceil((WINDOW_MS + GUARD_MS) / self.max_per_second)

        self._next_at: float = 0.0
        self._cool_until: float = 0.0
        self._cool_started_at: float = 0.0

        self._readers: list[_Waiter] = []
        self._background: list[_Waiter] = []
        self._reader_run: int = 0
        self._pump_task: asyncio.Task[None] | None = None
        self._absorbed: int = 0

        self._counts = {
            "granted": 0,
            "background_granted": 0,
            "refused": 0,
            "unwanted": 0,
            "cooldowns": 0,
            "burst_refusals": 0,
        }

    def _now_ms(self) -> float:
        return time.monotonic() * 1000.0

    def _open_at(self) -> float:
        return max(self._next_at, self._cool_until)

    def _reader_slot(self, k: int, bg_waiting: int) -> int:
        extra = min(bg_waiting, math.floor((self._reader_run + k) / READERS_PER_BACKGROUND)) if bg_waiting else 0
        return k + extra

    def _background_slot(self, j: int, readers_waiting: int) -> int:
        extra = min(readers_waiting, max(0, READERS_PER_BACKGROUND - self._reader_run) + j * READERS_PER_BACKGROUND)
        return j + extra

    def _drop_unwanted(self) -> None:
        for q in (self._readers, self._background):
            i = 0
            while i < len(q):
                if q[i].is_cancelled:
                    self._counts["unwanted"] += 1
                    q.pop(i)
                else:
                    i += 1

    async def _pump(self) -> None:
        try:
            while True:
                self._drop_unwanted()
                bg_turn = len(self._background) > 0 and (
                    len(self._readers) == 0 or self._reader_run >= READERS_PER_BACKGROUND
                )
                q = self._background if bg_turn else self._readers

                if not q:
                    break

                waiter = q[0]
                now = self._now_ms()

                if waiter.is_cancelled:
                    q.pop(0)
                    self._counts["unwanted"] += 1
                    continue

                open_at = self._open_at()
                target_at = max(open_at, now)

                if target_at > waiter.deadline_ms:
                    q.pop(0)
                    self._counts["refused"] += 1
                    reason = "cooldown" if self._cool_until > now else "queue"
                    waiter.future.set_result((False, reason, max(1.0, target_at - now)))
                    continue

                if target_at > now:
                    wait_sec = (target_at - now) / 1000.0
                    await asyncio.sleep(wait_sec)
                    continue

                q.pop(0)
                self._next_at = now + self.spacing_ms
                self._counts["granted"] += 1
                if bg_turn:
                    self._counts["background_granted"] += 1
                    self._reader_run = 0
                elif len(self._background) > 0:
                    self._reader_run += 1

                if not waiter.is_cancelled:
                    waiter.future.set_result((True, "ok", now))
        finally:
            self._pump_task = None

    def _ensure_pump(self) -> None:
        if self._pump_task is None or self._pump_task.done():
            self._pump_task = asyncio.create_task(self._pump())

    async def acquire(
        self,
        is_background: bool = True,
        max_wait_ms: float = float("inf"),
    ) -> tuple[bool, str, float]:
        """Request a send slot from the pacer.

        Args:
            is_background: True if caller is a background batch worker, False for interactive readers.
            max_wait_ms: Maximum milliseconds the caller is willing to wait.

        Returns:
            Tuple of (granted: bool, reason: str, retry_after_ms: float).
        """
        now = self._now_ms()
        ahead = (
            self._background_slot(len(self._background), len(self._readers))
            if is_background
            else self._reader_slot(len(self._readers), len(self._background))
        )
        projected_at = max(self._open_at(), now) + ahead * self.spacing_ms

        if projected_at - now > max_wait_ms:
            self._counts["refused"] += 1
            reason = "cooldown" if self._cool_until > now else "queue"
            return (False, reason, max(1.0, projected_at - now))

        loop = asyncio.get_running_loop()
        future: asyncio.Future[tuple[bool, str, float]] = loop.create_future()
        waiter = _Waiter(future, now + max_wait_ms, is_background)

        if is_background:
            self._background.append(waiter)
        else:
            self._readers.append(waiter)

        self._ensure_pump()
        return await future

    def burst_refused(self, fn_name: str = "", sent_at_ms: float | None = None) -> dict[str, Any]:
        """Notify pacer that Alpha Vantage replied with a burst rate warning.

        Enforces a global 2-second cooldown on all upcoming calls in this process.
        """
        now = self._now_ms()
        self._counts["burst_refusals"] += 1
        sent = sent_at_ms if sent_at_ms is not None else now

        if now < self._cool_until or sent <= self._cool_started_at:
            self._absorbed += 1
            return {"started": False, "retry_after_ms": max(0.0, self._cool_until - now)}

        self._cool_started_at = now
        self._cool_until = now + self.cooldown_ms
        self._counts["cooldowns"] += 1

        logger.warning(
            "[AlphaVantagePacer] Burst notice on '%s'. Process paused for %.0fms (%d/s ceiling; %d prior absorbed)",
            fn_name or "request",
            self.cooldown_ms,
            self.max_per_second,
            self._absorbed,
        )
        self._absorbed = 0
        self._ensure_pump()
        return {"started": True, "retry_after_ms": self.cooldown_ms}

    def cooling_for(self) -> float:
        """Return remaining milliseconds in current cooldown pause, or 0.0."""
        return max(0.0, self._cool_until - self._now_ms())

    def stats(self) -> dict[str, Any]:
        """Return telemetry counters and queue status."""
        return {
            **self._counts,
            "max_per_second": self.max_per_second,
            "spacing_ms": self.spacing_ms,
            "cooldown_ms": self.cooldown_ms,
            "queued_readers": len(self._readers),
            "queued_background": len(self._background),
            "cooling_for_ms": self.cooling_for(),
        }
