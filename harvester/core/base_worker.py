"""
GreeksView Data Harvester — Base Worker Interface
=================================================
Defines the standard contract for all background harvesting workers.
Each worker runs independently, exports structured metrics, and adheres
to domain rate limits and graceful degradation principles.
"""

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class WorkerResult(BaseModel):
    worker: str
    status: str = Field(..., description="success | partial | failed | skipped")
    records_harvested: int = 0
    records_upserted: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_success(self) -> bool:
        return self.status in ("success", "partial")


class BaseWorker(ABC):
    """Abstract base class for all standalone background harvesting workers."""

    name: str = "base-worker"
    description: str = "Generic background data harvester"
    frequency: str = "daily"
    target_views: list[str] = []

    def __init__(self, config: Any | None = None):
        # Declared Any, not Any | None: every subclass passes
        # `settings or config or get_settings()`, which is never None, and
        # each then narrows it to Settings. Typing the attribute as
        # optional made those four narrowings errors rather than the
        # documented contract they are.
        self.config: Any = config

    @abstractmethod
    async def run_once(self, **kwargs: Any) -> WorkerResult:
        """Execute a single harvesting cycle independently.

        Args:
            **kwargs: Worker-specific runtime parameters (e.g. year, ticker, limit)

        Returns:
            WorkerResult summarizing harvested counts, duration, and errors.
        """
        pass

    @abstractmethod
    async def health(self) -> dict[str, Any]:
        """Perform health check on upstream endpoints and local database connections."""
        pass

    def get_info(self) -> dict[str, Any]:
        """Return worker metadata for CLI and scheduler inspection."""
        return {
            "name": self.name,
            "description": self.description,
            "frequency": self.frequency,
            "target_views": self.target_views,
        }
