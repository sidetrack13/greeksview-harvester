"""
GreeksView Data Harvester — Centralized Worker Registry
========================================================
Registers all autonomous harvesting workers:
  - congressional: U.S. House & Senate Trading Disclosures
  - sec_edgar: SEC Form 4 (Insider Trades) & Form 13F (Holdings)
  - fred_macro: FRED Yield Curves & Macroeconomic Indicators
  - alphavantage: Alpha Vantage bars, options chains, fundamentals & reference data

Retired workers are listed in RETIRED_WORKERS. Asking for one by name fails with
the reason; it is never silently ignored and never falls through to a live worker.
"""

from typing import Any

from harvester.core.base_worker import BaseWorker
from harvester.workers.alphavantage.worker import AlphaVantageWorker
from harvester.workers.congressional.worker import CongressionalWorker
from harvester.workers.fred_macro.worker import FredMacroWorker
from harvester.workers.sec_edgar.worker import SecEdgarWorker

WORKER_REGISTRY: dict[str, type[BaseWorker]] = {
    "congressional": CongressionalWorker,
    "sec_edgar": SecEdgarWorker,
    "fred_macro": FredMacroWorker,
    "alphavantage": AlphaVantageWorker,
}

# Workers withdrawn from the product, with the reason a caller is shown. Both
# collected data GreeksView may not use, and both wrote a fabricated record on
# any failed fetch, so a network blip produced a row indistinguishable from a
# real one. Their code has been deleted, not merely unregistered.
RETIRED_WORKERS: dict[str, str] = {
    "cboe_options": (
        "Cboe's data terms require Cboe's written consent for commercial use, which GreeksView "
        "does not have. The worker also invented a record whenever a fetch failed, and derived "
        "the equity put/call ratio by multiplying the total ratio by 0.78 even when the fetch "
        "succeeded. Nothing collects Cboe daily options volume or put/call ratios any more."
    ),
    "finra_darkpool": (
        "FINRA's data terms permit non-commercial use only, and GreeksView is a paid product. "
        "The worker also invented a record whenever a fetch failed. Nothing collects FINRA OTC "
        "or off-exchange dark pool volume any more."
    ),
}


class RetiredWorkerError(KeyError):
    """Raised when a caller asks for a worker that has been withdrawn from the product.

    Subclasses KeyError so callers that already handle an unknown worker name keep
    working, while the message says why this particular name is gone.
    """

    def __str__(self) -> str:
        return str(self.args[0]) if self.args else self.__class__.__name__


def get_worker(name: str, config: Any = None) -> BaseWorker:
    """Retrieve and instantiate a worker by name.

    Args:
        name: Name of the worker registered in WORKER_REGISTRY.
        config: Optional Settings or custom configuration object.

    Returns:
        Instantiated BaseWorker subclass.

    Raises:
        RetiredWorkerError: If the worker was withdrawn from the product.
        KeyError: If worker name is not recognized.
    """
    key = name.lower().strip()
    if key in RETIRED_WORKERS:
        raise RetiredWorkerError(f"Worker '{key}' was retired and cannot be run. {RETIRED_WORKERS[key]}")
    worker_cls = WORKER_REGISTRY.get(key)
    if not worker_cls:
        available = ", ".join(WORKER_REGISTRY.keys())
        raise KeyError(f"Unknown worker '{name}'. Registered workers: {available}")
    return worker_cls(config=config)


def list_workers() -> list[dict[str, Any]]:
    """Return metadata for all registered workers."""
    return [cls().get_info() for cls in WORKER_REGISTRY.values()]


__all__ = [
    "WORKER_REGISTRY",
    "RETIRED_WORKERS",
    "RetiredWorkerError",
    "get_worker",
    "list_workers",
    "CongressionalWorker",
    "SecEdgarWorker",
    "FredMacroWorker",
    "AlphaVantageWorker",
]
