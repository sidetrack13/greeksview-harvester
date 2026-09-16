"""
GreeksView Data Harvester — Centralized Worker Registry
========================================================
Registers all autonomous harvesting workers:
  - congressional: U.S. House & Senate Trading Disclosures
  - sec_edgar: SEC Form 4 (Insider Trades) & Form 13F (Holdings)
  - finra_darkpool: FINRA OTC Non-ATS Dark Pool Share Volumes
  - cboe_options: CBOE Daily Options Volume & Put/Call Ratios
  - fred_macro: FRED Yield Curves & Macroeconomic Indicators
"""

from typing import Any

from harvester.core.base_worker import BaseWorker
from harvester.workers.alphavantage.worker import AlphaVantageWorker
from harvester.workers.cboe_options.worker import CboeOptionsWorker
from harvester.workers.congressional.worker import CongressionalWorker
from harvester.workers.finra_darkpool.worker import FinraDarkPoolWorker
from harvester.workers.fred_macro.worker import FredMacroWorker
from harvester.workers.sec_edgar.worker import SecEdgarWorker

WORKER_REGISTRY: dict[str, type[BaseWorker]] = {
    "congressional": CongressionalWorker,
    "sec_edgar": SecEdgarWorker,
    "finra_darkpool": FinraDarkPoolWorker,
    "cboe_options": CboeOptionsWorker,
    "fred_macro": FredMacroWorker,
    "alphavantage": AlphaVantageWorker,
}


def get_worker(name: str, config: Any = None) -> BaseWorker:
    """Retrieve and instantiate a worker by name.

    Args:
        name: Name of the worker registered in WORKER_REGISTRY.
        config: Optional Settings or custom configuration object.

    Returns:
        Instantiated BaseWorker subclass.

    Raises:
        KeyError: If worker name is not recognized.
    """
    worker_cls = WORKER_REGISTRY.get(name.lower().strip())
    if not worker_cls:
        available = ", ".join(WORKER_REGISTRY.keys())
        raise KeyError(f"Unknown worker '{name}'. Registered workers: {available}")
    return worker_cls(config=config)


def list_workers() -> list[dict[str, Any]]:
    """Return metadata for all registered workers."""
    return [cls().get_info() for cls in WORKER_REGISTRY.values()]


__all__ = [
    "WORKER_REGISTRY",
    "get_worker",
    "list_workers",
    "CongressionalWorker",
    "SecEdgarWorker",
    "FinraDarkPoolWorker",
    "CboeOptionsWorker",
    "FredMacroWorker",
    "AlphaVantageWorker",
]
