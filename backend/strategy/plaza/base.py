from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class StrategyMeta:
    strategy_key: str
    name: str
    description: str = ""
    enabled: bool = True
    display_order: int = 100
    engine_version: str = "v1"


@dataclass(frozen=True)
class ObservationCandidate:
    ts_code: str
    name: str
    observation_date: str
    entry_anchor_date: str
    reason: str
    tags: list[str] = field(default_factory=list)
    trace: dict[str, Any] = field(default_factory=dict)
    entry_price_source: str = "close_on_anchor"


class PlazaStrategy(Protocol):
    def meta(self) -> StrategyMeta: ...
    def run_for_date(self, trade_date: str, context: Any) -> list[ObservationCandidate]: ...
