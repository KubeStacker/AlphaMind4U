from __future__ import annotations

from strategy.plaza.base import PlazaStrategy
from strategy.plaza.builtin import BUILTIN_STRATEGIES


def list_registered_strategies() -> list[PlazaStrategy]:
    return [strategy for strategy in BUILTIN_STRATEGIES]


def list_enabled_strategies() -> list[PlazaStrategy]:
    return [strategy for strategy in list_registered_strategies() if strategy.meta().enabled]
