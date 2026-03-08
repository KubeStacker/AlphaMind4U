from __future__ import annotations

from typing import Any

from strategy.falcon.plugins.base import FalconPlugin
from strategy.falcon.plugins.falcon_momentum import FalconMomentumPlugin
from strategy.falcon.plugins.classic_kline_recommender import ClassicKlineRecommenderPlugin


class FalconRegistry:
    def __init__(self) -> None:
        self._plugins: dict[str, FalconPlugin] = {}
        self.register(FalconMomentumPlugin())
        self.register(ClassicKlineRecommenderPlugin())

    def register(self, plugin: FalconPlugin) -> None:
        self._plugins[plugin.strategy_id] = plugin

    def get(self, strategy_id: str) -> FalconPlugin:
        if strategy_id not in self._plugins:
            raise ValueError(f"未知策略: {strategy_id}")
        return self._plugins[strategy_id]

    def list_strategies(self) -> list[dict[str, Any]]:
        out = []
        for s in self._plugins.values():
            out.append(
                {
                    "strategy_id": s.strategy_id,
                    "name": s.display_name,
                    "version": s.version,
                    "default_params": s.default_params(),
                }
            )
        return out


falcon_registry = FalconRegistry()
