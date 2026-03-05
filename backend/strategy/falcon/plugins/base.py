from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class FalconPlugin(ABC):
    strategy_id: str = "base"
    display_name: str = "Base"
    version: str = "1.0.0"

    @abstractmethod
    def default_params(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def run(self, universe_df: pd.DataFrame, as_of_date: str, params: dict[str, Any]) -> pd.DataFrame:
        """Return columns: ts_code,name,strategy_score,confidence,signal_label,score_breakdown"""
        raise NotImplementedError
