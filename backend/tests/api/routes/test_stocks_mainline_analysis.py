import sys
import types
import unittest
from unittest.mock import patch

import pandas as pd

_pypinyin = types.ModuleType("pypinyin")
_pypinyin.lazy_pinyin = lambda value, style=None: []


class _Style:
    FIRST_LETTER = "FIRST_LETTER"


_pypinyin.Style = _Style
sys.modules.setdefault("pypinyin", _pypinyin)

import api.routes.stocks as stocks


class StockMainlineAnalysisTests(unittest.TestCase):
    def test_mainline_analysis_handles_empty_factor_scores_without_keyerror(self):
        date_df = pd.DataFrame([{"trade_date": pd.Timestamp("2026-04-08")}])
        stock_df = pd.DataFrame(
            [
                {
                    "ts_code": "688256.SH",
                    "close": 1164.0,
                    "pct_chg": 4.02,
                    "vol": 104833.0,
                    "amount": 120000.0,
                    "factors": "{}",
                    "name": "寒武纪",
                    "industry": "半导体",
                }
            ]
        )
        sector_df = pd.DataFrame([{"concept_name": "AI芯片"}])
        flow_df = pd.DataFrame(
            [
                {"trade_date": "2026-04-08", "net_mf_amount": 20000.0},
                {"trade_date": "2026-04-07", "net_mf_amount": 15000.0},
            ]
        )
        sector_stocks = [
            {
                "ts_code": "688256.SH",
                "pct_chg": 4.02,
                "net_mf_amount": 20000.0,
                "volume_ratio": 1.09,
                "turnover_rate": 2.51,
                "flow_positive_streak": 2,
                "flow_total_inflow": 35000.0,
            },
            {
                "ts_code": "688041.SH",
                "pct_chg": 6.50,
                "net_mf_amount": 10000.0,
            },
        ]

        with (
            patch.object(stocks, "fetch_df", side_effect=[date_df, stock_df, sector_df, flow_df]),
            patch.object(stocks, "get_market_environment", return_value={"trend": "up", "sentiment": 65}),
            patch.object(stocks, "get_sector_stocks", return_value=sector_stocks),
            patch("strategy.mainline.analyst.mainline_analyst.analyze", return_value=[]),
            patch("strategy.mainline.analyst.mainline_analyst.get_history", return_value={}),
            patch(
                "strategy.mainline.analyst.mainline_analyst.get_stock_mainline_map",
                return_value=pd.DataFrame([{"mapped_name": "半导体"}]),
            ),
            patch("etl.utils.scoring.calc_mainline_leader_score", return_value=(0.0, "非主线板块", {})),
            patch("etl.utils.scoring.calc_sector_resonance", return_value=48.2),
            patch("etl.utils.scoring.calc_breakout_score", return_value=61.3),
            patch("etl.utils.scoring.calc_flow_score", return_value=57.6),
            patch("etl.utils.scoring.calc_entry_stop_target", return_value={"entry_zone": [1100.0, 1120.0], "stop_loss": 1050.0, "target": 1250.0, "risk_reward": 2.5}),
            patch("etl.utils.scoring.calc_risk_reward", return_value=2.5),
            patch("etl.utils.scoring.calc_trend_leadership_score", return_value=52.1),
            patch("etl.utils.scoring.calc_theme_fit_score", return_value=41.0),
            patch("etl.utils.scoring.calc_sector_position_value", return_value=1.0),
        ):
            result = stocks.get_stock_mainline_analysis("688256.SH")

        self.assertEqual("success", result["status"])
        self.assertEqual("非主线板块", result["signal"]["reason"])
        self.assertEqual(48.2, result["analysis"]["sector_resonance"]["score"])
        self.assertEqual(61.3, result["analysis"]["breakout"]["score"])
        self.assertEqual(57.6, result["analysis"]["capital_flow"]["score"])


if __name__ == "__main__":
    unittest.main()
