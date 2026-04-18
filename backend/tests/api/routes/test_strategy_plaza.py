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

import api.routes.strategy_plaza as strategy_plaza


class StrategyPlazaRouteTests(unittest.TestCase):
    @patch("api.routes.strategy_plaza.strategy_plaza_service.sync_definitions", return_value=[])
    @patch("api.routes.strategy_plaza.fetch_df", return_value=pd.DataFrame(columns=["strategy_key"]))
    def test_list_strategies_returns_empty_success_payload(self, _df, _sync):
        result = strategy_plaza.list_strategies()

        self.assertEqual("success", result["status"])
        self.assertEqual([], result["data"]["strategies"])

    @patch("api.routes.strategy_plaza.fetch_df")
    def test_get_observations_merges_backtest_columns(self, mocked_fetch):
        mocked_fetch.side_effect = [
            pd.DataFrame(
                [
                    {
                        "strategy_key": "demo_strategy",
                        "trade_date": "2026-04-08",
                        "observation_date": "2026-04-08",
                        "ts_code": "300308.SZ",
                        "name": "中际旭创",
                        "reason": "示例观察",
                        "tags_json": '["demo"]',
                    }
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "strategy_key": "demo_strategy",
                        "observation_date": "2026-04-08",
                        "ts_code": "300308.SZ",
                        "ret_3d": 5.1,
                        "ret_5d": None,
                        "ret_10d": None,
                        "status": "PARTIAL",
                    }
                ]
            ),
        ]

        result = strategy_plaza.get_observations(strategy_key="demo_strategy", trade_date="2026-04-08")

        self.assertEqual(1, len(result["data"]["items"]))
        self.assertEqual(5.1, result["data"]["items"][0]["ret_3d"])
        self.assertEqual("PARTIAL", result["data"]["items"][0]["backtest_status"])

    @patch("api.routes.strategy_plaza.fetch_df")
    def test_get_summary_returns_null_when_no_summary_exists(self, mocked_fetch):
        mocked_fetch.return_value = pd.DataFrame(columns=["strategy_key"])

        result = strategy_plaza.get_summary(strategy_key="demo_strategy", trade_date="2026-04-08")

        self.assertIsNone(result["data"]["summary"])

    @patch("api.routes.strategy_plaza.fetch_df")
    def test_get_summary_normalizes_nan_values(self, mocked_fetch):
        mocked_fetch.return_value = pd.DataFrame(
            [
                {
                    "strategy_key": "demo_strategy",
                    "trade_date": "2026-04-08",
                    "observation_count": 12,
                    "win_rate_3d": float("nan"),
                    "avg_ret_3d": float("nan"),
                    "summary_text": "5日回测尚未补齐。",
                }
            ]
        )

        result = strategy_plaza.get_summary(strategy_key="demo_strategy", trade_date="2026-04-08")

        self.assertEqual("demo_strategy", result["data"]["summary"]["strategy_key"])
        self.assertIsNone(result["data"]["summary"]["win_rate_3d"])
        self.assertIsNone(result["data"]["summary"]["avg_ret_3d"])

    @patch("api.routes.strategy_plaza.fetch_df")
    def test_get_summary_returns_null_when_selected_date_has_no_observations(self, mocked_fetch):
        mocked_fetch.return_value = pd.DataFrame(
            [
                {
                    "strategy_key": "head7_dragon_return",
                    "trade_date": "2026-04-09",
                    "observation_count": 1,
                    "summary_text": "近窗共 1 条观察。",
                    "same_day_observation_count": 0,
                }
            ]
        )

        result = strategy_plaza.get_summary(strategy_key="head7_dragon_return", trade_date="2026-04-09")

        self.assertIsNone(result["data"]["summary"])

    @patch("api.routes.strategy_plaza.TaskRegistry.create_task", return_value=("abc12345", "PENDING"))
    def test_trigger_run_enqueues_strategy_plaza_task(self, mocked_create):
        payload = strategy_plaza.StrategyPlazaRunParams(trade_date="2026-04-08", strategy_key=None)

        result = strategy_plaza.trigger_strategy_run(payload)

        mocked_create.assert_called_once()
        self.assertEqual("abc12345", result["task_id"])


if __name__ == "__main__":
    unittest.main()
