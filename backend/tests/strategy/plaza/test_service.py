import unittest
from unittest.mock import patch
from unittest.mock import call

import pandas as pd

from strategy.plaza.registry import list_registered_strategies
from strategy.plaza.summarizer import build_strategy_summary_text
from strategy.plaza.base import ObservationCandidate, StrategyMeta
from strategy.plaza.service import StrategyPlazaService, build_horizon_metrics


class StrategyPlazaCoreTests(unittest.TestCase):
    @patch("strategy.plaza.registry.BUILTIN_STRATEGIES", ())
    def test_registry_defaults_to_empty_list_when_no_builtins_exist(self):
        self.assertEqual([], list_registered_strategies())

    def test_build_strategy_summary_text_mentions_win_rate_mean_and_drawdown(self):
        summary = build_strategy_summary_text(
            {
                "observation_count": 12,
                "completed_count_5d": 10,
                "win_rate_5d": 60.0,
                "avg_ret_5d": 3.2,
                "avg_max_drawdown_5d": -1.8,
            }
        )

        self.assertIn("12", summary)
        self.assertIn("5日胜率 60.0%", summary)
        self.assertIn("5日均值 +3.20%", summary)
        self.assertIn("回撤 -1.80%", summary)


class _FakeStrategy:
    def meta(self):
        return StrategyMeta(
            strategy_key="demo_strategy",
            name="Demo Strategy",
            description="demo",
            enabled=True,
            display_order=10,
            engine_version="v1",
        )

    def run_for_date(self, trade_date: str, context):
        return [
            ObservationCandidate(
                ts_code="300308.SZ",
                name="中际旭创",
                observation_date=trade_date,
                entry_anchor_date=trade_date,
                reason="示例观察",
                tags=["demo"],
                trace={"stage": "final"},
                entry_price_source="close_on_anchor",
            )
        ]


class _DisabledStrategy:
    def meta(self):
        return StrategyMeta(
            strategy_key="disabled_strategy",
            name="Disabled Strategy",
            description="disabled",
            enabled=False,
            display_order=20,
            engine_version="v1",
        )

    def run_for_date(self, trade_date: str, context):
        return []


class _FakeConnection:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self


class _FakeDBContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False


class StrategyPlazaServiceTests(unittest.TestCase):
    def test_build_horizon_metrics_uses_entry_price_and_future_closes(self):
        price_df = pd.DataFrame(
            [
                {"trade_date": "2026-04-01", "close": 10.0, "high": 10.2, "low": 9.8},
                {"trade_date": "2026-04-02", "close": 10.8, "high": 11.0, "low": 10.1},
                {"trade_date": "2026-04-03", "close": 11.2, "high": 11.4, "low": 10.6},
                {"trade_date": "2026-04-06", "close": 11.5, "high": 11.6, "low": 10.9},
            ]
        )

        metrics = build_horizon_metrics(price_df=price_df, entry_price=10.0, horizon=3)

        self.assertAlmostEqual(15.0, metrics["ret_pct"], places=4)
        self.assertAlmostEqual(16.0, metrics["max_gain_pct"], places=4)
        self.assertAlmostEqual(-2.0, metrics["max_drawdown_pct"], places=4)

    @patch.object(StrategyPlazaService, "sync_definitions", return_value=[])
    @patch("strategy.plaza.service.list_enabled_strategies", return_value=[])
    @patch.object(StrategyPlazaService, "complete_pending_backtests", return_value=0)
    def test_run_for_date_returns_zero_counts_when_no_strategies_are_enabled(
        self,
        _pending,
        _mocked,
        _sync,
    ):
        service = StrategyPlazaService()

        result = service.run_for_date("2026-04-08")

        self.assertEqual(0, result["strategy_count"])
        self.assertEqual(0, result["observation_count"])

    @patch.object(StrategyPlazaService, "sync_definitions", return_value=[])
    @patch("strategy.plaza.service.list_enabled_strategies", return_value=[_FakeStrategy()])
    @patch.object(StrategyPlazaService, "_persist_strategy_rows", return_value=1)
    @patch.object(StrategyPlazaService, "_refresh_strategy_summary", return_value=None)
    @patch.object(StrategyPlazaService, "complete_pending_backtests", return_value=0)
    def test_run_for_date_persists_rows_for_each_enabled_strategy(
        self,
        _pending,
        _summary,
        _persist,
        _strategies,
        _sync,
    ):
        service = StrategyPlazaService()

        result = service.run_for_date("2026-04-08")

        self.assertEqual(1, result["strategy_count"])
        self.assertEqual(1, result["observation_count"])

    @patch("strategy.plaza.service.list_registered_strategies", return_value=[_FakeStrategy(), _DisabledStrategy()])
    def test_sync_definitions_returns_registered_rows_and_writes_them(self, _strategies):
        connection = _FakeConnection()
        service = StrategyPlazaService()

        with patch("strategy.plaza.service.get_db_connection", return_value=_FakeDBContext(connection)):
            rows = service.sync_definitions()

        self.assertEqual(2, len(rows))
        self.assertEqual("demo_strategy", rows[0]["strategy_key"])
        self.assertEqual("disabled_strategy", rows[1]["strategy_key"])
        self.assertEqual(3, len(connection.calls))

    @patch("strategy.plaza.service.list_registered_strategies", return_value=[])
    def test_sync_definitions_clears_definition_rows_when_registry_is_empty(self, _strategies):
        connection = _FakeConnection()
        service = StrategyPlazaService()

        with patch("strategy.plaza.service.get_db_connection", return_value=_FakeDBContext(connection)):
            rows = service.sync_definitions()

        self.assertEqual([], rows)
        self.assertEqual(1, len(connection.calls))
        self.assertIn("DELETE FROM strategy_definitions", connection.calls[0][0])

    @patch.object(StrategyPlazaService, "sync_definitions", return_value=[])
    @patch("strategy.plaza.service.list_enabled_strategies", return_value=[_FakeStrategy()])
    @patch.object(StrategyPlazaService, "_persist_strategy_rows", return_value=1)
    @patch.object(StrategyPlazaService, "_refresh_strategy_summary", return_value=None)
    @patch.object(StrategyPlazaService, "complete_pending_backtests", return_value=1)
    def test_run_for_date_refreshes_summary_again_after_backtest_completion(
        self,
        _pending,
        mocked_summary,
        _persist,
        _strategies,
        _sync,
    ):
        service = StrategyPlazaService()

        result = service.run_for_date("2026-04-08")

        self.assertEqual(1, result["completed_backtests"])
        self.assertEqual(
            [
                call("demo_strategy", "2026-04-08"),
                call("demo_strategy", "2026-04-08"),
            ],
            mocked_summary.call_args_list,
        )

    @patch("strategy.plaza.service.fetch_df", return_value=pd.DataFrame([{"observation_count": 1}]))
    def test_refresh_strategy_summary_limits_window_to_selected_trade_date(self, mocked_fetch):
        connection = _FakeConnection()
        service = StrategyPlazaService()

        with patch("strategy.plaza.service.get_db_connection", return_value=_FakeDBContext(connection)):
            service._refresh_strategy_summary("demo_strategy", "2026-04-08")

        self.assertEqual(
            ["demo_strategy", "2026-04-08", 120, "demo_strategy"],
            mocked_fetch.call_args.args[1],
        )

    @patch("strategy.plaza.service._shift_trade_date", return_value="2026-04-09")
    @patch("strategy.plaza.service.fetch_df", return_value=pd.DataFrame([{"price": 11.2}]))
    def test_resolve_entry_price_supports_next_trade_day_sources(self, mocked_fetch, _shifted):
        service = StrategyPlazaService()

        price = service._resolve_entry_price("300308.SZ", "2026-04-08", "open_next_trade_day")

        self.assertEqual(11.2, price)
        mocked_fetch.assert_called_once()

    @patch("strategy.plaza.service.fetch_df")
    def test_load_history_frame_reuses_wider_cache_for_same_trade_date(self, mocked_fetch):
        history_df = pd.DataFrame(
            [
                {
                    "trade_date": "2026-04-08",
                    "ts_code": "300308.SZ",
                    "name": "中际旭创",
                    "close": 100.0,
                }
            ]
        )
        mocked_fetch.return_value = history_df
        service = StrategyPlazaService()

        first = service.load_history_frame("2026-04-08", lookback_days=140)
        second = service.load_history_frame("2026-04-08", lookback_days=30)

        self.assertIs(first, second)
        mocked_fetch.assert_called_once()

    @patch.object(StrategyPlazaService, "_resolve_entry_price", return_value=11.2)
    def test_persist_strategy_rows_clears_existing_rows_before_reinserting(self, _price):
        connection = _FakeConnection()
        service = StrategyPlazaService()
        rows = [
            ObservationCandidate(
                ts_code="300308.SZ",
                name="中际旭创",
                observation_date="2026-04-08",
                entry_anchor_date="2026-04-08",
                reason="示例观察",
                tags=["demo"],
                trace={"stage": "final"},
                entry_price_source="open_next_trade_day",
            )
        ]

        with patch("strategy.plaza.service.get_db_connection", return_value=_FakeDBContext(connection)):
            inserted = service._persist_strategy_rows("demo_strategy", "2026-04-08", rows)

        self.assertEqual(1, inserted)
        self.assertIn("DELETE FROM strategy_observations", connection.calls[0][0])
        self.assertIn("DELETE FROM strategy_backtest_runs", connection.calls[1][0])


if __name__ == "__main__":
    unittest.main()
