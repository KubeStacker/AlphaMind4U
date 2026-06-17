import unittest
from unittest.mock import patch

import pandas as pd

from etl.tasks.capital_flow_task import CapitalFlowTask
from etl.tasks.daily_market_data_task import DailyMarketDataTask
from etl.tasks.factor_data_task import FactorDataTask


class _FakeConnection:
    def __init__(self):
        self.calls = []
        self.registered = set()

    def register(self, name, df):
        self.calls.append(("register", name, list(df.columns)))
        self.registered.add(name)

    def unregister(self, name):
        self.calls.append(("unregister", name))
        if name not in self.registered:
            raise Exception("not registered")
        self.registered.remove(name)

    def execute(self, sql, params=None):
        self.calls.append(("execute", " ".join(sql.split()), params))
        return self


class _FakeDBContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False


def _executed_sql(connection):
    return " ".join(call[1] for call in connection.calls if call[0] == "execute")


class ArtSafeReplacementTests(unittest.TestCase):
    def test_daily_basic_updates_then_inserts_missing_without_art_conflict_paths(self):
        connection = _FakeConnection()
        task = FactorDataTask(provider=None)
        df = pd.DataFrame(
            [
                {
                    "trade_date": "2026-05-20",
                    "ts_code": "600519.SH",
                    "close": 100.0,
                }
            ]
        )

        with patch("etl.tasks.factor_data_task.get_db_connection", return_value=_FakeDBContext(connection)):
            task._upsert_daily_basic(df)

        sql = _executed_sql(connection)
        self.assertIn("BEGIN TRANSACTION", sql)
        self.assertIn("UPDATE stock_daily_basic", sql)
        self.assertIn("INSERT INTO stock_daily_basic", sql)
        self.assertIn("WHERE NOT EXISTS", sql)
        self.assertNotIn("DELETE FROM stock_daily_basic", sql)
        self.assertNotIn("ON CONFLICT", sql)

    def test_capital_flow_updates_then_inserts_missing_without_art_conflict_paths(self):
        connection = _FakeConnection()
        task = CapitalFlowTask(provider=None)
        df = pd.DataFrame(
            [
                {
                    "trade_date": "2026-05-20",
                    "ts_code": "600519.SH",
                    "net_mf_amount": 1000.0,
                }
            ]
        )

        with patch("etl.tasks.capital_flow_task.get_db_connection", return_value=_FakeDBContext(connection)):
            task._upsert_capital_flow_data(df)

        sql = _executed_sql(connection)
        self.assertIn("BEGIN TRANSACTION", sql)
        self.assertIn("UPDATE stock_moneyflow", sql)
        self.assertIn("INSERT INTO stock_moneyflow", sql)
        self.assertIn("WHERE NOT EXISTS", sql)
        self.assertNotIn("DELETE FROM stock_moneyflow", sql)
        self.assertNotIn("ON CONFLICT", sql)

    def test_daily_price_updates_then_inserts_missing_without_art_conflict_paths(self):
        connection = _FakeConnection()
        task = DailyMarketDataTask(provider=None)
        df = pd.DataFrame(
            [
                {
                    "trade_date": "2026-05-20",
                    "ts_code": "600519.SH",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "pre_close": 100.0,
                    "change": 0.5,
                    "pct_chg": 0.5,
                    "vol": 10000.0,
                    "amount": 100000.0,
                    "factors": "{}",
                    "adj_factor": 1.0,
                }
            ]
        )

        with patch("etl.tasks.daily_market_data_task.get_db_connection", return_value=_FakeDBContext(connection)):
            task._upsert_daily_data(df)

        sql = _executed_sql(connection)
        self.assertIn("BEGIN TRANSACTION", sql)
        self.assertIn("UPDATE daily_price", sql)
        self.assertIn("INSERT INTO daily_price", sql)
        self.assertIn("WHERE NOT EXISTS", sql)
        self.assertNotIn("DELETE FROM daily_price", sql)
        self.assertNotIn("ON CONFLICT", sql)


if __name__ == "__main__":
    unittest.main()
