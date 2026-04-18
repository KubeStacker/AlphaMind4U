import unittest

import pandas as pd

from strategy.plaza.builtin.dragon_return import Head7DragonReturnStrategy


class _FakeContext:
    def __init__(self, history_df: pd.DataFrame):
        self.history_df = history_df

    def load_history_frame(self, trade_date: str, lookback_days: int = 260, ts_codes: list[str] | None = None):
        return self.history_df

    def get_sector_meta(self, trade_date: str, ts_codes: list[str]):
        return {
            code: {
                "mapped_sector": "算力基建",
                "display_name": "算力基建",
                "focus_tags": ["液冷", "高速连接"],
                "sector_heat_rank": 1,
                "sector_heat_total": 6,
                "is_mainline": True,
            }
            for code in ts_codes
        }

    def get_market_regime(self, trade_date: str):
        return {"is_supportive": True, "reason": "000001.SH 站上 MA20"}


def _dragon_return_history_with_secondary_high():
    rows = [
        ("2026-03-27", 9.60, 9.82, 9.56, 9.74, 1.1, 920, 1800),
        ("2026-03-30", 9.76, 9.92, 9.70, 9.88, 1.4, 980, 1900),
        ("2026-03-31", 9.96, 11.08, 9.94, 11.02, 11.5, 5000, 10200),
        ("2026-04-01", 11.12, 11.18, 10.88, 10.96, -0.54, 3200, 6900),
        ("2026-04-02", 10.92, 10.98, 10.76, 10.84, -1.09, 2100, 4300),
        ("2026-04-03", 10.82, 10.88, 10.70, 10.78, -0.55, 1800, 3600),
        ("2026-04-06", 10.76, 10.84, 10.68, 10.74, -0.37, 1500, 3000),
        ("2026-04-07", 10.72, 10.82, 10.66, 10.76, 0.19, 1250, 2550),
        ("2026-04-08", 10.74, 10.80, 10.68, 10.75, -0.09, 1180, 2400),
        ("2026-04-09", 10.76, 10.88, 10.72, 10.84, 0.84, 1400, 2950),
    ]
    return pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "ts_code": "600009.SH",
                "name": "二波龙头",
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "pct_chg": pct_chg,
                "vol": vol,
                "amount": amount,
            }
            for trade_date, open_price, high, low, close, pct_chg, vol, amount in rows
        ]
    )


class Head7DragonReturnTests(unittest.TestCase):
    def test_head7_keeps_limit_up_anchor_when_secondary_high_is_bearish(self):
        strategy = Head7DragonReturnStrategy()
        result = strategy.run_for_date(
            "2026-04-09",
            _FakeContext(_dragon_return_history_with_secondary_high()),
        )

        self.assertEqual(1, len(result))
        self.assertEqual("600009.SH", result[0].ts_code)
        self.assertIn("小阴小阳止跌", result[0].reason)


if __name__ == "__main__":
    unittest.main()
