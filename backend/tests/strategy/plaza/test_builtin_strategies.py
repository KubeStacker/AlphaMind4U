import unittest

import pandas as pd

from strategy.plaza.builtin.dragon_return import Head7DragonReturnStrategy
from strategy.plaza.builtin.golden_eye import GoldenEyeStrategy
from strategy.plaza.builtin.common import infer_market_profile
from strategy.plaza.builtin.single_yang_hold import SingleYangHoldStrategy


class _FakeContext:
    def __init__(self, history_df: pd.DataFrame):
        self.history_df = history_df
        self.load_calls = []

    def load_history_frame(self, trade_date: str, lookback_days: int = 260, ts_codes: list[str] | None = None):
        self.load_calls.append((trade_date, lookback_days, None if ts_codes is None else list(ts_codes)))
        if not ts_codes:
            return self.history_df
        return self.history_df[self.history_df["ts_code"].isin(ts_codes)].copy()

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


def _dragon_return_history(
    *,
    ts_code: str = "600001.SH",
    latest_open: float = 10.76,
    latest_high: float = 10.90,
    latest_low: float = 10.70,
    latest_close: float = 10.84,
    latest_pct_chg: float = 0.84,
    latest_vol: float = 1400,
    latest_amount: float = 3100,
):
    rows = [
        ("2026-03-25", 9.60, 9.80, 9.50, 9.70, 1.0, 900, 1800),
        ("2026-03-26", 9.72, 9.90, 9.68, 9.88, 1.9, 980, 1900),
        ("2026-03-27", 9.88, 10.05, 9.82, 9.98, 1.0, 1050, 2100),
        ("2026-03-30", 10.00, 11.05, 9.98, 11.00, 10.1, 4800, 9800),
        ("2026-03-31", 10.96, 10.98, 10.72, 10.85, -1.36, 2200, 4200),
        ("2026-04-01", 10.83, 10.86, 10.66, 10.78, -0.65, 1800, 3600),
        ("2026-04-02", 10.76, 10.82, 10.64, 10.72, -0.56, 1500, 3000),
        ("2026-04-03", 10.70, 10.79, 10.63, 10.74, 0.19, 1200, 2600),
        ("2026-04-06", 10.72, 10.80, 10.66, 10.75, 0.09, 1100, 2400),
        ("2026-04-07", latest_open, latest_high, latest_low, latest_close, latest_pct_chg, latest_vol, latest_amount),
    ]
    return pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "ts_code": ts_code,
                "name": "测试龙头",
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


def _single_yang_history():
    rows = [
        ("2026-03-27", 9.60, 9.75, 9.52, 9.70, 1.2, 900),
        ("2026-03-30", 9.72, 9.90, 9.68, 9.84, 1.4, 920),
        ("2026-03-31", 9.88, 11.00, 9.85, 10.95, 11.3, 4200),
        ("2026-04-01", 10.92, 10.98, 10.58, 10.72, -2.10, 1800),
        ("2026-04-02", 10.70, 10.88, 10.60, 10.76, 0.37, 1500),
        ("2026-04-03", 10.74, 10.90, 10.64, 10.82, 0.56, 1300),
        ("2026-04-06", 10.80, 11.08, 10.50, 10.92, 1.30, 1600),
        ("2026-04-07", 10.96, 11.26, 10.90, 11.22, 2.75, 2600),
    ]
    return pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "ts_code": "600002.SH",
                "name": "测试平台",
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "pct_chg": pct_chg,
                "vol": vol,
                "amount": vol * 2.4,
            }
            for trade_date, open_price, high, low, close, pct_chg, vol in rows
        ]
    )


def _golden_eye_history():
    closes = [10.0, 10.1, 10.2, 10.35, 10.55, 10.80, 11.05, 11.20, 11.28, 11.24, 11.18, 11.16, 11.34]
    ma5_values = [9.85, 9.88, 9.92, 9.98, 10.06, 10.18, 10.34, 10.58, 10.86, 11.02, 11.08, 11.09, 11.16]
    ma20_values = [9.90, 9.92, 9.95, 9.98, 10.02, 10.10, 10.22, 10.38, 10.58, 10.78, 10.96, 11.04, 11.08]
    rows = []
    for idx, close in enumerate(closes):
        rows.append(
            {
                "trade_date": f"2026-03-{18 + idx:02d}" if idx < 14 else f"2026-04-{idx - 13:02d}",
                "ts_code": "600003.SH",
                "name": "测试眼睛",
                "open": close - 0.18 if idx == len(closes) - 1 else close - 0.03,
                "high": close + 0.08,
                "low": close - 0.10,
                "close": close,
                "pct_chg": 1.2 if idx == len(closes) - 1 else 0.8,
                "vol": 1800 if idx == len(closes) - 1 else (1100 + idx * 40),
                "amount": (1100 + idx * 40) * 2.6,
                "ma5": ma5_values[idx],
                "ma20": ma20_values[idx],
                "ma60": 9.80 + idx * 0.05,
                "ma120": 9.60 + idx * 0.04,
                "vol_ma5": 1200,
                "rps_20": 88.0,
                "rps_50": 82.0,
                "trend_score": 72.0,
            }
        )
    return pd.DataFrame(rows)


class BuiltinStrategyTests(unittest.TestCase):
    def test_infer_market_profile_treats_301_as_growth_board(self):
        profile = infer_market_profile("301280.SZ")

        self.assertEqual("growth_board", profile.board_key)
        self.assertEqual("创业板/科创板", profile.board_label)
        self.assertEqual(2.0, profile.k)

    def test_head7_dragon_return_finds_signal(self):
        strategy = Head7DragonReturnStrategy()
        result = strategy.run_for_date("2026-04-07", _FakeContext(_dragon_return_history()))

        self.assertEqual(1, len(result))
        self.assertEqual("600001.SH", result[0].ts_code)
        self.assertIn("总分", result[0].reason)

    def test_head7_requires_extreme_shrink_below_one_third_of_base_volume(self):
        strategy = Head7DragonReturnStrategy()
        result = strategy.run_for_date(
            "2026-04-07",
            _FakeContext(_dragon_return_history(latest_vol=1650, latest_amount=3600)),
        )

        self.assertEqual([], result)

    def test_head7_requires_small_body_stabilization_candle(self):
        strategy = Head7DragonReturnStrategy()
        result = strategy.run_for_date(
            "2026-04-07",
            _FakeContext(
                _dragon_return_history(
                    latest_open=10.40,
                    latest_high=10.90,
                    latest_low=10.38,
                    latest_close=10.84,
                    latest_pct_chg=0.84,
                    latest_vol=1200,
                    latest_amount=2600,
                )
            ),
        )

        self.assertEqual([], result)

    def test_head7_main_board_rejects_pullback_below_half_body_floor(self):
        strategy = Head7DragonReturnStrategy()
        result = strategy.run_for_date(
            "2026-04-07",
            _FakeContext(
                _dragon_return_history(
                    latest_open=10.72,
                    latest_high=10.82,
                    latest_low=10.44,
                    latest_close=10.76,
                    latest_pct_chg=0.19,
                    latest_vol=1200,
                    latest_amount=2600,
                )
            ),
        )

        self.assertEqual([], result)

    def test_single_yang_hold_finds_breakout_signal(self):
        strategy = SingleYangHoldStrategy()
        result = strategy.run_for_date("2026-04-07", _FakeContext(_single_yang_history()))

        self.assertEqual(1, len(result))
        self.assertEqual("600002.SH", result[0].ts_code)
        self.assertIn("突破平台高点", result[0].reason)

    def test_golden_eye_finds_signal(self):
        strategy = GoldenEyeStrategy()
        context = _FakeContext(_golden_eye_history())
        result = strategy.run_for_date("2026-03-30", context)

        self.assertEqual(1, len(result))
        self.assertEqual("600003.SH", result[0].ts_code)
        self.assertIn("眼角", result[0].reason)
        self.assertEqual(
            [
                ("2026-03-30", 1, None),
                ("2026-03-30", 140, ["600003.SH"]),
            ],
            context.load_calls,
        )


if __name__ == "__main__":
    unittest.main()
