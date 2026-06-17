from __future__ import annotations

import unittest

import pandas as pd

from etl.utils.kline_patterns import build_structural_price_levels


def _build_accelerating_uptrend_fixture() -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    price = 100.0
    for idx in range(30):
        price += 0.6 if idx < 20 else 2.2
        close = round(price, 2)
        low = round(close * (0.985 if idx < 20 else 0.992), 2)
        high = round(close * (1.015 if idx < 20 else 1.018), 2)
        open_price = round((close + low) / 2, 2)
        rows.append(
            {
                "trade_date": f"2026-03-{idx + 1:02d}",
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 100_000 + idx * 5_000,
            }
        )
    return pd.DataFrame(rows)


def _build_gap_breakout_fixture() -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    price = 98.0
    for idx in range(24):
        price += 0.35
        close = round(price, 2)
        rows.append(
            {
                "trade_date": f"2026-02-{idx + 1:02d}",
                "open": round(close * 0.997, 2),
                "high": round(close * 1.012, 2),
                "low": round(close * 0.988, 2),
                "close": close,
                "volume": 120_000 + idx * 1_500,
            }
        )

    breakout_days = [
        ("2026-02-25", 107.2, 109.4, 106.8, 108.9, 185_000),
        ("2026-02-26", 109.1, 110.2, 108.5, 109.8, 196_000),
        ("2026-02-27", 110.0, 111.5, 109.4, 110.9, 205_000),
        ("2026-02-28", 112.0, 117.4, 111.8, 116.6, 320_000),
    ]
    for trade_date, open_price, high, low, close, volume in breakout_days:
        rows.append(
            {
                "trade_date": trade_date,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )
    return pd.DataFrame(rows)


def _build_structural_resistance_fixture() -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    base_close = 9.62
    for idx in range(12):
        close = round(base_close + idx * 0.03, 2)
        open_price = round(close - 0.05, 2)
        high = round(close + 0.08, 2)
        low = round(open_price - 0.05, 2)
        rows.append(
            {
                "trade_date": f"2026-03-{idx + 1:02d}",
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 120_000 + idx * 4_000,
            }
        )

    rows.extend(
        [
            {"trade_date": "2026-03-13", "open": 9.98, "high": 10.20, "low": 9.94, "close": 10.18, "volume": 186_000},
            {"trade_date": "2026-03-14", "open": 10.06, "high": 10.12, "low": 9.99, "close": 10.04, "volume": 175_000},
            {"trade_date": "2026-03-15", "open": 10.03, "high": 10.19, "low": 9.97, "close": 10.02, "volume": 168_000},
            {"trade_date": "2026-03-16", "open": 10.00, "high": 10.10, "low": 9.93, "close": 9.97, "volume": 162_000},
            {"trade_date": "2026-03-17", "open": 9.98, "high": 10.18, "low": 9.92, "close": 10.00, "volume": 171_000},
            {"trade_date": "2026-03-18", "open": 9.99, "high": 10.08, "low": 9.94, "close": 10.01, "volume": 158_000},
            {"trade_date": "2026-03-19", "open": 10.00, "high": 10.07, "low": 9.95, "close": 10.03, "volume": 154_000},
            {"trade_date": "2026-03-20", "open": 10.01, "high": 10.09, "low": 9.96, "close": 10.04, "volume": 152_000},
        ]
    )
    return pd.DataFrame(rows)


def _build_structural_support_fixture() -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    base_close = 10.55
    for idx in range(10):
        close = round(base_close - idx * 0.05, 2)
        open_price = round(close + 0.04, 2)
        high = round(open_price + 0.05, 2)
        low = round(close - 0.06, 2)
        rows.append(
            {
                "trade_date": f"2026-04-{idx + 1:02d}",
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 110_000 - idx * 2_000,
            }
        )

    rows.extend(
        [
            {"trade_date": "2026-04-11", "open": 10.05, "high": 10.08, "low": 9.96, "close": 9.98, "volume": 102_000},
            {"trade_date": "2026-04-12", "open": 9.99, "high": 10.03, "low": 9.97, "close": 10.01, "volume": 98_000},
            {"trade_date": "2026-04-13", "open": 10.00, "high": 10.02, "low": 9.97, "close": 9.99, "volume": 96_000},
            {"trade_date": "2026-04-14", "open": 10.02, "high": 10.04, "low": 9.98, "close": 10.00, "volume": 94_000},
            {"trade_date": "2026-04-15", "open": 10.01, "high": 10.02, "low": 9.97, "close": 9.99, "volume": 95_000},
            {"trade_date": "2026-04-16", "open": 9.99, "high": 10.03, "low": 9.97, "close": 10.00, "volume": 97_000},
            {"trade_date": "2026-04-17", "open": 9.98, "high": 10.08, "low": 9.96, "close": 10.06, "volume": 135_000},
            {"trade_date": "2026-04-18", "open": 10.05, "high": 10.11, "low": 10.02, "close": 10.09, "volume": 142_000},
        ]
    )
    return pd.DataFrame(rows)


def _build_capital_flow_support_fixture() -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    price = 10.0
    start_date = pd.Timestamp("2026-04-01")
    for idx in range(36):
        if idx < 24:
            price += 0.02
        else:
            price += 0.05
        close = round(price, 2)
        low = round(close - 0.10, 2)
        high = round(close + 0.12, 2)
        net_flow = 800.0
        if idx in (25, 26, 27):
            low = 10.28 if idx == 25 else 10.32
            net_flow = 9500.0 + idx * 100
        rows.append(
            {
                "trade_date": (start_date + pd.Timedelta(days=idx)).strftime("%Y-%m-%d"),
                "open": round(close - 0.03, 2),
                "high": high,
                "low": low,
                "close": close,
                "volume": 130_000 + idx * 2_000,
                "main_net_flow": net_flow,
                "turnover_rate": 8.0,
            }
        )
    return pd.DataFrame(rows)


def _build_fibonacci_resonance_fixture() -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    start_date = pd.Timestamp("2026-01-01")
    for idx in range(45):
        close = round(10.0 + idx * 0.23, 2)
        rows.append(
            {
                "trade_date": (start_date + pd.Timedelta(days=idx)).strftime("%Y-%m-%d"),
                "open": round(close - 0.05, 2),
                "high": round(close + 0.18, 2),
                "low": round(close - 0.16, 2),
                "close": close,
                "volume": 100_000 + idx * 1_000,
            }
        )
    pullback = [
        ("2026-02-15", 20.35, 20.55, 19.85, 20.05, 165_000),
        ("2026-02-16", 19.50, 19.72, 18.90, 19.05, 150_000),
        ("2026-02-17", 18.35, 18.62, 17.88, 18.05, 148_000),
        ("2026-02-18", 17.20, 17.55, 16.68, 16.92, 190_000),
        ("2026-02-19", 16.45, 16.82, 16.10, 16.32, 210_000),
        ("2026-02-20", 16.25, 16.70, 16.08, 16.42, 205_000),
    ]
    for trade_date, open_price, high, low, close, volume in pullback:
        rows.append(
            {
                "trade_date": trade_date,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "turnover_rate": 16.5,
            }
        )
    return pd.DataFrame(rows)


class WatchlistLevelTests(unittest.TestCase):
    def test_adaptive_support_prefers_nearby_trend_level_in_accelerating_uptrend(self) -> None:
        bundle = build_structural_price_levels(
            _build_accelerating_uptrend_fixture(),
            top_n=2,
            market_board="gem",
        )

        support_levels = bundle["support_levels"]
        self.assertGreaterEqual(len(support_levels), 2)

        primary = support_levels[0]
        secondary = support_levels[1]

        self.assertGreaterEqual(primary["price"], 129.0)
        self.assertIsNotNone(primary["distance_atr"])
        self.assertLess(primary["distance_atr"], 1.2)
        self.assertLess(secondary["price"], primary["price"])

    def test_adaptive_support_adds_near_term_guard_on_breakout_days(self) -> None:
        bundle = build_structural_price_levels(
            _build_gap_breakout_fixture(),
            top_n=2,
            market_board="star",
        )

        support_levels = bundle["support_levels"]
        self.assertTrue(support_levels)

        primary = support_levels[0]
        close = 116.6
        expected_distance_pct = round((close - primary["price"]) / close * 100.0, 2)

        self.assertEqual(primary["source"], "近端波动防守位")
        self.assertGreaterEqual(primary["price"], 109.0)
        self.assertLessEqual(primary["price"], 115.0)
        self.assertIsNotNone(primary["distance_atr"])
        self.assertLessEqual(primary["distance_atr"], 1.0)
        self.assertEqual(primary["distance_pct"], expected_distance_pct)

    def test_adaptive_resistance_includes_structural_three_touch_source(self) -> None:
        bundle = build_structural_price_levels(
            _build_structural_resistance_fixture(),
            top_n=2,
            market_board="default",
        )

        resistance_levels = bundle["resistance_levels"]
        self.assertTrue(resistance_levels)

        primary = resistance_levels[0]
        self.assertGreaterEqual(primary["price"], 10.15)
        self.assertLessEqual(primary["price"], 10.22)
        self.assertNotEqual(primary.get("dominant_family"), "anchor")
        self.assertGreaterEqual(primary.get("source_resonance") or 0, 4)
        self.assertIn("trendline", primary.get("families", []))
        self.assertIn("structure", primary.get("families", []))
        self.assertTrue(
            any(str(source).startswith("STRUCT_RESISTANCE") for source in primary.get("sources", []))
        )
        self.assertIn("不再只由单根K线开收盘锚点决定", primary.get("note", ""))

    def test_adaptive_support_includes_structural_four_touch_source(self) -> None:
        bundle = build_structural_price_levels(
            _build_structural_support_fixture(),
            top_n=2,
            market_board="default",
        )

        support_levels = bundle["support_levels"]
        self.assertTrue(support_levels)

        primary = support_levels[0]
        self.assertGreaterEqual(primary["price"], 9.96)
        self.assertLessEqual(primary["price"], 10.02)
        self.assertTrue(
            any(str(source).startswith("STRUCT_SUPPORT") for source in primary.get("sources", []))
        )

    def test_capital_flow_inflow_zone_is_selected_as_support_source(self) -> None:
        bundle = build_structural_price_levels(
            _build_capital_flow_support_fixture(),
            top_n=3,
            market_board="default",
        )

        support_levels = bundle["support_levels"]
        self.assertTrue(support_levels)
        self.assertTrue(
            any(
                any(str(source).startswith("CAPITAL_IN_SUPPORT") for source in level.get("sources", []))
                for level in support_levels
            )
        )

    def test_fibonacci_resonance_raises_confidence_floor(self) -> None:
        bundle = build_structural_price_levels(
            _build_fibonacci_resonance_fixture(),
            top_n=3,
            market_board="default",
        )

        fib_levels = [
            level
            for level in bundle["support_levels"]
            if "fibonacci" in set(level.get("families", []))
        ]
        self.assertTrue(fib_levels)
        self.assertGreaterEqual(fib_levels[0].get("confidence") or 0.0, 0.85)


if __name__ == "__main__":
    unittest.main()
