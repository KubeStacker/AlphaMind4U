import json
import sys
import types
import unittest

import pandas as pd
from fastapi import HTTPException

_pypinyin = types.ModuleType("pypinyin")
_pypinyin.lazy_pinyin = lambda value, style=None: []


class _Style:
    FIRST_LETTER = "FIRST_LETTER"


_pypinyin.Style = _Style
sys.modules.setdefault("pypinyin", _pypinyin)

import api.routes.ai as ai


class _FakeResponse:
    def __init__(self, text: str, headers=None):
        self.text = text
        self.content = text.encode("utf-8")
        self.headers = headers or {}

    def json(self):
        return json.loads(self.text)


class AIAnalysisPromptTests(unittest.TestCase):
    def test_default_prompt_includes_sector_and_pattern_factor_sections(self):
        prompt = ai.DEFAULT_ANALYSIS_USER_PROMPT

        self.assertIn("{stock_snapshot}", prompt)
        self.assertIn("{capital_flow_snapshot}", prompt)
        self.assertIn("{sector_context}", prompt)
        self.assertIn("{holding_context}", prompt)
        self.assertIn("{pattern_factor_snapshot}", prompt)
        self.assertIn("{commentary_snapshot}", prompt)
        self.assertNotIn("{market_context}", prompt)
        self.assertNotIn("市场环境", prompt)

    def test_sanitize_template_keeps_sector_context_and_adds_pattern_factor_snapshot(self):
        sanitized = ai._sanitize_template_content(
            "\n".join(
                [
                    "请基于以下资料做交易判断。",
                    "",
                    "### 标的概览",
                    "{stock_snapshot}",
                    "",
                    "### 板块与概念共振",
                    "{sector_context}",
                ]
            )
        )

        self.assertIn("{stock_snapshot}", sanitized)
        self.assertIn("{capital_flow_snapshot}", sanitized)
        self.assertIn("{sector_context}", sanitized)
        self.assertIn("{holding_context}", sanitized)
        self.assertIn("{pattern_factor_snapshot}", sanitized)
        self.assertIn("{commentary_snapshot}", sanitized)
        self.assertNotIn("{market_context}", sanitized)
        self.assertNotIn("### 市场环境", sanitized)

    def test_market_profile_uses_board_specific_limit_and_k(self):
        main_board = ai._infer_market_profile("600519.SH")
        growth_board = ai._infer_market_profile("300308.SZ")

        self.assertEqual("main_board", main_board["board_key"])
        self.assertEqual(9.8, main_board["limit_up_pct"])
        self.assertEqual(1.0, main_board["k"])
        self.assertEqual("growth_board", growth_board["board_key"])
        self.assertEqual(19.8, growth_board["limit_up_pct"])
        self.assertEqual(2.0, growth_board["k"])

    def test_pattern_factor_snapshot_mentions_board_k_and_three_core_strategies(self):
        analysis_df = ai._prepare_analysis_df(
            pd.DataFrame(
                [
                    {"trade_date": "2026-03-30", "open": 10.0, "high": 10.2, "low": 9.9, "close": 10.0, "vol": 1000, "amount": 10000, "pct_chg": 0.0},
                    {"trade_date": "2026-03-31", "open": 10.0, "high": 11.1, "low": 9.95, "close": 11.0, "vol": 8000, "amount": 88000, "pct_chg": 10.0},
                    {"trade_date": "2026-04-01", "open": 10.9, "high": 11.0, "low": 10.7, "close": 10.85, "vol": 4200, "amount": 46000, "pct_chg": -1.36},
                    {"trade_date": "2026-04-02", "open": 10.82, "high": 10.9, "low": 10.65, "close": 10.78, "vol": 2800, "amount": 30100, "pct_chg": -0.65},
                    {"trade_date": "2026-04-03", "open": 10.8, "high": 10.88, "low": 10.7, "close": 10.82, "vol": 2400, "amount": 25900, "pct_chg": 0.37},
                    {"trade_date": "2026-04-06", "open": 10.83, "high": 10.9, "low": 10.72, "close": 10.84, "vol": 2100, "amount": 22800, "pct_chg": 0.18},
                    {"trade_date": "2026-04-07", "open": 10.85, "high": 10.92, "low": 10.74, "close": 10.86, "vol": 1900, "amount": 20600, "pct_chg": 0.18},
                    {"trade_date": "2026-04-08", "open": 10.88, "high": 11.02, "low": 10.8, "close": 10.98, "vol": 2600, "amount": 28500, "pct_chg": 1.10},
                ]
            )
        )

        snapshot = ai._build_pattern_factor_snapshot(
            analysis_df,
            ts_code="600519.SH",
            sector_meta={
                "mapped_sector": "算力基建",
                "display_name": "算力基建",
                "focus_tags": ["液冷", "高速连接"],
                "sector_heat_rank": 2,
                "sector_heat_total": 8,
                "is_mainline": True,
            },
        )

        self.assertIn("板块系数", snapshot)
        self.assertIn("K=1.0", snapshot)
        self.assertIn("头7龙回头", snapshot)
        self.assertIn("单阳不破", snapshot)
        self.assertIn("大眼睛/空中加油", snapshot)

    def test_commentary_snapshot_keeps_objective_fields_only(self):
        snapshot = ai._build_commentary_snapshot(
            pd.DataFrame(
                [
                    {
                        "turnover_rate": 2.51,
                        "volume_ratio": 1.09,
                        "net_mf_ratio": 3.25,
                        "big_order_ratio": 1.12,
                        "rps_20": 96.4,
                        "trend_score": 78.0,
                        "factor_score": 81.5,
                    }
                ]
            )
        )

        self.assertIn("换手率 2.51%", snapshot)
        self.assertIn("量比 1.09", snapshot)
        self.assertIn("主力净额占比 +3.25%", snapshot)
        self.assertNotIn("RPS_20", snapshot)
        self.assertNotIn("trend_score", snapshot)
        self.assertNotIn("factor_score", snapshot)

    def test_price_snapshot_formats_daily_volume_and_amount_with_correct_units(self):
        snapshot, _ = ai._build_price_snapshot(
            pd.DataFrame(
                [
                    {
                        "trade_date": "2026-04-08",
                        "open": 273.98,
                        "high": 285.90,
                        "low": 270.05,
                        "close": 285.88,
                        "pct_chg": 7.93,
                        "volume": 457691.27,
                        "amount": 12781562.886,
                        "ma5": 265.74,
                        "ma10": 263.44,
                        "ma20": 271.00,
                        "ma60": 275.12,
                    }
                ]
            )
        )

        self.assertIn("成交量 45.77万手", snapshot)
        self.assertIn("成交额 127.82亿", snapshot)
        self.assertNotIn("成交额 0.13亿", snapshot)

    def test_parse_ai_response_raises_http_502_for_non_json_body(self):
        response = _FakeResponse(
            "<html><title>bad gateway</title></html>",
            headers={"content-type": "text/html"},
        )

        with self.assertRaises(HTTPException) as ctx:
            ai._parse_ai_response_json(response, model_provider="deepseek", model="deepseek-reasoner")

        self.assertEqual(502, ctx.exception.status_code)
        self.assertIn("非 JSON", ctx.exception.detail)


if __name__ == "__main__":
    unittest.main()
