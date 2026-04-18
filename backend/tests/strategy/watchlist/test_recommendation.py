import unittest

from strategy.watchlist.recommendation import (
    build_watch_recommendation,
    sort_watch_candidates,
)


def _sample_snapshot(**overrides):
    payload = {
        "ts_code": "300308.SZ",
        "close": 21.40,
        "pct_today": 2.8,
        "volume_ratio": 1.72,
        "turnover": 9.4,
        "net_mf_amount": 8200.0,
        "net_mf_ratio": 3.8,
        "factor_score": 68.0,
        "trend_factor": 71.0,
        "flow_factor": 66.0,
        "quality_factor": 63.0,
        "ma5": 21.05,
        "ma10": 20.72,
        "ma20": 20.18,
        "ma60": 18.94,
        "amount": 156000.0,
        "theme_alignment_score": 72.0,
        "market_regime": "strong",
    }
    payload.update(overrides)
    return payload


def _sample_levels(**overrides):
    payload = {
        "support_1": {"price": 20.95, "level_strength_score": 78.0, "distance_pct": 2.10},
        "support_2": {"price": 20.32, "level_strength_score": 69.0, "distance_pct": 5.05},
        "resistance_1": {
            "price": 21.68,
            "level_strength_score": 81.0,
            "distance_pct": 1.31,
            "break_confirm_price": 21.75,
            "expected_move_pct": 6.40,
        },
        "resistance_2": {"price": 22.48, "level_strength_score": 70.0, "distance_pct": 5.04},
        "fail_threshold_price": 20.78,
        "overhead_supply_score": 24.0,
    }
    payload.update(overrides)
    return payload


class WatchlistRecommendationTests(unittest.TestCase):
    def test_breakout_ready_candidate_scores_ahead_of_pullback_candidate(self):
        breakout_ready = build_watch_recommendation(
            snapshot=_sample_snapshot(),
            levels=_sample_levels(),
        )
        pullback_ready = build_watch_recommendation(
            snapshot=_sample_snapshot(
                ts_code="002463.SZ",
                close=18.12,
                pct_today=0.6,
                volume_ratio=1.08,
                turnover=4.8,
                net_mf_amount=2300.0,
                net_mf_ratio=1.1,
                factor_score=61.0,
                trend_factor=58.0,
                flow_factor=52.0,
                theme_alignment_score=55.0,
                market_regime="range",
            ),
            levels=_sample_levels(
                support_1={"price": 17.98, "level_strength_score": 84.0, "distance_pct": 0.77},
                resistance_1={
                    "price": 18.92,
                    "level_strength_score": 67.0,
                    "distance_pct": 4.42,
                    "break_confirm_price": 19.02,
                    "expected_move_pct": 4.10,
                },
                fail_threshold_price=17.72,
                overhead_supply_score=18.0,
            ),
        )

        ordered = sort_watch_candidates(
            [
                {"ts_code": "002463.SZ", "recommendation": pullback_ready},
                {"ts_code": "300308.SZ", "recommendation": breakout_ready},
            ]
        )

        self.assertEqual("A_BREAKOUT_READY", breakout_ready["state_bucket"])
        self.assertEqual("B_PULLBACK_READY", pullback_ready["state_bucket"])
        self.assertGreater(breakout_ready["breakout"]["score"], pullback_ready["breakout"]["score"])
        self.assertEqual("300308.SZ", ordered[0]["ts_code"])

    def test_breakout_payload_exposes_confirmation_and_failure_levels(self):
        recommendation = build_watch_recommendation(
            snapshot=_sample_snapshot(),
            levels=_sample_levels(),
        )

        self.assertEqual(21.75, recommendation["breakout"]["confirm_price"])
        self.assertEqual(20.78, recommendation["breakout"]["fail_price"])
        self.assertEqual(6.4, recommendation["breakout"]["expected_move_pct"])
        self.assertEqual(78.0, recommendation["levels"]["support_1"]["level_strength_score"])

    def test_sort_watch_candidates_prefers_higher_liquidity_when_primary_scores_tie(self):
        liquid = {
            "ts_code": "300502.SZ",
            "recommendation": {
                "state_bucket": "A_BREAKOUT_READY",
                "recommendation_score": 78.4,
                "breakout": {"score": 79.0},
                "entry_quality": {"score": 66.0},
                "risk_penalty_score": 18.0,
            },
            "amount": 220000.0,
            "pct": 3.4,
            "volume_ratio": 1.82,
        }
        thin = {
            "ts_code": "601138.SH",
            "recommendation": {
                "state_bucket": "A_BREAKOUT_READY",
                "recommendation_score": 78.4,
                "breakout": {"score": 79.0},
                "entry_quality": {"score": 66.0},
                "risk_penalty_score": 18.0,
            },
            "amount": 91000.0,
            "pct": 3.4,
            "volume_ratio": 1.20,
        }

        ordered = sort_watch_candidates([thin, liquid])

        self.assertEqual("300502.SZ", ordered[0]["ts_code"])


if __name__ == "__main__":
    unittest.main()
