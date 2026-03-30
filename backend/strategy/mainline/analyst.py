# /backend/strategy/mainline/analyst.py

import json
import logging
import re
from collections import defaultdict
from functools import lru_cache

import arrow
import numpy as np
import pandas as pd

from core.constants import CONCEPT_BLACKLIST
from db.connection import fetch_df, get_db_connection
from .config import (
    CATEGORY_WEIGHTS,
    CONCEPT_MAPPING,
    CONCEPT_NOISE_PATTERNS,
    DRIVER_DISPLAY_MODE,
    DRIVER_DOMINANCE_THRESHOLD,
    DRIVER_REPLACE_SECTOR_MIN_SCORE_SHARE,
    DRIVER_REPLACE_SECTOR_MIN_SUPPORT_COUNT,
    DRIVER_REPLACE_SECTOR_MIN_SUPPORT_RATIO,
    INDUSTRY_ANCHOR_RULES,
    SECTOR_GENERIC_TAGS,
)

logger = logging.getLogger(__name__)


class MainlineAnalyst:
    """
    主线分析器。

    核心思路：
    1. 先把概念/行业映射到更稳定的产业主题。
    2. 事件型、一次性概念不直接充当主线，优先回退到行业主题。
    3. 主线选取不仅看单日得分，也看近阶段持续性、强度和龙头活跃度。
    """

    def __init__(self, concept_mapping=None, category_weights=None, noise_patterns=None):
        self.concept_mapping = concept_mapping if concept_mapping else CONCEPT_MAPPING
        self.category_weights = category_weights if category_weights else CATEGORY_WEIGHTS
        self.noise_patterns = noise_patterns if noise_patterns else CONCEPT_NOISE_PATTERNS
        self.industry_anchor_rules = INDUSTRY_ANCHOR_RULES
        self.sector_generic_tags = SECTOR_GENERIC_TAGS
        self._concept_score_cache = {}
        self._sector_mapping_cache = {}
        self._compiled_noise_patterns = [re.compile(p, re.IGNORECASE) for p in self.noise_patterns]
        self._prepared_industry_anchor_rules = self._prepare_industry_anchor_rules()
        self._prepared_sector_generic_tags = self._prepare_sector_generic_tags()

    def invalidate_cache(self):
        self._concept_score_cache.clear()
        self._sector_mapping_cache.clear()
        self.analyze.cache_clear()
        self.get_history.cache_clear()
        self._load_concept_snapshot.cache_clear()
        self._load_stock_basic_snapshot.cache_clear()
        self._load_tag_snapshot.cache_clear()
        self._get_stock_mainline_map_snapshot.cache_clear()

    @lru_cache(maxsize=1)
    def _load_concept_snapshot(self) -> pd.DataFrame:
        concept_df = fetch_df(
            """
            SELECT DISTINCT ts_code, concept_name
            FROM stock_concept_details
            WHERE concept_name IS NOT NULL
            """
        )
        if concept_df.empty:
            return pd.DataFrame(columns=["ts_code", "concept_name"])

        concept_df["ts_code"] = concept_df["ts_code"].astype(str).str.strip()
        concept_df["concept_name"] = concept_df["concept_name"].astype(str).str.strip()
        concept_df = concept_df[(concept_df["ts_code"] != "") & (concept_df["concept_name"] != "")]
        return concept_df.drop_duplicates().reset_index(drop=True)

    @lru_cache(maxsize=1)
    def _load_stock_basic_snapshot(self) -> pd.DataFrame:
        stock_basic_df = fetch_df(
            """
            SELECT ts_code, industry
            FROM stock_basic
            """
        )
        if stock_basic_df.empty:
            return pd.DataFrame(columns=["ts_code", "industry"])

        stock_basic_df["ts_code"] = stock_basic_df["ts_code"].astype(str).str.strip()
        stock_basic_df["industry"] = stock_basic_df["industry"].fillna("").astype(str).str.strip()
        stock_basic_df = stock_basic_df[stock_basic_df["ts_code"] != ""]
        return stock_basic_df.drop_duplicates(subset=["ts_code"]).reset_index(drop=True)

    @lru_cache(maxsize=1)
    def _load_tag_snapshot(self) -> pd.DataFrame:
        concept_df = self._load_concept_snapshot()
        if concept_df.empty:
            return pd.DataFrame(columns=["ts_code", "tag_name", "tag_type"])

        tag_df = concept_df.rename(columns={"concept_name": "tag_name"}).copy()
        tag_df["tag_type"] = "concept"
        return tag_df[["ts_code", "tag_name", "tag_type"]].drop_duplicates().reset_index(drop=True)

    @lru_cache(maxsize=1)
    def _get_stock_mainline_map_snapshot(self) -> pd.DataFrame:
        return self._build_stock_mainline_map("", "")

    def refresh_recent_scores(self, days: int = 30) -> int:
        date_df = fetch_df(
            """
            SELECT trade_date
            FROM daily_price
            GROUP BY trade_date
            HAVING COUNT(*) > 1000
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            params=[max(1, int(days))],
        )
        if date_df.empty:
            return 0

        refreshed = 0
        for trade_date in reversed(pd.to_datetime(date_df["trade_date"]).tolist()):
            self.save_results(trade_date.strftime("%Y-%m-%d"))
            refreshed += 1
        self.invalidate_cache()
        return refreshed

    def _clean_concept_name(self, concept_name: str) -> str:
        if not concept_name:
            return ""
        cleaned = str(concept_name).strip()
        cleaned = cleaned.replace("_THS", "")
        cleaned = cleaned.replace("（", "(").replace("）", ")")
        for token in ("概念股", "概念", "题材", "板块", "指数", "产业链", "同花顺"):
            cleaned = cleaned.replace(token, "")
        cleaned = re.sub(r"[\s/,_\-]+", "", cleaned)
        return cleaned.strip()

    def _is_noise_concept(self, concept_name: str) -> bool:
        cleaned = self._clean_concept_name(concept_name)
        if not cleaned:
            return True
        return any(pattern.search(cleaned) for pattern in self._compiled_noise_patterns)

    def _get_concept_scores(self, concept_name: str):
        """为原始概念/行业打分，映射到上层可交易主题。"""
        cleaned_name = self._clean_concept_name(concept_name)
        cache_key = cleaned_name.upper()
        if cache_key in self._concept_score_cache:
            return self._concept_score_cache[cache_key]

        if (
            not cleaned_name
            or cleaned_name in CONCEPT_BLACKLIST
            or self._is_noise_concept(cleaned_name)
        ):
            self._concept_score_cache[cache_key] = []
            return []

        normalized_name = cleaned_name.upper()
        scores = []
        for sector, keywords in self.concept_mapping.items():
            weight = float(self.category_weights.get(sector, 1.0))
            sector_key = self._clean_concept_name(sector).upper()
            match_score = 0.0

            if sector_key and sector_key in normalized_name:
                match_score = len(sector_key) * 2.0

            keyword_score = 0.0
            for keyword in keywords:
                keyword_key = self._clean_concept_name(keyword).upper()
                if not keyword_key or keyword_key not in normalized_name:
                    continue
                boost = 1.15 if (
                    normalized_name.startswith(keyword_key) or normalized_name.endswith(keyword_key)
                ) else 1.0
                keyword_score = max(keyword_score, len(keyword_key) * 1.5 * boost)

            final_score = max(match_score, keyword_score) * weight
            if final_score > 0:
                scores.append({"sector": sector, "score": round(final_score, 4)})

        scores.sort(key=lambda item: item["score"], reverse=True)
        self._concept_score_cache[cache_key] = scores
        return scores

    def _get_mapped_concept(self, original_concept: str) -> str:
        cleaned = self._clean_concept_name(original_concept)
        scores = self._get_concept_scores(cleaned)
        if not scores:
            return "" if self._is_noise_concept(cleaned) else (cleaned or str(original_concept or ""))
        return scores[0]["sector"]

    def _prepare_industry_anchor_rules(self) -> dict:
        prepared = {}
        for sector, rule in (self.industry_anchor_rules or {}).items():
            industry_keywords = []
            for item in rule.get("industry_keywords", []):
                cleaned = self._clean_concept_name(item).upper()
                if cleaned:
                    industry_keywords.append(cleaned)

            concept_keywords = []
            for item in rule.get("concept_keywords", []):
                cleaned = self._clean_concept_name(item).upper()
                if cleaned:
                    concept_keywords.append(cleaned)

            prepared[sector] = {
                "industry_keywords": industry_keywords,
                "concept_keywords": concept_keywords,
                "anchor_score": float(rule.get("anchor_score", 0.0)),
                "remap_score": float(rule.get("remap_score", 0.0)),
            }
        return prepared

    def _prepare_sector_generic_tags(self) -> dict:
        prepared = {}
        for sector, tags in (self.sector_generic_tags or {}).items():
            cleaned_tags = set()
            for tag in tags or []:
                cleaned = self._clean_concept_name(tag).upper()
                if cleaned:
                    cleaned_tags.add(cleaned)

            sector_clean = self._clean_concept_name(sector).upper()
            if sector_clean:
                cleaned_tags.add(sector_clean)

            prepared[sector] = cleaned_tags
        return prepared

    def _get_industry_anchor(self, industry: str):
        cleaned = self._clean_concept_name(industry)
        if not cleaned:
            return None

        scores = self._get_concept_scores(cleaned)
        if not scores:
            return None

        best = scores[0]
        cleaned_upper = cleaned.upper()
        sector = best["sector"]
        anchor_score = max(float(best["score"]) * 0.4, 5.0)
        rule = self._prepared_industry_anchor_rules.get(sector, {})

        concept_keywords = []
        remap_score = 0.0
        if rule and any(keyword in cleaned_upper for keyword in rule.get("industry_keywords", [])):
            anchor_score = max(anchor_score, float(rule.get("anchor_score", 0.0)))
            concept_keywords = rule.get("concept_keywords", [])
            remap_score = float(rule.get("remap_score", 0.0))

        return {
            "sector": sector,
            "industry": cleaned,
            "anchor_score": round(anchor_score, 4),
            "concept_keywords": concept_keywords,
            "remap_score": remap_score,
        }

    def _apply_industry_anchor(self, concept_name: str, best: dict, anchor: dict | None):
        if not anchor:
            return best["sector"], float(best["score"])

        cleaned = self._clean_concept_name(concept_name).upper()
        if any(keyword in cleaned for keyword in anchor.get("concept_keywords", [])):
            return anchor["sector"], max(float(best["score"]), float(anchor.get("remap_score", 0.0)))

        if best["sector"] == anchor["sector"]:
            boosted = float(best["score"]) + float(anchor.get("anchor_score", 0.0)) * 0.15
            return best["sector"], boosted

        return best["sector"], float(best["score"])

    def _get_industry_evidence_score(self, industry: str, anchor: dict | None = None) -> float:
        scores = self._get_concept_scores(industry)
        best_score = float(scores[0]["score"]) if scores else 0.0
        base_score = max(best_score * 1.35, 12.0 if best_score > 0 else 0.0)
        if anchor:
            base_score = max(base_score, float(anchor.get("anchor_score", 0.0)))
        return round(base_score, 4)

    def _resolve_tag_sector(self, tag_name: str, tag_type: str = "concept") -> str:
        cleaned = self._clean_concept_name(tag_name)
        if not cleaned or cleaned in CONCEPT_BLACKLIST or self._is_noise_concept(cleaned):
            return ""

        cache_key = f"{tag_type}:{cleaned.upper()}"
        cached = self._sector_mapping_cache.get(cache_key)
        if cached is not None:
            return cached

        if tag_type == "industry":
            anchor = self._get_industry_anchor(cleaned)
            resolved = anchor["sector"] if anchor else self._get_mapped_concept(cleaned)
            self._sector_mapping_cache[cache_key] = resolved
            return resolved

        scores = self._get_concept_scores(cleaned)
        resolved = scores[0]["sector"] if scores else ""
        self._sector_mapping_cache[cache_key] = resolved
        return resolved

    def _get_tag_priority_multiplier(self, sector_name: str, tag_name: str, tag_type: str) -> float:
        cleaned = self._clean_concept_name(tag_name).upper()
        multiplier = 1.0

        if cleaned in self._prepared_sector_generic_tags.get(sector_name, set()):
            multiplier *= 0.42

        if tag_type == "industry":
            multiplier *= 0.94
        else:
            multiplier *= 1.08

        return multiplier

    def _decayed_sector_score(self, scores: list[float]) -> float:
        if not scores:
            return 0.0

        weights = [1.0, 0.6, 0.35, 0.2, 0.12]
        total = 0.0
        ordered_scores = sorted((float(score) for score in scores if score is not None), reverse=True)
        for idx, score in enumerate(ordered_scores):
            if idx < len(weights):
                weight = weights[idx]
            else:
                weight = weights[-1] * (0.65 ** (idx - len(weights) + 1))
            total += score * weight
        return round(total, 4)

    def _build_generic_sector_override_mask(self, merged: pd.DataFrame) -> pd.Series:
        if merged.empty:
            return pd.Series(dtype=bool)

        concept_weight = merged["mapped_name"].map(self.category_weights).fillna(1.0)
        fallback_weight = merged["mapped_name_fallback"].map(self.category_weights).fillna(1.0)
        min_required_score = np.maximum(
            merged["fallback_score"] * 1.2,
            merged["fallback_score"] + 3.5,
        )
        return (
            merged["mapped_name"].notna()
            & merged["mapped_name_fallback"].notna()
            & (merged["mapped_name"] != merged["mapped_name_fallback"])
            & (merged["support_count"] >= 1)
            & (concept_weight >= 2.5)
            & (fallback_weight <= 1.9)
            & ((concept_weight - fallback_weight) >= 0.7)
            & (merged["map_score"] >= min_required_score)
        )

    def _resolve_trade_window(self, days: int, trade_date: str | None = None):
        if trade_date:
            dates_df = fetch_df(
                """
                SELECT trade_date
                FROM daily_price
                WHERE trade_date <= ?
                GROUP BY trade_date
                HAVING COUNT(*) > 1000
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                params=[trade_date, days],
            )
        else:
            dates_df = fetch_df(
                """
                SELECT trade_date
                FROM daily_price
                GROUP BY trade_date
                HAVING COUNT(*) > 1000
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                params=[days],
            )

        if dates_df.empty:
            return [], None, None

        recent_dates = sorted(pd.to_datetime(dates_df["trade_date"]).tolist())
        min_date = recent_dates[0].strftime("%Y-%m-%d")
        max_date = recent_dates[-1].strftime("%Y-%m-%d")
        return recent_dates, min_date, max_date

    def get_stock_mainline_map(
        self, min_date: str | None = None, max_date: str | None = None, ts_codes: list[str] | None = None
    ) -> pd.DataFrame:
        df = self._get_stock_mainline_map_snapshot().copy()
        if ts_codes:
            df = df[df["ts_code"].isin(ts_codes)]
        return df.reset_index(drop=True)

    def _build_stock_mainline_map(self, min_date: str, max_date: str) -> pd.DataFrame:
        """
        为每只股票构建一个稳定的主线归属：
        1. 优先对概念做主题聚合，同一主题的多条概念证据会累加。
        2. 事件类/杂项概念不直接作为主线，避免噪声抢占排名。
        3. 用行业锚点纠偏多主题冲突，例如电力运营类公司优先归入电力公用。
        4. 若概念不足，则回退到 stock_basic.industry。
        """
        concept_df = self._load_concept_snapshot()

        concept_evidence = {}
        concept_supports = {}
        fallback_df = self._load_stock_basic_snapshot()
        fallback_rows = []
        industry_anchor_map = {}
        if not fallback_df.empty:
            for _, row in fallback_df.iterrows():
                ts_code = row["ts_code"]
                industry = str(row["industry"]).strip()
                if not industry:
                    continue

                anchor = self._get_industry_anchor(industry)
                mapped = anchor["sector"] if anchor else self._get_mapped_concept(industry)
                if anchor:
                    industry_anchor_map[ts_code] = anchor

                if not mapped:
                    fallback_rows.append({"ts_code": ts_code, "mapped_name": industry, "fallback_score": 0.0})
                    continue

                industry_score = self._get_industry_evidence_score(industry, anchor=anchor)
                fallback_rows.append(
                    {
                        "ts_code": ts_code,
                        "mapped_name": mapped,
                        "fallback_score": industry_score,
                    }
                )
                if industry_score <= 0:
                    continue

                key = (ts_code, mapped)
                concept_evidence.setdefault(key, []).append(industry_score)
                concept_supports.setdefault(key, set()).add(f"industry:{industry}")

        if not concept_df.empty:
            for _, row in concept_df.iterrows():
                ts_code = row["ts_code"]
                concept_name = str(row["concept_name"]).strip()
                cleaned = self._clean_concept_name(concept_name)
                if (
                    not cleaned
                    or cleaned in CONCEPT_BLACKLIST
                    or self._is_noise_concept(cleaned)
                ):
                    continue

                scores = self._get_concept_scores(cleaned)
                if not scores:
                    continue

                best = scores[0]
                resolved_sector, resolved_score = self._apply_industry_anchor(
                    concept_name=cleaned,
                    best=best,
                    anchor=industry_anchor_map.get(ts_code),
                )
                key = (ts_code, resolved_sector)
                concept_evidence.setdefault(key, []).append(float(resolved_score))
                concept_supports.setdefault(key, set()).add(concept_name)

        concept_rows = []
        for (ts_code, sector), evidence_scores in concept_evidence.items():
            supports = sorted(concept_supports.get((ts_code, sector), set()))
            concept_rows.append(
                {
                    "ts_code": ts_code,
                    "mapped_name": sector,
                    "map_score": self._decayed_sector_score(evidence_scores),
                    "support_count": len(supports),
                }
            )

        df_map = (
            pd.DataFrame(concept_rows)
            if concept_rows
            else pd.DataFrame(columns=["ts_code", "mapped_name", "map_score", "support_count"])
        )
        if not df_map.empty:
            df_map = (
                df_map.sort_values(
                    ["ts_code", "map_score", "support_count"],
                    ascending=[True, False, False],
                )
                .drop_duplicates(subset=["ts_code"])
            )

        fallback_map = (
            pd.DataFrame(fallback_rows)
            if fallback_rows
            else pd.DataFrame(columns=["ts_code", "mapped_name", "fallback_score"])
        )

        if df_map.empty:
            return fallback_map[["ts_code", "mapped_name"]].drop_duplicates(subset=["ts_code"])
        if fallback_map.empty:
            return df_map[["ts_code", "mapped_name"]].drop_duplicates(subset=["ts_code"])

        merged = fallback_map.merge(df_map, on="ts_code", how="left", suffixes=("_fallback", ""))
        merged["fallback_score"] = merged["fallback_score"].fillna(0.0)
        merged["map_score"] = merged["map_score"].fillna(0.0)
        merged["support_count"] = merged["support_count"].fillna(0).astype(int)
        generic_sector_override = self._build_generic_sector_override_mask(merged)
        concept_wins = (
            merged["mapped_name"].notna()
            & (
                merged["mapped_name_fallback"].isna()
                | (merged["mapped_name"] == merged["mapped_name_fallback"])
                | (merged["support_count"] >= 2)
                | (merged["map_score"] >= merged["fallback_score"] * 1.65)
                | generic_sector_override
            )
        )
        merged["mapped_name"] = np.where(
            concept_wins,
            merged["mapped_name"],
            merged["mapped_name_fallback"],
        )
        merged = merged[["ts_code", "mapped_name"]].drop_duplicates(subset=["ts_code"])
        return merged

    def _load_mainline_market_data(self, min_date: str, max_date: str) -> pd.DataFrame:
        stock_map = self.get_stock_mainline_map(min_date=min_date, max_date=max_date)
        if stock_map.empty:
            return pd.DataFrame()

        market_df = fetch_df(
            """
            SELECT p.trade_date, p.ts_code, p.pct_chg, p.amount, b.name AS stock_name, b.industry,
                   COALESCE(m.net_mf_amount, 0) AS net_mf_amount
            FROM daily_price p
            LEFT JOIN stock_moneyflow m
              ON p.ts_code = m.ts_code AND p.trade_date = m.trade_date
            LEFT JOIN stock_basic b
              ON p.ts_code = b.ts_code
            WHERE p.trade_date BETWEEN ? AND ?
            """,
            params=[min_date, max_date],
        )
        if market_df.empty:
            return pd.DataFrame()

        market_df["trade_date"] = pd.to_datetime(market_df["trade_date"])
        merged = market_df.merge(stock_map, on="ts_code", how="left")
        merged = merged[merged["mapped_name"].notna()].copy()
        return merged

    def _build_grouped_scores(self, merged: pd.DataFrame) -> pd.DataFrame:
        if merged.empty:
            return pd.DataFrame()

        grouped = (
            merged.groupby(["trade_date", "mapped_name"])
            .agg(
                avg_ret=("pct_chg", "mean"),
                total_amt=("amount", "sum"),
                stock_count=("ts_code", "nunique"),
                limit_ups=("pct_chg", lambda x: int((x >= 9.5).sum())),
                up_count=("pct_chg", lambda x: int((x > 0).sum())),
                strong_count=("pct_chg", lambda x: int((x >= 5).sum())),
                leader_count=("pct_chg", lambda x: int((x >= 7).sum())),
                net_mf=("net_mf_amount", "sum"),
            )
            .reset_index()
        )
        if grouped.empty:
            return grouped

        grouped["lu_ratio"] = grouped["limit_ups"] / grouped["stock_count"].clip(lower=1)
        grouped["breadth"] = grouped["up_count"] / grouped["stock_count"].clip(lower=1)
        grouped["strong_ratio"] = grouped["strong_count"] / grouped["stock_count"].clip(lower=1)
        grouped["leader_ratio"] = grouped["leader_count"] / grouped["stock_count"].clip(lower=1)
        grouped["net_mf_ratio"] = grouped["net_mf"] / grouped["total_amt"].replace(0, np.nan)
        grouped["net_mf_ratio"] = grouped["net_mf_ratio"].fillna(0.0)

        industry_source = merged.copy()
        industry_source["industry"] = industry_source["industry"].fillna("").astype(str).str.strip()
        industry_source = industry_source[industry_source["industry"] != ""].copy()
        if not industry_source.empty:
            industry_source["industry_weight"] = (
                industry_source["pct_chg"].clip(lower=0.0) * 0.8
                + (industry_source["pct_chg"] > 0).astype(float) * 0.8
                + (industry_source["pct_chg"] >= 5.0).astype(float) * 3.0
                + np.log(industry_source["amount"] + 1.0) * 0.08
            )
            industry_grouped = (
                industry_source.groupby(["trade_date", "mapped_name", "industry"])
                .agg(
                    industry_stock_count=("ts_code", "nunique"),
                    industry_weight=("industry_weight", "sum"),
                    industry_strong_count=("pct_chg", lambda x: int((x >= 5.0).sum())),
                )
                .reset_index()
            )
            industry_grouped["industry_signal"] = (
                industry_grouped["industry_weight"]
                + industry_grouped["industry_strong_count"] * 1.4
            )
            dominant_industry = (
                industry_grouped.sort_values(
                    ["trade_date", "mapped_name", "industry_signal", "industry_stock_count"],
                    ascending=[True, True, False, False],
                )
                .drop_duplicates(subset=["trade_date", "mapped_name"])
                .rename(
                    columns={
                        "industry": "dominant_industry",
                        "industry_signal": "dominant_industry_signal",
                        "industry_stock_count": "dominant_industry_stock_count",
                    }
                )
            )
            industry_summary = (
                industry_grouped.groupby(["trade_date", "mapped_name"])
                .agg(
                    industry_signal_total=("industry_signal", "sum"),
                    industry_count=("industry", "nunique"),
                )
                .reset_index()
            )
            industry_summary = dominant_industry.merge(
                industry_summary,
                on=["trade_date", "mapped_name"],
                how="left",
            )
            industry_summary["dominant_industry_share"] = (
                industry_summary["dominant_industry_signal"]
                / industry_summary["industry_signal_total"].replace(0, np.nan)
            ).fillna(0.0)
            grouped = grouped.merge(
                industry_summary[
                    [
                        "trade_date",
                        "mapped_name",
                        "dominant_industry",
                        "industry_count",
                        "dominant_industry_stock_count",
                        "dominant_industry_share",
                    ]
                ],
                on=["trade_date", "mapped_name"],
                how="left",
            )
        else:
            grouped["dominant_industry"] = ""
            grouped["industry_count"] = 0
            grouped["dominant_industry_stock_count"] = 0
            grouped["dominant_industry_share"] = 0.0

        grouped["industry_count"] = grouped["industry_count"].fillna(0).astype(int)
        grouped["dominant_industry_stock_count"] = (
            grouped["dominant_industry_stock_count"].fillna(0).astype(int)
        )
        grouped["dominant_industry_share"] = grouped["dominant_industry_share"].fillna(0.0)
        grouped["industry_alignment"] = grouped["dominant_industry_share"].clip(lower=0.0, upper=1.0)

        grouped["score"] = (
            grouped["avg_ret"] * 1.6
            + grouped["lu_ratio"] * 65.0
            + grouped["breadth"] * 22.0
            + grouped["strong_ratio"] * 16.0
            + grouped["leader_ratio"] * 12.0
            + np.log(grouped["total_amt"] + 1.0) * 0.35
            + grouped["net_mf_ratio"].clip(-0.02, 0.02) * 800.0
            + grouped["industry_alignment"] * 6.5
        )

        weak_mask = (
            (grouped["stock_count"] < 8)
            | ((grouped["breadth"] < 0.15) & (grouped["strong_ratio"] < 0.02))
        )
        grouped.loc[weak_mask, "score"] *= 0.35
        noisy_mask = (
            (grouped["stock_count"] >= 16)
            & (grouped["industry_alignment"] < 0.14)
            & (grouped["leader_ratio"] < 0.08)
        )
        grouped.loc[noisy_mask, "score"] *= 0.92
        grouped["score"] = grouped["score"].fillna(0.0).round(2)
        grouped["rank"] = grouped.groupby("trade_date")["score"].rank(method="first", ascending=False)
        return grouped

    def _build_top_stocks_map(self, merged: pd.DataFrame, top_concepts: list[str], pct_threshold: float = 3.0):
        if merged.empty or not top_concepts:
            return {}

        leader_df = merged[
            (merged["pct_chg"] >= pct_threshold) & (merged["mapped_name"].isin(top_concepts))
        ].copy()
        if leader_df.empty:
            return {}

        top_stocks_map = {}
        leader_df = leader_df.sort_values(
            ["trade_date", "mapped_name", "pct_chg", "amount"],
            ascending=[True, True, False, False],
        )
        for (trade_date, concept), rows in leader_df.groupby(["trade_date", "mapped_name"]):
            top_stocks_map[(pd.to_datetime(trade_date), concept)] = [
                {
                    "ts_code": r["ts_code"],
                    "name": r["stock_name"] or r["ts_code"],
                    "pct_chg": round(float(r["pct_chg"]), 2),
                }
                for _, r in rows.drop_duplicates(subset=["ts_code"]).head(5).iterrows()
            ]
        return top_stocks_map

    def _recent_true_streak(self, flags: list[int | bool]) -> int:
        streak = 0
        for flag in reversed(flags):
            if flag:
                streak += 1
            else:
                break
        return streak

    def _select_focus_mainlines(
        self,
        grouped: pd.DataFrame,
        recent_dates: list[pd.Timestamp],
        limit: int = 8,
    ) -> pd.DataFrame:
        if grouped.empty or not recent_dates:
            return pd.DataFrame()

        work = grouped.copy()
        work["top3_flag"] = (work["rank"] <= 3).astype(int)
        work["active_flag"] = (
            (work["rank"] <= 6)
            | (work["score"] >= 18)
            | (work["limit_ups"] >= 2)
        ).astype(int)
        work["strong_flag"] = (
            (work["limit_ups"] >= 3)
            | (work["strong_ratio"] >= 0.18)
            | (work["leader_ratio"] >= 0.08)
        ).astype(int)
        recent_window = recent_dates[-min(5, len(recent_dates)):]
        recent_weights = [0.45, 0.7, 1.0, 1.35, 1.8][-len(recent_window):]
        recent_weight_map = {
            pd.to_datetime(trade_date): weight
            for trade_date, weight in zip(recent_window, recent_weights)
        }
        work["recent_weight"] = work["trade_date"].map(recent_weight_map).fillna(0.0)
        work["recent_score_component"] = work["score"] * work["recent_weight"]
        work["recent_limit_component"] = work["limit_ups"] * work["recent_weight"]
        work["recent_top3_component"] = work["top3_flag"] * work["recent_weight"]
        work["recent_alignment_component"] = work["industry_alignment"] * work["recent_weight"]

        summary = (
            work.groupby("mapped_name")
            .agg(
                score_sum=("score", "sum"),
                avg_score=("score", "mean"),
                active_days=("active_flag", "sum"),
                top3_days=("top3_flag", "sum"),
                strong_days=("strong_flag", "sum"),
                max_limit_ups=("limit_ups", "max"),
                recent_score_sum=("recent_score_component", "sum"),
                recent_limit_ups_sum=("recent_limit_component", "sum"),
                recent_top3_sum=("recent_top3_component", "sum"),
                recent_alignment_sum=("recent_alignment_component", "sum"),
            )
            .reset_index()
        )

        streak_df = (
            work.sort_values("trade_date")
            .groupby("mapped_name")["active_flag"]
            .apply(lambda series: self._recent_true_streak(series.tolist()))
            .reset_index(name="consecutive_days")
        )
        summary = summary.merge(streak_df, on="mapped_name", how="left")

        latest_date = recent_dates[-1]
        latest_rows = (
            work[work["trade_date"] == latest_date][
                [
                    "mapped_name",
                    "score",
                    "rank",
                    "limit_ups",
                    "breadth",
                    "leader_ratio",
                    "stock_count",
                    "industry_alignment",
                ]
            ]
            .rename(
                columns={
                    "score": "latest_score",
                    "rank": "latest_rank",
                    "limit_ups": "latest_limit_ups",
                    "breadth": "latest_breadth",
                    "leader_ratio": "latest_leader_ratio",
                    "stock_count": "latest_stock_count",
                    "industry_alignment": "latest_industry_alignment",
                }
            )
        )
        summary = summary.merge(latest_rows, on="mapped_name", how="left")

        summary["consecutive_days"] = summary["consecutive_days"].fillna(0).astype(int)
        summary["latest_score"] = summary["latest_score"].fillna(0.0)
        summary["latest_rank"] = summary["latest_rank"].fillna(999.0)
        summary["latest_limit_ups"] = summary["latest_limit_ups"].fillna(0).astype(int)
        summary["latest_breadth"] = summary["latest_breadth"].fillna(0.0)
        summary["latest_leader_ratio"] = summary["latest_leader_ratio"].fillna(0.0)
        summary["latest_stock_count"] = summary["latest_stock_count"].fillna(0).astype(int)
        summary["latest_industry_alignment"] = summary["latest_industry_alignment"].fillna(0.0)

        summary["selection_score"] = (
            summary["score_sum"] * 0.45
            + summary["avg_score"] * 1.1
            + summary["recent_score_sum"] * 2.5
            + summary["recent_limit_ups_sum"] * 5.5
            + summary["recent_top3_sum"] * 10.0
            + summary["active_days"] * 6.5
            + summary["top3_days"] * 10.0
            + summary["strong_days"] * 8.5
            + summary["consecutive_days"] * 12.0
            + summary["latest_score"] * 1.8
            + summary["latest_limit_ups"] * 4.5
            + summary["latest_breadth"] * 18.0
            + summary["recent_alignment_sum"] * 10.0
            + summary["latest_industry_alignment"] * 12.0
            + summary["max_limit_ups"] * 2.5
        )
        summary["current_relevance"] = (
            summary["recent_score_sum"]
            + summary["latest_score"] * 1.4
            + summary["latest_limit_ups"] * 2.8
        )

        qualified = summary[
            (summary["active_days"] >= 2)
            | (summary["consecutive_days"] >= 2)
            | (summary["recent_score_sum"] >= 45)
            | ((summary["latest_score"] >= 24) & (summary["top3_days"] >= 1))
        ].copy()
        if qualified.empty:
            qualified = summary.copy()

        qualified = qualified.sort_values(
            ["selection_score", "current_relevance", "latest_score", "score_sum"],
            ascending=[False, False, False, False],
        )
        return qualified.head(max(1, int(limit))).reset_index(drop=True)

    def _build_leader_theme_map(
        self,
        ts_codes: list[str],
        sector_name: str,
        focus_tags: list[str] | None = None,
    ) -> dict[str, dict]:
        codes = [str(code).strip() for code in (ts_codes or []) if str(code).strip()]
        if not codes:
            return {}

        keyword_map = {}
        for item in [sector_name, *((focus_tags or []))]:
            cleaned = self._clean_concept_name(item).upper()
            if cleaned and cleaned not in keyword_map:
                keyword_map[cleaned] = str(item)
        if not keyword_map:
            return {}

        tag_df = self._load_sector_tag_rows(codes)
        basic_df = self._load_stock_basic_snapshot()
        if not basic_df.empty:
            basic_df = basic_df[basic_df["ts_code"].isin(codes)].copy()

        candidate_map = defaultdict(list)
        if not tag_df.empty:
            for _, row in tag_df.iterrows():
                code = str(row.get("ts_code") or "").strip()
                tag_name = str(row.get("tag_name") or "").strip()
                if code and tag_name:
                    candidate_map[code].append(tag_name)
        if not basic_df.empty:
            for _, row in basic_df.iterrows():
                code = str(row.get("ts_code") or "").strip()
                industry_name = str(row.get("industry") or "").strip()
                if code and industry_name:
                    candidate_map[code].append(industry_name)

        hit_map = {}
        for code in codes:
            hits = []
            seen_hits = set()
            for raw_tag in candidate_map.get(code, []):
                cleaned_tag = self._clean_concept_name(raw_tag).upper()
                if not cleaned_tag:
                    continue
                for keyword, display_name in keyword_map.items():
                    if keyword in cleaned_tag or cleaned_tag in keyword:
                        if display_name not in seen_hits:
                            hits.append(display_name)
                            seen_hits.add(display_name)
                        break
            hit_map[code] = {
                "theme_hit_count": len(hits),
                "theme_hit_names": hits[:3],
            }

        return hit_map

    def _build_leader_review(
        self,
        line_df: pd.DataFrame,
        review_dates: list[pd.Timestamp],
        sector_name: str = "",
        focus_tags: list[str] | None = None,
        top_n: int = 3,
    ) -> list[dict]:
        if line_df.empty or not review_dates:
            return []

        line_df = line_df[~line_df["ts_code"].astype(str).str.upper().str.endswith(".BJ")].copy()
        if line_df.empty:
            return []

        latest_date = review_dates[-1]
        recent_dates = review_dates[-min(3, len(review_dates)):]
        latest_slice = (
            line_df[line_df["trade_date"] == latest_date][["ts_code", "pct_chg", "amount", "net_mf_amount"]]
            .rename(
                columns={
                    "pct_chg": "latest_pct",
                    "amount": "latest_amount",
                    "net_mf_amount": "latest_net_mf_amount",
                }
            )
        )

        theme_map = self._build_leader_theme_map(
            line_df["ts_code"].dropna().tolist(),
            sector_name=sector_name,
            focus_tags=focus_tags,
        )
        review_days = max(1, len(review_dates))
        recent_window = max(1, len(recent_dates))
        leader_rows = []
        for (ts_code, stock_name), rows in line_df.groupby(["ts_code", "stock_name"]):
            rows = rows.sort_values("trade_date").reset_index(drop=True)
            pct_list = [float(item) if item is not None else 0.0 for item in rows["pct_chg"].tolist()]
            amount_list = [float(item) if item is not None else 0.0 for item in rows["amount"].tolist()]
            flow_list = [float(item) if item is not None else 0.0 for item in rows["net_mf_amount"].tolist()]
            strong_flags = [pct >= 3.0 for pct in pct_list]
            positive_flow_flags = [flow > 0 for flow in flow_list]
            active_days = int(sum(strong_flags))
            recent_active_days = int(sum(strong_flags[-recent_window:])) if strong_flags else 0
            strong_streak = self._recent_true_streak(strong_flags)
            positive_flow_days = int(sum(positive_flow_flags))
            total_amt = float(sum(amount_list))
            positive_inflow = float(sum(max(flow, 0.0) for flow in flow_list))
            net_inflow = float(sum(flow_list))
            first_strong_idx = next((idx for idx, flag in enumerate(strong_flags) if flag), None)
            pioneer_score = 0.0
            if first_strong_idx is not None:
                pioneer_score = round((review_days - first_strong_idx) / review_days * 100, 2)

            latest_pct = float(pct_list[-1]) if pct_list else 0.0
            latest_amount = float(amount_list[-1]) if amount_list else 0.0
            latest_net_mf_amount = float(flow_list[-1]) if flow_list else 0.0
            limit_ups = int(sum(1 for pct in pct_list if pct >= 9.5))

            theme_meta = theme_map.get(ts_code, {})
            theme_hit_names = theme_meta.get("theme_hit_names", [])
            theme_hit_count = int(theme_meta.get("theme_hit_count", 0))

            trend_score = min(
                100.0,
                active_days * 10.0
                + recent_active_days * 16.0
                + strong_streak * 14.0
                + pioneer_score * 0.18
                + min(limit_ups, 2) * 8.0,
            )
            capital_score = min(
                100.0,
                positive_flow_days * 9.0
                + (18.0 if positive_inflow > 100000 else 12.0 if positive_inflow > 50000 else 6.0 if positive_inflow > 10000 else 0.0)
                + (min(max(net_inflow / total_amt, 0.0), 0.05) * 700 if total_amt > 0 else 0.0)
                + (8.0 if latest_net_mf_amount > 0 else 0.0),
            )
            heat_score = min(
                100.0,
                max(latest_pct, 0.0) * 2.2
                + np.log(total_amt + 1.0) * 3.5
                + np.log(latest_amount + 1.0) * 1.6,
            )
            theme_score = min(100.0, theme_hit_count * 26.0 + len(theme_hit_names) * 8.0)
            current_score = min(
                100.0,
                max(latest_pct, 0.0) * 3.2
                + (16.0 if latest_net_mf_amount > 0 else 0.0)
                + (10.0 if latest_amount > total_amt / max(1, review_days) else 0.0),
            )
            leader_score = (
                trend_score * 0.34
                + capital_score * 0.24
                + heat_score * 0.18
                + theme_score * 0.14
                + current_score * 0.10
            )

            reason_parts = []
            if active_days:
                reason_parts.append(f"近10日强势{active_days}天")
            if recent_active_days:
                reason_parts.append(f"最近3日走强{recent_active_days}天")
            if positive_flow_days:
                reason_parts.append(f"资金净流入{positive_flow_days}天")
            if theme_hit_names:
                reason_parts.append(f"题材命中{' / '.join(theme_hit_names[:2])}")
            if not reason_parts:
                reason_parts.append(f"最近涨幅{latest_pct:.2f}%")

            leader_rows.append(
                {
                    "ts_code": ts_code,
                    "stock_name": stock_name or ts_code,
                    "active_days": active_days,
                    "recent_active_days": recent_active_days,
                    "strong_streak": strong_streak,
                    "positive_flow_days": positive_flow_days,
                    "limit_ups": limit_ups,
                    "avg_pct": round(float(np.mean(pct_list)) if pct_list else 0.0, 2),
                    "max_pct": round(float(max(pct_list)) if pct_list else 0.0, 2),
                    "latest_pct": round(latest_pct, 2),
                    "latest_amount": latest_amount,
                    "latest_net_mf_amount": latest_net_mf_amount,
                    "total_amt": total_amt,
                    "trend_pioneer_score": pioneer_score,
                    "theme_hit_count": theme_hit_count,
                    "theme_hit_names": theme_hit_names,
                    "trend_score": round(trend_score, 1),
                    "capital_score": round(capital_score, 1),
                    "heat_score": round(heat_score, 1),
                    "theme_score": round(theme_score, 1),
                    "leader_reason": "，".join(reason_parts),
                    "leader_score": round(float(leader_score), 1),
                }
            )

        if not leader_rows:
            return []

        leader_df = pd.DataFrame(leader_rows)
        if latest_slice is not None and not latest_slice.empty:
            leader_df = leader_df.merge(latest_slice, on="ts_code", how="left", suffixes=("", "_latest"))
            leader_df["latest_pct"] = leader_df["latest_pct_latest"].fillna(leader_df["latest_pct"]).round(2)
            leader_df["latest_amount"] = leader_df["latest_amount_latest"].fillna(leader_df["latest_amount"])
            leader_df["latest_net_mf_amount"] = (
                leader_df["latest_net_mf_amount_latest"].fillna(leader_df["latest_net_mf_amount"])
            )
            leader_df = leader_df.drop(
                columns=[
                    col for col in [
                        "latest_pct_latest",
                        "latest_amount_latest",
                        "latest_net_mf_amount_latest",
                    ]
                    if col in leader_df.columns
                ]
            )

        leader_df["latest_amount_rank_pct"] = leader_df["latest_amount"].rank(pct=True, method="max")
        leader_df["total_amount_rank_pct"] = leader_df["total_amt"].rank(pct=True, method="max")
        leader_df["leader_score"] = (
            leader_df["leader_score"]
            + leader_df["latest_amount_rank_pct"] * 6.0
            + leader_df["total_amount_rank_pct"] * 4.0
        )

        leader_df = leader_df.sort_values(
            ["leader_score", "recent_active_days", "positive_flow_days", "latest_pct", "total_amt"],
            ascending=[False, False, False, False, False],
        ).head(max(1, int(top_n)))

        return [
            {
                "ts_code": row["ts_code"],
                "name": row["stock_name"] or row["ts_code"],
                "active_days": int(row["active_days"]),
                "recent_active_days": int(row["recent_active_days"]),
                "strong_streak": int(row["strong_streak"]),
                "limit_ups": int(row["limit_ups"]),
                "positive_flow_days": int(row["positive_flow_days"]),
                "avg_pct": round(float(row["avg_pct"]), 2),
                "max_pct": round(float(row["max_pct"]), 2),
                "latest_pct": round(float(row["latest_pct"]), 2),
                "trend_pioneer_score": round(float(row["trend_pioneer_score"]), 1),
                "theme_hit_count": int(row["theme_hit_count"]),
                "theme_hit_names": row["theme_hit_names"] if isinstance(row["theme_hit_names"], list) else [],
                "trend_score": round(float(row["trend_score"]), 1),
                "capital_score": round(float(row["capital_score"]), 1),
                "heat_score": round(float(row["heat_score"]), 1),
                "theme_score": round(float(row["theme_score"]), 1),
                "leader_reason": str(row.get("leader_reason") or ""),
                "leader_score": round(float(row["leader_score"]), 1),
            }
            for _, row in leader_df.iterrows()
        ]

    def _load_sector_tag_rows(self, ts_codes: list[str]) -> pd.DataFrame:
        codes = sorted({str(code).strip() for code in (ts_codes or []) if str(code).strip()})
        if not codes:
            return pd.DataFrame(columns=["ts_code", "tag_name", "tag_type"])
        tag_df = self._load_tag_snapshot()
        if tag_df.empty:
            return pd.DataFrame(columns=["ts_code", "tag_name", "tag_type"])

        tag_df = tag_df[tag_df["ts_code"].isin(codes)].copy()
        if tag_df.empty:
            return pd.DataFrame(columns=["ts_code", "tag_name", "tag_type"])
        return tag_df.reset_index(drop=True)

    def _build_sector_driver_profile(
        self,
        line_df: pd.DataFrame,
        sector_name: str,
        review_dates: list[pd.Timestamp],
        top_n: int = 3,
    ) -> dict:
        if line_df.empty or not sector_name or not review_dates:
            return {
                "focus_tags": [],
                "display_name": sector_name,
                "driver_summary": "",
                "driver_details": [],
            }

        latest_date = review_dates[-1]
        recent_focus_dates = review_dates[-min(3, len(review_dates)):]
        latest_slice = (
            line_df[line_df["trade_date"] == latest_date][["ts_code", "pct_chg"]]
            .rename(columns={"pct_chg": "latest_pct"})
        )
        recent_active = (
            line_df[
                (line_df["trade_date"].isin(recent_focus_dates)) & (line_df["pct_chg"] >= 3.0)
            ]
            .groupby("ts_code")
            .size()
            .to_dict()
        )

        stock_stats = (
            line_df.groupby(["ts_code", "stock_name"])
            .agg(
                active_days=("trade_date", "nunique"),
                strong_days=("pct_chg", lambda x: int((x >= 5.0).sum())),
                limit_ups=("pct_chg", lambda x: int((x >= 9.5).sum())),
                avg_positive_pct=("pct_chg", lambda x: float(x[x > 0].mean()) if (x > 0).any() else 0.0),
                total_amt=("amount", "sum"),
                industry=("industry", lambda x: next((str(v).strip() for v in x if str(v).strip()), "")),
            )
            .reset_index()
        )
        if stock_stats.empty:
            return {
                "focus_tags": [],
                "display_name": sector_name,
                "driver_summary": "",
                "driver_details": [],
            }

        stock_stats = stock_stats.merge(latest_slice, on="ts_code", how="left")
        stock_stats["latest_pct"] = stock_stats["latest_pct"].fillna(0.0)
        stock_stats["recent_active_days"] = stock_stats["ts_code"].map(recent_active).fillna(0).astype(int)
        stock_stats["tag_weight"] = (
            stock_stats["active_days"] * 7.0
            + stock_stats["recent_active_days"] * 12.0
            + stock_stats["strong_days"] * 11.0
            + stock_stats["limit_ups"] * 18.0
            + stock_stats["avg_positive_pct"].clip(lower=0.0) * 1.8
            + stock_stats["latest_pct"].clip(lower=0.0) * 2.6
            + np.log(stock_stats["total_amt"] + 1.0) * 0.16
        )
        stock_stats = stock_stats[stock_stats["tag_weight"] > 0].copy()
        if stock_stats.empty:
            return {
                "focus_tags": [],
                "display_name": sector_name,
                "driver_summary": "",
                "driver_details": [],
            }

        tag_df = self._load_sector_tag_rows(stock_stats["ts_code"].tolist())

        stock_meta = {
            row["ts_code"]: {
                "stock_name": row["stock_name"] or row["ts_code"],
                "latest_pct": float(row["latest_pct"]),
                "tag_weight": float(row["tag_weight"]),
                "industry": str(row.get("industry") or "").strip(),
            }
            for _, row in stock_stats.iterrows()
        }
        sector_stock_total = len(stock_stats)

        candidates = defaultdict(
            lambda: {
                "name": "",
                "tag_type": "",
                "score": 0.0,
                "support_stocks": set(),
                "top_samples": [],
                "latest_pct": 0.0,
            }
        )

        for ts_code, meta in stock_meta.items():
            industry_name = meta["industry"]
            if not industry_name:
                continue
            if self._resolve_tag_sector(industry_name, tag_type="industry") != sector_name:
                continue

            multiplier = self._get_tag_priority_multiplier(
                sector_name,
                industry_name,
                "industry",
            ) * 1.06
            candidate = candidates[industry_name]
            candidate["name"] = industry_name
            candidate["tag_type"] = "industry"
            candidate["score"] += meta["tag_weight"] * multiplier
            candidate["support_stocks"].add(ts_code)
            candidate["latest_pct"] = max(candidate["latest_pct"], meta["latest_pct"])
            candidate["top_samples"].append((meta["latest_pct"], meta["stock_name"]))

        for ts_code, rows in tag_df.groupby("ts_code"):
            meta = stock_meta.get(ts_code)
            if not meta:
                continue

            valid_tags = []
            seen_cleaned = set()
            for _, row in rows.iterrows():
                tag_name = str(row["tag_name"]).strip()
                tag_type = str(row["tag_type"]).strip() or "concept"
                cleaned = self._clean_concept_name(tag_name)
                if not cleaned or cleaned in seen_cleaned:
                    continue

                resolved_sector = self._resolve_tag_sector(tag_name, tag_type=tag_type)
                if resolved_sector != sector_name:
                    continue

                seen_cleaned.add(cleaned)
                valid_tags.append(
                    (
                        tag_name,
                        tag_type,
                        self._get_tag_priority_multiplier(sector_name, tag_name, tag_type),
                    )
                )

            if not valid_tags:
                continue

            distributed_weight = meta["tag_weight"] / max(1, len(valid_tags))
            for tag_name, tag_type, multiplier in valid_tags:
                candidate = candidates[tag_name]
                candidate["name"] = tag_name
                candidate["tag_type"] = tag_type
                candidate["score"] += distributed_weight * multiplier
                candidate["support_stocks"].add(ts_code)
                candidate["latest_pct"] = max(candidate["latest_pct"], meta["latest_pct"])
                candidate["top_samples"].append((meta["latest_pct"], meta["stock_name"]))

        if not candidates:
            return {
                "focus_tags": [],
                "display_name": sector_name,
                "driver_summary": "",
                "driver_details": [],
            }

        ranked = []
        total_candidate_score = sum(float(item["score"]) for item in candidates.values()) or 1.0
        for candidate in candidates.values():
            samples = sorted(candidate["top_samples"], key=lambda item: item[0], reverse=True)
            cleaned = self._clean_concept_name(candidate["name"]).upper()
            support_count = len(candidate["support_stocks"])
            support_ratio = support_count / max(1, sector_stock_total)
            score = round(float(candidate["score"]), 2)
            ranked.append(
                {
                    "name": candidate["name"],
                    "tag_type": candidate["tag_type"],
                    "score": score,
                    "support_count": support_count,
                    "support_ratio": round(support_ratio, 4),
                    "score_share": round(score / total_candidate_score, 4),
                    "latest_pct": round(float(candidate["latest_pct"]), 2),
                    "sample_stocks": [name for _, name in samples[:3]],
                    "is_generic": cleaned in self._prepared_sector_generic_tags.get(sector_name, set()),
                }
            )

        ranked = sorted(
            ranked,
            key=lambda item: (
                item["score"],
                item["support_count"],
                item["support_ratio"],
                item["latest_pct"],
                1 if item["tag_type"] == "concept" else 0,
            ),
            reverse=True,
        )

        concept_candidates = [
            item for item in ranked
            if item["tag_type"] == "concept" and not item["is_generic"]
        ]
        industry_candidates = [
            item for item in ranked
            if item["tag_type"] == "industry" and not item["is_generic"]
        ]
        non_generic = [item for item in ranked if not item["is_generic"]]
        generic = [item for item in ranked if item["is_generic"]]
        top_details = []

        if concept_candidates and concept_candidates[0]["support_ratio"] >= 0.1:
            top_details.append(concept_candidates[0])
        if (
            industry_candidates
            and industry_candidates[0]["support_ratio"] >= 0.16
            and all(item["name"] != industry_candidates[0]["name"] for item in top_details)
        ):
            top_details.append(industry_candidates[0])

        preferred_pool = concept_candidates[1:] + industry_candidates[1:] + non_generic + generic
        for item in preferred_pool:
            if len(top_details) >= max(1, int(top_n)):
                break
            if item["support_ratio"] < 0.08 and item["score_share"] < 0.12:
                continue
            if any(existing["name"] == item["name"] for existing in top_details):
                continue
            top_details.append(item)

        if len(top_details) < max(1, int(top_n)):
            for item in generic:
                if len(top_details) >= max(1, int(top_n)):
                    break
                if any(existing["name"] == item["name"] for existing in top_details):
                    continue
                top_details.append(item)

        focus_tags = [item["name"] for item in top_details]

        # 根据细分驱动强度决定显示模式
        allow_replace_sector = False
        if focus_tags and DRIVER_DISPLAY_MODE == "auto":
            top_driver = top_details[0]
            allow_replace_sector = (
                top_driver["tag_type"] == "concept"
                and float(top_driver["score"]) >= DRIVER_DOMINANCE_THRESHOLD
                and float(top_driver["support_ratio"]) >= DRIVER_REPLACE_SECTOR_MIN_SUPPORT_RATIO
                and int(top_driver["support_count"]) >= DRIVER_REPLACE_SECTOR_MIN_SUPPORT_COUNT
                and float(top_driver["score_share"]) >= DRIVER_REPLACE_SECTOR_MIN_SCORE_SHARE
            )
        elif focus_tags and DRIVER_DISPLAY_MODE == "driver_first":
            allow_replace_sector = True

        if allow_replace_sector and focus_tags:
            display_name = (
                " / ".join(focus_tags[:2])
                if len(focus_tags) >= 2
                else focus_tags[0]
            )
        else:
            display_name = (
                f"{sector_name} · {' / '.join(focus_tags[:2])}"
                if focus_tags
                else sector_name
            )
        driver_summary = (
            f"细分驱动：{' / '.join(focus_tags[:3])}"
            if focus_tags
            else ""
        )
        return {
            "focus_tags": focus_tags,
            "display_name": display_name,
            "driver_summary": driver_summary,
            "driver_details": top_details,
        }

    def _build_ten_day_review(
        self,
        merged: pd.DataFrame,
        grouped: pd.DataFrame,
        recent_dates: list[pd.Timestamp],
        limit: int = 4,
    ) -> dict:
        review_dates = recent_dates[-min(10, len(recent_dates)):] if recent_dates else []
        if grouped.empty or not review_dates:
            return {
                "trade_days": 0,
                "date_range": {},
                "summary": "近10个交易日无可用主线数据。",
                "mainlines": [],
            }

        grouped_review = grouped[grouped["trade_date"].isin(review_dates)].copy()
        selected = self._select_focus_mainlines(grouped_review, review_dates, limit=max(limit, 6))
        if selected.empty:
            return {
                "trade_days": len(review_dates),
                "date_range": {
                    "start": review_dates[0].strftime("%Y-%m-%d"),
                    "end": review_dates[-1].strftime("%Y-%m-%d"),
                },
                "summary": "近10个交易日未识别出具备持续性的主线。",
                "mainlines": [],
            }

        mainlines = []
        for _, row in selected.head(max(1, int(limit))).iterrows():
            line_name = row["mapped_name"]
            line_df = merged[
                (merged["trade_date"].isin(review_dates)) & (merged["mapped_name"] == line_name)
            ].copy()
            latest_line = grouped_review[
                (grouped_review["trade_date"] == review_dates[-1]) & (grouped_review["mapped_name"] == line_name)
            ].copy()
            driver_profile = self._build_sector_driver_profile(
                line_df=line_df,
                sector_name=line_name,
                review_dates=review_dates,
                top_n=3,
            )
            leaders = self._build_leader_review(
                line_df=line_df,
                review_dates=review_dates,
                sector_name=line_name,
                focus_tags=driver_profile.get("focus_tags", []),
                top_n=5,
            )
            coach_fit = {
                "sustained": int(row["consecutive_days"]) >= 3,
                "strength": int(row["max_limit_ups"]) >= 3 or int(row["strong_days"]) >= 2,
                "leaders_ready": len(leaders) > 0,
            }
            latest_stock_count = 0
            if not latest_line.empty and "stock_count" in latest_line.columns:
                latest_stock_count = int(latest_line.iloc[0]["stock_count"])
            elif not line_df.empty:
                latest_stock_count = int(
                    line_df[line_df["trade_date"] == review_dates[-1]]["ts_code"].nunique()
                )
            mainlines.append(
                {
                    "name": line_name,
                    "active_days": int(row["active_days"]),
                    "top3_days": int(row["top3_days"]),
                    "consecutive_days": int(row["consecutive_days"]),
                    "stock_count": latest_stock_count,
                    "latest_score": round(float(row["latest_score"]), 2),
                    "avg_score": round(float(row["avg_score"]), 2),
                    "latest_rank": int(row["latest_rank"]) if row["latest_rank"] < 999 else 0,
                    "latest_breadth": round(float(row["latest_breadth"]), 4),
                    "max_limit_ups": int(row["max_limit_ups"]),
                    "leaders": leaders,
                    "focus_tags": driver_profile.get("focus_tags", []),
                    "display_name": driver_profile.get("display_name") or line_name,
                    "driver_summary": driver_profile.get("driver_summary", ""),
                    "driver_details": driver_profile.get("driver_details", []),
                    "coach_fit": coach_fit,
                }
            )

        summary_parts = []
        for line in mainlines[:2]:
            leader_names = "、".join(item["name"] for item in line["leaders"][:2]) or "暂无明确龙头"
            driver_text = ""
            if line.get("focus_tags"):
                driver_text = f"细分以{'、'.join(line['focus_tags'][:2])}为主，"
            summary_parts.append(
                f"{line['name']}上榜{line['active_days']}天，最近连续{line['consecutive_days']}天，"
                f"{driver_text}"
                f"最高{line['max_limit_ups']}家涨停，龙头以{leader_names}为主"
            )
        summary = (
            "最近10个交易日，" + "；".join(summary_parts) + "。"
            if summary_parts
            else "近10个交易日未识别出具备持续性的主线。"
        )

        return {
            "trade_days": len(review_dates),
            "date_range": {
                "start": review_dates[0].strftime("%Y-%m-%d"),
                "end": review_dates[-1].strftime("%Y-%m-%d"),
            },
            "summary": summary,
            "mainlines": mainlines,
        }

    def _prepare_context(self, days: int, trade_date: str | None = None):
        recent_dates, min_date, max_date = self._resolve_trade_window(days, trade_date=trade_date)
        if not recent_dates:
            return recent_dates, pd.DataFrame(), pd.DataFrame()

        merged = self._load_mainline_market_data(min_date, max_date)
        if merged.empty:
            return recent_dates, pd.DataFrame(), merged

        grouped = self._build_grouped_scores(merged)
        return recent_dates, grouped, merged

    def _safe_float(self, value, default=0.0):
        try:
            number = float(value)
            if np.isnan(number) or np.isinf(number):
                return default if default is None else float(default)
            return number
        except Exception:
            return default if default is None else float(default)

    def _extract_rt_pct(self, row: pd.Series):
        row_map = {str(k).lower(): row[k] for k in row.index}
        for key in ("pct_chg", "pct_change", "changepercent"):
            if key in row_map and row_map[key] is not None:
                value = self._safe_float(row_map[key], None)
                if value is not None:
                    return value
        price = None
        pre_close = None
        for key in ("price", "current", "close"):
            if key in row_map:
                price = self._safe_float(row_map[key], None)
                if price is not None:
                    break
        for key in ("pre_close", "yclose"):
            if key in row_map:
                pre_close = self._safe_float(row_map[key], None)
                if pre_close is not None:
                    break
        if price is not None and pre_close is not None and pre_close > 0:
            return (price - pre_close) / pre_close * 100.0
        return None

    def preview_intraday(self, provider, limit: int = 5, leaders_per_mainline: int = 8, src: str = "dc"):
        """
        盘中主线预估（不落库）：
        - 基于最近交易日的主线归属和龙头池
        - 用 realtime_quote 替换最新涨跌幅，输出盘中主线强弱
        """
        date_df = fetch_df(
            """
            SELECT trade_date
            FROM daily_price
            GROUP BY trade_date
            HAVING COUNT(*) > 1000
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        if date_df.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        latest_trade_date = (
            date_df.iloc[0]["trade_date"].strftime("%Y-%m-%d")
            if hasattr(date_df.iloc[0]["trade_date"], "strftime")
            else str(date_df.iloc[0]["trade_date"])
        )

        stock_map = self._build_stock_mainline_map(latest_trade_date, latest_trade_date)
        if stock_map.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        universe_df = fetch_df(
            """
            SELECT d.ts_code, d.pct_chg, d.amount, b.name AS stock_name
            FROM daily_price d
            LEFT JOIN stock_basic b ON d.ts_code = b.ts_code
            WHERE d.trade_date = ?
            """,
            params=[latest_trade_date],
        )
        if universe_df.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        merged = universe_df.merge(stock_map, on="ts_code", how="left")
        merged = merged[merged["mapped_name"].notna()].copy()
        if merged.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        by_line = (
            merged.groupby("mapped_name")
            .agg(total_amt=("amount", "sum"), avg_ret=("pct_chg", "mean"), stock_count=("ts_code", "nunique"))
            .reset_index()
            .sort_values(["total_amt", "avg_ret"], ascending=[False, False])
            .head(max(limit * 2, 10))
        )
        selected_lines = by_line["mapped_name"].tolist()
        leader_pool = []
        for line in selected_lines:
            slice_df = (
                merged[merged["mapped_name"] == line]
                .sort_values(["pct_chg", "amount"], ascending=[False, False])
                .head(max(3, leaders_per_mainline))
            )
            for _, row in slice_df.iterrows():
                leader_pool.append(
                    {
                        "mapped_name": line,
                        "ts_code": row["ts_code"],
                        "stock_name": row.get("stock_name") or row["ts_code"],
                    }
                )
        if not leader_pool:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        leader_df = pd.DataFrame(leader_pool).drop_duplicates(subset=["ts_code"])
        codes = leader_df["ts_code"].tolist()

        rt_df = pd.DataFrame()
        try:
            rt_df = provider.realtime_quote(ts_code=",".join(codes), src=src)
        except Exception as exc:
            logger.warning(f"mainline preview realtime bulk quote failed: {exc}")

        if rt_df is None or rt_df.empty:
            rows = []
            for code in codes:
                try:
                    quote_df = provider.realtime_quote(ts_code=code, src=src)
                    if quote_df is not None and not quote_df.empty:
                        rows.append(quote_df.iloc[0].to_dict())
                except Exception:
                    continue
            rt_df = pd.DataFrame(rows) if rows else pd.DataFrame()

        if rt_df.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        key_col = "ts_code" if "ts_code" in rt_df.columns else ("code" if "code" in rt_df.columns else None)
        if not key_col:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        rt_df["ts_code"] = rt_df[key_col].astype(str)
        rt_df["rt_pct_chg"] = rt_df.apply(self._extract_rt_pct, axis=1)
        rt_df = rt_df[rt_df["rt_pct_chg"].notna()][["ts_code", "rt_pct_chg"]]
        if rt_df.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        joined = leader_df.merge(rt_df, on="ts_code", how="inner")
        if joined.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        result_rows = []
        for line, frame in joined.groupby("mapped_name"):
            sample_size = len(frame)
            if sample_size < 2:
                continue
            avg_ret = float(frame["rt_pct_chg"].mean())
            up_ratio = float((frame["rt_pct_chg"] > 0).sum() / sample_size)
            strong_ratio = float((frame["rt_pct_chg"] >= 2.0).sum() / sample_size)
            score = avg_ret * 2.0 + up_ratio * 24.0 + strong_ratio * 22.0 + np.log(sample_size + 1.0) * 1.2
            tops = (
                frame.sort_values("rt_pct_chg", ascending=False)
                .head(5)[["stock_name", "rt_pct_chg"]]
                .to_dict("records")
            )
            result_rows.append(
                {
                    "name": line,
                    "score": round(float(score), 2),
                    "avg_ret": round(avg_ret, 2),
                    "up_ratio": round(up_ratio, 4),
                    "strong_ratio": round(strong_ratio, 4),
                    "sample_size": int(sample_size),
                    "top_stocks": [
                        {"name": item["stock_name"], "pct_chg": round(float(item["rt_pct_chg"]), 2)}
                        for item in tops
                    ],
                }
            )

        result_rows = sorted(result_rows, key=lambda item: item["score"], reverse=True)[:max(1, int(limit))]
        return {
            "as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
            "baseline_trade_date": latest_trade_date,
            "data": result_rows,
        }

    @lru_cache(maxsize=32)
    def analyze(self, days=3, limit=5, trade_date: str = None):
        recent_dates, grouped, merged = self._prepare_context(days=days, trade_date=trade_date)
        if grouped.empty or not recent_dates:
            return []

        focus = self._select_focus_mainlines(grouped, recent_dates, limit=max(limit * 2, 8))
        if focus.empty:
            return []

        latest_date = recent_dates[-1]
        latest_rows = grouped[grouped["trade_date"] == latest_date].set_index("mapped_name")
        top_concepts = focus["mapped_name"].tolist()
        top_stocks_map = self._build_top_stocks_map(merged, top_concepts)

        results = []
        for _, row in focus.head(max(1, int(limit))).iterrows():
            name = row["mapped_name"]
            latest = latest_rows.loc[name] if name in latest_rows.index else row
            limit_ups = int(latest.get("limit_ups", 0))
            breadth = float(latest.get("breadth", 0.0))
            leader_ratio = float(latest.get("leader_ratio", 0.0))
            net_mf_ratio = float(latest.get("net_mf_ratio", 0.0))
            reason = (
                f"连续性较强，最新涨停{limit_ups}家，上涨占比{breadth*100:.1f}%，"
                f"龙头占比{leader_ratio*100:.1f}%，净流入占比{net_mf_ratio*100:.2f}%"
            )
            results.append(
                {
                    "name": name,
                    "score": round(float(latest.get("score", row.get("latest_score", 0.0))), 2),
                    "stock_count": int(latest.get("stock_count", row.get("latest_stock_count", 0))),
                    "limit_ups": limit_ups,
                    "reason": reason,
                    "top_stocks": top_stocks_map.get((latest_date, name), []),
                }
            )

        return results

    def save_results(self, trade_date: str):
        """执行指定日期的分析并将结果持久化到 mainline_scores。"""
        try:
            results = self.analyze(days=3, limit=100, trade_date=trade_date)
            if not results:
                logger.warning(f"save_results: {trade_date} 没有分析结果")
                return

            with get_db_connection() as con:
                for res in results:
                    con.execute(
                        """
                        INSERT INTO mainline_scores (trade_date, mapped_name, score, limit_ups, stock_count, top_stocks)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT (trade_date, mapped_name) DO UPDATE SET
                            score = EXCLUDED.score,
                            limit_ups = EXCLUDED.limit_ups,
                            stock_count = EXCLUDED.stock_count,
                            top_stocks = EXCLUDED.top_stocks
                        """,
                        (
                            trade_date,
                            res["name"],
                            res["score"],
                            res.get("limit_ups", 0),
                            res.get("stock_count", 0),
                            json.dumps(res["top_stocks"]),
                        ),
                    )
            logger.info(f"已成功持久化 {trade_date} 的主线评分数据")
        except Exception as exc:
            logger.error(f"持久化主线数据失败: {exc}")

    @lru_cache(maxsize=16)
    def get_history(self, days=30):
        """
        获取历史主线演变数据，用于前端可视化展示。
        """
        recent_dates, grouped, merged = self._prepare_context(days=days)
        if not recent_dates:
            return {"dates": [], "series": [], "analysis": {"phase": "No_Data", "deduction": "无可用行情数据"}}
        if grouped.empty:
            return {
                "dates": [date.strftime("%m-%d") for date in recent_dates],
                "series": [],
                "analysis": {"phase": "No_Map", "deduction": "概念与行业映射为空，无法识别主线"},
            }

        focus = self._select_focus_mainlines(grouped, recent_dates, limit=8)
        if focus.empty:
            return {
                "dates": [date.strftime("%m-%d") for date in recent_dates],
                "series": [],
                "analysis": {"phase": "Chaos", "deduction": "近期无通过持续性筛选的主线"},
            }

        top_concepts = focus["mapped_name"].tolist()
        top_stocks_map = self._build_top_stocks_map(merged, top_concepts)
        unique_dates = sorted(grouped["trade_date"].unique())
        dates = [pd.to_datetime(date).strftime("%m-%d") for date in unique_dates]
        latest_date = unique_dates[-1]

        latest_focus = grouped[
            (grouped["trade_date"] == latest_date) & (grouped["mapped_name"].isin(top_concepts))
        ].sort_values("score", ascending=False)
        best_display_name = best_name = ""
        best_focus_tags = []
        best_driver_summary = ""
        if latest_focus.empty:
            best_name = focus.iloc[0]["mapped_name"]
            best_display_name = best_name
            best_score = float(focus.iloc[0]["latest_score"])
            best_reason = "近阶段持续性最强"
        else:
            row = latest_focus.iloc[0]
            best_name = row["mapped_name"]
            best_display_name = best_name
            best_score = float(row["score"])
            best_reason = (
                f"涨停{int(row['limit_ups'])}家，上涨占比{row['breadth']*100:.1f}%，"
                f"强势股占比{row['strong_ratio']*100:.1f}%，净流入占比{row['net_mf_ratio']*100:.2f}%"
            )

        series = []
        for concept in top_concepts:
            data = []
            for trade_date in unique_dates:
                date_slice = grouped[
                    (grouped["trade_date"] == trade_date) & (grouped["mapped_name"] == concept)
                ]
                if date_slice.empty:
                    value = 0.0
                    limit_ups = 0
                    breadth = 0.0
                    stock_count = 0
                else:
                    point = date_slice.iloc[0]
                    value = float(point["score"])
                    limit_ups = int(point["limit_ups"])
                    breadth = float(point["breadth"])
                    stock_count = int(point["stock_count"])
                date_str = pd.to_datetime(trade_date).strftime("%m-%d")
                data.append(
                    {
                        "value": round(value, 2),
                        "limit_ups": limit_ups,
                        "breadth": round(breadth, 4),
                        "stock_count": stock_count,
                        "top_stocks": top_stocks_map.get((pd.to_datetime(trade_date), concept), []),
                    }
                )
            series.append({"name": concept, "data": data})

        review_10d = self._build_ten_day_review(merged, grouped, recent_dates, limit=4)
        review_mainlines = review_10d.get("mainlines", [])
        if review_mainlines:
            stable_mainline = review_mainlines[0]
            stable_names = {item["name"] for item in review_mainlines[:2]}
            stable_score = float(stable_mainline.get("latest_score") or stable_mainline.get("avg_score") or 0.0)
            matched = next((item for item in review_mainlines if item.get("name") == best_name), None)
            if matched:
                best_display_name = matched.get("display_name") or best_name
                best_focus_tags = matched.get("focus_tags", []) or []
                best_driver_summary = matched.get("driver_summary", "")

            should_use_stable = (
                (best_score < 18 and int(stable_mainline.get("consecutive_days", 0)) >= 3)
                or (
                    best_name not in stable_names
                    and best_score < stable_score + max(4.0, stable_score * 0.15)
                    and int(stable_mainline.get("consecutive_days", 0)) >= 4
                    and stable_score >= 18
                )
            )
            if should_use_stable:
                best_name = stable_mainline["name"]
                best_display_name = stable_mainline.get("display_name") or best_name
                best_score = stable_score
                best_reason = (
                    f"近10日上榜{stable_mainline.get('active_days', 0)}天，"
                    f"连续{stable_mainline.get('consecutive_days', 0)}天，"
                    f"最高{stable_mainline.get('max_limit_ups', 0)}家涨停"
                )

            if best_name == stable_mainline.get("name"):
                best_focus_tags = stable_mainline.get("focus_tags", best_focus_tags)
                best_driver_summary = stable_mainline.get("driver_summary", best_driver_summary)

        if not best_display_name:
            best_display_name = best_name

        deduction_parts = [
            f"【当前主线：{best_display_name}】",
            f"评分：{best_score:.2f}",
            f"依据：{best_reason}",
        ]
        if best_driver_summary:
            deduction_parts.append(best_driver_summary)
        if review_10d.get("summary"):
            deduction_parts.append(f"近10日复盘：{review_10d['summary']}")

        return {
            "dates": dates,
            "series": series,
            "analysis": {
                "phase": "Mainline_MultiFactor",
                "top_mainline": {
                    "name": best_name,
                    "display_name": best_display_name,
                    "score": round(best_score, 2),
                    "reason": best_reason,
                    "focus_tags": best_focus_tags,
                    "driver_summary": best_driver_summary,
                },
                "review_10d": review_10d,
                "deduction": "\n".join(deduction_parts),
            },
        }


mainline_analyst = MainlineAnalyst()
