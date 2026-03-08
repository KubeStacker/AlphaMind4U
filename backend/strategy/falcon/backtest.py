from __future__ import annotations

import json
from typing import Any

import pandas as pd

from strategy.falcon.ports import FalconDataProviderPort


PICK_FRAME_COLUMNS = [
    "ts_code",
    "name",
    "strategy_score",
    "confidence",
    "signal_label",
    "score_breakdown",
]


def build_forward_eval_rows(
    provider: FalconDataProviderPort,
    run_id: int,
    strategy_id: str,
    trade_date: str,
    picks_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    if picks_df is None or picks_df.empty:
        return []

    codes = picks_df["ts_code"].astype(str).tolist()
    f5 = provider.forward_returns(trade_date=trade_date, ts_codes=codes, horizon=5)
    f10 = provider.forward_returns(trade_date=trade_date, ts_codes=codes, horizon=10)

    map5 = {str(r["ts_code"]): float(r["fwd_ret"]) for _, r in f5.iterrows()} if not f5.empty else {}
    map10 = {str(r["ts_code"]): float(r["fwd_ret"]) for _, r in f10.iterrows()} if not f10.empty else {}

    map_rule_ret: dict[str, float | None] = {}
    map_rule_hold: dict[str, int | None] = {}
    map_rule_reason: dict[str, str] = {}
    if strategy_id == "falcon_momentum":
        stop_map: dict[str, float] = {}
        for _, row in picks_df.iterrows():
            code = str(row["ts_code"])
            sb = row.get("score_breakdown")
            if isinstance(sb, str):
                try:
                    sb = json.loads(sb)
                except Exception:
                    sb = {}
            stop_price = 0.0
            if isinstance(sb, dict):
                try:
                    stop_price = float(sb.get("stop_loss_price") or 0.0)
                except Exception:
                    stop_price = 0.0
            if stop_price > 0:
                stop_map[code] = stop_price

        if stop_map:
            ev = provider.event_exit_returns(
                trade_date=trade_date,
                ts_codes=codes,
                stop_loss_map=stop_map,
                max_horizon=20,
            )
            if not ev.empty:
                for _, r in ev.iterrows():
                    c = str(r["ts_code"])
                    map_rule_ret[c] = None if pd.isna(r.get("fwd_ret")) else float(r["fwd_ret"])
                    map_rule_hold[c] = None if pd.isna(r.get("hold_days")) else int(r.get("hold_days"))
                    map_rule_reason[c] = str(r.get("exit_reason") or "")

    rows: list[dict[str, Any]] = []
    for _, row in picks_df.iterrows():
        code = str(row["ts_code"])
        r5 = map5.get(code)
        r10 = map_rule_ret.get(code) if code in map_rule_ret else map10.get(code)
        rows.append(
            {
                "run_id": run_id,
                "strategy_id": strategy_id,
                "trade_date": trade_date,
                "ts_code": code,
                "ret_5d": r5,
                "ret_10d": r10,
                "hit_5d": None if r5 is None else bool(r5 > 0),
                "hit_10d": None if r10 is None else bool(r10 > 0),
                "hold_days": map_rule_hold.get(code),
                "exit_reason": map_rule_reason.get(code, ""),
            }
        )
    return rows


def summarize_run(picks_df: pd.DataFrame, eval_rows: list[dict[str, Any]]) -> dict[str, Any]:
    n = 0 if picks_df is None else int(len(picks_df))
    avg_score = float(picks_df["strategy_score"].mean()) if n else None
    avg_conf = float(picks_df["confidence"].mean()) if n else None

    eval_df = pd.DataFrame(eval_rows) if eval_rows else pd.DataFrame()
    if eval_df.empty:
        return {
            "pick_count": n,
            "avg_score": avg_score,
            "avg_confidence": avg_conf,
            "hit_5d": None,
            "hit_10d": None,
            "ret_5d": None,
            "ret_10d": None,
        }

    valid_5 = eval_df["ret_5d"].dropna()
    valid_10 = eval_df["ret_10d"].dropna()

    return {
        "pick_count": n,
        "avg_score": avg_score,
        "avg_confidence": avg_conf,
        "hit_5d": None if valid_5.empty else float((valid_5 > 0).mean()),
        "hit_10d": None if valid_10.empty else float((valid_10 > 0).mean()),
        "ret_5d": None if valid_5.empty else float(valid_5.mean()),
        "ret_10d": None if valid_10.empty else float(valid_10.mean()),
    }


def coerce_pick_frame(rows: pd.DataFrame | list[dict[str, Any]] | None) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        df = rows.copy()
    else:
        df = pd.DataFrame(rows or [])

    if df.empty:
        return pd.DataFrame(columns=PICK_FRAME_COLUMNS)

    if "ts_code" not in df.columns:
        return pd.DataFrame(columns=PICK_FRAME_COLUMNS)

    if "name" not in df.columns:
        df["name"] = ""
    if "strategy_score" not in df.columns:
        df["strategy_score"] = 0.0
    if "confidence" not in df.columns:
        df["confidence"] = 0.0
    if "signal_label" not in df.columns:
        df["signal_label"] = "观察"
    if "score_breakdown" not in df.columns:
        df["score_breakdown"] = [{} for _ in range(len(df))]

    df["ts_code"] = df["ts_code"].astype(str)
    df["name"] = df["name"].fillna("").astype(str)
    df["strategy_score"] = pd.to_numeric(df["strategy_score"], errors="coerce").fillna(0.0)
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)
    df["signal_label"] = df["signal_label"].fillna("观察").astype(str)
    df["score_breakdown"] = df["score_breakdown"].apply(lambda x: x if isinstance(x, dict) else {})
    return df[PICK_FRAME_COLUMNS].copy()


def summarize_eval_frame(
    picks_df: pd.DataFrame | list[dict[str, Any]] | None,
    eval_rows: list[dict[str, Any]],
    success_threshold_5d: float = 0.0,
    success_threshold_10d: float = 0.0,
) -> dict[str, Any]:
    pick_frame = coerce_pick_frame(picks_df)
    summary = summarize_run(pick_frame, eval_rows)

    eval_df = pd.DataFrame(eval_rows) if eval_rows else pd.DataFrame()
    if eval_df.empty:
        summary["sample_count_5d"] = 0
        summary["sample_count_10d"] = 0
        if success_threshold_5d > 0:
            summary["success_threshold_5d"] = float(success_threshold_5d)
            summary["hit_target_5d"] = None
        if success_threshold_10d > 0:
            summary["success_threshold_10d"] = float(success_threshold_10d)
            summary["hit_target_10d"] = None
        return summary

    valid_5 = pd.to_numeric(eval_df.get("ret_5d"), errors="coerce").dropna()
    valid_10 = pd.to_numeric(eval_df.get("ret_10d"), errors="coerce").dropna()
    summary["sample_count_5d"] = int(valid_5.shape[0])
    summary["sample_count_10d"] = int(valid_10.shape[0])

    if success_threshold_5d > 0:
        summary["success_threshold_5d"] = float(success_threshold_5d)
        summary["hit_target_5d"] = None if valid_5.empty else float((valid_5 >= float(success_threshold_5d)).mean())
    if success_threshold_10d > 0:
        summary["success_threshold_10d"] = float(success_threshold_10d)
        summary["hit_target_10d"] = None if valid_10.empty else float((valid_10 >= float(success_threshold_10d)).mean())

    return summary
