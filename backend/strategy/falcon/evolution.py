from __future__ import annotations

from typing import Any

import pandas as pd


def evolve_params_from_history(
    history_df: pd.DataFrame,
    current_params: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    最小可用进化器：基于历史 +10 命中率选择更优 top_n。
    """
    params = dict(current_params or {})
    candidates = [8, 12, 16, 20, 25]

    if history_df is None or history_df.empty:
        return params, {"reason": "history_empty", "selected_top_n": params.get("top_n", 20)}

    scored = []
    for top_n in candidates:
        sub = history_df[history_df["rank_no"] <= top_n].copy()
        if sub.empty:
            continue
        valid = sub["ret_10d"].dropna()
        if valid.empty:
            continue
        hit10 = float((valid > 0).mean())
        mean10 = float(valid.mean())
        score = 0.75 * hit10 + 0.25 * max(-0.3, min(0.3, mean10))
        scored.append({"top_n": top_n, "hit10": hit10, "mean10": mean10, "score": score})

    if not scored:
        return params, {"reason": "no_valid_eval", "selected_top_n": params.get("top_n", 20)}

    best = sorted(scored, key=lambda x: (x["score"], x["hit10"], x["mean10"]), reverse=True)[0]
    next_params = dict(params)
    next_params["top_n"] = int(best["top_n"])

    details = {
        "reason": "ok",
        "selected_top_n": int(best["top_n"]),
        "grid": scored,
    }
    return next_params, details
