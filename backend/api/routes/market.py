# /backend/api/routes/market.py

import logging
import json
import math
import arrow
import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from db.connection import fetch_df
from etl.sync import sync_engine

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Market"])

FACTOR_FIELD_LABELS = {
    "trend_score": "趋势因子",
    "liquidity_score": "流动性因子",
    "quality_score": "质量因子",
    "value_score": "估值因子",
    "flow_score": "资金因子",
    "event_score": "事件因子",
    "factor_score": "综合因子",
    "rps_250": "RPS250",
    "turnover_rate": "换手率",
    "volume_ratio": "量比",
    "net_mf_ratio": "净流入占比",
    "roe": "ROE",
    "pe_ttm": "PE_TTM",
}

def _safe_float(v):
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None

def _extract_pct_from_quote(df):
    if df is None or df.empty:
        return None
    row = df.iloc[0]
    row_map = {str(k).lower(): row[k] for k in row.index}
    for key in ("pct_chg", "pct_change", "changepercent"):
        if key in row_map and row_map[key] is not None:
            val = _safe_float(row_map[key])
            if val is not None:
                return val
    price = None
    pre_close = None
    for k in ("price", "current", "close"):
        if k in row_map:
            price = _safe_float(row_map[k])
            if price is not None:
                break
    for k in ("pre_close", "yclose"):
        if k in row_map:
            pre_close = _safe_float(row_map[k])
            if pre_close is not None:
                break
    if price is not None and pre_close is not None and pre_close > 0:
        return (price - pre_close) / pre_close * 100.0
    return None

def _sanitize_json_value(val):
    if val is None:
        return 0
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return 0.0
        return val
    if isinstance(val, dict):
        return {k: _sanitize_json_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_sanitize_json_value(v) for v in val]
    return val


def _normalize_weight_series(raw: pd.Series, target_total: float) -> pd.Series:
    if raw.empty or raw.sum() <= 0 or target_total <= 0:
        return pd.Series(0.0, index=raw.index)
    return raw / raw.sum() * target_total


def _apply_weight_cap(weights: pd.Series, cap: float, target_total: float) -> pd.Series:
    weights = weights.clip(lower=0.0).copy()
    if weights.empty or target_total <= 0:
        return weights
    if cap <= 0:
        return _normalize_weight_series(weights, target_total)

    for _ in range(8):
        over_mask = weights > cap
        if not over_mask.any():
            break
        excess = float((weights[over_mask] - cap).sum())
        weights.loc[over_mask] = cap
        under_mask = weights < cap - 1e-9
        if excess <= 0 or not under_mask.any():
            break
        room = (cap - weights[under_mask]).clip(lower=0.0)
        if room.sum() <= 0:
            break
        weights.loc[under_mask] += room / room.sum() * excess

    total = float(weights.sum())
    if total > 0:
        weights *= target_total / total
    return weights


def _select_low_correlation_candidates(candidates: pd.DataFrame, trade_date: str, top_n: int) -> pd.DataFrame:
    if candidates.empty or len(candidates) <= 1:
        return candidates.head(top_n).copy()

    lookback_start = arrow.get(trade_date).shift(days=-90).format("YYYY-MM-DD")
    codes = candidates["ts_code"].dropna().astype(str).tolist()
    placeholders = ",".join(["?"] * len(codes))
    returns_df = fetch_df(
        f"""
        SELECT trade_date, ts_code, pct_chg
        FROM daily_price
        WHERE trade_date BETWEEN ? AND ?
          AND ts_code IN ({placeholders})
        ORDER BY trade_date, ts_code
        """,
        params=[lookback_start, trade_date, *codes],
    )
    if returns_df.empty:
        return candidates.head(top_n).copy()

    pivot = returns_df.pivot(index="trade_date", columns="ts_code", values="pct_chg")
    corr = pivot.corr(min_periods=15)

    selected_codes = []
    overflow_codes = []
    for code in codes:
        if len(selected_codes) >= top_n:
            break
        if code not in corr.columns:
            selected_codes.append(code)
            continue
        if all(
            other not in corr.columns
            or pd.isna(corr.loc[code, other])
            or abs(float(corr.loc[code, other])) <= 0.8
            for other in selected_codes
        ):
            selected_codes.append(code)
        else:
            overflow_codes.append(code)

    for code in overflow_codes:
        if len(selected_codes) >= top_n:
            break
        if code not in selected_codes:
            selected_codes.append(code)

    selected = candidates[candidates["ts_code"].isin(selected_codes)].copy()
    selected["selection_order"] = selected["ts_code"].map({code: idx for idx, code in enumerate(selected_codes)})
    return selected.sort_values(["selection_order", "composite_score"], ascending=[True, False]).head(top_n)

@router.get("/market_sentiment")
def get_market_sentiment(days: int = 365):
    """获取市场情绪历史数据 - 返回从旧到新的时间序列"""
    try:
        # 先获取最近 N 天的数据（按日期倒序），然后反转成正序
        df = fetch_df(f"SELECT trade_date, score, label, details FROM market_sentiment ORDER BY trade_date DESC LIMIT {days}")
        if df.empty:
            return {"status": "success", "data": {"dates": [], "sentiment": [], "index": []}}
        
        # 反转数据顺序，实现从旧到新
        records = df.iloc[::-1].to_dict('records')
        dates = [str(r['trade_date'])[:10] for r in records]
        sentiment = []
        for r in records:
            details = r.get('details')
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except Exception:
                    details = {}
            details = _sanitize_json_value(details)
            sentiment.append({
                'trade_date': str(r['trade_date'])[:10],
                'value': _sanitize_json_value(r['score']),
                'label': r['label'] or "观望",
                'details': details
            })
        
        # 获取上证指数数据（指数数据在 market_index 表）
        min_date = dates[0]
        max_date = dates[-1]
        index_df = fetch_df(
            """
            SELECT trade_date, close
            FROM market_index
            WHERE ts_code = '000001.SH'
              AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date
            """,
            (min_date, max_date)
        )
        index_map = {}
        if not index_df.empty:
            for _, row in index_df.iterrows():
                index_map[str(row['trade_date'])[:10]] = _sanitize_json_value(row['close'])
        index = []
        last_index = 0.0
        for d in dates:
            iv = _sanitize_json_value(index_map.get(d, last_index))
            last_index = iv
            index.append(iv)
        
        return {"status": "success", "data": {"dates": dates, "sentiment": sentiment, "index": index}}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/sentiment/preview")
def get_sentiment_preview(
    index_ts_code: str = "000300.SH",
    star50_ts_code: str = "000688.SH",
    index_pct_chg: float | None = None,
    star50_pct_chg: float | None = None,
    src: str = "dc"
):
    """
    盘中情绪预估（建议 14:50 调用）：
    - 若未显式传入涨跌幅，则尝试通过 Tushare realtime_quote 获取
    - 结果仅用于预估，不写入 market_sentiment
    """
    from strategy.sentiment import sentiment_analyst

    realtime_debug = {}
    try:
        if index_pct_chg is None or star50_pct_chg is None:
            provider = sync_engine.provider

            if index_pct_chg is None:
                q_idx = provider.realtime_quote(ts_code=index_ts_code, src=src)
                index_pct_chg = _extract_pct_from_quote(q_idx)
                realtime_debug["index_quote_rows"] = 0 if q_idx is None else len(q_idx)

            if star50_pct_chg is None:
                q_star = provider.realtime_quote(ts_code=star50_ts_code, src=src)
                star50_pct_chg = _extract_pct_from_quote(q_star)
                realtime_debug["star50_quote_rows"] = 0 if q_star is None else len(q_star)
    except Exception as e:
        logger.warning(f"获取实时行情失败，将使用手动入参: {e}")
        realtime_debug["warning"] = str(e)

    if index_pct_chg is None:
        raise HTTPException(
            status_code=400,
            detail="无法获取 index_pct_chg。请手动传参 index_pct_chg，例如 -0.35"
        )

    try:
        result = sentiment_analyst.preview_next_day(
            index_pct_chg=index_pct_chg,
            star50_pct_chg=star50_pct_chg
        )
        result["source"] = {
            "index_ts_code": index_ts_code,
            "star50_ts_code": star50_ts_code,
            "src": src,
            "realtime_debug": realtime_debug
        }
        return {"status": "success", "data": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"盘中预估失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"盘中预估失败: {e}")

@router.get("/backtest_result")
def get_backtest_result(optimize: bool = True):
    """获取情绪策略回测结果（用于前端弹窗展示）"""
    try:
        from strategy.sentiment import sentiment_analyst
        import datetime

        if optimize:
            result, best_policy = sentiment_analyst.optimize_backtest_policy()
        else:
            result = sentiment_analyst.backtest_star50()
            best_policy = result.get('policy') if result else {}

        if not result:
            return {"status": "error", "message": "暂无可用回测结果"}

        return {
            "status": "success",
            "data": {
                "metrics": result.get("metrics", {}),
                "attribution": result.get("attribution", {}),
                "trades": result.get("trades", []),
                "policy": best_policy or result.get("policy", {}),
                "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
    except Exception as e:
        logger.error(f"获取回测结果失败: {e}")
        return {"status": "error", "message": str(e)}

@router.get("/backtest_grid")
def get_backtest_grid():
    """诊断：对每个leverage+floor组合跑回测，返回metrics对比"""
    try:
        from strategy.sentiment import sentiment_analyst
        from strategy.sentiment.config import SENTIMENT_CONFIG
        bt_cfg = SENTIMENT_CONFIG.get("backtest", {})
        opt_cfg = bt_cfg.get("optimizer", {})
        leverage_grid = opt_cfg.get("leverage_grid", [1.0, 1.2, 1.5, 2.0])
        trend_floor_grid = opt_cfg.get("trend_floor_grid", [0.0])
        max_dd_limit = float(opt_cfg.get("max_drawdown_limit", 0.35))

        results = []
        for lev in leverage_grid:
            for floor in trend_floor_grid:
                policy = {
                    "leverage": float(lev),
                    "trend_floor_enabled": True,
                    "trend_floor_pos": float(floor),
                    "fee_rate": float(bt_cfg.get("fee_rate", 0.0015)),
                    "ma_window": int(bt_cfg.get("ma_window", 20))
                }
                res = sentiment_analyst.backtest_star50(initial_capital=100000, policy=policy)
                if not res:
                    continue
                m = res.get("metrics", {})
                total_ret = float(str(m.get("total_return", "0%")).replace("%", "")) / 100.0
                max_dd = abs(float(str(m.get("max_drawdown", "0%")).replace("%", "")) / 100.0)
                sharpe = float(m.get("sharpe", 0))
                score = total_ret * 1.0 + sharpe * 0.5 - max_dd * 2.0
                if total_ret >= float(opt_cfg.get("target_total_return", 1.0)):
                    score += 2.0
                passed = max_dd <= max_dd_limit
                results.append({
                    "leverage": lev, "floor": floor,
                    "return": f"{total_ret*100:.2f}%",
                    "max_dd": f"{max_dd*100:.2f}%",
                    "sharpe": round(sharpe, 2),
                    "score": round(score, 4),
                    "dd_passed": passed,
                    "trades": m.get("total_trades"),
                    "win_rate": m.get("win_rate"),
                })
        return {"status": "success", "dd_limit": f"{max_dd_limit*100:.0f}%", "grid": results}
    except Exception as e:
        logger.error(f"回测网格诊断失败: {e}")
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}

@router.get("/backtest_walkforward")
def get_walkforward_result(train_days: int = 120, test_days: int = 40):
    """Walk-Forward 回测：滚动窗口训练+验证，消除过拟合"""
    try:
        from strategy.sentiment import sentiment_analyst
        import datetime

        result = sentiment_analyst.walk_forward_backtest(
            train_days=train_days, test_days=test_days
        )
        if not result:
            return {"status": "error", "message": "数据不足，无法执行 walk-forward 回测"}

        return {
            "status": "success",
            "data": result,
            "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logger.error(f"Walk-forward 回测失败: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

@router.get("/mainline_history")
def get_mainline_history(days: int = 30):
    """获取主线演变历史数据"""
    try:
        from strategy.mainline import mainline_analyst

        data = mainline_analyst.get_history(days=days)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"获取主线历史失败: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

@router.get("/market/suggestion")
def get_market_suggestion(
    use_preview: bool = False,
    index_pct_chg: float | None = None,
    star50_pct_chg: float | None = None,
    src: str = "dc"
):
    """
    统一市场建议:
    - use_preview=false: 使用已落库的最新EOD情绪 + 历史主线
    - use_preview=true: 使用盘中预估情绪 + 盘中主线预估
    """
    from strategy.sentiment import sentiment_analyst
    from strategy.mainline import mainline_analyst

    if use_preview:
        if index_pct_chg is None:
            q_idx = sync_engine.provider.realtime_quote(ts_code="000300.SH", src=src)
            index_pct_chg = _extract_pct_from_quote(q_idx)
        if index_pct_chg is None:
            raise HTTPException(status_code=400, detail="preview 模式下无法获得 index_pct_chg，请手动传参")

        if star50_pct_chg is None:
            q_star = sync_engine.provider.realtime_quote(ts_code="000688.SH", src=src)
            star50_pct_chg = _extract_pct_from_quote(q_star)

        sent = sentiment_analyst.preview_next_day(index_pct_chg=index_pct_chg, star50_pct_chg=star50_pct_chg)
        main = mainline_analyst.preview_intraday(provider=sync_engine.provider, limit=3, leaders_per_mainline=8, src=src)
        sent_exec = sent.get("plan", {}).get("execution", {})
    else:
        latest_df = fetch_df("SELECT trade_date, score, label, details FROM market_sentiment ORDER BY trade_date DESC LIMIT 1")
        if latest_df.empty:
            raise HTTPException(status_code=400, detail="market_sentiment 为空，请先完成情绪计算")
        row = latest_df.iloc[0]
        details = row.get("details")
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except Exception:
                details = {}
        details = details if isinstance(details, dict) else {}
        sent = {
            "baseline_trade_date": str(row["trade_date"])[:10],
            "projected_score": float(row.get("score", 50.0)),
            "plan": {
                "signal": details.get("signal", "PLAN_WATCH"),
                "next_day_strategy": details.get("strategy", ""),
                "execution": details.get("execution", {})
            }
        }
        main_hist = mainline_analyst.get_history(days=10)
        top_m = (((main_hist or {}).get("analysis") or {}).get("top_mainline") or {})
        main = {
            "as_of": None,
            "baseline_trade_date": sent["baseline_trade_date"],
            "data": [{
                "name": top_m.get("name", "混沌"),
                "score": float(top_m.get("score", 0.0)),
                "avg_ret": None,
                "up_ratio": None,
                "strong_ratio": None,
                "sample_size": 0,
                "top_stocks": []
            }]
        }
        sent_exec = sent.get("plan", {}).get("execution", {})

    top_line = main.get("data", [None])[0] if main.get("data") else None
    top_line_score = float(top_line.get("score", 0.0)) if top_line else 0.0
    top_line_name = top_line.get("name", "混沌") if top_line else "混沌"

    action = sent_exec.get("action") or ("BUY" if "BUY" in str(sent.get("plan", {}).get("signal", "")) else "WATCH")
    target_position = float(sent_exec.get("target_position", 0.0))
    confidence = float(sent_exec.get("confidence", 50.0))

    # 主线校验：主线太弱时降仓，避免"有情绪无主线"硬做
    if action == "BUY" and top_line_score < 12:
        target_position *= 0.7
        confidence = min(confidence, 62.0)
    if action == "BUY" and top_line_score < 7:
        action = "WATCH"
        target_position = min(target_position, 0.25)
        confidence = min(confidence, 55.0)

    suggestion = {
        "mode": "preview" if use_preview else "eod",
        "action": action,
        "target_position": round(max(0.0, min(1.0, target_position)), 2),
        "confidence": round(max(0.0, min(100.0, confidence)), 1),
        "risk_controls": {
            "stop_loss_pct": sent_exec.get("stop_loss_pct", 5.0),
            "take_profit_pct": sent_exec.get("take_profit_pct", 8.0),
            "tranche_count": sent_exec.get("tranche_count", 1)
        },
        "rationale": {
            "sentiment_signal": sent.get("plan", {}).get("signal"),
            "sentiment_strategy": sent.get("plan", {}).get("next_day_strategy"),
            "sentiment_score": sent.get("projected_score"),
            "top_mainline": top_line_name,
            "top_mainline_score": round(top_line_score, 2)
        },
        "sentiment": sent,
        "mainline": main
    }
    return {"status": "success", "data": suggestion}


@router.get("/factor/diagnostics")
def get_factor_diagnostics(
    factor: str = "factor_score",
    horizon: int = 5,
    days: int = 120,
    neutralize_industry: bool = False,
):
    if factor not in FACTOR_FIELD_LABELS:
        raise HTTPException(status_code=400, detail=f"不支持的因子字段: {factor}")
    horizon = max(1, min(int(horizon), 20))
    days = max(20, min(int(days), 720))

    latest_df = fetch_df("SELECT MAX(trade_date) AS trade_date FROM stock_factor_daily")
    if latest_df.empty or pd.isna(latest_df.iloc[0]["trade_date"]):
        raise HTTPException(status_code=400, detail="stock_factor_daily 为空，请先同步因子宽表")

    end_date = latest_df.iloc[0]["trade_date"]
    end_str = end_date.strftime("%Y-%m-%d") if hasattr(end_date, "strftime") else str(end_date)
    start_str = arrow.get(end_str).shift(days=-days).format("YYYY-MM-DD")
    future_end = arrow.get(end_str).shift(days=horizon + 10).format("YYYY-MM-DD")

    query = f"""
    WITH price_window AS (
        SELECT
            trade_date,
            ts_code,
            LEAD(close, {horizon}) OVER (PARTITION BY ts_code ORDER BY trade_date) / NULLIF(close, 0) - 1 AS forward_return
        FROM daily_price
        WHERE trade_date BETWEEN ? AND ?
    )
    SELECT
        f.trade_date,
        f.ts_code,
        COALESCE(NULLIF(f.sw_l1_name, ''), NULLIF(f.industry, ''), '未知行业') AS industry_name,
        f.{factor} AS factor_value,
        p.forward_return
    FROM stock_factor_daily f
    JOIN price_window p
      ON f.trade_date = p.trade_date AND f.ts_code = p.ts_code
    WHERE f.trade_date BETWEEN ? AND ?
      AND f.{factor} IS NOT NULL
      AND p.forward_return IS NOT NULL
    ORDER BY f.trade_date, f.ts_code
    """
    df = fetch_df(query, params=[start_str, future_end, start_str, end_str])
    if df.empty:
        raise HTTPException(status_code=400, detail="可用于因子诊断的数据不足")

    df["factor_value"] = pd.to_numeric(df["factor_value"], errors="coerce")
    df["forward_return"] = pd.to_numeric(df["forward_return"], errors="coerce")
    df = df.dropna(subset=["factor_value", "forward_return"])
    if df.empty:
        raise HTTPException(status_code=400, detail="因子值或未来收益为空，无法诊断")

    if neutralize_industry:
        df["factor_value"] = df["factor_value"] - df.groupby(["trade_date", "industry_name"])["factor_value"].transform("mean")
        df["forward_return"] = df["forward_return"] - df.groupby(["trade_date", "industry_name"])["forward_return"].transform("mean")

    daily_metrics = []
    quintile_frames = []
    for trade_date, group in df.groupby("trade_date"):
        grp = group.dropna(subset=["factor_value", "forward_return"]).copy()
        if len(grp) < 20 or grp["factor_value"].nunique() < 5:
            continue

        ic = grp["factor_value"].corr(grp["forward_return"], method="pearson")
        rank_ic = grp["factor_value"].rank(method="average").corr(
            grp["forward_return"].rank(method="average"),
            method="pearson",
        )
        if pd.notna(ic) or pd.notna(rank_ic):
            daily_metrics.append({
                "trade_date": str(trade_date)[:10],
                "ic": None if pd.isna(ic) else float(ic),
                "rank_ic": None if pd.isna(rank_ic) else float(rank_ic),
                "sample_count": int(len(grp)),
            })

        try:
            grp["quintile"] = pd.qcut(grp["factor_value"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
            quintile_frames.append(grp[["trade_date", "quintile", "forward_return"]])
        except Exception:
            continue

    if not daily_metrics:
        raise HTTPException(status_code=400, detail="样本不足，无法形成稳定的 IC/RankIC 序列")

    metrics_df = pd.DataFrame(daily_metrics)
    ic_std = metrics_df["ic"].std(ddof=0)
    rank_ic_std = metrics_df["rank_ic"].std(ddof=0)

    quintile_summary = []
    if quintile_frames:
        quintile_df = pd.concat(quintile_frames, ignore_index=True)
        q_stats = (
            quintile_df.groupby("quintile")["forward_return"]
            .agg(["mean", "count"])
            .reset_index()
            .sort_values("quintile")
        )
        for _, row in q_stats.iterrows():
            quintile_summary.append({
                "quintile": int(row["quintile"]),
                "avg_forward_return": round(float(row["mean"]) * 100, 3),
                "sample_count": int(row["count"]),
            })

    top_bottom_spread = None
    if len(quintile_summary) >= 5:
        top_bottom_spread = round(
            float(quintile_summary[-1]["avg_forward_return"] - quintile_summary[0]["avg_forward_return"]),
            3,
        )

    return {
        "status": "success",
        "data": {
            "factor": factor,
            "factor_label": FACTOR_FIELD_LABELS[factor],
            "horizon_days": horizon,
            "window_days": days,
            "industry_neutralized": neutralize_industry,
            "sample_days": int(len(metrics_df)),
            "mean_ic": round(float(metrics_df["ic"].mean()), 4),
            "mean_rank_ic": round(float(metrics_df["rank_ic"].mean()), 4),
            "ic_ir": round(float(metrics_df["ic"].mean() / ic_std), 4) if ic_std and not math.isnan(ic_std) else None,
            "rank_ic_ir": round(float(metrics_df["rank_ic"].mean() / rank_ic_std), 4) if rank_ic_std and not math.isnan(rank_ic_std) else None,
            "avg_sample_count": round(float(metrics_df["sample_count"].mean()), 1),
            "quintiles": quintile_summary,
            "top_bottom_spread_pct": top_bottom_spread,
            "daily_metrics": daily_metrics[-60:],
        },
    }


@router.get("/portfolio/recommendation")
def get_portfolio_recommendation(
    top_n: int = 8,
    leaders_per_mainline: int = 5,
    min_leader_score: float = 60.0,
    min_factor_score: float = 55.0,
    use_preview: bool = False,
    index_pct_chg: float | None = None,
    star50_pct_chg: float | None = None,
    src: str = "dc",
):
    from api.routes.stocks import get_mainline_leaders

    top_n = max(1, min(int(top_n), 20))
    leaders_per_mainline = max(1, min(int(leaders_per_mainline), 10))

    suggestion_payload = get_market_suggestion(
        use_preview=use_preview,
        index_pct_chg=index_pct_chg,
        star50_pct_chg=star50_pct_chg,
        src=src,
    )
    suggestion = suggestion_payload.get("data", {})
    target_position = float(suggestion.get("target_position", 0.0) or 0.0)
    action = suggestion.get("action", "WATCH")

    if target_position <= 0.01 or action == "WATCH":
        return {
            "status": "success",
            "data": {
                "trade_mode": action,
                "target_position": round(target_position, 2),
                "cash_weight": round(1 - target_position, 4),
                "positions": [],
                "regime": suggestion,
            },
        }

    leaders_payload = get_mainline_leaders(limit=leaders_per_mainline, min_score=min_leader_score)
    mainlines = leaders_payload.get("mainlines", []) if isinstance(leaders_payload, dict) else []
    if not mainlines:
        raise HTTPException(status_code=400, detail="当前主线龙头池为空，无法构建组合")

    latest_factor_df = fetch_df("SELECT MAX(trade_date) AS trade_date FROM stock_factor_daily")
    if latest_factor_df.empty or pd.isna(latest_factor_df.iloc[0]["trade_date"]):
        raise HTTPException(status_code=400, detail="stock_factor_daily 为空，请先同步因子宽表")

    factor_trade_date = latest_factor_df.iloc[0]["trade_date"]
    factor_trade_date_str = (
        factor_trade_date.strftime("%Y-%m-%d")
        if hasattr(factor_trade_date, "strftime")
        else str(factor_trade_date)
    )

    sector_strength = {}
    leader_rows = []
    for mainline in mainlines[:3]:
        sector_name = mainline.get("display_sector") or mainline.get("sector") or ""
        sector_strength[sector_name] = float(mainline.get("strength", 0) or 0)
        for leader in mainline.get("leaders", []):
            leader_rows.append(
                {
                    "sector": sector_name,
                    "ts_code": leader.get("ts_code"),
                    "name": leader.get("name"),
                    "leader_score": float(leader.get("score", 0) or 0),
                    "leader_reason": leader.get("reason", ""),
                    "close": _safe_float(leader.get("close")) or 0.0,
                    "pct_chg": _safe_float(leader.get("pct_chg")) or 0.0,
                    "volume_ratio": _safe_float(leader.get("volume_ratio")) or 0.0,
                    "turnover_rate": _safe_float(leader.get("turnover_rate")) or 0.0,
                    "net_mf_amount": _safe_float(leader.get("net_mf_amount")) or 0.0,
                }
            )

    if not leader_rows:
        raise HTTPException(status_code=400, detail="主线存在，但没有可用龙头样本")

    leader_df = pd.DataFrame(leader_rows).drop_duplicates(subset=["ts_code"], keep="first")
    codes = leader_df["ts_code"].dropna().astype(str).tolist()
    placeholders = ",".join(["?"] * len(codes))
    factor_df = fetch_df(
        f"""
        SELECT
            ts_code,
            COALESCE(NULLIF(sw_l1_name, ''), NULLIF(industry, ''), '未知行业') AS industry_name,
            factor_score,
            trend_score,
            liquidity_score,
            quality_score,
            value_score,
            flow_score,
            event_score
        FROM stock_factor_daily
        WHERE trade_date = ?
          AND ts_code IN ({placeholders})
        """,
        params=[factor_trade_date_str, *codes],
    )

    merged = leader_df.merge(factor_df, on="ts_code", how="left")
    merged["industry_name"] = merged["industry_name"].fillna("未知行业")
    for col in ("factor_score", "trend_score", "liquidity_score", "quality_score", "value_score", "flow_score", "event_score"):
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(50.0)

    merged = merged[merged["factor_score"] >= float(min_factor_score)].copy()
    if merged.empty:
        raise HTTPException(status_code=400, detail="主线候选中没有达到最小因子分的股票")

    merged["composite_score"] = (merged["factor_score"] * 0.55 + merged["leader_score"] * 0.45).round(2)
    merged = merged.sort_values(["composite_score", "leader_score", "factor_score"], ascending=False)
    merged = _select_low_correlation_candidates(merged, factor_trade_date_str, top_n)

    raw_weights = merged["composite_score"].clip(lower=1.0)
    merged["raw_weight"] = _normalize_weight_series(raw_weights, target_position)

    sector_totals = merged.groupby("sector")["raw_weight"].sum().sort_values(ascending=False)
    sector_cap = min(0.55 * target_position, 0.45)
    adjusted_sector_totals = _apply_weight_cap(sector_totals, sector_cap, target_position)
    merged["sector_target_weight"] = merged["sector"].map(adjusted_sector_totals.to_dict()).fillna(0.0)

    position_weights = []
    name_cap = min(0.20, max(target_position / max(top_n - 1, 2), 0.08))
    for sector_name, group in merged.groupby("sector", sort=False):
        sector_budget = float(group["sector_target_weight"].iloc[0])
        sector_raw = _normalize_weight_series(group["composite_score"].clip(lower=1.0), sector_budget)
        sector_final = _apply_weight_cap(sector_raw, name_cap, sector_budget)
        position_weights.append(sector_final)

    merged["target_weight"] = pd.concat(position_weights).sort_index()
    merged["target_weight"] = _normalize_weight_series(merged["target_weight"], target_position)

    positions = []
    for _, row in merged.sort_values("target_weight", ascending=False).iterrows():
        positions.append(
            {
                "ts_code": row["ts_code"],
                "name": row["name"],
                "sector": row["sector"],
                "industry_name": row["industry_name"],
                "target_weight": round(float(row["target_weight"]), 4),
                "composite_score": round(float(row["composite_score"]), 2),
                "leader_score": round(float(row["leader_score"]), 2),
                "factor_score": round(float(row["factor_score"]), 2),
                "factor_breakdown": {
                    "trend_score": round(float(row["trend_score"]), 2),
                    "liquidity_score": round(float(row["liquidity_score"]), 2),
                    "quality_score": round(float(row["quality_score"]), 2),
                    "value_score": round(float(row["value_score"]), 2),
                    "flow_score": round(float(row["flow_score"]), 2),
                    "event_score": round(float(row["event_score"]), 2),
                },
                "leader_reason": row["leader_reason"],
                "close": round(float(row["close"]), 2),
                "pct_chg": round(float(row["pct_chg"]), 2),
                "turnover_rate": round(float(row["turnover_rate"]), 2),
                "volume_ratio": round(float(row["volume_ratio"]), 2),
                "net_mf_amount": round(float(row["net_mf_amount"]), 2),
            }
        )

    sector_allocations = []
    for sector_name, group in merged.groupby("sector"):
        sector_allocations.append(
            {
                "sector": sector_name,
                "strength": round(float(sector_strength.get(sector_name, 0.0)), 2),
                "target_weight": round(float(group["target_weight"].sum()), 4),
                "stock_count": int(len(group)),
            }
        )
    sector_allocations.sort(key=lambda item: item["target_weight"], reverse=True)

    return {
        "status": "success",
        "data": {
            "trade_date": factor_trade_date_str,
            "trade_mode": action,
            "target_position": round(target_position, 4),
            "cash_weight": round(1 - target_position, 4),
            "risk_controls": suggestion.get("risk_controls", {}),
            "regime": suggestion.get("rationale", {}),
            "sector_allocations": sector_allocations,
            "positions": positions,
        },
    }
