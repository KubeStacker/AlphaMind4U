# /backend/api/routes/market.py

import logging
import json
import math
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from db.connection import fetch_df
from etl.sync import sync_engine

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Market"])

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