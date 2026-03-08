# /backend/api/admin.py

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional
import arrow
import asyncio
import time
import logging
import math
import json
import pandas as pd

from etl.sync import sync_engine
from etl.utils.quality import quality_checker
from db.connection import get_db_connection, fetch_df
from passlib.context import CryptContext
from etl.calendar import trading_calendar

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Admin"])

# 统一加密上下文
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# --- 模型定义 ---

class SyncTaskParams(BaseModel):
    task: str = Field(..., description="任务类型: basic, calendar, price, index, moneyflow, financials, margin, fx, factors")
    years: int = 0
    days: int = 3
    ts_code: Optional[str] = None
    force: bool = False

class TrainKlinePatternParams(BaseModel):
    horizons: str = "3,5,10"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    full_history: bool = False
    min_confidence: float = 0.55
    positive_return_threshold: float = 0.0

class IntegrityParams(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class UserCreate(BaseModel):
    username: str
    password: str
    role: str = "viewer"

class PasswordChange(BaseModel):
    user_id: int
    new_password: str

class UserLogin(BaseModel):
    username: str
    password: str

# --- 任务持久化 ---
class TaskRegistry:
    @staticmethod
    def create_task(task_type: str, params: dict, task_key: str = None):
        import uuid
        import json
        import hashlib
        
        task_id = str(uuid.uuid4())[:8]
        params_str = json.dumps(params, sort_keys=True)
        if not task_key:
            task_key = hashlib.md5(f"{task_type}_{params_str}".encode()).hexdigest()
        
        with get_db_connection() as con:
            # 检查是否已有相同 key 的任务在排队或运行
            existing = con.execute(
                "SELECT task_id, status FROM etl_tasks WHERE task_key = ? AND status IN ('PENDING', 'RUNNING')", 
                (task_key,)
            ).fetchone()
            if existing:
                return existing[0], existing[1]
            
            # 如果是训练任务，确保全局只有一个 RUNNING/PENDING
            if task_type == "KLINE_TRAIN":
                global_running = con.execute(
                    "SELECT task_id FROM etl_tasks WHERE task_type = 'KLINE_TRAIN' AND status IN ('PENDING', 'RUNNING')"
                ).fetchone()
                if global_running:
                    return global_running[0], "ALREADY_EXISTS"

            con.execute(
                "INSERT INTO etl_tasks (task_id, task_key, task_type, params_json, status) VALUES (?, ?, ?, ?, ?)",
                (task_id, task_key, task_type, params_str, "PENDING")
            )
        return task_id, "PENDING"

    @staticmethod
    def update_status(task_id: str, status: str, error: str = None, progress: float = None):
        with get_db_connection() as con:
            updates = ["status = ?", "heartbeat_at = CURRENT_TIMESTAMP"]
            params = [status]
            if status == "RUNNING":
                updates.append("started_at = CURRENT_TIMESTAMP")
            elif status in ("COMPLETED", "FAILED"):
                updates.append("finished_at = CURRENT_TIMESTAMP")
            
            if error is not None:
                updates.append("error = ?")
                params.append(str(error))
            if progress is not None:
                updates.append("progress = ?")
                params.append(float(progress))
            
            params.append(task_id)
            con.execute(f"UPDATE etl_tasks SET {', '.join(updates)} WHERE task_id = ?", params)

    @staticmethod
    def get_pending_task():
        try:
            with get_db_connection() as con:
                # 恢复僵尸任务：10分钟没心跳的 RUNNING 改回 PENDING
                con.execute(
                    "UPDATE etl_tasks SET status = 'PENDING' WHERE status = 'RUNNING' AND (heartbeat_at < CURRENT_TIMESTAMP - INTERVAL 10 MINUTE OR heartbeat_at IS NULL)"
                )
                
                row = con.execute(
                    "SELECT task_id, task_type, params_json FROM etl_tasks WHERE status = 'PENDING' ORDER BY created_at LIMIT 1"
                ).fetchone()
                if row:
                    return {"task_id": row[0], "task_type": row[1], "params": json.loads(row[2])}
        except Exception as e:
            logger.error(f"获取待执行任务失败: {e}")
        return None

# --- 辅助函数 ---

def _normalize_ts_code(code: str) -> str:
    if not code: return ""
    code = str(code).upper().strip()
    if "." in code: return code
    # 简单启发式补齐
    if code.startswith("6"): return f"{code}.SH"
    if code.startswith("0") or code.startswith("3"): return f"{code}.SZ"
    if code.startswith("8") or code.startswith("4"): return f"{code}.BJ"
    return code

def _safe_float(v, default=None):
    try:
        if v is None: return default
        f = float(v)
        if math.isnan(f) or math.isinf(f): return default
        return f
    except:
        return default

def _build_watch_analyse(ts_code: str):
    """ 为自选股生成详细的技术面分析（包含 10D 历史） """
    try:
        # 获取最近 70 天数据，确保计算 MA60 后还有足够的历史展示 10D
        df = fetch_df(f"SELECT trade_date, open, high, low, close, vol as volume, amount, pct_chg FROM daily_price WHERE ts_code = '{ts_code}' ORDER BY trade_date DESC LIMIT 75")
        if df.empty or len(df) < 20:
            return {"summary": "数据不足", "history": [], "suggestion": "观望", "detail": {}}
        
        df = df.iloc[::-1].reset_index(drop=True) # 转为正序
        
        from etl.utils.patterns import PatternRecognizer, get_professional_commentary_detailed
        
        history = []
        # 计算最近 10 个交易日的简易信号
        for i in range(len(df) - 10, len(df)):
            sub_df = df.iloc[:i+1]
            if len(sub_df) < 5: continue
            
            recognizer = PatternRecognizer(sub_df)
            patterns = recognizer.recognize()
            detail = get_professional_commentary_detailed(sub_df, patterns)
            
            # 简单的信号推导逻辑
            suggestion = "观望"
            tone = "中性"
            
            # 1. 根据形态和涨跌幅推导 suggestion
            pct_today = sub_df.iloc[-1].get('pct_chg', 0)
            if any(p in ['老鸭头', '放量突破', '红三兵', '出水芙蓉'] for p in patterns) or pct_today > 5:
                suggestion = "关注"
            elif any(p in ['仙人指路', '多方炮', '探底回升'] for p in patterns):
                suggestion = "试错"
            elif any(p in ['三只乌鸦', '乌云盖顶', '高位放量'] for p in patterns) or pct_today < -5:
                suggestion = "减仓"
            
            # 2. 根据趋势推导 tone
            inst_view = detail.get("institution", [])
            for v in inst_view:
                if v.get("type") == "trend":
                    if v.get("level") == "strong": tone = "看多(强)"
                    elif v.get("level") == "medium": tone = "看多"
                    elif v.get("level") == "bearish": tone = "看空"
            
            if pct_today > 7: tone = "爆发"
            elif pct_today < -7: tone = "杀跌"
            
            history.append({
                "date": str(sub_df.iloc[-1]['trade_date'])[:10],
                "suggestion": suggestion,
                "tone": tone,
                "patterns": patterns
            })

        # 最新一天的完整分析
        latest_recognizer = PatternRecognizer(df)
        latest_patterns = latest_recognizer.recognize()
        latest_detail = get_professional_commentary_detailed(df, latest_patterns)
        
        return {
            "summary": latest_detail.get("summary", ""),
            "history": history,
            "suggestion": history[-1]["suggestion"] if history else "观望",
            "detail": latest_detail,
            "patterns": latest_patterns
        }
    except Exception as e:
        logger.warning(f"分析股票 {ts_code} 失败: {e}", exc_info=True)
        return {"summary": "分析失败", "history": [], "suggestion": "观望", "detail": {}}

def _build_sync_task(p: SyncTaskParams):
    task = p.task.lower()
    if task == "basic":
        return "基础信息同步", sync_engine.sync_stock_basic
    elif task == "calendar":
        return "交易日历同步", (sync_engine.sync_trade_cal, {"years": p.years})
    elif task == "price":
        return "日线行情同步", (sync_engine.sync_daily_price, {"years": p.years, "force": p.force})
    elif task == "index":
        return "指数数据同步", (sync_engine.sync_core_indices, {"years": p.years, "days": p.days})
    elif task == "moneyflow":
        return "资金流向同步", (sync_engine.sync_moneyflow, {"years": p.years, "days": p.days})
    elif task == "financials":
        return "财务数据同步", sync_engine.sync_financials
    elif task == "margin":
        return "两融数据同步", (sync_engine.sync_margin, {"days": p.days})
    elif task == "fx":
        return "外汇数据同步", sync_engine.sync_fx
    elif task == "factors":
        return "因子数据计算", sync_engine.fill_missing_factors
    else:
        raise ValueError(f"不支持的任务类型: {p.task}")

# 全局任务循环
async def task_worker():
    logger.info("持久化任务消费者已启动")
    while True:
        task_info = TaskRegistry.get_pending_task()
        if not task_info:
            await asyncio.sleep(5)
            continue
            
        task_id = task_info["task_id"]
        task_type = task_info["task_type"]
        params = task_info["params"]
        
        TaskRegistry.update_status(task_id, "RUNNING")
        logger.info(f"开始执行持久化任务 [{task_id}]: {task_type}")
        
        try:
            if task_type == "KLINE_TRAIN":
                await asyncio.to_thread(_run_kline_train_task, task_id, params)
            elif task_type == "SYNC":
                await asyncio.to_thread(_run_sync_task, task_id, params)
            elif task_type == "SENTIMENT":
                await asyncio.to_thread(_run_sentiment_task, task_id, params)
            else:
                raise ValueError(f"未知任务类型: {task_type}")
                
            TaskRegistry.update_status(task_id, "COMPLETED", progress=100.0)
            logger.info(f"任务 [{task_id}] 完成")
        except Exception as e:
            logger.error(f"任务 [{task_id}] 失败: {e}", exc_info=True)
            TaskRegistry.update_status(task_id, "FAILED", error=str(e))
        
        await asyncio.sleep(1)

def _run_sync_task(task_id, params):
    # 模拟原来的 sync 逻辑
    from pydantic import parse_obj_as
    p = SyncTaskParams(**params)
    task_name, task_obj = _build_sync_task(p)
    if isinstance(task_obj, tuple):
        fn, kwargs = task_obj
        fn(**kwargs)
    else:
        task_obj()

def _run_sentiment_task(task_id, params):
    days = params.get("days", 365)
    sync_index = params.get("sync_index", True)
    if sync_index:
        sync_engine.sync_core_indices(years=0, days=max(int(days), 30))
    sync_engine.calculate_market_sentiment(days=days)

def _run_kline_train_task(task_id, params):
    """
    优化的 K 线训练任务：
    1. 批量分块读取数据
    2. 单遍扫描完成训练与评估
    """
    from etl.utils.kline_patterns import build_combined_training_stats, save_pattern_calibration
    from pathlib import Path
    
    horizons = tuple(int(x.strip()) for x in params["horizons"].split(",") if x.strip())
    start_date = params.get("start_date")
    end_date = params.get("end_date")
    
    # 默认回溯 2 年，除非显式 full_history
    if not start_date and not params.get("full_history"):
        start_date = arrow.now().shift(years=-2).format("YYYY-MM-DD")
        
    output_path = Path(__file__).parent.parent / "etl" / "utils" / "kline_pattern_calibration.json"
    
    # 获取需要处理的所有 ts_code
    codes_df = fetch_df("SELECT DISTINCT ts_code FROM daily_price")
    all_codes = codes_df["ts_code"].tolist()
    total_codes = len(all_codes)
    
    chunk_size = 200
    all_parts = []
    
    for i in range(0, total_codes, chunk_size):
        chunk_codes = all_codes[i:i+chunk_size]
        placeholders = ",".join(["?"] * len(chunk_codes))
        
        conditions = [f"ts_code IN ({placeholders})"]
        q_params = list(chunk_codes)
        
        if start_date:
            conditions.append("trade_date >= ?")
            q_params.append(start_date)
        if end_date:
            conditions.append("trade_date <= ?")
            q_params.append(end_date)
            
        sql = f"SELECT trade_date, ts_code, open, high, low, close, pct_chg, vol, amount FROM daily_price WHERE {' AND '.join(conditions)} ORDER BY ts_code, trade_date"
        df_chunk = fetch_df(sql, q_params)
        
        if not df_chunk.empty:
            all_parts.append(df_chunk)
            
        progress = min(90.0, (i + len(chunk_codes)) / total_codes * 100 * 0.8) # 预留 20% 给计算
        TaskRegistry.update_status(task_id, "RUNNING", progress=progress)
        logger.info(f"Task [{task_id}] loading data: {i+len(chunk_codes)}/{total_codes} codes")

    if not all_parts:
        raise ValueError("未查询到有效数据")
        
    full_df = pd.concat(all_parts, ignore_index=True)
    TaskRegistry.update_status(task_id, "RUNNING", progress=85.0)
    
    # 单遍扫描
    calibration, summary = build_combined_training_stats(
        df=full_df,
        horizons=horizons,
        min_confidence=params["min_confidence"],
        positive_return_threshold=params["positive_return_threshold"]
    )
    
    save_pattern_calibration(calibration, str(output_path))
    TaskRegistry.update_status(task_id, "RUNNING", progress=100.0)
    
    logger.info(f"K线形态校准文件已原子化更新: {output_path}")
    if not summary.empty:
        primary = horizons[0]
        logger.info(f"Top patterns by {primary}d edge:\n{summary.head(10).to_string()}")

# 修改 API 接口使用新任务系统

@router.get("/tasks/status")
def get_tasks_status(limit: int = 20):
    """ 获取持久化任务状态 """
    with get_db_connection() as con:
        history = con.execute(
            "SELECT task_id, task_type, status, error, progress, CAST(created_at AS VARCHAR), CAST(finished_at AS VARCHAR) FROM etl_tasks ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        
    return {
        "tasks": [
            {
                "task_id": r[0],
                "task_type": r[1],
                "status": r[2],
                "error": r[3],
                "progress": r[4],
                "created_at": r[5],
                "finished_at": r[6]
            } for r in history
        ]
    }

@router.post("/etl/train_kline_patterns", status_code=202)
async def trigger_kline_pattern_training(params: TrainKlinePatternParams):
    tid, status = TaskRegistry.create_task("KLINE_TRAIN", params.dict())
    if status == "ALREADY_EXISTS":
        return {"message": "已有相同任务在排队或运行", "task_id": tid}
    return {"message": "训练任务已加入持久化队列", "task_id": tid}

@router.post("/etl/sync", status_code=202)
async def trigger_sync(params: SyncTaskParams):
    tid, status = TaskRegistry.create_task("SYNC", params.dict())
    return {"message": "同步任务已加入持久化队列", "task_id": tid}

@router.post("/etl/sentiment", status_code=202)
async def trigger_sentiment_sync(days: int = 365, sync_index: bool = True):
    params = {"days": days, "sync_index": sync_index}
    tid, status = TaskRegistry.create_task("SENTIMENT", params)
    return {"message": "情绪计算任务已加入持久化队列", "task_id": tid}

@router.get("/integrity")
def get_data_integrity_report(params: IntegrityParams = Depends()):
    if params.end_date is None:
        params.end_date = arrow.now().format("YYYY-MM-DD")
    if params.start_date is None:
        params.start_date = arrow.now().shift(years=-1).format("YYYY-MM-DD")
    try:
        report = quality_checker.get_comprehensive_report(start_date=params.start_date, end_date=params.end_date)
        return report
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"生成报告失败: {e}")

@router.get("/users")
def list_users():
    with get_db_connection() as con:
        users = con.execute("SELECT id, username, role, CAST(created_at AS VARCHAR) FROM users").fetchall()
        return [{"id": u[0], "username": u[1], "role": u[2], "created_at": u[3]} for u in users]

@router.post("/users")
def create_user(user: UserCreate):
    hashed_password = pwd_context.hash(user.password)
    try:
        with get_db_connection() as con:
            con.execute("INSERT INTO users (username, hashed_password, role) VALUES (?, ?, ?)", (user.username, hashed_password, user.role))
        return {"message": f"用户 {user.username} 创建成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"创建用户失败: {e}")

@router.delete("/users/{user_id}")
def delete_user(user_id: int):
    with get_db_connection() as con:
        con.execute("DELETE FROM users WHERE id = ?", (user_id,))
    return {"message": "用户已删除"}

@router.put("/users/password")
def change_password(data: PasswordChange):
    hashed_password = pwd_context.hash(data.new_password)
    with get_db_connection() as con:
        con.execute("UPDATE users SET hashed_password = ? WHERE id = ?", (hashed_password, data.user_id))
    return {"message": "密码修改成功"}

@router.get("/market_sentiment")
def get_market_sentiment(days: int = 365):
    """获取市场情绪历史数据 - 返回从旧到新的时间序列"""
    try:
        import json
        import math

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
    import math
    from strategy.sentiment import sentiment_analyst

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

        # 回退：price / pre_close 反推
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


@router.get("/mainline/preview")
def get_mainline_preview(limit: int = 5, leaders_per_mainline: int = 8, src: str = "dc"):
    """盘中主线预估（建议14:50附近调用）"""
    from strategy.mainline import mainline_analyst
    try:
        data = mainline_analyst.preview_intraday(
            provider=sync_engine.provider,
            limit=max(1, int(limit)),
            leaders_per_mainline=max(3, int(leaders_per_mainline)),
            src=src,
        )
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"盘中主线预估失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"盘中主线预估失败: {e}")


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
    import json
    import math
    from strategy.sentiment import sentiment_analyst
    from strategy.mainline import mainline_analyst

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

    # 主线校验：主线太弱时降仓，避免“有情绪无主线”硬做
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

@router.get("/margin_heatmap")
def get_margin_heatmap(days: int = 10, top_n: int = 30):
    """获取融资数据热力图 - 显示融资余额变化"""
    import arrow
    import pandas as pd
    try:
        start_date = arrow.now().shift(days=-days).format("YYYY-MM-DD")
        
        # 获取所有在日期范围内的交易日
        dates_query = f"""
            SELECT DISTINCT trade_date FROM stock_margin 
            WHERE trade_date >= '{start_date}'
            ORDER BY trade_date ASC
        """
        dates_df = fetch_df(dates_query)
        
        if dates_df.empty:
            return {"status": "success", "data": {"dates": [], "stocks": [], "matrix": []}}
        
        # 转换日期格式为 YYYY-MM-DD
        dates = []
        for d in dates_df['trade_date']:
            date_str = str(d)
            if ' ' in date_str:
                date_str = date_str.split(' ')[0]
            dates.append(date_str)
        
        # 获取所有股票在最近日期的融资余额排名
        latest_date = dates[-1] if dates else None
        if not latest_date:
            return {"status": "success", "data": {"dates": [], "stocks": [], "matrix": []}}
            
        top_stocks_df = fetch_df(f"""
            SELECT m.ts_code, s.name, m.rzye as rz_balance
            FROM stock_margin m
            LEFT JOIN stock_basic s ON m.ts_code = s.ts_code
            WHERE m.trade_date = '{latest_date}'
            ORDER BY m.rzye DESC
            LIMIT {top_n}
        """)
        
        stocks = [f"{row['ts_code']} {str(row['name'])[:6] if pd.notna(row['name']) else ''}" 
                  for _, row in top_stocks_df.iterrows()]
        
        # 获取这些股票在所有日期的数据
        if not stocks:
            return {"status": "success", "data": {"dates": dates, "stocks": [], "matrix": []}}
        
        ts_codes = [s.split()[0] for s in stocks]
        placeholders = ','.join([f"'{c}'" for c in ts_codes])
        
        matrix_df = fetch_df(f"""
            SELECT trade_date, ts_code, rzye
            FROM stock_margin 
            WHERE trade_date IN ({','.join([f"'{d}'" for d in dates])})
            AND ts_code IN ({placeholders})
            ORDER BY ts_code, trade_date
        """)
        
        # 构建矩阵
        matrix = []
        for ts_code in ts_codes:
            row_data = []
            stock_data = matrix_df[matrix_df['ts_code'] == ts_code]
            date_to_val = {str(row['trade_date']): float(row['rzye']) / 10000 if pd.notna(row['rzye']) else None 
                          for _, row in stock_data.iterrows()}
            for date in dates:
                row_data.append(date_to_val.get(date, None))
            matrix.append(row_data)
        
        return {"status": "success", "data": {
            "dates": dates,
            "stocks": stocks,
            "matrix": matrix,
            "title": "融资余额热力图 (亿元)"
        }}
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()}

@router.get("/data_verify")
def verify_data_accuracy(ts_code: str = "688256.SH"):
    """校验数据准确性 - 对比API与数据库"""
    import tushare as ts
    import pandas as pd
    from etl.config import settings
    import json
    
    def convert(obj):
        """转换numpy类型为Python原生类型"""
        if hasattr(obj, 'item'):  # numpy types
            return obj.item()
        elif isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(i) for i in obj]
        return obj
    
    try:
        # 获取最近3个交易日
        trade_dates = fetch_df("""
            SELECT trade_date FROM daily_price 
            WHERE ts_code = '000001.SH' 
            ORDER BY trade_date DESC 
            LIMIT 3
        """)
        if trade_dates.empty:
            return {"status": "error", "message": "没有交易日数据"}
        
        dates = [str(d).split()[0] for d in trade_dates['trade_date']]
        start_date = dates[-1]
        end_date = dates[0]
        
        token = settings.long_tushare_token if settings.tushare_token_type == "long" else settings.short_tushare_token
        pro = ts.pro_api(token)
        
        results = {}
        
        def normalize_date(d):
            """标准化日期格式为 YYYY-MM-DD"""
            s = str(d).replace('-', '').replace('/', '')[:8]
            if len(s) == 8:
                return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
            return str(d)[:10]
        
        # 1. 日线数据 - 对比close价格
        try:
            api_df = pro.daily(ts_code=ts_code, start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''))
            db_df = fetch_df(f"SELECT trade_date, close FROM daily_price WHERE ts_code='{ts_code}' AND trade_date BETWEEN '{start_date}' AND '{end_date}' ORDER BY trade_date")
            
            match = False
            if not api_df.empty and not db_df.empty:
                api_dict = {normalize_date(r['trade_date']): round(float(r['close']), 2) for _, r in api_df.iterrows()}
                db_dict = {normalize_date(r['trade_date']): round(float(r['close']), 2) for _, r in db_df.iterrows()}
                common_dates = set(api_dict.keys()) & set(db_dict.keys())
                if common_dates:
                    match = all(abs(api_dict[d] - db_dict[d]) < 0.01 for d in common_dates)
            
            results['daily_price'] = {
                'api': len(api_df),
                'db': len(db_df),
                'match': match,
                'dates': dates
            }
        except Exception as e:
            results['daily_price'] = {'error': str(e)}
        
        # 2. 资金流向 - 对比net_mf_vol
        try:
            api_df = pro.moneyflow(ts_code=ts_code, start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''))
            db_df = fetch_df(f"SELECT trade_date, net_mf_vol FROM stock_moneyflow WHERE ts_code='{ts_code}' AND trade_date BETWEEN '{start_date}' AND '{end_date}' ORDER BY trade_date")
            
            match = False
            if not api_df.empty and not db_df.empty:
                api_dict = {normalize_date(r['trade_date']): float(r['net_mf_vol']) for _, r in api_df.iterrows()}
                db_dict = {normalize_date(r['trade_date']): float(r['net_mf_vol']) for _, r in db_df.iterrows()}
                common_dates = set(api_dict.keys()) & set(db_dict.keys())
                if common_dates:
                    match = all(abs(api_dict[d] - db_dict[d]) < 1 for d in common_dates)
            
            results['stock_moneyflow'] = {
                'api': len(api_df),
                'db': len(db_df),
                'match': match,
                'dates': dates
            }
        except Exception as e:
            results['stock_moneyflow'] = {'error': str(e)}
        
        # 3. 融资余额 - 对比rzye
        try:
            api_df = pro.margin_detail()
            api_df = api_df[api_df['ts_code'] == ts_code] if not api_df.empty else api_df
            db_df = fetch_df(f"SELECT trade_date, rzye FROM stock_margin WHERE ts_code='{ts_code}' AND trade_date BETWEEN '{start_date}' AND '{end_date}' ORDER BY trade_date")
            
            match = False
            if not api_df.empty and not db_df.empty:
                api_dict = {normalize_date(r['trade_date']): float(r['rzye']) for _, r in api_df.iterrows()}
                db_dict = {normalize_date(r['trade_date']): float(r['rzye']) for _, r in db_df.iterrows()}
                common_dates = set(api_dict.keys()) & set(db_dict.keys())
                if common_dates:
                    match = all(abs(api_dict[d] - db_dict[d]) < 100 for d in common_dates)
            
            results['stock_margin'] = {
                'api': len(api_df),
                'db': len(db_df),
                'match': match,
                'dates': dates
            }
        except Exception as e:
            results['stock_margin'] = {'error': str(e)}
        
        # 4. 季度利润 - 对比n_income
        try:
            api_df = pro.income(ts_code=ts_code)
            db_df = fetch_df(f"SELECT end_date, n_income FROM stock_income WHERE ts_code='{ts_code}' ORDER BY end_date")
            
            match = False
            if not api_df.empty and not db_df.empty:
                api_dict = {normalize_date(r['end_date']): float(r['n_income']) for _, r in api_df.iterrows() if pd.notna(r['n_income'])}
                db_dict = {normalize_date(r['end_date']): float(r['n_income']) for _, r in db_df.iterrows() if pd.notna(r['n_income'])}
                common_dates = set(api_dict.keys()) & set(db_dict.keys())
                if common_dates:
                    match = all(abs(api_dict[d] - db_dict[d]) < 1 for d in common_dates)
            
            results['stock_income'] = {
                'api': len(api_df),
                'db': len(db_df),
                'match': match
            }
        except Exception as e:
            results['stock_income'] = {'error': str(e)}
        
        # 5. 财务指标 - 对比roe
        try:
            api_df = pro.fina_indicator(ts_code=ts_code)
            db_df = fetch_df(f"SELECT end_date, roe FROM stock_fina_indicator WHERE ts_code='{ts_code}' ORDER BY end_date")
            
            match = False
            if not api_df.empty and not db_df.empty:
                api_dict = {normalize_date(r['end_date']): float(r['roe']) for _, r in api_df.iterrows() if pd.notna(r['roe'])}
                db_dict = {normalize_date(r['end_date']): float(r['roe']) for _, r in db_df.iterrows() if pd.notna(r['roe'])}
                common_dates = set(api_dict.keys()) & set(db_dict.keys())
                if common_dates:
                    match = all(abs(api_dict[d] - db_dict[d]) < 0.01 for d in common_dates)
            
            results['stock_fina_indicator'] = {
                'api': len(api_df),
                'db': len(db_df),
                'match': match
            }
        except Exception as e:
            results['stock_fina_indicator'] = {'error': str(e)}
        
        return {"status": "success", "data": convert(results), "ts_code": ts_code, "trade_dates": dates}
    except Exception as e:
        return {"status": "error", "message": str(e)}


from pydantic import BaseModel, Field
from etl.utils.patterns import PatternRecognizer, get_professional_commentary, get_professional_commentary_detailed

class WatchlistStock(BaseModel):
    ts_code: str
    name: Optional[str] = None
    remark: Optional[str] = None

@router.get("/watchlist")
def list_watchlist():
    """获取自选股列表"""
    try:
        df = fetch_df("SELECT * FROM watchlist ORDER BY created_at DESC")
        return {"status": "success", "data": df.to_dict('records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/watchlist")
def add_to_watchlist(stock: WatchlistStock):
    """添加股票到自选"""
    try:
        ts_code = _normalize_ts_code(stock.ts_code)
        if not ts_code:
            raise HTTPException(status_code=400, detail="无效股票代码")
        
        # 尝试从 stock_basic 获取名称
        if not stock.name:
            basic = fetch_df("SELECT name FROM stock_basic WHERE ts_code = ?", (ts_code,))
            if not basic.empty:
                stock.name = basic.iloc[0]['name']

        with get_db_connection() as con:
            con.execute(
                "INSERT OR REPLACE INTO watchlist (ts_code, name, remark) VALUES (?, ?, ?)",
                (ts_code, stock.name, stock.remark)
            )
        return {"status": "success", "message": f"已添加 {ts_code}"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/watchlist/{ts_code}")
def remove_from_watchlist(ts_code: str):
    """从自选删除股票"""
    try:
        norm_code = _normalize_ts_code(ts_code)
        with get_db_connection() as con:
            con.execute("DELETE FROM watchlist WHERE ts_code = ?", (norm_code,))
        return {"status": "success", "message": f"已从自选删除 {norm_code}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stock/search")
def search_stocks(q: str, limit: int = 10):
    """搜索股票，支持代码、名称、首字母"""
    try:
        if not q or len(q.strip()) < 1:
            return {"status": "success", "data": []}

        q = q.strip()
        q_upper = q.upper()
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="limit 必须为整数")
        limit = max(1, min(limit, 50))

        contains_pattern = f"%{q_upper}%"
        prefix_pattern = f"{q_upper}%"

        # 检查是否有pinyin字段
        try:
            pinyin_check = fetch_df("SELECT pinyin FROM stock_basic LIMIT 1")
            has_pinyin = 'pinyin' in pinyin_check.columns
        except:
            has_pinyin = False

        if has_pinyin:
            query = """
                SELECT ts_code, name FROM stock_basic 
                WHERE UPPER(ts_code) LIKE ?
                   OR UPPER(name) LIKE ?
                   OR UPPER(pinyin) LIKE ?
                   OR UPPER(pinyin_short) LIKE ?
                ORDER BY 
                    CASE WHEN UPPER(ts_code) = ? THEN 0
                         WHEN UPPER(ts_code) LIKE ? THEN 1
                         WHEN UPPER(name) LIKE ? THEN 2
                         ELSE 3 END,
                    ts_code
                LIMIT ?
            """
            params = (
                contains_pattern,
                contains_pattern,
                prefix_pattern,
                prefix_pattern,
                q_upper,
                prefix_pattern,
                prefix_pattern,
                limit,
            )
        else:
            query = """
                SELECT ts_code, name FROM stock_basic 
                WHERE UPPER(ts_code) LIKE ?
                   OR UPPER(name) LIKE ?
                ORDER BY 
                    CASE WHEN UPPER(ts_code) = ? THEN 0
                         WHEN UPPER(ts_code) LIKE ? THEN 1
                         WHEN UPPER(name) LIKE ? THEN 2
                         ELSE 3 END,
                    ts_code
                LIMIT ?
            """
            params = (
                contains_pattern,
                contains_pattern,
                q_upper,
                prefix_pattern,
                prefix_pattern,
                limit,
            )

        df = fetch_df(query, params)
        
        result = []
        if not df.empty:
            result = df.to_dict('records')
        
        return {"status": "success", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stock/{ts_code}/kline")
def get_stock_kline(ts_code: str, limit: int = 200):
    """获取股票日K线数据，包含均线、指标及融资融券"""
    try:
        norm_code = _normalize_ts_code(ts_code)
        # 获取行情
        df = fetch_df(
            """
            SELECT trade_date, open, high, low, close, vol, amount, factors
            FROM daily_price
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (norm_code, limit),
        )
        if df.empty:
            return {"status": "success", "data": []}
        
        df = df.iloc[::-1].reset_index(drop=True)
        
        # 获取两融数据
        margin_df = fetch_df(
            """
            SELECT trade_date, rzye, rzmre, rqye
            FROM stock_margin
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (norm_code, limit * 2), # 获取多一点以便对齐
        )
        
        # 合并
        if not margin_df.empty:
            df = df.merge(margin_df, on='trade_date', how='left')
        
        # 获取主力资金数据
        moneyflow_df = fetch_df(
            """
            SELECT trade_date, net_mf_vol, net_mf_amount
            FROM stock_moneyflow
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (norm_code, limit * 2),
        )
        
        # 合并主力资金
        if not moneyflow_df.empty:
            df = df.merge(moneyflow_df, on='trade_date', how='left')
        
        # 处理 factors (均线)，并处理 NaN 值
        result = []
        for _, row in df.iterrows():
            item = row.to_dict()
            if row.factors:
                try:
                    factors = json.loads(row.factors) if isinstance(row.factors, str) else row.factors
                    item.update(factors)
                except: pass
            # 将 NaN 转换为 None (JSON null)
            for k, v in item.items():
                if isinstance(v, float) and (v != v):  # NaN check
                    item[k] = None
            result.append(item)

        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/watchlist/realtime")
def get_watchlist_realtime(codes: Optional[str] = None, src: str = "sina"):
    """
    获取自选股实时行情（盘中刷新）。
    - 盘中：获取实时行情
    - 盘后：获取最近交易日收盘数据
    - 如果未指定 codes，则从数据库加载。
    """
    if codes:
        raw_codes = [c.strip() for c in codes.split(",") if c.strip()]
        norm_codes = [_normalize_ts_code(c) for c in raw_codes]
        norm_codes = [c for c in norm_codes if c]
    else:
        db_watchlist = fetch_df("SELECT ts_code FROM watchlist")
        norm_codes = db_watchlist['ts_code'].tolist() if not db_watchlist.empty else []

    if not norm_codes:
        return {"status": "success", "is_trading_time": False, "message": "自选股为空", "data": []}

    is_trading = trading_calendar.is_trading_time()
    rows = []

    if is_trading:
        # 实时行情逻辑
        quote_df = sync_engine.provider.realtime_quote(ts_code=",".join(norm_codes), src=src or "sina")
        if quote_df is not None and not quote_df.empty:
            col_map = {str(c).lower(): c for c in quote_df.columns}
            price_col = col_map.get("price") or col_map.get("current") or col_map.get("close")
            pre_close_col = col_map.get("pre_close") or col_map.get("yclose")
            pct_col = col_map.get("pct_chg") or col_map.get("pct_change") or col_map.get("changepercent")
            name_col = col_map.get("name")
            vol_col = col_map.get("vol") or col_map.get("volume")
            amount_col = col_map.get("amount") or col_map.get("turnover")

            for _, row in quote_df.iterrows():
                ts_code = _normalize_ts_code(str(row.get(col_map.get("ts_code", ""), "")))
                if not ts_code: continue
                
                price = _safe_float(row.get(price_col)) if price_col else None
                pre_close = _safe_float(row.get(pre_close_col)) if pre_close_col else None
                pct = _safe_float(row.get(pct_col)) if pct_col else None
                if price and pre_close and pct is None:
                    pct = (price - pre_close) / pre_close * 100.0

                rows.append({
                    "ts_code": ts_code,
                    "name": str(row.get(name_col, "")) if name_col else "",
                    "price": price,
                    "pre_close": pre_close,
                    "pct": pct,
                    "vol": _safe_float(row.get(vol_col)) if vol_col else None,
                    "amount": _safe_float(row.get(amount_col)) if amount_col else None,
                    "analyze": _build_watch_analyse(ts_code)
                })
    
    # 如果实时没数据或非交易时段，补齐静态数据
    processed_codes = {r['ts_code'] for r in rows}
    remaining_codes = [c for c in norm_codes if c not in processed_codes]
    
    if remaining_codes:
        placeholders = ','.join([f"'{c}'" for c in remaining_codes])
        static_df = fetch_df(f"""
            SELECT ts_code, close as price, pre_close, pct_chg as pct, vol, amount, trade_date
            FROM daily_price
            WHERE (ts_code, trade_date) IN (
                SELECT ts_code, MAX(trade_date) FROM daily_price WHERE ts_code IN ({placeholders}) GROUP BY ts_code
            )
        """)
        
        # 补充名称
        names_df = fetch_df(f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})")
        name_map = dict(zip(names_df['ts_code'], names_df['name']))

        for _, row in static_df.iterrows():
            tc = row['ts_code']
            rows.append({
                "ts_code": tc,
                "name": name_map.get(tc, tc),
                "price": row['price'],
                "pre_close": row['pre_close'],
                "pct": row['pct'],
                "vol": row['vol'],
                "amount": row['amount'],
                "analyze": _build_watch_analyse(tc)
            })

    # 排序
    idx_map = {c: i for i, c in enumerate(norm_codes)}
    rows.sort(key=lambda x: idx_map.get(x.get("ts_code"), 999))

    return {
        "status": "success",
        "refresh_mode": "realtime" if is_trading else "static",
        "is_trading_time": is_trading,
        "message": "实时刷新中" if is_trading else "非交易时段，已展示最近收盘数据",
        "data": rows,
    }

