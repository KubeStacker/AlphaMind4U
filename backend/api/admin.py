# /backend/api/admin.py

from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel, Field
from typing import Optional
import arrow
import asyncio
import time
import logging
import math
import json
import pandas as pd
import numpy as np

from etl.sync import sync_engine
from etl.utils.quality import quality_checker
from db.connection import get_db_connection, fetch_df
from passlib.context import CryptContext
from etl.calendar import trading_calendar

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Admin"])

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

async def get_current_user_id(request: Request) -> int:
    """从请求头提取当前用户ID"""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="未授权")
    
    token = auth_header[7:]
    parts = token.split(".")
    if len(parts) < 3:
        raise HTTPException(status_code=401, detail="无效token")
    
    username = parts[1]
    with get_db_connection() as con:
        user = con.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        if not user:
            raise HTTPException(status_code=401, detail="用户不存在")
        return user[0]

# --- 模型定义 ---

class SyncTaskParams(BaseModel):
    task: str = Field(..., description="任务类型: basic, calendar, price, index, moneyflow, financials, margin, fx, factors")
    years: int = 0
    days: int = 3
    ts_code: Optional[str] = None
    force: bool = False
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    calc_factors: bool = True

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

class UserAIConfig(BaseModel):
    model_provider: str = "openai"
    model_name: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    system_prompt: Optional[str] = None
    max_tokens: int = 4096
    temperature: float = 0.7

class UserAIConfigUpdate(BaseModel):
    model_provider: Optional[str] = None
    model_name: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    system_prompt: Optional[str] = None
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None

class DBQueryRequest(BaseModel):
    sql: str

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
            
            # 这里的改进：如果已经存在 COMPLETED 或 FAILED 的相同 task_key，则先删除它，
            # 否则会触发 UNIQUE 约束错误。
            con.execute("DELETE FROM etl_tasks WHERE task_key = ? AND status NOT IN ('PENDING', 'RUNNING')", (task_key,))

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
        
        from etl.utils.kline_patterns import PatternRecognizer, get_professional_commentary_detailed
        
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
        return "交易日历同步", (sync_engine.sync_trade_calendar, {"start_date": p.start_date or "2020-01-01", "end_date": p.end_date or "2026-12-31"})
    elif task in ("price", "daily"):
        if p.start_date and p.end_date and p.start_date == p.end_date:
            return "日线行情同步(指定日期)", (sync_engine.sync_daily_data_by_date, {"trade_date": p.start_date, "calc_factors": p.calc_factors})
        kwargs = {"years": p.years if p.years > 0 else 1, "force": p.force, "calc_factors": p.calc_factors}
        return "日线行情同步", (sync_engine.sync_daily_market_data, kwargs)
    elif task == "index":
        if p.start_date:
            from etl.calendar import trading_calendar
            target_date = arrow.get(p.start_date)
            days_back = (arrow.now() - target_date).days + 1
            return "指数数据同步", (sync_engine.sync_core_market_indices, {"years": 0, "days": max(days_back, 1)})
        return "指数数据同步", (sync_engine.sync_core_market_indices, {"years": p.years, "days": p.days})
    elif task == "moneyflow":
        if p.start_date:
            target_date = arrow.get(p.start_date)
            days_back = (arrow.now() - target_date).days + 1
            return "资金流向同步", (sync_engine.sync_capital_flow, {"years": 0, "days": max(days_back, 1), "force": p.force})
        return "资金流向同步", (sync_engine.sync_capital_flow, {"years": p.years, "days": p.days, "force": p.force})
    elif task == "financials":
        return "财务数据同步", sync_engine.sync_financial_statements
    elif task == "fina_indicator":
        return "财务指标同步", (sync_engine.sync_financial_indicators, {"ts_code": p.ts_code})
    elif task == "quarterly_income":
        return "季度利润同步", (sync_engine.sync_quarterly_income_statement, {"ts_code": p.ts_code, "force": p.force})
    elif task == "margin":
        if p.start_date:
            target_date = arrow.get(p.start_date)
            days_back = (arrow.now() - target_date).days + 1
            return "两融数据同步", (sync_engine.sync_margin_trading_data, {"days": max(days_back, 1)})
        return "两融数据同步", (sync_engine.sync_margin_trading_data, {"days": p.days})
    elif task == "fx":
        return "外汇数据同步", sync_engine.sync_forex_data
    elif task == "factors":
        return "因子数据计算", sync_engine.fill_missing_technical_factors
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
    1. 使用更小的分块（50只股票/批）减少内存峰值
    2. 流式处理，边加载边计算，避免一次性合并所有数据
    3. 及时释放中间结果
    """
    import gc
    from etl.utils.kline_patterns import build_combined_training_stats, save_pattern_calibration
    from pathlib import Path

    horizons = tuple(int(x.strip()) for x in params["horizons"].split(",") if x.strip())
    start_date = params.get("start_date")
    end_date = params.get("end_date")

    # 默认回溯 1 年（优化：从2年减少到1年，减少数据量）
    if not start_date and not params.get("full_history"):
        start_date = arrow.now().shift(years=-1).format("YYYY-MM-DD")

    output_path = Path(__file__).parent.parent / "etl" / "utils" / "kline_pattern_calibration.json"

    # 获取需要处理的所有 ts_code
    codes_df = fetch_df("SELECT DISTINCT ts_code FROM daily_price")
    all_codes = codes_df["ts_code"].tolist()
    total_codes = len(all_codes)
    del codes_df  # 立即释放
    gc.collect()

    # 优化：减小chunk_size从200到50，降低单次内存占用
    chunk_size = 50
    all_results = []

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
            # 立即处理这批数据，获取校准结果
            try:
                calibration_part, summary_part = build_combined_training_stats(
                    df=df_chunk,
                    horizons=horizons,
                    min_confidence=params["min_confidence"],
                    positive_return_threshold=params["positive_return_threshold"]
                )
                all_results.append(calibration_part)
            except Exception as e:
                logger.warning(f"处理K线批次 {i} 失败: {e}")

        # 立即释放chunk数据
        del df_chunk
        gc.collect()

        progress = min(90.0, (i + len(chunk_codes)) / total_codes * 100)
        TaskRegistry.update_status(task_id, "RUNNING", progress=progress)
        logger.info(f"Task [{task_id}] processing: {i+len(chunk_codes)}/{total_codes} codes")

    if not all_results:
        raise ValueError("未查询到有效数据")

    # 合并所有批次的校准结果
    TaskRegistry.update_status(task_id, "RUNNING", progress=92.0)
    final_calibration = {}
    for cal in all_results:
        if cal:
            final_calibration.update(cal)

    save_pattern_calibration(final_calibration, str(output_path))
    TaskRegistry.update_status(task_id, "RUNNING", progress=100.0)

    # 清理内存
    del all_results, final_calibration
    gc.collect()

    logger.info(f"K线形态校准文件已原子化更新: {output_path}")

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
        
        summaries = {}
        for table_name, daily_data in report.items():
            if not daily_data:
                summaries[table_name] = {"total_days": 0, "full_days": 0, "partial_days": 0, "missing_days": 0, "completeness": 0}
                continue
            total = len(daily_data)
            full = sum(1 for d in daily_data if d["status"] == "FULL")
            partial = sum(1 for d in daily_data if d["status"] == "PARTIAL")
            missing = sum(1 for d in daily_data if d["status"] == "MISSING")
            trading_days = full + partial + missing
            completeness = round((full + partial * 0.5) / trading_days * 100, 1) if trading_days > 0 else 0
            summaries[table_name] = {
                "total_days": total,
                "full_days": full,
                "partial_days": partial,
                "missing_days": missing,
                "completeness": completeness
            }
        
        return {"daily_data": report, "summaries": summaries}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"生成报告失败: {e}")


@router.get("/data/dashboard")
def get_data_dashboard():
    """数据管理仪表盘 - 提供所有数据表的全局状态概览"""
    try:
        tables_info = {}
        
        table_queries = {
            "stock_basic": {"date_col": "list_date", "label": "股票基础信息"},
            "daily_price": {"date_col": "trade_date", "label": "日线行情数据"},
            "stock_moneyflow": {"date_col": "trade_date", "label": "资金流向数据"},
            "market_index": {"date_col": "trade_date", "label": "市场指数数据"},
            "stock_margin": {"date_col": "trade_date", "label": "融资融券数据"},
            "stock_concepts": {"date_col": None, "label": "概念分类"},
            "stock_concept_details": {"date_col": None, "label": "概念明细"},
            "market_sentiment": {"date_col": "trade_date", "label": "市场情绪指标"},
            "mainline_scores": {"date_col": "trade_date", "label": "主线评分"},
            "stock_fina_indicator": {"date_col": "end_date", "label": "财务指标"},
            "stock_income": {"date_col": "end_date", "label": "季度利润表"},
            "etl_tasks": {"date_col": "created_at", "label": "ETL任务记录"},
        }
        
        for table, config in table_queries.items():
            info = {"label": config["label"], "count": 0, "last_date": None, "first_date": None}
            try:
                df_cnt = fetch_df(f"SELECT COUNT(*) as cnt FROM {table}")
                if not df_cnt.empty:
                    info["count"] = int(df_cnt.iloc[0]["cnt"])
                
                if config["date_col"] and info["count"] > 0:
                    df_range = fetch_df(f"""
                        SELECT 
                            MIN(CAST({config['date_col']} AS VARCHAR)) as first_date,
                            MAX(CAST({config['date_col']} AS VARCHAR)) as last_date
                        FROM {table}
                        WHERE {config['date_col']} IS NOT NULL
                    """)
                    if not df_range.empty:
                        info["first_date"] = str(df_range.iloc[0]["first_date"])[:10] if df_range.iloc[0]["first_date"] else None
                        info["last_date"] = str(df_range.iloc[0]["last_date"])[:10] if df_range.iloc[0]["last_date"] else None
            except Exception as e:
                info["error"] = str(e)
            
            tables_info[table] = info
        
        latest_task = None
        try:
            df_task = fetch_df("""
                SELECT task_id, task_type, status, error, progress, 
                       CAST(created_at AS VARCHAR) as created_at,
                       CAST(finished_at AS VARCHAR) as finished_at
                FROM etl_tasks 
                ORDER BY created_at DESC LIMIT 1
            """)
            if not df_task.empty:
                row = df_task.iloc[0]
                latest_task = {
                    "task_id": row["task_id"],
                    "task_type": row["task_type"],
                    "status": row["status"],
                    "error": row["error"],
                    "progress": row["progress"],
                    "created_at": row["created_at"],
                    "finished_at": row["finished_at"]
                }
        except Exception:
            pass
        
        return {"tables": tables_info, "latest_task": latest_task}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"获取数据仪表盘失败: {e}")


@router.get("/data/day_status")
def get_day_data_status(date: str):
    """获取指定日期各数据表的状态"""
    try:
        from etl.calendar import trading_calendar
        target_date = arrow.get(date).date()
        is_trading = trading_calendar.is_trading_day(target_date)
        
        status = {"date": date, "is_trading_day": is_trading, "tables": {}}
        
        day_queries = {
            "daily_price": {"label": "日线行情", "expected_min": 1000},
            "stock_moneyflow": {"label": "资金流向", "expected_min": 1000},
            "market_index": {"label": "市场指数", "expected_min": 1},
            "stock_margin": {"label": "融资融券", "expected_min": 1000},
        }
        
        for table, config in day_queries.items():
            try:
                df = fetch_df(f"SELECT COUNT(*) as cnt FROM {table} WHERE trade_date = ?", [target_date])
                count = int(df.iloc[0]["cnt"]) if not df.empty else 0
                
                table_status = "full" if count >= config["expected_min"] else ("partial" if count > 0 else "missing")
                if not is_trading:
                    table_status = "holiday" if count == 0 else "full"
                
                status["tables"][table] = {
                    "label": config["label"],
                    "count": count,
                    "status": table_status
                }
            except Exception as e:
                status["tables"][table] = {"label": config["label"], "count": 0, "status": "error", "error": str(e)}
        
        return status
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"获取日期状态失败: {e}")


@router.post("/data/sync_date", status_code=202)
async def sync_specific_date(date: str, tables: Optional[str] = None):
    """触发指定日期的全量数据刷新
    
    Args:
        date: 目标日期 (YYYY-MM-DD)
        tables: 逗号分隔的表名列表，为空则刷新所有核心表
    """
    target_tables = ["daily_price", "stock_moneyflow", "market_index", "stock_margin"]
    if tables:
        target_tables = [t.strip() for t in tables.split(",") if t.strip()]
    
    task_ids = []
    for table in target_tables:
        if table == "daily_price":
            params = {"task": "daily", "start_date": date, "end_date": date, "calc_factors": True}
        elif table == "stock_moneyflow":
            params = {"task": "moneyflow", "start_date": date, "force": True}
        elif table == "market_index":
            params = {"task": "index", "start_date": date}
        elif table == "stock_margin":
            params = {"task": "margin", "start_date": date}
        else:
            continue
        
        tid, status = TaskRegistry.create_task("SYNC", params, task_key=f"sync_{table}_{date.replace('-', '')}")
        task_ids.append({"table": table, "task_id": tid, "status": status})
    
    return {"message": f"已为 {date} 创建 {len(task_ids)} 个同步任务", "tasks": task_ids}


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

@router.get("/users/me/ai-config")
async def get_my_ai_config(request: Request):
    """获取当前用户的AI配置"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            row = con.execute(
                "SELECT model_provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature FROM user_ai_config WHERE user_id = ?",
                (user_id,)
            ).fetchone()
        if row:
            return {
                "model_provider": row[0],
                "model_name": row[1],
                "api_key": row[2],
                "base_url": row[3],
                "system_prompt": row[4],
                "max_tokens": row[5],
                "temperature": row[6]
            }
        return {
            "model_provider": "openai",
            "model_name": None,
            "api_key": None,
            "base_url": None,
            "system_prompt": None,
            "max_tokens": 4096,
            "temperature": 0.7
        }
    except Exception as e:
        logger.error(f"获取AI配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/ai-config")
async def update_my_ai_config(request: Request, config: UserAIConfigUpdate):
    """更新当前用户的AI配置"""
    user_id = await get_current_user_id(request)
    try:
        updates = []
        params = []
        
        if config.model_provider is not None:
            updates.append("model_provider = ?")
            params.append(config.model_provider)
        if config.model_name is not None:
            updates.append("model_name = ?")
            params.append(config.model_name)
        if config.api_key is not None:
            updates.append("api_key = ?")
            params.append(config.api_key)
        if config.base_url is not None:
            updates.append("base_url = ?")
            params.append(config.base_url)
        if config.system_prompt is not None:
            updates.append("system_prompt = ?")
            params.append(config.system_prompt)
        if config.max_tokens is not None:
            updates.append("max_tokens = ?")
            params.append(config.max_tokens)
        if config.temperature is not None:
            updates.append("temperature = ?")
            params.append(config.temperature)
        
        if not updates:
            return {"message": "没有需要更新的字段"}
        
        updates.append("updated_at = CURRENT_TIMESTAMP")
        
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_ai_config WHERE user_id = ?", (user_id,)).fetchone()
            if exists:
                sql = f"UPDATE user_ai_config SET {', '.join(updates)} WHERE user_id = ?"
                params.append(user_id)
                con.execute(sql, params)
            else:
                cols = [u.split(' = ')[0] for u in updates if 'CURRENT_TIMESTAMP' not in u]
                vals_placeholders = ', '.join(['?' for _ in cols])
                insert_sql = f"INSERT INTO user_ai_config (user_id, {', '.join(cols)}) VALUES (?, {vals_placeholders})"
                insert_params = [user_id] + params
                con.execute(insert_sql, insert_params)
        
        return {"message": "AI配置已更新"}
    except Exception as e:
        logger.error(f"更新AI配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- 用户提示词模板管理 ---
class PromptTemplateCreate(BaseModel):
    name: str
    content: str
    is_default: bool = False

class PromptTemplateUpdate(BaseModel):
    name: Optional[str] = None
    content: Optional[str] = None
    is_default: Optional[bool] = None

@router.get("/users/me/prompt-templates")
async def get_prompt_templates(request: Request):
    """获取当前用户的提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            rows = con.execute(
                "SELECT id, name, content, is_default, created_at, updated_at FROM user_prompt_templates WHERE user_id = ? ORDER BY is_default DESC, created_at DESC",
                (user_id,)
            ).fetchall()
        return [{"id": r[0], "name": r[1], "content": r[2], "is_default": r[3], "created_at": r[4], "updated_at": r[5]} for r in rows]
    except Exception as e:
        logger.error(f"获取提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/users/me/prompt-templates")
async def create_prompt_template(request: Request, template: PromptTemplateCreate):
    """创建提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            if template.is_default:
                con.execute("UPDATE user_prompt_templates SET is_default = FALSE WHERE user_id = ?", (user_id,))
            max_id = con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM user_prompt_templates").fetchone()[0]
            con.execute(
                "INSERT INTO user_prompt_templates (id, user_id, name, content, is_default) VALUES (?, ?, ?, ?, ?)",
                (max_id, user_id, template.name, template.content, template.is_default)
            )
        return {"message": "模板创建成功"}
    except Exception as e:
        logger.error(f"创建提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/prompt-templates/{template_id}")
async def update_prompt_template(request: Request, template_id: int, template: PromptTemplateUpdate):
    """更新提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_prompt_templates WHERE id = ? AND user_id = ?", (template_id, user_id)).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="模板不存在")
            
            if template.is_default:
                con.execute("UPDATE user_prompt_templates SET is_default = FALSE WHERE user_id = ?", (user_id,))
            
            updates = []
            params = []
            if template.name is not None:
                updates.append("name = ?")
                params.append(template.name)
            if template.content is not None:
                updates.append("content = ?")
                params.append(template.content)
            if template.is_default is not None:
                updates.append("is_default = ?")
                params.append(template.is_default)
            
            if updates:
                updates.append("updated_at = CURRENT_TIMESTAMP")
                params.append(template_id)
                con.execute(f"UPDATE user_prompt_templates SET {', '.join(updates)} WHERE id = ?", params)
        return {"message": "模板更新成功"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/users/me/prompt-templates/{template_id}")
async def delete_prompt_template(request: Request, template_id: int):
    """删除提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            con.execute("DELETE FROM user_prompt_templates WHERE id = ? AND user_id = ?", (template_id, user_id))
        return {"message": "模板已删除"}
    except Exception as e:
        logger.error(f"删除提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- 用户持仓管理 ---
class HoldingUpdate(BaseModel):
    shares: float
    avg_cost: Optional[float] = None

@router.get("/users/me/holdings")
async def get_holdings(request: Request):
    """获取当前用户的持仓（含盈亏计算）"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            # 获取持仓基本信息
            rows = con.execute("""
                SELECT h.ts_code, h.shares, h.avg_cost, h.updated_at,
                       b.name, p.close as current_price
                FROM user_holdings h
                LEFT JOIN stock_basic b ON h.ts_code = b.ts_code
                LEFT JOIN (
                    SELECT ts_code, close,
                           ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) as rn
                    FROM daily_price
                ) p ON h.ts_code = p.ts_code AND p.rn = 1
                WHERE h.user_id = ?
            """, (user_id,)).fetchall()
        
        holdings = []
        total_market_value = 0
        
        for r in rows:
            ts_code, shares, avg_cost, updated_at, name, current_price = r
            shares = float(shares) if shares else 0
            avg_cost = float(avg_cost) if avg_cost else 0
            current_price = float(current_price) if current_price else 0
            
            # 计算市值和盈亏
            market_value = shares * current_price if current_price else 0
            cost_value = shares * avg_cost
            profit_loss = market_value - cost_value
            profit_loss_pct = (profit_loss / cost_value * 100) if cost_value > 0 else 0
            
            total_market_value += market_value
            
            holdings.append({
                "ts_code": ts_code,
                "name": name or ts_code,
                "shares": shares,
                "avg_cost": avg_cost,
                "current_price": current_price,
                "market_value": round(market_value, 2),
                "cost_value": round(cost_value, 2),
                "profit_loss": round(profit_loss, 2),
                "profit_loss_pct": round(profit_loss_pct, 2),
                "updated_at": str(updated_at) if updated_at else None
            })
        
        # 计算持仓占比
        for h in holdings:
            h["weight_pct"] = round(h["market_value"] / total_market_value * 100, 2) if total_market_value > 0 else 0
        
        return {
            "holdings": holdings,
            "summary": {
                "total_market_value": round(total_market_value, 2),
                "total_cost_value": round(sum(h["cost_value"] for h in holdings), 2),
                "total_profit_loss": round(sum(h["profit_loss"] for h in holdings), 2),
                "total_profit_loss_pct": round(
                    sum(h["profit_loss"] for h in holdings) / sum(h["cost_value"] for h in holdings) * 100
                    if sum(h["cost_value"] for h in holdings) > 0 else 0, 2
                ),
                "stock_count": len(holdings)
            }
        }
    except Exception as e:
        logger.error(f"获取持仓失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/holdings/{ts_code}")
async def update_holding(request: Request, ts_code: str, holding: HoldingUpdate):
    """更新持仓"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_holdings WHERE user_id = ? AND ts_code = ?", (user_id, ts_code)).fetchone()
            if exists:
                con.execute(
                    "UPDATE user_holdings SET shares = ?, avg_cost = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND ts_code = ?",
                    (holding.shares, holding.avg_cost or 0, user_id, ts_code)
                )
            else:
                con.execute(
                    "INSERT INTO user_holdings (user_id, ts_code, shares, avg_cost) VALUES (?, ?, ?, ?)",
                    (user_id, ts_code, holding.shares, holding.avg_cost or 0)
                )
        return {"message": "持仓已更新"}
    except Exception as e:
        logger.error(f"更新持仓失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/users/me/holdings/{ts_code}")
async def delete_holding(request: Request, ts_code: str):
    """删除持仓"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            con.execute("DELETE FROM user_holdings WHERE user_id = ? AND ts_code = ?", (user_id, ts_code))
        return {"message": "持仓已删除"}
    except Exception as e:
        logger.error(f"删除持仓失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- AI 智能分析 ---
@router.get("/users/me/selected-template")
async def get_selected_template(request: Request):
    """获取当前选中的模板ID"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            row = con.execute(
                "SELECT selected_template_id FROM user_ai_config WHERE user_id = ?",
                (user_id,)
            ).fetchone()
        return {"selected_template_id": row[0] if row else None}
    except Exception as e:
        logger.error(f"获取选中模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class SelectTemplateRequest(BaseModel):
    template_id: Optional[int] = None

@router.put("/users/me/selected-template")
async def select_template(request: Request, body: SelectTemplateRequest):
    """设置选中的模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_ai_config WHERE user_id = ?", (user_id,)).fetchone()
            if exists:
                con.execute("UPDATE user_ai_config SET selected_template_id = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", 
                    (body.template_id, user_id))
            else:
                con.execute("INSERT INTO user_ai_config (user_id, selected_template_id) VALUES (?, ?)", 
                    (user_id, body.template_id))
        return {"message": "模板已选中"}
    except Exception as e:
        logger.error(f"选择模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class AIAnalyzeRequest(BaseModel):
    ts_code: str
    template_id: Optional[int] = None
    force_refresh: Optional[bool] = False

@router.post("/stock/analyze")
async def analyze_stock_with_ai(request: Request, body: AIAnalyzeRequest):
    """使用AI分析股票"""
    user_id = await get_current_user_id(request)
    try:
        import httpx
        
        # 获取最新交易日
        with get_db_connection() as con:
            latest_trade = con.execute(
                "SELECT MAX(trade_date) FROM daily_price WHERE ts_code = ?",
                (body.ts_code,)
            ).fetchone()
        
        if not latest_trade or not latest_trade[0]:
            raise HTTPException(status_code=400, detail=f"未找到股票 {body.ts_code} 的行情数据")
        
        latest_trade_date = str(latest_trade[0])
        
        # 检查缓存（如果不是强制刷新）
        if not body.force_refresh:
            with get_db_connection() as con:
                cache = con.execute(
                    "SELECT analysis_result, created_at FROM ai_analysis_cache WHERE user_id = ? AND ts_code = ? AND trade_date = ? ORDER BY created_at DESC LIMIT 1",
                    (user_id, body.ts_code, latest_trade_date)
                ).fetchone()
            
            if cache:
                logger.info(f"返回缓存的分析结果: {body.ts_code} {latest_trade_date}")
                return {
                    "analysis": cache[0],
                    "ts_code": body.ts_code,
                    "trade_date": latest_trade_date,
                    "from_cache": True
                }
        
        # 获取用户AI配置
        with get_db_connection() as con:
            config = con.execute(
                "SELECT model_provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature FROM user_ai_config WHERE user_id = ?",
                (user_id,)
            ).fetchone()
        
        if not config or not config[2]:
            raise HTTPException(status_code=400, detail="请先在设置中配置API Key")
        
        model_provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature = config
        
        # 获取模板
        template_id = body.template_id
        if not template_id:
            with get_db_connection() as con:
                tpl = con.execute(
                    "SELECT content FROM user_prompt_templates WHERE user_id = ? AND is_default = TRUE",
                    (user_id,)
                ).fetchone()
                template_content = tpl[0] if tpl else None
        else:
            with get_db_connection() as con:
                tpl = con.execute(
                    "SELECT content FROM user_prompt_templates WHERE id = ? AND user_id = ?",
                    (template_id, user_id)
                ).fetchone()
                template_content = tpl[0] if tpl else None
        
        # 获取股票基本信息
        stock_basic = None
        with get_db_connection() as con:
            basic = con.execute(
                "SELECT ts_code, name, industry, market FROM stock_basic WHERE ts_code = ?",
                (body.ts_code,)
            ).fetchone()
            if basic:
                stock_basic = {"ts_code": basic[0], "name": basic[1], "industry": basic[2], "market": basic[3]}
        
        # 获取30日行情数据
        with get_db_connection() as con:
            prices = con.execute("""
                SELECT trade_date, open, high, low, close, vol, amount, pct_chg 
                FROM daily_price WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 30
            """, (body.ts_code,)).fetchall()
        
        price_data = []
        for p in reversed(prices):
            price_data.append({
                "date": str(p[0]),
                "open": float(p[1]) if p[1] else 0,
                "high": float(p[2]) if p[2] else 0,
                "low": float(p[3]) if p[3] else 0,
                "close": float(p[4]) if p[4] else 0,
                "vol": float(p[5]) if p[5] else 0,
                "amount": float(p[6]) if p[6] else 0,
                "pct_chg": float(p[7]) if p[7] else 0
            })
        
        logger.debug(f"price_data rows: {len(price_data)}")
        
        # 获取资金流向（详细版）- 最近10日
        money_flow = None
        with get_db_connection() as con:
            mf = con.execute("""
                SELECT trade_date, buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount,
                       buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount,
                       buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount,
                       buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount,
                       net_mf_vol, net_mf_amount, net_mf_ratio
                FROM stock_moneyflow WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 10
            """, (body.ts_code,)).fetchall()
        
        if mf:
            money_flow = []
            for m in reversed(mf):
                money_flow.append({
                    "date": str(m[0]),
                    "小单净流入": float(m[2] or 0) - float(m[4] or 0),
                    "中单净流入": float(m[6] or 0) - float(m[8] or 0),
                    "大单净流入": float(m[10] or 0) - float(m[12] or 0),
                    "超大单净流入": float(m[14] or 0) - float(m[16] or 0),
                    "主力净流入额": float(m[18]) if m[18] else 0,
                    "主力净流入占比": float(m[19]) if m[19] else 0
                })
        
        # 获取融资融券数据 - 最近10日
        margin_data = None
        with get_db_connection() as con:
            mg = con.execute("""
                SELECT trade_date, rzye, rzmre, rzche, rqye, rqmcl, rzrqye
                FROM stock_margin WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 10
            """, (body.ts_code,)).fetchall()
        
        if mg:
            margin_data = []
            for m in reversed(mg):
                margin_data.append({
                    "date": str(m[0]),
                    "融资余额(万)": round(float(m[1]) / 10000, 2) if m[1] else 0,
                    "融资买入额(万)": round(float(m[2]) / 10000, 2) if m[2] else 0,
                    "融资偿还额(万)": round(float(m[3]) / 10000, 2) if m[3] else 0,
                    "融券余额(万)": round(float(m[4]) / 10000, 2) if m[4] else 0,
                    "融资融券余额(万)": round(float(m[6]) / 10000, 2) if m[6] else 0
                })
        
        # 获取持仓信息
        holding = None
        with get_db_connection() as con:
            h = con.execute(
                "SELECT shares, avg_cost FROM user_holdings WHERE user_id = ? AND ts_code = ?",
                (user_id, body.ts_code)
            ).fetchone()
            if h:
                holding = {"shares": float(h[0]), "avg_cost": float(h[1]) if h[1] else 0}
        
        # 获取市场情绪
        market_sentiment = None
        with get_db_connection() as con:
            ms = con.execute("""
                SELECT trade_date, score, label, details FROM market_sentiment ORDER BY trade_date DESC LIMIT 1
            """).fetchone()
            if ms:
                details = {}
                if ms[3]:
                    try:
                        details = json.loads(ms[3])
                    except Exception:
                        logger.warning(f"Failed to parse market_sentiment details: {ms[3]}")
                market_sentiment = {
                    "date": str(ms[0]),
                    "score": float(ms[1]) if ms[1] else 0,
                    "label": ms[2],
                    "details": details
                }
                logger.debug(f"market_sentiment date: {ms[0]}, score: {ms[1]}")
        
        # 获取主线评分
        mainline = None
        with get_db_connection() as con:
            ml = con.execute("""
                SELECT trade_date, mapped_name, score, top_stocks 
                FROM mainline_scores ORDER BY trade_date DESC, score DESC LIMIT 5
            """).fetchall()
            if ml:
                mainline = []
                for m in ml:
                    top_stocks = []
                    if m[3]:
                        try:
                            top_stocks = json.loads(m[3])
                        except Exception:
                            logger.warning(f"Failed to parse mainline top_stocks: {m[3]}")
                    mainline.append({
                        "date": str(m[0]),
                        "name": m[1],
                        "score": float(m[2]) if m[2] else 0,
                        "top_stocks": top_stocks
                    })
                logger.debug(f"mainline count: {len(mainline)}, first date: {mainline[0]['date'] if mainline else 'N/A'}")
        
        # 构建提示词
        if template_content:
            prompt = template_content
        else:
            prompt = """你是一个专业的A股交易分析师。请根据以下信息对股票进行分析并给出投资建议。

## 股票基本信息
{stock_basic}

## 近30日行情数据（含成交量、成交额）
{price_data}

## 主力资金流向（近10日）
{money_flow}

## 融资融券数据（近10日）
{margin_data}

## 用户持仓情况
{holding}

## 当前市场情绪
{market_sentiment}

## 市场主线热点
{mainline}

---

请分析并给出：
1. **技术面分析**（结合K线形态、成交量变化、均线趋势等）
2. **资金面分析**（主力资金流向变化、融资融券余额趋势）
3. **市场环境判断**（结合市场情绪和主线热点）
4. **操作建议**（买入/卖出/观望，给出明确结论）
5. **持仓建议**（如有持仓，结合成本价分析盈亏和操作策略）
6. **风险提示**

请用简洁专业的语言回答，重点突出结论和建议。使用Markdown格式组织回答。"""
        
        # 替换变量 - 缺失数据显示"暂无"
        # 记录数据状态以便调试
        logger.info(f"AI分析数据准备: stock_basic={'有' if stock_basic else '无'}, "
                    f"price_data行数={len(price_data) if price_data else 0}, "
                    f"money_flow行数={len(money_flow) if money_flow else 0}, "
                    f"margin_data行数={len(margin_data) if margin_data else 0}, "
                    f"holding={'有' if holding else '无'}, "
                    f"market_sentiment={'有' if market_sentiment else '无'}, "
                    f"mainline行数={len(mainline) if mainline else 0}")
        def format_data(data, name=""):
            import math
            def clean_nan(obj):
                if isinstance(obj, dict):
                    return {k: clean_nan(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [clean_nan(v) for v in obj]
                elif isinstance(obj, (float, np.floating)) and (math.isnan(obj) or math.isinf(obj)):
                    return None
                return obj
            if data is None:
                return "暂无"
            if isinstance(data, list) and len(data) == 0:
                return "暂无"
            if isinstance(data, dict) and len(data) == 0:
                return "暂无"
            cleaned = clean_nan(data)
            return json.dumps(cleaned, ensure_ascii=False, indent=2)
        
        prompt = prompt.replace("{stock_basic}", format_data(stock_basic))
        prompt = prompt.replace("{price_data}", format_data(price_data))
        prompt = prompt.replace("{money_flow}", format_data(money_flow))
        prompt = prompt.replace("{margin_data}", format_data(margin_data))
        prompt = prompt.replace("{holding}", format_data(holding))
        prompt = prompt.replace("{market_sentiment}", format_data(market_sentiment))
        prompt = prompt.replace("{mainline}", format_data(mainline))
        
        # 记录提示词长度和部分内容
        logger.info(f"AI分析提示词长度: {len(prompt)}")
        if len(prompt) > 1000:
            logger.debug(f"AI分析提示词前1000字符: {prompt[:1000]}")
        else:
            logger.debug(f"AI分析提示词: {prompt}")
        
        # 调用AI
        model = model_name or "deepseek-chat"
        logger.info(f"AI provider: {model_provider}, model: {model}")
        if model_provider == "deepseek":
            if not base_url:
                base_url = "https://api.deepseek.com/v1"
            base_url = base_url.rstrip('/')
            url = f"{base_url}/chat/completions"
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt or "你是一个专业的A股交易分析师。"},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": max_tokens or 4096,
                "temperature": temperature or 0.7
            }
        else:  # openai
            if not base_url:
                base_url = "https://api.openai.com/v1"
            base_url = base_url.rstrip('/')
            url = f"{base_url}/chat/completions"
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            payload = {
                "model": model or "gpt-4",
                "messages": [
                    {"role": "system", "content": system_prompt or "你是一个专业的A股交易分析师。"},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": max_tokens or 4096,
                "temperature": temperature or 0.7
            }
        
        logger.info(f"AI分析请求: {body.ts_code}, 模型: {model}, 交易日: {latest_trade_date}")
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                error_detail = resp.text
                logger.error(f"AI API error: {error_detail}")
                raise HTTPException(status_code=502, detail=f"AI服务调用失败: {error_detail}")
            result = resp.json()
            analysis = result["choices"][0]["message"]["content"]
        
        # 保存到缓存
        with get_db_connection() as con:
            # 先删除旧缓存
            con.execute(
                "DELETE FROM ai_analysis_cache WHERE user_id = ? AND ts_code = ? AND trade_date = ?",
                (user_id, body.ts_code, latest_trade_date)
            )
            # 生成新id
            max_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM ai_analysis_cache").fetchone()[0]
            new_id = max_id + 1
            # 插入新缓存
            con.execute(
                "INSERT INTO ai_analysis_cache (id, user_id, ts_code, trade_date, analysis_result, model_name) VALUES (?, ?, ?, ?, ?, ?)",
                (new_id, user_id, body.ts_code, latest_trade_date, analysis, model)
            )
        
        logger.info(f"AI分析完成并缓存: {body.ts_code} {latest_trade_date}")
        
        return {
            "analysis": analysis,
            "ts_code": body.ts_code,
            "trade_date": latest_trade_date,
            "from_cache": False
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"AI分析失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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

@router.get("/data_verify")
def verify_data_accuracy(ts_code: str = "688256.SH"):
    """校验数据准确性 - 对比API与数据库"""
    import tushare as ts
    import pandas as pd
    from core.config import settings
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

@router.post("/db/query")
def execute_sql_query(req: DBQueryRequest):
    """ 执行自定义 SQL 查询 (限 Admin) """
    try:
        sql_upper = req.sql.strip().upper()
        
        if not sql_upper.startswith("SELECT"):
            raise HTTPException(status_code=400, detail="仅支持 SELECT 查询")
        
        dangerous_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE", "EXEC", "EXECUTE"]
        for keyword in dangerous_keywords:
            if f" {keyword} " in f" {sql_upper} ":
                raise HTTPException(status_code=400, detail=f"禁止执行包含 {keyword} 的查询")
        
        if "LIMIT" not in sql_upper:
            if sql_upper.rstrip().endswith(";"):
                req.sql = req.sql.rstrip()[:-1] + " LIMIT 1000;"
            else:
                req.sql = req.sql.rstrip() + " LIMIT 1000"
        
        import time
        start_time = time.time()
        timeout_seconds = 30
        
        df = fetch_df(req.sql)
        
        if time.time() - start_time > timeout_seconds:
            logger.warning(f"SQL 查询超时: {req.sql[:100]}...")
        
        if len(df) > 10000:
            logger.warning(f"SQL 查询返回行数过多: {len(df)}，已截断到 10000 行")
            df = df.head(10000)
        
        df = df.replace([np.inf, -np.inf], np.nan)
        df_obj = df.astype(object).where(pd.notnull(df), None)
        return {
            "status": "success",
            "data": df_obj.to_dict('records'),
            "columns": df.columns.tolist(),
            "row_count": len(df_obj)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"SQL 执行失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

from pydantic import BaseModel, Field
from etl.utils.kline_patterns import PatternRecognizer, get_professional_commentary, get_professional_commentary_detailed

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
def search_stocks(q: str = "", limit: int = 10):
    """搜索股票，支持代码、名称、拼音首字母；q为空时返回所有股票（用于前端缓存）"""
    try:
        q = q.strip() if q else ""
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="limit 必须为整数")
        limit = max(1, min(limit, 5000))

        # 空查询：返回所有股票（用于前端缓存）
        if not q:
            query = "SELECT ts_code, name, pinyin, pinyin_abbr FROM stock_basic ORDER BY ts_code LIMIT ?"
            df = fetch_df(query, (limit,))
            result = df.to_dict('records') if not df.empty else []
            return {"status": "success", "data": result}

        # 判断输入类型：纯数字优先匹配代码，中文匹配名称，英文匹配代码或拼音
        is_digit = q.isdigit()
        is_chinese = any('\u4e00' <= c <= '\u9fff' for c in q)
        q_upper = q.upper()
        q_lower = q.lower()
        
        if is_digit:
            # 纯数字输入：优先匹配股票代码（如 600000、000001）
            query = """
                SELECT ts_code, name, pinyin, pinyin_abbr FROM stock_basic 
                WHERE ts_code LIKE ? OR symbol LIKE ?
                ORDER BY 
                    CASE WHEN symbol = ? THEN 0
                         WHEN symbol LIKE ? THEN 1
                         ELSE 2 END,
                    ts_code
                LIMIT ?
            """
            prefix = f"{q}%"
            params = (prefix, prefix, q, f"{q}%", limit)
        elif is_chinese:
            # 中文输入：匹配名称
            contains_pattern = f"%{q}%"
            prefix_pattern = f"{q}%"
            query = """
                SELECT ts_code, name, pinyin, pinyin_abbr FROM stock_basic 
                WHERE name LIKE ?
                   OR name LIKE ?
                ORDER BY 
                    CASE WHEN name = ? THEN 0
                         WHEN name LIKE ? THEN 1
                         ELSE 2 END,
                    ts_code
                LIMIT ?
            """
            params = (
                prefix_pattern,
                contains_pattern,
                q,
                prefix_pattern,
                limit,
            )
        else:
            # 英文输入：匹配代码或拼音首字母
            contains_pattern = f"%{q_upper}%"
            prefix_pattern = f"{q_upper}%"
            pinyin_pattern = f"%{q_lower}%"
            pinyin_prefix = f"{q_lower}%"
            query = """
                SELECT ts_code, name, pinyin, pinyin_abbr FROM stock_basic 
                WHERE UPPER(ts_code) LIKE ?
                   OR UPPER(ts_code) LIKE ?
                   OR pinyin_abbr LIKE ?
                   OR pinyin_abbr LIKE ?
                   OR pinyin LIKE ?
                   OR pinyin LIKE ?
                ORDER BY 
                    CASE WHEN UPPER(ts_code) = ? THEN 0
                         WHEN UPPER(ts_code) LIKE ? THEN 1
                         WHEN pinyin_abbr LIKE ? THEN 2
                         WHEN pinyin_abbr LIKE ? THEN 3
                         WHEN pinyin LIKE ? THEN 4
                         ELSE 5 END,
                    ts_code
                LIMIT ?
            """
            params = (
                prefix_pattern,
                contains_pattern,
                pinyin_prefix,
                pinyin_pattern,
                pinyin_prefix,
                pinyin_pattern,
                q_upper,
                prefix_pattern,
                pinyin_prefix,
                pinyin_pattern,
                pinyin_prefix,
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
            # 将 NaN / Inf 转换为 None (JSON null)
            for k, v in item.items():
                if isinstance(v, float) and not math.isfinite(v):
                    item[k] = None
            result.append(item)

        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/watchlist/realtime")
def get_watchlist_realtime(codes: Optional[str] = None, src: str = "sina", include_analysis: bool = True):
    """
    获取自选股实时行情（盘中刷新）。
    - 盘中：获取实时行情
    - 盘后：获取最近交易日收盘数据
    - 如果未指定 codes，则从数据库加载。
    - include_analysis: 是否包含技术分析（默认开启以提供完整数据）
    """
    if codes:
        raw_codes = [c.strip() for c in codes.split(",") if c.strip()]
        norm_codes = [_normalize_ts_code(c) for c in raw_codes]
        norm_codes = [c for c in norm_codes if c]
    else:
        db_watchlist = fetch_df("SELECT ts_code FROM watchlist")
        raw_codes = db_watchlist['ts_code'].tolist() if not db_watchlist.empty else []
        norm_codes = [_normalize_ts_code(c) for c in raw_codes]
        norm_codes = [c for c in norm_codes if c]

    if len(norm_codes) > 50:
        norm_codes = norm_codes[:50]
        logger.warning(f"自选股数量超过50只，已截断到50只")

    if not norm_codes:
        return {"status": "success", "is_trading_time": False, "message": "自选股为空", "data": []}

    is_trading = trading_calendar.is_trading_time()
    rows = []

    if is_trading:
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

                analyze_result = _build_watch_analyse(ts_code) if include_analysis else {}
                
                rows.append({
                    "ts_code": ts_code,
                    "name": str(row.get(name_col, "")) if name_col else "",
                    "price": price,
                    "pre_close": pre_close,
                    "pct": pct,
                    "vol": _safe_float(row.get(vol_col)) if vol_col else None,
                    "amount": _safe_float(row.get(amount_col)) if amount_col else None,
                    "analyze": analyze_result
                })
    
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
        
        names_df = fetch_df(f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})")
        name_map = dict(zip(names_df['ts_code'], names_df['name']))

        for _, row in static_df.iterrows():
            tc = row['ts_code']
            analyze_result = _build_watch_analyse(tc) if include_analysis else {}
            rows.append({
                "ts_code": tc,
                "name": name_map.get(tc, tc),
                "price": row['price'],
                "pre_close": row['pre_close'],
                "pct": row['pct'],
                "vol": row['vol'],
                "amount": row['amount'],
                "analyze": analyze_result
            })

    idx_map = {c: i for i, c in enumerate(norm_codes)}
    rows.sort(key=lambda x: idx_map.get(x.get("ts_code"), 999))

    return {
        "status": "success",
        "refresh_mode": "realtime" if is_trading else "static",
        "is_trading_time": is_trading,
        "message": "实时刷新中" if is_trading else "非交易时段，已展示最近收盘数据",
        "data": rows,
    }

