# /backend/api/routes/etl.py

import logging
import json
import hashlib
import uuid
import asyncio
import arrow
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from db.connection import get_db_connection, fetch_df
from etl.sync import sync_engine
from etl.utils.quality import quality_checker

logger = logging.getLogger(__name__)
router = APIRouter(tags=["ETL"])

class SyncTaskParams(BaseModel):
    task: str = Field(..., description="任务类型: basic, concepts, calendar, price, index, moneyflow, financials, margin, fx, factors")
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

# --- 任务持久化 ---
class TaskRegistry:
    @staticmethod
    def create_task(task_type: str, params: dict, task_key: str = None):
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

def _build_sync_task(p: SyncTaskParams):
    task = p.task.lower()
    if task == "basic":
        return "基础信息同步", sync_engine.sync_stock_basic
    elif task == "concepts":
        return "概念分类同步", sync_engine.sync_concept_classification
    elif task == "calendar":
        return "交易日历同步", (sync_engine.sync_trade_calendar, {"start_date": p.start_date or "2020-01-01", "end_date": p.end_date or "2026-12-31"})
    elif task in ("price", "daily"):
        if p.start_date and p.end_date and p.start_date == p.end_date:
            return "日线行情同步(指定日期)", (sync_engine.sync_daily_data_by_date, {"trade_date": p.start_date, "calc_factors": p.calc_factors})
        kwargs = {"years": p.years if p.years > 0 else 1, "force": p.force, "calc_factors": p.calc_factors}
        return "日线行情同步", (sync_engine.sync_daily_market_data, kwargs)
    elif task == "index":
        if p.start_date:
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

def _run_sync_task(task_id, params):
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
def get_data_integrity_report(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    if end_date is None:
        end_date = arrow.now().format("YYYY-MM-DD")
    if start_date is None:
        start_date = arrow.now().shift(years=-1).format("YYYY-MM-DD")
    try:
        report = quality_checker.get_comprehensive_report(start_date=start_date, end_date=end_date)
        
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
        
        return {"tables": tables_info}
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

@router.get("/data_verify")
def verify_data_accuracy(ts_code: str = "688256.SH"):
    """校验数据准确性 - 对比API与数据库"""
    import tushare as ts
    import pandas as pd
    from core.config import settings
    
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