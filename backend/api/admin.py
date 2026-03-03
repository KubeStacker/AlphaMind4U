# /backend/api/admin.py

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
import arrow
import asyncio
import time
import logging

from etl.sync import sync_engine
from etl.utils.quality import quality_checker
from db.connection import get_db_connection, fetch_df
from passlib.context import CryptContext

logger = logging.getLogger(__name__)

# 统一加密上下文
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# --- 任务队列优化 ---
class TaskItem:
    def __init__(self, name, func, *args, **kwargs):
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.status = "PENDING"
        self.created_at = time.time()
        self.started_at = None
        self.finished_at = None
        self.error = None

# 全局任务队列和状态
_task_queue = asyncio.Queue()
_task_history = []
_current_task = None

async def task_worker():
    """ 任务消费者，顺序执行队列中的任务 """
    global _current_task
    logger.info("任务中心消费者已启动")
    while True:
        task = await _task_queue.get()
        _current_task = task
        task.status = "RUNNING"
        task.started_at = time.time()
        logger.info(f"开始执行任务: {task.name}")
        
        try:
            # 执行任务（目前大部分是同步函数，在 threadpool 中执行以防阻塞事件循环）
            if asyncio.iscoroutinefunction(task.func):
                await task.func(*task.args, **task.kwargs)
            else:
                await asyncio.to_thread(task.func, *task.args, **task.kwargs)
            task.status = "COMPLETED"
        except Exception as e:
            logger.error(f"任务 {task.name} 失败: {e}")
            task.status = "FAILED"
            task.error = str(e)
        finally:
            task.finished_at = time.time()
            _task_queue.task_done()
            _current_task = None
            # 只保留最近 20 条历史记录
            _task_history.append(task)
            if len(_task_history) > 20:
                _task_history.pop(0)

async def add_to_queue(name, func, *args, **kwargs):
    """ 向队列添加任务 """
    task = TaskItem(name, func, *args, **kwargs)
    await _task_queue.put(task)
    return task

# 创建一个专门用于管理和ETL任务的路由
router = APIRouter(
    prefix="/admin",
    tags=["Admin & ETL"], # 在OpenAPI文档中分组
)

# --- 数据模型 ---
class IntegrityParams(BaseModel):
    start_date: str | None = None
    end_date: str | None = None

class SyncDailyParams(BaseModel):
    start_date: str | None = None
    end_date: str | None = None
    years: int = 1
    force: bool = False
    calc_factors: bool = True

class SyncTaskParams(BaseModel):
    task: str
    start_date: str | None = None
    end_date: str | None = None
    years: int = 0
    days: int = 3
    force: bool = False
    calc_factors: bool = True
    ts_code: str | None = None
    limit: int = 1000
    force_sync: bool = False
    refresh_sentiment: bool = False
    sentiment_days: int = 30

class UserCreate(BaseModel):
    username: str
    password: str
    role: str = "viewer"

class PasswordChange(BaseModel):
    user_id: int
    new_password: str

class DBQuery(BaseModel):
    sql: str

# --- API 接口 ---

@router.get("/tasks/status")
def get_tasks_status():
    """ 获取当前任务执行状态和历史 """
    return {
        "queue_size": _task_queue.qsize(),
        "current_task": {
            "name": _current_task.name,
            "started_at": _current_task.started_at,
            "status": _current_task.status
        } if _current_task else None,
        "history": [
            {
                "name": t.name,
                "status": t.status,
                "error": t.error,
                "finished_at": t.finished_at
            } for t in reversed(_task_history)
        ]
    }

@router.post("/db/query")
def execute_db_query(query: DBQuery):
    """ 执行自定义 SQL 查询 (限 SELECT) """
    if not query.sql.strip().lower().startswith("select"):
        raise HTTPException(status_code=400, detail="仅支持 SELECT 查询")
    
    try:
        import math
        from db.connection import fetch_df_read_only
        df = fetch_df_read_only(query.sql)
        if not df.empty:
            for col in df.columns:
                if df[col].dtype == 'object' or hasattr(df[col], 'dt'):
                    df[col] = df[col].astype(str)
            
            data = df.to_dict('records')
            for record in data:
                for key, value in record.items():
                    if isinstance(value, float) and math.isnan(value):
                        record[key] = None

            return {
                "columns": df.columns.tolist(),
                "data": data
            }
        return {"columns": [], "data": []}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"查询执行失败: {e}")

def _build_sync_task(params: SyncTaskParams):
    task = (params.task or "").strip().lower()
    if task == "daily":
        name = f"同步当日行情:{params.start_date}" if params.start_date else "同步当日行情"
        def logic():
            # 兼容仅传 start_date 的手动补数场景
            if params.start_date and (not params.end_date or params.start_date == params.end_date):
                sync_engine.sync_daily_by_date(params.start_date, calc_factors=params.calc_factors)
            else:
                years = params.years if params.years > 0 else 1
                sync_engine.sync_daily_price(years=years, force=params.force, calc_factors=params.calc_factors)
        return name, logic

    if task == "index":
        ts_code = params.ts_code or "000001.SH"
        years = params.years if params.years >= 0 else 0
        days = params.days if params.days > 0 else 3
        return f"同步指数:{ts_code}", (sync_engine.sync_market_index, {"ts_code": ts_code, "years": years, "days": days})

    if task == "basic":
        return "同步股票基本信息", sync_engine.sync_stock_basic
    if task == "concepts":
        return "同步概念板块", sync_engine.sync_concepts
    if task == "calendar":
        return "同步交易日历", sync_engine.sync_trade_cal
    if task == "financials":
        return "同步财务指标", (sync_engine.sync_financials, {"limit": params.limit})

    if task == "quarterly_income":
        from etl.tasks.financials_task import FinancialsTask
        from etl.providers.tushare_pro import TushareProvider
        provider = TushareProvider()
        financial_task = FinancialsTask(provider=provider)
        return "同步季度利润表", (financial_task.sync_quarterly_income, {"ts_code": params.ts_code, "force_sync": params.force_sync})

    if task == "fina_indicator":
        from etl.tasks.financials_task import FinancialsTask
        from etl.providers.tushare_pro import TushareProvider
        provider = TushareProvider()
        financial_task = FinancialsTask(provider=provider)
        return "同步财务指标(全部)", (financial_task.sync_fina_indicator, {"ts_code": params.ts_code})

    if task == "moneyflow":
        years = params.years if params.years >= 0 else 0
        days = params.days if params.days > 0 else 3
        return "同步资金流向", (sync_engine.sync_moneyflow, {"years": years, "days": days, "force": params.force})

    if task == "margin":
        days = params.days if params.days > 0 else 90
        return f"同步融资融券({days}天)", (sync_engine.sync_margin, {"days": days})

    if task == "fx":
        return "同步外汇数据", sync_engine.sync_fx

    raise HTTPException(status_code=400, detail=f"不支持的同步任务: {params.task}")


@router.post("/etl/sync", status_code=202)
async def trigger_sync(params: SyncTaskParams):
    task_name, task_obj = _build_sync_task(params)
    sentiment_days = max(int(params.sentiment_days), 1)

    def run_task():
        if isinstance(task_obj, tuple):
            fn, kwargs = task_obj
            fn(**kwargs)
        else:
            task_obj()

    if params.refresh_sentiment:
        def wrapped():
            run_task()
            sync_engine.sync_core_indices(years=0, days=max(5, sentiment_days))
            sync_engine.calculate_market_sentiment(days=sentiment_days)
        wrapped_name = f"{task_name}+重算市场情绪({sentiment_days}天)"
        await add_to_queue(wrapped_name, wrapped)
        return {"message": f"{wrapped_name}任务已加入队列"}

    await add_to_queue(task_name, run_task)
    return {"message": f"{task_name}任务已加入队列"}

@router.post("/etl/sentiment", status_code=202)
async def trigger_sentiment_sync(days: int = 365, sync_index: bool = True):
    """
    统一情绪任务入口：
    - sync_index=True: 先同步指数再重算情绪
    - sync_index=False: 仅重算情绪
    """
    def task_logic():
        if sync_index:
            # 先补齐情绪模型依赖的核心指数，并覆盖本次重算窗口
            sync_days = max(int(days), 30)
            sync_engine.sync_core_indices(years=0, days=sync_days)
        sync_engine.calculate_market_sentiment(days=days)

    task_name = f"{'同步并重算' if sync_index else '重算'}市场情绪({days}天)"
    await add_to_queue(task_name, task_logic)
    return {"message": f"{task_name}任务已加入队列"}

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
                "policy": best_policy or result.get("policy", {}),
                "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
    except Exception as e:
        logger.error(f"获取回测结果失败: {e}")
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
