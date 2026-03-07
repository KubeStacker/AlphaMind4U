# /backend/api/admin.py

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
import arrow
import asyncio
import time
import logging
import math
import json

from etl.sync import sync_engine
from etl.utils.quality import quality_checker
from db.connection import get_db_connection, fetch_df
from passlib.context import CryptContext
from etl.calendar import trading_calendar

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


def _safe_float(value):
    try:
        x = float(value)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def _normalize_ts_code(raw_code: str) -> str:
    code = (raw_code or "").strip().upper()
    if not code:
        return ""
    if "." in code:
        return code
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


def _detect_kline_patterns(df):
    """向量化K线形态识别 — 委托给 kline_patterns 引擎"""
    if df is None or df.empty or len(df) < 5:
        return []

    try:
        from etl.utils.kline_patterns import detect_all_patterns, get_latest_signals
        result_df = detect_all_patterns(df)
        signals = get_latest_signals(result_df, min_confidence=0.5)
        return [s['pattern'] for s in signals][:5]
    except Exception as e:
        logger.error(f"K线形态识别失败: {e}", exc_info=True)
        return []


def _get_single_day_analysis(df_slice):
    if df_slice.empty or len(df_slice) < 5:
        return {"patterns": [], "suggestion": "观望", "tone": "中性"}
    
    # 使用增强版识别器
    recognizer = PatternRecognizer(df_slice)
    patterns = recognizer.recognize()
    
    latest = df_slice.iloc[-1]
    pct = _safe_float(latest.get("pct_chg"))
    
    suggestion = "观望"
    tone = "中性"
    
    bullish_pats = {"底分型", "早晨之星", "金针探底", "红三兵", "大阳线", "老鸭头",
                     "放量突破", "仙人指路", "锤子线", "看涨吞没", "启明星",
                     "上升三法", "量价齐升"}
    bearish_pats = {"顶分型", "黄昏之星", "墓碑线", "三只乌鸦", "大阴线",
                     "上吊线", "看跌吞没", "下降三法", "量价背离"}
    
    p_set = set(patterns)
    bull_hits = p_set.intersection(bullish_pats)
    bear_hits = p_set.intersection(bearish_pats)
    
    if bull_hits:
        suggestion = "试错关注" if "老鸭头" in bull_hits or "放量突破" in bull_hits else "择机关注"
        tone = "看多"
    elif bear_hits:
        suggestion = "减仓避险" if "顶分型" in bear_hits else "谨慎持币"
        tone = "看空"
        
    if pct is not None:
        if pct >= 5: tone = "强力爆发"
        elif pct <= -5: tone = "恐慌杀跌"
        
    return {
        "patterns": patterns,
        "suggestion": suggestion,
        "tone": tone,
        "date": str(latest.get("trade_date"))[:10]
    }


def _build_watch_analyse(ts_code: str):
    """
    站在机构研究员和游资大佬视角，结合收盘日线给出专业点评。
    """
    # 获取更多历史数据以计算均线
    hist = fetch_df(
        """
        SELECT trade_date, open, high, low, close, pct_chg, vol, amount, factors
        FROM daily_price
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT 100
        """,
        (ts_code,),
    )
    
    if hist.empty:
        return {"summary": "历史数据不足", "patterns": [], "suggestion": "观望", "history": []}

    hist = hist.iloc[::-1].reset_index(drop=True)
    
    # 展开 factors
    for idx, row in hist.iterrows():
        if row['factors']:
            try:
                factors = json.loads(row['factors']) if isinstance(row['factors'], str) else row['factors']
                for k, v in factors.items():
                    hist.at[idx, k] = v
            except: pass
    
    # 填充缺失均线 (如果SQL没算出来，手动补一下基础的)
    for ma in [5, 10, 20, 60]:
        col = f'ma{ma}'
        if col not in hist.columns:
            hist[col] = hist['close'].rolling(ma).mean()

    # 获取融资数据 (如果有)
    margin = fetch_df(
        """
        SELECT rzye, rzmre, rqye
        FROM stock_margin
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        (ts_code,),
    )
    if not margin.empty:
        for col in margin.columns:
            hist.at[len(hist)-1, col] = margin.iloc[0][col]

    # 获取最近 10 天的简要形态历史 (前端点点)
    history = []
    n = len(hist)
    history_window = max(1, min(10, n - 19))
    for i in range(history_window):
        idx = n - i
        day_analysis = _get_single_day_analysis(hist.iloc[:idx])
        history.append(day_analysis)
    
    # 生成专业点评 (最近 5 日)
    recognizer = PatternRecognizer(hist)
    latest_patterns = recognizer.recognize()
    pro_commentary = get_professional_commentary(hist, latest_patterns)
    pro_detail = get_professional_commentary_detailed(hist, latest_patterns)
    
    latest_analysis = history[0] if history else {"suggestion": "观望"}
    
    return {
        "summary": pro_commentary,
        "patterns": latest_patterns,
        "suggestion": latest_analysis["suggestion"],
        "history": history,
        "detail": pro_detail
    }

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
            SELECT trade_date, net_mf_vol
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

    col_map = {str(c).lower(): c for c in quote_df.columns}
    ts_col = col_map.get("ts_code")
    if ts_col is None:
        raise HTTPException(status_code=500, detail="实时行情缺少 ts_code 字段")

    name_col = col_map.get("name")
    price_col = col_map.get("price") or col_map.get("current") or col_map.get("close")
    pre_close_col = col_map.get("pre_close") or col_map.get("yclose")
    open_col = col_map.get("open")
    high_col = col_map.get("high")
    low_col = col_map.get("low")
    pct_col = col_map.get("pct_chg") or col_map.get("pct_change") or col_map.get("changepercent")
    vol_col = col_map.get("vol") or col_map.get("volume")
    amount_col = col_map.get("amount") or col_map.get("turnover")

    rows = []
    for _, row in quote_df.iterrows():
        ts_code = _normalize_ts_code(str(row.get(ts_col, "")))
        if ts_code not in norm_codes:
            continue

        price = _safe_float(row.get(price_col)) if price_col else None
        pre_close = _safe_float(row.get(pre_close_col)) if pre_close_col else None
        open_val = _safe_float(row.get(open_col)) if open_col else None
        high_val = _safe_float(row.get(high_col)) if high_col else None
        low_val = _safe_float(row.get(low_col)) if low_col else None
        
        pct = _safe_float(row.get(pct_col)) if pct_col else None
        diff = None
        amplitude = None

        if price is not None and pre_close is not None:
            diff = price - pre_close
            if pct is None and pre_close != 0:
                pct = diff / pre_close * 100.0
        
        if high_val is not None and low_val is not None and pre_close and pre_close > 0:
            amplitude = (high_val - low_val) / pre_close * 100.0

        rows.append(
            {
                "ts_code": ts_code,
                "name": str(row.get(name_col, "")) if name_col else "",
                "price": price,
                "pre_close": pre_close,
                "open": open_val,
                "high": high_val,
                "low": low_val,
                "pct": pct,
                "diff": diff,
                "amplitude": amplitude,
                "vol": _safe_float(row.get(vol_col)) if vol_col else None,
                "amount": _safe_float(row.get(amount_col)) if amount_col else None,
                "analyze": _build_watch_analyse(ts_code),
            }
        )

    idx_map = {c: i for i, c in enumerate(norm_codes)}
    rows.sort(key=lambda x: idx_map.get(x.get("ts_code"), 10**9))

    return {
        "status": "success",
        "refresh_mode": "realtime",
        "is_trading_time": True,
        "message": "ok",
        "data": rows,
    }
