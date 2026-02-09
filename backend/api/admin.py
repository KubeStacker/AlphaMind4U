# /backend/api/admin.py

from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends
from pydantic import BaseModel
import arrow
import asyncio
import time

from etl.sync import sync_engine
from etl.quality import quality_checker
from db.connection import get_db_connection, fetch_df
from passlib.context import CryptContext

# 统一加密上下文
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# 全局同步锁/信号量，防止并发请求导致 OOM
_SYNC_SEMAPHORE = asyncio.Semaphore(1)
_LAST_SYNC_TIME = 0
_SYNC_COOLDOWN = 60  # 60秒冷却时间

# 创建一个专门用于管理和ETL任务的路由
router = APIRouter(
    prefix="/admin",
    tags=["Admin & ETL"], # 在OpenAPI文档中分组
)

# --- 数据模型 ---
class IntegrityParams(BaseModel):
    """ 'GET /integrity' 接口的查询参数模型 """
    start_date: str | None = None
    end_date: str | None = None

class SyncDailyParams(BaseModel):
    start_date: str | None = None
    end_date: str | None = None
    years: int = 1
    force: bool = False
    calc_factors: bool = True

class SyncFactorsParams(BaseModel):
    start_date: str
    end_date: str

class UserCreate(BaseModel):
    username: str
    password: str
    role: str = "viewer"

class PasswordChange(BaseModel):
    user_id: int
    new_password: str

class DBQuery(BaseModel):
    sql: str

# --- 辅助函数 ---
async def acquire_sync_lock():
    global _LAST_SYNC_TIME
    if _SYNC_SEMAPHORE.locked():
        raise HTTPException(status_code=429, detail="系统正在执行同步任务，请稍后再试")
    
    # 简单的冷却时间检查
    if time.time() - _LAST_SYNC_TIME < _SYNC_COOLDOWN:
         raise HTTPException(status_code=429, detail=f"请求过于频繁，请等待 {_SYNC_COOLDOWN} 秒")
    
    await _SYNC_SEMAPHORE.acquire()
    _LAST_SYNC_TIME = time.time()
    return True

def release_sync_lock(task_func, *args, **kwargs):
    """ 包装任务函数，执行完毕后释放锁 """
    try:
        task_func(*args, **kwargs)
    finally:
        try:
            _SYNC_SEMAPHORE.release()
        except ValueError:
            pass # 忽略多次释放错误

# --- API 接口 ---

@router.post("/db/query")
def execute_db_query(query: DBQuery):
    """ 执行自定义 SQL 查询 (限 SELECT) """
    if not query.sql.strip().lower().startswith("select"):
        raise HTTPException(status_code=400, detail="仅支持 SELECT 查询")
    
    try:
        import math
        # 建议这里也使用 read_only 逻辑，但目前先保持一致
        from db.connection import fetch_df_read_only
        df = fetch_df_read_only(query.sql)
        # 将结果转换为适合 JSON 传输的格式
        # 处理日期和时间对象，因为它们默认不能直接被 json.dumps
        if not df.empty:
            for col in df.columns:
                if df[col].dtype == 'object' or hasattr(df[col], 'dt'):
                    df[col] = df[col].astype(str)
            
            data = df.to_dict('records')
            # Handle NaN for JSON compliance
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

class SyncIndexParams(BaseModel):
    ts_code: str
    years: int = 1

@router.post("/etl/sync/index", status_code=202)
async def trigger_sync_index(params: SyncIndexParams, background_tasks: BackgroundTasks):
    """ 手动触发指定指数同步 """
    await acquire_sync_lock()
    def task():
        release_sync_lock(sync_engine.sync_market_index, ts_code=params.ts_code, years=params.years)
    background_tasks.add_task(task)
    return {"message": f"指数 {params.ts_code} 同步任务已启动"}

@router.post("/etl/sync/daily", status_code=202)
async def trigger_granular_daily_sync(params: SyncDailyParams, background_tasks: BackgroundTasks):
    """
    精细化控制的日线行情同步。
    - start_date/end_date: 指定同步范围 (YYYY-MM-DD)
    - years: 如果未指定日期，同步最近 N 年
    - calc_factors: 是否同时计算因子
    """
    await acquire_sync_lock()
    
    def task():
        try:
            sync_engine.sync_daily_price(years=params.years, force=params.force, calc_factors=params.calc_factors)
        finally:
            _SYNC_SEMAPHORE.release()

    background_tasks.add_task(task)
    return {"message": f"同步任务已启动 (Years={params.years})"}


@router.post("/etl/sync/basic", status_code=202)
async def trigger_sync_basic(background_tasks: BackgroundTasks):
    """ 手动触发股票基础信息同步 """
    await acquire_sync_lock()
    def task():
        release_sync_lock(sync_engine.sync_stock_basic)
    background_tasks.add_task(task)
    return {"message": "股票基础信息同步任务已启动"}


@router.post("/etl/sync/concepts", status_code=202)
async def trigger_sync_concepts(background_tasks: BackgroundTasks):
    """ 手动触发概念及板块明细同步 """
    await acquire_sync_lock()
    def task():
        release_sync_lock(sync_engine.sync_concepts)
    background_tasks.add_task(task)
    return {"message": "概念板块同步任务已启动"}


@router.post("/etl/sync/financials", status_code=202)
async def trigger_sync_financials(background_tasks: BackgroundTasks, limit: int = 1000):
    """ 手动触发财务指标同步 """
    await acquire_sync_lock()
    def task():
        release_sync_lock(sync_engine.sync_financials, limit=limit)
    background_tasks.add_task(task)
    return {"message": f"前 {limit} 只股票的财务指标同步任务已启动"}


@router.post("/strategy/mainline/backfill", status_code=202)
async def trigger_mainline_backfill(background_tasks: BackgroundTasks, days: int = 30):
    """
    回溯并补全历史主线评分数据。
    """
    await acquire_sync_lock()
    
    def task():
        try:
            from strategy.mainline import mainline_analyst
            from db.connection import fetch_df
            # 获取最近 N 个交易日
            date_query = f"SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date DESC LIMIT {int(days)}"
            dates_df = fetch_df(date_query)
            if not dates_df.empty:
                dates = dates_df['trade_date'].tolist()
                for i, d in enumerate(dates):
                    d_str = d.strftime('%Y-%m-%d')
                    mainline_analyst.save_results(d_str)
                    # 每处理 3 天数据休眠一下，平滑 IO
                    if (i + 1) % 3 == 0:
                        time.sleep(1.0)
                    else:
                        time.sleep(0.2)
        finally:
            _SYNC_SEMAPHORE.release()

    background_tasks.add_task(task)
    return {"message": f"过去 {days} 天的主线数据回溯任务已启动"}

@router.post("/etl/sync/factors", status_code=202)
async def trigger_granular_factor_sync(params: SyncFactorsParams, background_tasks: BackgroundTasks):
    """
    批量计算指定日期范围内的因子 (低内存占用)。
    """
    await acquire_sync_lock()
    
    def task():
        try:
            sync_engine.calculate_factors_batch(params.start_date, params.end_date)
        finally:
            _SYNC_SEMAPHORE.release()

    background_tasks.add_task(task)
    return {"message": f"因子计算任务 ({params.start_date} -> {params.end_date}) 已启动"}

@router.post("/etl/sync_moneyflow", status_code=202)
async def trigger_sync_moneyflow(background_tasks: BackgroundTasks, years: int = 1):
    """ 手动触发资金流向同步任务 """
    await acquire_sync_lock()
    def task():
        release_sync_lock(sync_engine.sync_moneyflow, years=years)
    background_tasks.add_task(task)
    return {"message": f"最近 {years} 年资金流向同步任务已在后台启动。"}


@router.post("/etl/fill_factors", status_code=202)
async def trigger_fill_factors(background_tasks: BackgroundTasks):
    """ 手动触发补全缺失因子任务 """
    await acquire_sync_lock()
    def task():
        release_sync_lock(sync_engine.fill_missing_factors)
    background_tasks.add_task(task)
    return {"message": "因子补全任务已在后台启动。"}


@router.post("/etl/sentiment", status_code=202)
async def trigger_sentiment_sync(background_tasks: BackgroundTasks):
    """ 手动触发情绪指标计算与指数同步 """
    await acquire_sync_lock()
    def task():
        try:
            sync_engine.sync_market_index(years=1)
            sync_engine.calculate_market_sentiment(days=365)
        finally:
            _SYNC_SEMAPHORE.release()
    
    background_tasks.add_task(task)
    return {"message": "情绪与指数同步任务已在后台启动。"}


@router.get("/integrity")
def get_data_integrity_report(params: IntegrityParams = Depends()):
    """
    获取全方位的指标数据监控报告。
    
    可指定时间范围，默认为过去一年。
    """
    # 如果没有提供日期，则默认查询过去一年的数据
    if params.end_date is None:
        params.end_date = arrow.now().format("YYYY-MM-DD")
    if params.start_date is None:
        params.start_date = arrow.now().shift(years=-1).format("YYYY-MM-DD")
    
    try:
        report = quality_checker.get_comprehensive_report(
            start_date=params.start_date,
            end_date=params.end_date
        )
        return report
    except Exception as e:
        # 捕获潜在的错误，例如日期格式不正确
        raise HTTPException(status_code=400, detail=f"生成报告失败: {e}")

# --- 用户管理 ---

@router.get("/users")
def list_users():
    """ 列出系统所有用户 """
    with get_db_connection() as con:
        # 显式转换 created_at 为字符串，避免 JSON 序列化问题
        users = con.execute("SELECT id, username, role, CAST(created_at AS VARCHAR) FROM users").fetchall()
        return [{"id": u[0], "username": u[1], "role": u[2], "created_at": u[3]} for u in users]

@router.post("/users")
def create_user(user: UserCreate):
    """ 创建新用户 """
    hashed_password = pwd_context.hash(user.password)
    
    try:
        with get_db_connection() as con:
            con.execute(
                "INSERT INTO users (username, hashed_password, role) VALUES (?, ?, ?)",
                (user.username, hashed_password, user.role)
            )
        return {"message": f"用户 {user.username} 创建成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"创建用户失败: {e}")

@router.delete("/users/{user_id}")
def delete_user(user_id: int):
    """ 删除用户 """
    with get_db_connection() as con:
        con.execute("DELETE FROM users WHERE id = ?", (user_id,))
    return {"message": "用户已删除"}

@router.put("/users/password")
def change_password(data: PasswordChange):
    """ 修改用户密码 """
    hashed_password = pwd_context.hash(data.new_password)
    
    with get_db_connection() as con:
        con.execute(
            "UPDATE users SET hashed_password = ? WHERE id = ?",
            (hashed_password, data.user_id)
        )
    return {"message": "密码修改成功"}
