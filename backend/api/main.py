"""
FastAPI主应用 - 重构版
"""
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, Dict
from datetime import datetime, date
from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict

from db.database import get_db
from auth.auth import authenticate_user, create_access_token, get_current_user
from sqlalchemy import text
from services.data_collection_service import DataCollectionService
from services.sheep_service import SheepService
from services.hot_rank_service import HotRankService
from services.concept_service import ConceptService
from services.trending_sector_service import TrendingSectorService
from services.concept_management_service import ConceptManagementService
from services.ai_service import AIService
from db.ai_config_repository import AIConfigRepository
from services.user_service import UserService
from scheduler import start_scheduler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("QuantAPI")

# 应用生命周期
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # 初始化管理员用户
        from auth.init_admin import init_admin
        init_admin()
        
        # 启动定时任务
        start_scheduler()
        
        logger.info("系统初始化完成")
    except Exception as e:
        logger.error(f"系统启动异常: {e}")
    yield

app = FastAPI(title="肥羊数据API", version="3.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# 添加请求日志中间件（在CORS之后，这样才能捕获所有请求）
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """记录所有请求的中间件"""
    path = request.url.path
    if path.startswith("/api/auth/login"):
        logger.info("=" * 60)
        logger.info(f"[REQUEST LOG] 收到登录请求")
        logger.info(f"  Method: {request.method}")
        logger.info(f"  Path: {path}")
        logger.info(f"  Client: {request.client.host if request.client else 'Unknown'}")
        logger.info(f"  User-Agent: {request.headers.get('user-agent', 'Unknown')}")
        logger.info(f"  Content-Type: {request.headers.get('content-type', 'Unknown')}")
    
    try:
        response = await call_next(request)
        
        if path.startswith("/api/auth/login"):
            logger.info(f"[REQUEST LOG] 登录请求响应: {response.status_code}")
            logger.info("=" * 60)
        
        return response
    except Exception as e:
        logger.error(f"[REQUEST LOG] 请求处理异常: {path}, 错误: {e}", exc_info=True)
        raise

# ========== 认证API（保留原有逻辑）==========
class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    username: str

class UserInfo(BaseModel):
    id: int
    username: str

@app.post("/api/auth/login", response_model=LoginResponse)
async def login(login_data: LoginRequest, request: Request):
    """用户登录"""
    logger.info(f"[LOGIN HANDLER] 登录处理器被调用，用户名: {login_data.username}")
    
    try:
        logger.info(f"[LOGIN HANDLER] 开始处理登录: 用户名={login_data.username}")
        ip_address = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")
        
        try:
            user = authenticate_user(login_data.username, login_data.password, ip_address, user_agent)
        except HTTPException as e:
            logger.warning(f"登录失败: 用户名={login_data.username}, 原因={e.detail}")
            raise
        except Exception as e:
            logger.error(f"认证过程异常: 用户名={login_data.username}, 错误={e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"认证过程出错: {str(e)}")
        
        if not user:
            logger.warning(f"登录失败: 用户名={login_data.username}, 原因=用户名或密码错误")
            raise HTTPException(status_code=401, detail="用户名或密码错误")
        
        try:
            access_token = create_access_token(data={"sub": user["username"]})
            logger.info(f"登录成功: 用户名={user['username']}")
            return LoginResponse(access_token=access_token, username=user["username"])
        except Exception as e:
            logger.error(f"生成token失败: 用户名={user['username']}, 错误={e}", exc_info=True)
            raise HTTPException(status_code=500, detail="生成访问令牌失败")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"登录失败: 用户名={login_data.username}, 错误={e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"登录失败: {str(e)}")

class UserInfoResponse(BaseModel):
    id: int
    username: str
    is_admin: bool

@app.get("/api/auth/me", response_model=UserInfoResponse)
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """获取当前用户信息"""
    username = current_user["username"]
    is_admin = username == "admin"
    
    return UserInfoResponse(
        id=current_user["id"],
        username=username,
        is_admin=is_admin
    )

def is_admin_user(current_user: dict = Depends(get_current_user)) -> dict:
    """检查是否为admin用户"""
    if current_user.get("username") != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可访问此功能")
    return current_user

def check_api_key_available(current_user: dict = Depends(get_current_user)) -> dict:
    """检查是否有可用的API Key（模型配置或系统默认）"""
    if not current_user:
        raise HTTPException(status_code=401, detail="未登录")
    
    from db.ai_model_config_repository import AIModelConfigRepository
    from db.ai_config_repository import AIConfigRepository
    
    # 检查是否有启用的模型配置了API Key
    models = AIModelConfigRepository.get_active_models()
    has_model_api_key = any(model.get("api_key") for model in models)
    
    if not has_model_api_key:
        raise HTTPException(status_code=400, detail="API Key未配置，请在AI管理设置中配置模型API Key")
    
    return current_user


@app.post("/api/auth/logout")
async def logout(current_user: dict = Depends(get_current_user)):
    return {"message": "登出成功"}

# ========== 肥羊相关API ==========
@app.get("/api/sheep/search")
async def search_sheep(q: str = ""):
    """搜索肥羊"""
    if not q or not q.strip():
        return {"sheep": []}
    
    try:
        keyword = q.strip()
        logger.info(f"搜索肥羊: {keyword}")
        sheep = SheepService.search_sheep(keyword)
        logger.info(f"搜索成功: 找到 {len(sheep)} 条结果")
        return {"sheep": sheep}
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"搜索失败: {e}\n{error_detail}")
        raise HTTPException(status_code=500, detail=f"搜索服务暂时不可用: {str(e)}")

@app.get("/api/sheep/{sheep_code}/daily")
async def get_sheep_daily(sheep_code: str, current_user: dict = Depends(get_current_user)):
    """获取肥羊日K数据"""
    try:
        data = SheepService.get_sheep_daily(sheep_code)
        return {"data": data}
    except Exception as e:
        logger.error(f"获取肥羊日K数据失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sheep/{sheep_code}/capital-flow")
async def get_sheep_capital_flow(sheep_code: str, days: int = 60, current_user: dict = Depends(get_current_user)):
    """
    获取资金流入数据
    
    Args:
        sheep_code: 肥羊代码
        days: 返回最近N天的数据，默认60天，可选30或60
    """
    try:
        # 限制days参数只能是30或60
        if days not in [30, 60]:
            days = 60
        
        data = SheepService.get_sheep_capital_flow(sheep_code, limit=days)
        # 确保返回数组，即使没有数据也返回空数组
        if not data:
            data = []
        return {"data": data}
    except Exception as e:
        logger.error(f"获取资金流数据失败: {e}", exc_info=True)
        # 即使出错也返回空数组，不影响前端显示
        return {"data": []}

@app.post("/api/sheep/{sheep_code}/capital-flow/refresh")
async def refresh_sheep_capital_flow(sheep_code: str, current_user: dict = Depends(get_current_user)):
    """
    刷新资金流数据（仅admin用户）
    如果数据不足（少于60天），则获取历史数据补全
    """
    try:
        # 检查是否是admin用户
        username = current_user.get('username', '')
        # 检查数据库中用户是否是admin（users表没有is_admin字段，只有username='admin'的是管理员）
        if username != 'admin':
            raise HTTPException(status_code=403, detail="只有管理员可以刷新资金流数据")
        
        from db.money_flow_repository import MoneyFlowRepository
        from etl.sheep_adapter import SheepAdapter
        from datetime import date, timedelta
        
        logger.info(f"管理员 {current_user.get('username')} 请求刷新肥羊 {sheep_code} 的资金流数据")
        
        # 检查当前数据量
        existing_data = MoneyFlowRepository.get_sheep_money_flow(sheep_code, limit=1000)
        existing_count = len(existing_data) if existing_data else 0
        
        # 如果数据充足（>=60天），不需要刷新
        if existing_count >= 60:
            logger.info(f"肥羊 {sheep_code} 资金流数据充足（{existing_count}条），无需刷新")
            return {
                "message": f"数据充足，已有 {existing_count} 条记录，无需刷新",
                "data_count": existing_count,
                "refreshed": False
            }
        
        # 数据不足，获取历史数据
        logger.info(f"肥羊 {sheep_code} 资金流数据不足（{existing_count}条），开始获取历史数据...")
        
        adapter = SheepAdapter()
        flow_df = adapter.get_sheep_money_flow_history(sheep_code)
        
        if flow_df is None or flow_df.empty:
            return {
                "message": "无法获取资金流历史数据",
                "data_count": existing_count,
                "refreshed": False
            }
        
        # 格式化数据
        data_list = []
        for _, row in flow_df.iterrows():
            data_list.append({
                'code': sheep_code,
                'date': row['trade_date'],
                'main': float(row.get('main_net_inflow', 0)),
                'super_large': float(row.get('super_large_inflow', 0)),
                'large': float(row.get('large_inflow', 0)),
                'medium': float(row.get('medium_inflow', 0)),
                'small': float(row.get('small_inflow', 0)),
            })
        
        # 批量保存
        MoneyFlowRepository.batch_upsert_money_flow(data_list)
        
        new_count = len(data_list)
        logger.info(f"肥羊 {sheep_code} 资金流数据刷新成功，新增/更新 {new_count} 条记录")
        
        return {
            "message": f"数据刷新成功，新增/更新 {new_count} 条记录",
            "data_count": new_count,
            "refreshed": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"刷新资金流数据失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"刷新资金流数据失败: {str(e)}")

@app.post("/api/sheep/{sheep_code}/refresh")
async def refresh_sheep_data(sheep_code: str, current_user: dict = Depends(get_current_user)):
    """刷新单个肥羊的最新市场数据（仅在交易时段）"""
    try:
        from etl.trade_date_adapter import TradeDateAdapter
        
        # 判断是否为交易时段
        if not TradeDateAdapter.is_trading_hours():
            raise HTTPException(status_code=400, detail="当前不是交易时段，无法刷新数据。交易时段：9:30-11:30, 13:00-15:00")
        
        service = DataCollectionService()
        success = service.refresh_single_sheep_data(sheep_code)
        
        if success:
            return {"message": "肥羊数据刷新成功"}
        else:
            raise HTTPException(status_code=400, detail="数据刷新失败，可能不在交易时段或非交易日")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"刷新肥羊数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== 热度榜相关API ==========
@app.get("/api/hot-sheep")
async def get_hot_sheep(source: Optional[str] = None):
    """获取热度榜"""
    try:
        sheep = HotRankService.get_hot_sheep(source)
        return {"sheep": sheep}
    except Exception as e:
        logger.error(f"获取热度榜异常: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hot-sectors")
async def get_hot_sectors():
    """获取热门板块（基于集聚效应算法）"""
    try:
        sectors = HotRankService.get_hot_sectors()
        return {"sectors": sectors}
    except Exception as e:
        logger.error(f"获取热门板块失败: {e}", exc_info=True)
        return {"sectors": []}

@app.post("/api/refresh-hot-sheep")
async def refresh_hot_sheep(
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: dict = Depends(get_current_user)
):
    """手动刷新热度榜数据（异步执行，立即返回）"""
    try:
        def run_refresh():
            try:
                service = DataCollectionService()
                service.collect_hot_rank_data()
                logger.info("热度榜数据刷新任务完成")
            except Exception as e:
                logger.error(f"后台热度榜数据刷新失败: {e}", exc_info=True)
        
        background_tasks.add_task(run_refresh)
        return {"message": "热度榜数据刷新任务已启动，正在后台执行", "status": "running"}
    except Exception as e:
        logger.error(f"启动热度榜刷新任务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== 板块相关API ==========
@app.get("/api/sectors/{sector_name}/daily")
async def get_sector_daily(sector_name: str, current_user: dict = Depends(get_current_user)):
    """获取板块K线"""
    try:
        data = ConceptService.get_sector_daily(sector_name)
        return {"data": data}
    except Exception as e:
        logger.error(f"获取板块K线数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sectors/{sector_name}/sheep")
async def get_sector_sheep(sector_name: str, current_user: dict = Depends(get_current_user)):
    """获取板块肥羊"""
    try:
        sheep = ConceptService.get_sector_sheep(sector_name)
        return {"sheep": sheep}
    except Exception as e:
        logger.error(f"获取板块肥羊失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sectors/{sector_name}/stocks-by-change")
async def get_sector_stocks_by_change(sector_name: str, limit: int = 10, current_user: dict = Depends(get_current_user)):
    """获取板块涨幅前N的概念股"""
    try:
        stocks = ConceptService.get_sector_stocks_by_change_pct(sector_name, limit)
        return {"stocks": stocks}
    except Exception as e:
        logger.error(f"获取板块涨幅前N概念股失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/capital-inflow/recommend")
async def get_capital_inflow_recommend(days: int = 5, current_user: dict = Depends(get_current_user)):
    """获取资金持续流入推荐（最近N天持续流入的标的）"""
    try:
        if days not in [5, 10, 20]:
            raise HTTPException(status_code=400, detail="days参数必须是5、10或20")
        stocks = SheepService.get_continuous_inflow_stocks(days=days)
        return {"stocks": stocks, "days": days}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取资金持续流入推荐失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sector-money-flow/recommend")
async def get_sector_money_flow_recommend(days: int = 1, limit: int = 30, current_user: dict = Depends(get_current_user)):
    """
    获取概念资金净流入推荐（API路径保持sector以保持向后兼容）
    
    Args:
        days: 统计天数（1=当日，3=最近3天，5=最近5天）
        limit: 返回数量，默认30，最大30
    """
    try:
        if days not in [1, 3, 5]:
            raise HTTPException(status_code=400, detail="days参数必须是1、3或5")
        
        # 限制最大返回数量为30
        if limit > 30:
            limit = 30
        
        from services.concept_money_flow_service import ConceptMoneyFlowService
        import asyncio
        
        # 使用异步执行避免阻塞
        concepts, metadata = await asyncio.to_thread(
            ConceptMoneyFlowService.get_top_concepts_by_inflow, 
            days=days, 
            limit=limit
        )
        
        return {
            "sectors": concepts,  # 保持字段名为sectors以保持向后兼容
            "days": days,
            "metadata": metadata
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取概念资金净流入推荐失败: {e}", exc_info=True)
        import traceback
        logger.error(f"异常详情:\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"获取板块资金净流入推荐失败: {str(e)}")

@app.get("/api/trending-sectors")
async def get_real_time_trending_sectors(limit: int = 10, current_user: dict = Depends(get_current_user)):
    """
    获取实时热门板块推荐（基于概念资金流、个股表现和综合指标的实时分析）
    
    Args:
        limit: 返回板块数量限制，默认10
    """
    try:
        sectors = TrendingSectorService.get_real_time_trending_sectors(limit=limit)
        return {
            "sectors": sectors,
            "timestamp": datetime.now().isoformat(),
            "limit": limit
        }
    except Exception as e:
        logger.error(f"获取实时热门板块推荐失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取实时热门板块推荐失败: {str(e)}")

# ========== 数据采集管理API ==========
@app.post("/api/admin/trigger-missed-tasks")
async def trigger_missed_tasks(current_user: dict = Depends(is_admin_user)):
    """手动触发错过的任务（仅admin）"""
    try:
        from scheduler import check_and_trigger_missed_tasks
        check_and_trigger_missed_tasks()
        return {"message": "已检查并触发错过的任务"}
    except Exception as e:
        logger.error(f"触发错过任务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/admin/data-collection/collect-specific")
async def collect_specific_data(
    data_type: str,
    days: Optional[int] = None,
    target_date: Optional[str] = None,
    force: bool = False,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: dict = Depends(get_current_user)
):
    """
    采集特定数据表（异步执行，立即返回）
    
    Args:
        data_type: 数据类型（sheep_daily, money_flow, concept_money_flow, hot_rank, concept_data, index_data, concept_metadata_sync）
        days: 采集天数（仅对sheep_daily和index_data有效）
        target_date: 目标日期（YYYY-MM-DD格式，可选，如果不是交易日，将自动使用最近交易日）
    """
    if not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="需要管理员权限")
    
    try:
        kwargs = {}
        if days is not None:
            kwargs['days'] = days
        if target_date:
            try:
                kwargs['target_date'] = datetime.strptime(target_date, '%Y-%m-%d').date()
            except ValueError:
                raise HTTPException(status_code=400, detail="日期格式错误，请使用YYYY-MM-DD格式")
        
        # 在后台异步执行数据采集
        def run_collection():
            try:
                service = DataCollectionService()
                result = service.collect_specific_data(data_type, **kwargs)
                logger.info(f"数据采集任务完成: {data_type}, 结果: {result}")
            except Exception as e:
                logger.error(f"后台数据采集任务失败: {data_type}, 错误: {e}", exc_info=True)
        
        background_tasks.add_task(run_collection)
        
        return {
            "message": f"数据采集任务已启动（{data_type}），正在后台执行",
            "data_type": data_type,
            "status": "running"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"启动数据采集任务失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/api/admin/data-collection/types")
async def get_data_collection_types(current_user: dict = Depends(get_current_user)):
    """获取可用的数据类型列表（包含统计信息）"""
    if not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="需要管理员权限")
    
    from services.data_collection_service import DataCollectionService
    from etl.trade_date_adapter import TradeDateAdapter
    from config import Config
    from datetime import date, timedelta
    from sqlalchemy import text
    from db.database import get_db
    
    service = DataCollectionService()
    today = date.today()
    
    # 定义数据类型配置
    type_configs = [
        {
            'value': 'realtime_data',
            'label': '实时交易数据',
            'description': '所有肥羊的实时行情和今日资金流向（每分钟刷新）',
            'requires_trading_day': True,
            'table_name': 'sheep_daily',
            'date_column': 'trade_date',
            'schedule_time': '交易时间每1分钟',
            'retention_days': Config.SHEEP_DATA_RETENTION_DAYS
        },
        {
            'value': 'sheep_daily',
            'label': '肥羊日K数据',
            'description': '肥羊日K线数据（开高低收、成交量、技术指标等）',
            'requires_trading_day': True,
            'table_name': 'sheep_daily',
            'date_column': 'trade_date',
            'schedule_time': '交易日15:00',
            'retention_days': Config.SHEEP_DATA_RETENTION_DAYS
        },
        {
            'value': 'money_flow',
            'label': '资金流向数据',
            'description': '肥羊主力资金流向（主力、超大单、大单、中单、小单）',
            'requires_trading_day': True,
            'table_name': 'sheep_money_flow',
            'date_column': 'trade_date',
            'schedule_time': '交易日15:00',
            'retention_days': Config.MONEY_FLOW_RETENTION_DAYS
        },
        {
            'value': 'concept_money_flow',
            'label': '概念资金流向数据',
            'description': '概念资金净流入数据',
            'requires_trading_day': True,
            'table_name': 'sector_money_flow',
            'date_column': 'trade_date',
            'schedule_time': '交易日15:00（实时每30分钟）',
            'retention_days': Config.SECTOR_MONEY_FLOW_RETENTION_DAYS
        },
        {
            'value': 'hot_rank',
            'label': '热度榜数据',
            'description': '市场热门肥羊排名（多数据源）',
            'requires_trading_day': False,
            'table_name': 'market_hot_rank',
            'date_column': 'trade_date',
            'schedule_time': '每10分钟',
            'retention_days': Config.HOT_RANK_RETENTION_DAYS  # 保留30天
        },
        {
            'value': 'concept_data',
            'label': '概念板块数据',
            'description': '概念主题列表、肥羊与概念的关联关系',
            'requires_trading_day': False,
            'table_name': 'concept_theme',
            'date_column': None,  # 无日期字段
            'schedule_time': '每天03:00',
            'retention_days': None
        },
        {
            'value': 'index_data',
            'label': '大盘指数数据',
            'description': '大盘指数日K数据（用于RSRS市场状态识别）',
            'requires_trading_day': True,
            'table_name': 'market_index_daily',
            'date_column': 'trade_date',
            'schedule_time': '交易日15:00',
            'retention_days': Config.SHEEP_DATA_RETENTION_DAYS  # 使用相同的保留天数
        },
        {
            'value': 'concept_metadata_sync',
            'label': '概念元数据同步',
            'description': '从EastMoney同步最新概念列表',
            'requires_trading_day': False,
            'table_name': 'concept_theme',
            'date_column': None,
            'schedule_time': '每天08:00',
            'retention_days': None
        },
        {
            'value': 'financial_data',
            'label': '财务数据',
            'description': '肥羊财务数据（研发费用、净利润、营业收入等，季度/年度数据）',
            'requires_trading_day': False,
            'table_name': 'sheep_financial',
            'date_column': 'report_date',
            'schedule_time': '每周00:00',
            'retention_days': None
        },

    ]
    
    # 为每个类型添加统计信息
    types_with_stats = []
    for config in type_configs:
        stats = {
            'table_name': config['table_name'],
            'schedule_time': config['schedule_time'],
            'retention_days': config['retention_days'],
            'trading_days_in_period': None,
            'actual_data_days': None
        }
        
        # 如果有保留天数，计算统计信息
        if config['retention_days'] and config['date_column']:
            try:
                start_date = today - timedelta(days=config['retention_days'])
                
                # 判断是否需要交易日（对于自然日数据，如热度榜，使用自然日天数）
                if config['requires_trading_day']:
                    # 计算最近N天的交易日天数
                    trading_days_list = TradeDateAdapter.get_trading_days_in_range(start_date, today)
                    stats['trading_days_in_period'] = len(trading_days_list)
                else:
                    # 对于自然日数据，使用自然日天数
                    stats['trading_days_in_period'] = config['retention_days']
                
                # 查询数据库中实际存储的数据天数
                with get_db() as db:
                    query = text(f"""
                        SELECT COUNT(DISTINCT {config['date_column']}) as day_count
                        FROM {config['table_name']}
                        WHERE {config['date_column']} >= :start_date
                          AND {config['date_column']} <= :end_date
                    """)
                    result = db.execute(query, {
                        'start_date': start_date,
                        'end_date': today
                    })
                    row = result.fetchone()
                    if row:
                        stats['actual_data_days'] = row[0] if row[0] is not None else 0
            except Exception as e:
                logger.warning(f"获取 {config['value']} 统计信息失败: {e}")
                stats['trading_days_in_period'] = None
                stats['actual_data_days'] = None
        
        # 对于无日期字段的表，查询总记录数
        elif config['table_name'] and not config['date_column']:
            try:
                with get_db() as db:
                    query = text(f"SELECT COUNT(*) as total_count FROM {config['table_name']}")
                    result = db.execute(query)
                    row = result.fetchone()
                    if row:
                        stats['actual_data_days'] = row[0] if row[0] is not None else 0
            except Exception as e:
                logger.warning(f"获取 {config['value']} 统计信息失败: {e}")
                stats['actual_data_days'] = None
        
        type_info = {
            'value': config['value'],
            'label': config['label'],
            'description': config['description'],
            'requires_trading_day': config['requires_trading_day'],
            'stats': stats
        }
        types_with_stats.append(type_info)
    
    return {
        'types': types_with_stats
    }

@app.post("/api/admin/collect-all-data")
async def collect_all_data(
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: dict = Depends(get_current_user)
):
    """
    手动触发全量数据采集（管理接口，异步执行，立即返回）
    采集顺序：
    1. 概念板块数据
    2. 肥羊日K数据
    3. 资金流向数据（仅在交易日）
    4. 热度榜数据
    5. 大盘指数数据（用于RSRS计算）
    """
    try:
        logger.info("手动触发全量数据采集（后台任务）...")
        
        # 在后台异步执行全量数据采集
        def run_collection():
            try:
                service = DataCollectionService()
                
                results = {
                    "concept_data": False,
                    "sheep_daily_data": False,
                    "money_flow_data": False,
                    "hot_rank_data": False,
                    "index_data": False,
                    "messages": []
                }
                
                # 1. 采集概念板块数据
                try:
                    logger.info("开始采集概念板块数据...")
                    service.collect_concept_data()
                    results["concept_data"] = True
                    results["messages"].append("概念板块数据采集完成")
                except Exception as e:
                    logger.error(f"概念板块数据采集失败: {e}")
                    results["messages"].append(f"概念板块数据采集失败: {str(e)}")
                
                # 2. 采集肥羊日K数据
                try:
                    logger.info("开始采集肥羊日K数据...")
                    service.collect_sheep_daily_data()
                    results["sheep_daily_data"] = True
                    results["messages"].append("肥羊日K数据采集完成")
                except Exception as e:
                    logger.error(f"肥羊日K数据采集失败: {e}")
                    results["messages"].append(f"肥羊日K数据采集失败: {str(e)}")
                
                # 3. 采集资金流向数据（仅在交易日）
                try:
                    from etl.trade_date_adapter import TradeDateAdapter
                    from datetime import date
                    today = date.today()
                    if TradeDateAdapter.is_trading_day(today):
                        logger.info("开始采集资金流向数据...")
                        service.collect_money_flow_data()
                        results["money_flow_data"] = True
                        results["messages"].append("资金流向数据采集完成")
                    else:
                        results["messages"].append("今天不是交易日，跳过资金流向数据采集")
                except Exception as e:
                    logger.error(f"资金流向数据采集失败: {e}")
                    results["messages"].append(f"资金流向数据采集失败: {str(e)}")
                
                # 4. 采集热度榜数据
                try:
                    logger.info("开始采集热度榜数据...")
                    service.collect_hot_rank_data()
                    results["hot_rank_data"] = True
                    results["messages"].append("热度榜数据采集完成")
                except Exception as e:
                    logger.error(f"热度榜数据采集失败: {e}")
                    results["messages"].append(f"热度榜数据采集失败: {str(e)}")
                
                # 5. 采集大盘指数数据（用于RSRS计算）
                try:
                    logger.info("开始采集大盘指数数据...")
                    service.collect_index_data(index_code='CSI1000')
                    results["index_data"] = True
                    results["messages"].append("大盘指数数据采集完成")
                except Exception as e:
                    logger.error(f"大盘指数数据采集失败: {e}")
                    results["messages"].append(f"大盘指数数据采集失败: {str(e)}")
                
                logger.info(f"全量数据采集任务完成，结果: {results}")
            except Exception as e:
                logger.error(f"后台全量数据采集任务失败: {e}", exc_info=True)
        
        background_tasks.add_task(run_collection)
        
        return {
            "message": "全量数据采集任务已启动，正在后台执行",
            "status": "running"
        }
        
    except Exception as e:
        logger.error(f"启动全量数据采集任务失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/check-data-gaps")
async def check_data_gaps(
    days: int = 30,
    data_type: str = 'all',
    current_user: dict = Depends(get_current_user)
):
    """
    检查数据缺失（仅admin）
    
    Args:
        days: 检查最近N天的数据
        data_type: 数据类型 ('sheep_daily', 'money_flow', 'hot_rank', 'index', 'all')
    """
    try:
        from scripts.check_data_gaps import DataGapChecker
        
        checker = DataGapChecker()
        results = {}
        
        if data_type in ('sheep_daily', 'all'):
            results['sheep_daily'] = checker.check_sheep_daily_gaps(days=days)
        
        if data_type in ('money_flow', 'all'):
            results['money_flow'] = checker.check_money_flow_gaps(days=days)
        
        if data_type in ('hot_rank', 'all'):
            results['hot_rank'] = checker.check_hot_rank_gaps(days=min(days, 7))
        
        if data_type in ('index', 'all'):
            results['index'] = checker.check_index_data_gaps(days=days)
        
        return {
            "message": "数据缺失检查完成",
            "days": days,
            "data_type": data_type,
            "results": results
        }
    except Exception as e:
        logger.error(f"检查数据缺失失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/admin/refresh-missing-data")
async def refresh_missing_data(
    data_type: str = 'all',
    days: int = 30,
    current_user: dict = Depends(get_current_user)
):
    """
    刷新缺失的数据（仅admin）
    
    Args:
        data_type: 数据类型 ('sheep_daily', 'money_flow', 'hot_rank', 'index', 'all')
        days: 刷新最近N天的数据
    """
    try:
        from scripts.check_data_gaps import DataGapChecker
        
        checker = DataGapChecker()
        checker.refresh_missing_data(data_type=data_type, days=days)
        
        return {
            "message": "数据刷新任务已启动",
            "data_type": data_type,
            "days": days
        }
    except Exception as e:
        logger.error(f"刷新缺失数据失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/data-report")
async def get_data_report(
    days: int = 30,
    current_user: dict = Depends(get_current_user)
):
    """
    生成数据完整性报告（仅admin）
    
    Args:
        days: 检查最近N天的数据
    """
    try:
        from scripts.check_data_gaps import DataGapChecker
        
        checker = DataGapChecker()
        report = checker.generate_report(days=days)
        
        return {
            "message": "数据完整性报告",
            "days": days,
            "report": report
        }
    except Exception as e:
        logger.error(f"生成数据报告失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ========== 概念管理API ==========
class ConceptCreate(BaseModel):
    concept_name: str
    concept_code: Optional[str] = None
    source: str = 'ths'
    description: Optional[str] = None

class ConceptUpdate(BaseModel):
    concept_name: Optional[str] = None
    concept_code: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None

class VirtualBoardMappingCreate(BaseModel):
    virtual_board_name: str
    source_concept_name: str
    weight: float = 1.0
    description: Optional[str] = None

@app.get("/api/concepts")
async def get_concepts(limit: int = 100, offset: int = 0, current_user: dict = Depends(get_current_user)):
    """获取概念列表"""
    try:
        result = ConceptManagementService.get_concepts(limit, offset)
        return result
    except Exception as e:
        logger.error(f"获取概念列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/concepts")
async def create_concept(concept: ConceptCreate, current_user: dict = Depends(get_current_user)):
    """创建概念"""
    try:
        concept_id = ConceptManagementService.create_concept(
            concept.concept_name,
            concept.concept_code,
            concept.source,
            concept.description
        )
        return {"message": "概念创建成功", "concept_id": concept_id}
    except Exception as e:
        logger.error(f"创建概念失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/concepts/{concept_id}")
async def update_concept(concept_id: int, concept: ConceptUpdate, current_user: dict = Depends(get_current_user)):
    """更新概念"""
    try:
        success = ConceptManagementService.update_concept(
            concept_id,
            concept.concept_name,
            concept.concept_code,
            concept.description,
            concept.is_active
        )
        if success:
            return {"message": "概念更新成功"}
        else:
            raise HTTPException(status_code=400, detail="没有需要更新的字段")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新概念失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/concepts/{concept_id}")
async def delete_concept(concept_id: int, current_user: dict = Depends(get_current_user)):
    """删除概念（软删除）"""
    try:
        success = ConceptManagementService.delete_concept(concept_id)
        if success:
            return {"message": "概念删除成功"}
        else:
            raise HTTPException(status_code=404, detail="概念不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除概念失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/virtual-boards/import-sector-mapping")
async def import_sector_mapping(current_user: dict = Depends(get_current_user)):
    """从sector_mapping表导入到virtual_board_aggregation"""
    try:
        result = ConceptManagementService.import_sector_mapping()
        return result
    except Exception as e:
        logger.error(f"导入sector_mapping失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/virtual-boards")
async def get_virtual_boards(current_user: dict = Depends(get_current_user)):
    """获取虚拟板块列表"""
    try:
        boards = ConceptManagementService.get_virtual_boards()
        return {"boards": boards}
    except Exception as e:
        logger.error(f"获取虚拟板块列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/virtual-boards/mappings")
async def create_virtual_board_mapping(mapping: VirtualBoardMappingCreate, current_user: dict = Depends(get_current_user)):
    """创建虚拟板块映射"""
    try:
        success = ConceptManagementService.create_virtual_board_mapping(
            mapping.virtual_board_name,
            mapping.source_concept_name,
            mapping.weight,
            mapping.description
        )
        return {"message": "虚拟板块映射创建成功"}
    except Exception as e:
        logger.error(f"创建虚拟板块映射失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/virtual-boards/mappings")
async def delete_virtual_board_mapping(
    virtual_board_name: str,
    source_concept_name: str,
    current_user: dict = Depends(get_current_user)
):
    """删除虚拟板块映射"""
    try:
        success = ConceptManagementService.delete_virtual_board_mapping(
            virtual_board_name,
            source_concept_name
        )
        if success:
            return {"message": "虚拟板块映射删除成功"}
        else:
            raise HTTPException(status_code=404, detail="映射不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除虚拟板块映射失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== 概念映射管理API（兼容前端调用）==========
class SectorMappingCreate(BaseModel):
    source_sector: str
    target_sector: str
    description: Optional[str] = None

class SectorMappingUpdate(BaseModel):
    target_sector: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None

@app.get("/api/sector-mappings")
async def get_sector_mappings(current_user: dict = Depends(get_current_user)):
    """获取板块映射列表（展示virtual_board_aggregation表数据）"""
    try:
        mappings = ConceptManagementService.get_all_virtual_board_mappings()
        return {"mappings": mappings}
    except Exception as e:
        logger.error(f"获取板块映射列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sector-mappings")
async def create_sector_mapping(
    mapping: SectorMappingCreate,
    current_user: dict = Depends(get_current_user)
):
    """创建板块映射"""
    try:
        success = ConceptManagementService.create_virtual_board_mapping(
            mapping.target_sector,  # virtual_board_name
            mapping.source_sector,  # source_concept_name
            1.0,  # weight
            mapping.description
        )
        if success:
            return {"message": "板块映射创建成功"}
        else:
            raise HTTPException(status_code=500, detail="创建失败")
    except Exception as e:
        logger.error(f"创建板块映射失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/sector-mappings/{mapping_id}")
async def update_sector_mapping(
    mapping_id: int,
    mapping: SectorMappingUpdate,
    current_user: dict = Depends(get_current_user)
):
    """更新板块映射"""
    try:
        success = ConceptManagementService.update_virtual_board_mapping_by_id(
            mapping_id,
            mapping.target_sector,
            mapping.description,
            mapping.is_active
        )
        if success:
            return {"message": "板块映射更新成功"}
        else:
            raise HTTPException(status_code=400, detail="没有需要更新的字段或映射不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新板块映射失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/sector-mappings/{mapping_id}")
async def delete_sector_mapping(
    mapping_id: int,
    current_user: dict = Depends(get_current_user)
):
    """删除板块映射"""
    try:
        success = ConceptManagementService.delete_virtual_board_mapping_by_id(mapping_id)
        if success:
            return {"message": "板块映射删除成功"}
        else:
            raise HTTPException(status_code=404, detail="映射不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除板块映射失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sector-mappings/refresh-cache")
async def refresh_sector_mapping_cache(current_user: dict = Depends(get_current_user)):
    """刷新板块映射缓存"""
    try:
        result = ConceptManagementService.refresh_virtual_board_cache()
        return result
    except Exception as e:
        logger.error(f"刷新缓存失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== 用户管理API（仅admin）==========
class UserCreate(BaseModel):
    username: str
    password: str

class UserUpdate(BaseModel):
    password: Optional[str] = None
    is_active: Optional[bool] = None

@app.get("/api/users")
async def get_users(current_user: dict = Depends(is_admin_user)):
    """获取用户列表"""
    try:
        users = UserService.get_all_users()
        return {"users": users}
    except Exception as e:
        logger.error(f"获取用户列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/users")
async def create_user(
    user: UserCreate,
    current_user: dict = Depends(is_admin_user)
):
    """创建用户"""
    try:
        user_id = UserService.create_user(
            user.username,
            user.password
        )
        return {"message": "用户创建成功", "user_id": user_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建用户失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/users/{user_id}")
async def update_user(
    user_id: int,
    user: UserUpdate,
    current_user: dict = Depends(is_admin_user)
):
    """更新用户"""
    try:
        success = UserService.update_user(
            user_id,
            user.password,
            user.is_active
        )
        if success:
            return {"message": "用户更新成功"}
        else:
            raise HTTPException(status_code=400, detail="没有需要更新的字段或用户不存在")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新用户失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/users/{user_id}")
async def delete_user(
    user_id: int,
    current_user: dict = Depends(is_admin_user)
):
    """删除用户（软删除）"""
    try:
        success = UserService.delete_user(user_id)
        if success:
            return {"message": "用户删除成功"}
        else:
            raise HTTPException(status_code=404, detail="用户不存在")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除用户失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== AI模型配置管理API（仅admin）==========
class AIModelConfigUpdate(BaseModel):
    api_key: Optional[str] = None
    sort_order: Optional[int] = None
    is_active: Optional[bool] = None

class AIModelConfigCreate(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    model_name: str
    model_display_name: str
    api_key: str
    api_url: str
    sort_order: int = 0

@app.get("/api/ai/models")
async def get_ai_models(current_user: dict = Depends(get_current_user)):
    """获取所有AI模型配置（所有用户可查看）"""
    try:
        from db.ai_model_config_repository import AIModelConfigRepository
        models = AIModelConfigRepository.get_all_models()
        return {"models": models}
    except Exception as e:
        logger.error(f"获取AI模型配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ai/models/active")
async def get_active_ai_models(current_user: dict = Depends(get_current_user)):
    """获取所有启用的AI模型配置（所有用户可查看）"""
    try:
        from db.ai_model_config_repository import AIModelConfigRepository
        models = AIModelConfigRepository.get_active_models()
        return {"models": models}
    except Exception as e:
        logger.error(f"获取启用的AI模型配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/ai/models/{model_id}")
async def update_ai_model(
    model_id: int,
    config: AIModelConfigUpdate,
    current_user: dict = Depends(is_admin_user)
):
    """更新AI模型配置（仅admin）"""
    try:
        from db.ai_model_config_repository import AIModelConfigRepository
        success = AIModelConfigRepository.update_model(
            model_id,
            api_key=config.api_key,
            sort_order=config.sort_order,
            is_active=config.is_active
        )
        if success:
            return {"message": "模型配置更新成功"}
        else:
            raise HTTPException(status_code=500, detail="模型配置更新失败")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新AI模型配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ai/models")
async def create_ai_model(
    config: AIModelConfigCreate,
    current_user: dict = Depends(is_admin_user)
):
    """创建新AI模型配置（仅admin）"""
    try:
        from db.ai_model_config_repository import AIModelConfigRepository
        model_id = AIModelConfigRepository.create_model(
            config.model_name,
            config.model_display_name,
            config.api_key,
            config.api_url,
            config.sort_order
        )
        return {"message": "模型配置创建成功", "model_id": model_id}
    except Exception as e:
        logger.error(f"创建AI模型配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/ai/models/{model_id}")
async def delete_ai_model(
    model_id: int,
    current_user: dict = Depends(is_admin_user)
):
    """删除AI模型配置（仅admin）"""
    try:
        from db.ai_model_config_repository import AIModelConfigRepository
        success = AIModelConfigRepository.delete_model(model_id)
        if success:
            return {"message": "模型配置删除成功"}
        else:
            raise HTTPException(status_code=500, detail="模型配置删除失败")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除AI模型配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== AI管理API（仅admin）==========
class AIConfigUpdate(BaseModel):
    config_value: str
    description: Optional[str] = None

class AIPromptUpdate(BaseModel):
    prompt_content: str

@app.get("/api/ai/config")
async def get_ai_config(current_user: dict = Depends(is_admin_user)):
    """获取AI配置"""
    try:
        configs = AIConfigRepository.get_all_configs()
        return {"configs": configs}
    except Exception as e:
        logger.error(f"获取AI配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/ai/config/{config_key}")
async def update_ai_config(
    config_key: str,
    config: AIConfigUpdate,
    current_user: dict = Depends(is_admin_user)
):
    """更新AI配置"""
    try:
        success = AIConfigRepository.set_config(
            config_key,
            config.config_value,
            config.description
        )
        if success:
            return {"message": "配置更新成功"}
        else:
            raise HTTPException(status_code=500, detail="配置更新失败")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新AI配置失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"配置更新失败: {str(e)}")

@app.get("/api/ai/prompts")
async def get_ai_prompts(current_user: dict = Depends(get_current_user)):
    """获取所有Prompt模板（所有用户可查看）"""
    try:
        prompts = {
            "recommend": AIConfigRepository.get_config("prompt_recommend") or "",
            "analyze": AIConfigRepository.get_config("prompt_analyze") or ""
        }
        return {"prompts": prompts}
    except Exception as e:
        logger.error(f"获取Prompt模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/ai/prompts/{prompt_type}")
async def update_ai_prompt(
    prompt_type: str,
    prompt: AIPromptUpdate,
    current_user: dict = Depends(is_admin_user)
):
    """更新Prompt模板"""
    try:
        if prompt_type not in ["recommend", "analyze"]:
            raise HTTPException(status_code=400, detail="无效的prompt类型")
        
        success = AIConfigRepository.set_config(
            f"prompt_{prompt_type}",
            prompt.prompt_content,
            f"AI {prompt_type} prompt模板"
        )
        if success:
            return {"message": "Prompt模板更新成功"}
        else:
            raise HTTPException(status_code=500, detail="Prompt模板更新失败")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新Prompt模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ai/clear-cache")
async def clear_ai_cache(current_user: dict = Depends(is_admin_user)):
    """清空AI分析缓存"""
    try:
        from db.ai_cache_repository import AICacheRepository
        
        deleted_count = AICacheRepository.clear_all_cache()
        return {"message": "缓存清空成功", "deleted_count": deleted_count}
    except Exception as e:
        logger.error(f"清空AI缓存失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== AI推荐和分析API（仅admin）==========
class AIRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    model_name: Optional[str] = None
    custom_prompt: Optional[str] = None  # 自定义提示词（可选）

@app.post("/api/ai/recommend-sheep")
async def ai_recommend_sheep(
    request: AIRequest = AIRequest(),
    current_user: dict = Depends(check_api_key_available)
):
    """AI推荐肥羊（带3小时缓存，开放给所有用户）"""
    try:
        from db.ai_cache_repository import AICacheRepository
        
        user_id = current_user.get("id")
        
        # 先检查缓存
        cached_result = AICacheRepository.get_cache("recommend", "recommend")
        if cached_result:
            logger.info("使用缓存的AI推荐结果")
            return {
                "recommendation": cached_result,
                "timestamp": datetime.now().isoformat(),
                "cached": True
            }
        
        # 缓存未命中，调用AI服务
        # 获取热门肥羊和板块数据
        hot_sheep = HotRankService.get_hot_sheep()
        hot_sectors = HotRankService.get_hot_sectors()
        
        # 调用AI服务（传入user_id、model_name和自定义提示词）
        model_name = request.model_name if request else None
        custom_prompt = request.custom_prompt if request else None
        recommendation = AIService.recommend_sheep(user_id, hot_sheep, hot_sectors, model_name, custom_prompt)
        
        # 保存到缓存
        AICacheRepository.set_cache("recommend", "recommend", recommendation)
        
        return {
            "recommendation": recommendation,
            "timestamp": datetime.now().isoformat(),
            "cached": False
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"AI推荐肥羊失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ai/analyze-sheep/{sheep_code}")
async def ai_analyze_sheep(
    sheep_code: str,
    request: AIRequest = AIRequest(),
    current_user: dict = Depends(check_api_key_available)
):
    """AI分析肥羊（带3小时缓存，开放给所有用户）"""
    try:
        from db.ai_cache_repository import AICacheRepository
        
        user_id = current_user.get("id")
        
        # 先检查缓存
        cached_result = AICacheRepository.get_cache(sheep_code, "analyze")
        if cached_result:
            logger.info(f"使用缓存的AI分析结果: {sheep_code}")
            # 获取肥羊名称
            hot_sheep = HotRankService.get_hot_sheep()
            sheep_info = next((s for s in hot_sheep if s.get("sheep_code") == sheep_code), None)
            sheep_name = sheep_info.get("sheep_name") if sheep_info else sheep_code
            
            return {
                "analysis": cached_result,
                "sheep_code": sheep_code,
                "sheep_name": sheep_name,
                "timestamp": datetime.now().isoformat(),
                "cached": True
            }
        
        # 缓存未命中，调用AI服务
        # 获取肥羊数据
        sheep_daily = SheepService.get_sheep_daily(sheep_code)
        capital_flow = SheepService.get_sheep_capital_flow(sheep_code)
        
        # 获取肥羊基本信息
        hot_sheep = HotRankService.get_hot_sheep()
        sheep_info = next((s for s in hot_sheep if s.get("sheep_code") == sheep_code), None)
        
        # sheep_daily 和 capital_flow 返回的是 List[Dict]，不是 {"data": [...]}
        kline_data = sheep_daily[-30:] if sheep_daily else []  # 最近30天
        money_flow_data = capital_flow[-30:] if capital_flow else []  # 最近30天
        
        sheep_data = {
            "current_price": sheep_info.get("current_price") if sheep_info else None,
            "change_pct": sheep_info.get("change_pct") if sheep_info else None,
            "volume": sheep_info.get("volume") if sheep_info else None,
            "sectors": sheep_info.get("sectors", []) if sheep_info else [],
            "kline": kline_data,
            "money_flow": money_flow_data
        }
        
        sheep_name = sheep_info.get("sheep_name") if sheep_info else sheep_code
        
        # 调用AI服务（传入user_id、model_name和自定义提示词）
        model_name = request.model_name if request and request.model_name else None
        custom_prompt = request.custom_prompt if request and request.custom_prompt else None
        analysis = AIService.analyze_sheep(user_id, sheep_code, sheep_name, sheep_data, model_name, custom_prompt)
        
        # 保存到缓存
        AICacheRepository.set_cache(sheep_code, "analyze", analysis)
        
        return {
            "analysis": analysis,
            "sheep_code": sheep_code,
            "sheep_name": sheep_name,
            "timestamp": datetime.now().isoformat(),
            "cached": False
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"AI分析肥羊失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== 模型老K API ==========

from services.alpha_model_t7_concept_flow import AlphaModelT7ConceptFlow
from services.backtest_engine import BacktestEngine
from db.strategy_recommendation_repository import StrategyRecommendationRepository

@app.get("/api/model-k/default-params")
async def get_default_params():
    """获取T7模型的默认参数配置（前端自动同步）"""
    try:
        return {
            "params": AlphaModelT7ConceptFlow.DEFAULT_PARAMS,
            "veto_conditions": AlphaModelT7ConceptFlow.VETO_CONDITIONS,
            "version": AlphaModelT7ConceptFlow().model_version
        }
    except Exception as e:
        logger.error(f"获取默认参数失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class BacktestRequest(BaseModel):
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    params: Dict  # 策略参数

class RecommendRequest(BaseModel):
    params: Dict  # 策略参数
    trade_date: Optional[str] = None  # YYYY-MM-DD，默认今天
    top_n: Optional[int] = None  # 返回肥羊数量限制，None表示返回所有符合条件的肥羊（按分数排序）
    # model_version removed - only T7 supported

@app.post("/api/model-k/backtest")
async def run_backtest(
    request: BacktestRequest,
    current_user: dict = Depends(get_current_user)
):
    """执行回测（时光机逻辑）"""
    try:
        start_date = datetime.strptime(request.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(request.end_date, "%Y-%m-%d").date()
        
        # 验证日期范围（最多6个月，避免超时）
        if (end_date - start_date).days > 180:
            raise HTTPException(
                status_code=400, 
                detail=f"回测日期范围不能超过6个月（180天）。当前范围：{(end_date - start_date).days}天。建议缩短日期范围或使用采样模式。"
            )
        
        # 计算预期的交易日数量（约）
        days_diff = (end_date - start_date).days
        estimated_trading_days = int(days_diff * 0.7)  # 约70%是交易日
        
        # 根据交易日数量动态设置超时时间
        if estimated_trading_days > 80:
            timeout_seconds = 600.0  # 10分钟（对于3个月以上的回测）
        elif estimated_trading_days > 40:
            timeout_seconds = 450.0  # 7.5分钟
        else:
            timeout_seconds = 300.0  # 5分钟
        
        logger.info(f"回测预计交易日数: {estimated_trading_days}天，超时时间: {timeout_seconds}秒")
        
        # 设置超时保护
        async def run_backtest_async():
            engine = BacktestEngine()
            return await asyncio.to_thread(
                engine.run_backtest, start_date, end_date, request.params
            )
        
        try:
            logger.info(f"开始回测: {start_date} 至 {end_date}")
            result = await asyncio.wait_for(run_backtest_async(), timeout=timeout_seconds)
            logger.info(f"回测完成: {result.get('success', False)}")
            return result
        except asyncio.TimeoutError:
            logger.error(f"回测超时（{timeout_seconds}秒）: {start_date} 至 {end_date}")
            raise HTTPException(
                status_code=504, 
                detail=f"回测超时（{int(timeout_seconds)}秒）。建议：1) 缩短日期范围（当前{days_diff}天，约{estimated_trading_days}个交易日） 2) 放宽筛选条件 3) 系统会自动启用采样模式加速回测"
            )
            
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {str(e)}")
    except Exception as e:
        logger.error(f"回测失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"回测失败: {str(e)}")

@app.post("/api/model-k/recommend")
async def get_recommendations(
    request: RecommendRequest,
    current_user: dict = Depends(get_current_user)
):
    """获取智能推荐（基于最新数据）"""
    import asyncio
    from datetime import date
    
    try:
        # 确定交易日期
        from etl.trade_date_adapter import TradeDateAdapter
        
        # 首先获取数据库中实际存在的最新交易日
        def get_latest_trade_date_in_db() -> Optional[date]:
            """获取数据库中实际存在的最新交易日"""
            try:
                with get_db() as db:
                    result = db.execute(text("SELECT MAX(trade_date) AS max_date FROM sheep_daily"))
                    row = result.fetchone()
                    if row and row[0]:
                        return row[0] if isinstance(row[0], date) else datetime.strptime(str(row[0]), "%Y-%m-%d").date()
            except Exception as e:
                logger.error(f"获取数据库最新交易日失败: {e}", exc_info=True)
            return None
        
        def get_latest_trade_date_before(target_date: date) -> Optional[date]:
            """获取数据库中不超过target_date的最新交易日"""
            try:
                with get_db() as db:
                    result = db.execute(
                        text("SELECT MAX(trade_date) AS max_date FROM sheep_daily WHERE trade_date <= :target_date"),
                        {"target_date": target_date}
                    )
                    row = result.fetchone()
                    if row and row[0]:
                        return row[0] if isinstance(row[0], date) else datetime.strptime(str(row[0]), "%Y-%m-%d").date()
            except Exception as e:
                logger.error(f"获取数据库交易日失败: {e}", exc_info=True)
            return None
        
        db_latest_date = get_latest_trade_date_in_db()
        if not db_latest_date:
            raise HTTPException(status_code=500, detail="无法获取数据库中的交易日期，请检查数据是否已导入")
        
        if request.trade_date:
            # 用户指定了日期，解析日期
            selected_date = datetime.strptime(request.trade_date, "%Y-%m-%d").date()
            
            # 验证日期不能是未来日期
            today = date.today()
            if selected_date > today:
                raise HTTPException(status_code=400, detail=f"不能选择未来日期，当前日期为 {today.isoformat()}")
            
            # 验证日期不能晚于数据库中的最新日期
            if selected_date > db_latest_date:
                raise HTTPException(
                    status_code=400, 
                    detail=f"选择的日期 {selected_date.isoformat()} 晚于数据库最新日期 {db_latest_date.isoformat()}，请选择更早的日期"
                )
            
            # 如果选择的日期是交易日，先检查数据库中是否有该日期的数据
            if TradeDateAdapter.is_trading_day(selected_date):
                # 检查数据库中是否有该日期的数据
                db_date_for_selected = get_latest_trade_date_before(selected_date)
                if db_date_for_selected and db_date_for_selected == selected_date:
                    trade_date = selected_date
                    logger.info(f"用户选择的日期 {selected_date} 是交易日且数据库中有数据，直接使用")
                else:
                    # 数据库中没有该日期的数据，使用数据库中不超过该日期的最新交易日
                    if db_date_for_selected:
                        trade_date = db_date_for_selected
                        logger.warning(f"用户选择的日期 {selected_date} 是交易日但数据库中没有数据，使用数据库中该日期之前的最新交易日: {trade_date}")
                    else:
                        # 数据库中没有任何该日期之前的数据，说明选择的日期太早
                        raise HTTPException(
                            status_code=400,
                            detail=f"选择的日期 {selected_date.isoformat()} 太早，数据库中最早有数据的日期是 {db_latest_date.isoformat()}，请选择更晚的日期"
                        )
            else:
                # 不是交易日，获取该日期之前的最近交易日
                trade_date = TradeDateAdapter.get_last_trading_day(selected_date)
                logger.info(f"用户选择的日期 {selected_date} 不是交易日，自动调整为最近的交易日: {trade_date}")
                
                # 确保调整后的日期在数据库中存在
                db_date_for_trade = get_latest_trade_date_before(trade_date)
                if db_date_for_trade:
                    if db_date_for_trade < trade_date:
                        trade_date = db_date_for_trade
                        logger.warning(f"调整后的交易日 {trade_date} 在数据库中没有数据，使用数据库中该日期之前的最新交易日: {db_date_for_trade}")
                else:
                    # 数据库中没有任何该日期之前的数据，说明选择的日期太早
                    raise HTTPException(
                        status_code=400,
                        detail=f"选择的日期 {selected_date.isoformat()} 对应的交易日 {trade_date.isoformat()} 在数据库中没有数据，数据库中最早有数据的日期是 {db_latest_date.isoformat()}，请选择更晚的日期"
                    )
        else:
            # 用户未指定日期，使用数据库中的最新交易日
            trade_date = db_latest_date
            logger.info(f"未指定日期，使用数据库最新交易日: {trade_date}")
            
            # 交易时段自动刷新当日概念资金流数据
            today = date.today()
            if TradeDateAdapter.is_trading_day(today) and TradeDateAdapter.is_trading_hours():
                try:
                    logger.info("检测到交易时段，自动刷新概念资金流数据...")
                    from services.concept_money_flow_service import ConceptMoneyFlowService
                    ConceptMoneyFlowService.collect_concept_money_flow_data(today)
                    logger.info("概念资金流数据刷新完成")
                except Exception as e:
                    logger.warning(f"刷新概念资金流数据失败（不影响推荐）: {e}")
        
        # 最终验证：确保trade_date不超过数据库最新日期
        if trade_date > db_latest_date:
            trade_date = db_latest_date
            logger.warning(f"最终交易日调整为数据库最新日期: {db_latest_date}")
        
        logger.info(f"T7概念资金双驱推荐最终使用交易日期: {trade_date} (数据库最新日期: {db_latest_date})")
        
        # 如果没有指定top_n，强制限制为20，避免返回过多数据和超时
        if request.top_n is None or request.top_n <= 0:
            request.top_n = 20
            logger.info(f"未指定top_n或top_n<=0，自动限制为20只，避免超时")
        
        # 根据top_n参数动态调整超时时间
        top_n_value = request.top_n
        if top_n_value <= 10:
            timeout_seconds = 300.0  # 5分钟（小批量）
        elif top_n_value <= 50:
            timeout_seconds = 450.0  # 7.5分钟（中批量）
        else:
            timeout_seconds = 600.0  # 10分钟（大批量）
        
        logger.info(f"T7推荐超时设置: {timeout_seconds}秒 (top_n: {request.top_n})")
        
        # 设置超时保护（因为模型包含多种计算，可能需要较长时间）
        async def run_model():
            # T7 概念资金双驱模型
            model = AlphaModelT7ConceptFlow()
            logger.info("使用 T7 概念资金双驱模型")
            
            return await asyncio.to_thread(
                model.run_full_pipeline, trade_date, request.params, request.top_n
            )
        
        # 使用asyncio.wait_for设置超时
        try:
            logger.info(f"开始执行T7概念资金双驱推荐，交易日期: {trade_date}, top_n: {request.top_n}, 超时: {timeout_seconds}秒")
            start_time = datetime.now()
            result = await asyncio.wait_for(run_model(), timeout=timeout_seconds)
            elapsed_time = (datetime.now() - start_time).total_seconds()
            # 处理新的返回值格式：(recommendations, diagnostic_info, metadata)
            if isinstance(result, tuple) and len(result) == 3:
                recommendations, diagnostic_info, metadata = result
            elif isinstance(result, tuple) and len(result) == 2:
                recommendations, diagnostic_info = result
                metadata = None
            else:
                recommendations = result
                diagnostic_info = None
                metadata = None
            logger.info(f"T7概念资金双驱推荐完成，耗时: {elapsed_time:.2f}秒，返回 {len(recommendations) if recommendations else 0} 条结果")
        except asyncio.TimeoutError:
            elapsed_time = timeout_seconds
            logger.error(f"T7概念资金双驱推荐超时（{timeout_seconds}秒），可能原因：肥羊数据量过大或筛选条件导致计算时间过长")
            raise HTTPException(
                status_code=504, 
                detail=f"推荐计算超时（{int(timeout_seconds)}秒）。建议：1) 限制返回数量(top_n参数，建议<=50) 2) 放宽筛选条件 3) 检查数据库性能 4) 稍后重试"
            )
        except Exception as e:
            logger.error(f"T-4推荐失败: {e}", exc_info=True)
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"T-4推荐异常详情:\n{error_detail}")
            raise HTTPException(status_code=500, detail=f"推荐计算失败: {str(e)}")
        
        if diagnostic_info:
            logger.info(f"诊断信息: {diagnostic_info}")
        
        # 保存推荐记录（关联到当前用户，立即保存）
        user_id = current_user.get('id', 0)
        if recommendations:
            strategy_version = "T7_Concept_Flow"
            
            logger.info(f"开始保存 {len(recommendations)} 条推荐记录到数据库（用户ID: {user_id}, 模型: {strategy_version}）")
            for rec in recommendations:
                try:
                    StrategyRecommendationRepository.create_recommendation(
                        user_id=user_id,
                        run_date=trade_date,
                        sheep_code=rec['sheep_code'],
                        sheep_name=rec['sheep_name'],
                        params_snapshot=request.params,
                        entry_price=rec['entry_price'],
                        ai_score=rec['ai_score'],
                        win_probability=rec['win_probability'],
                        reason_tags=rec['reason_tags'],
                        stop_loss_price=rec['stop_loss_price'],
                        strategy_version=strategy_version  # 保存模型版本
                    )
                except Exception as e:
                    logger.error(f"保存推荐记录失败 (sheep_code={rec.get('sheep_code')}): {e}")
            logger.info(f"推荐记录保存完成（模型: {strategy_version}）")
        else:
            logger.warning("推荐结果为空，不保存记录")
        
        response = {
            "trade_date": trade_date.isoformat(),
            "recommendations": recommendations or [],
            "count": len(recommendations) if recommendations else 0,
            "diagnostic_info": diagnostic_info if diagnostic_info else None,
            "metadata": metadata if metadata else None
        }
        
        # 调试日志：检查metadata是否正确返回
        if metadata:
            logger.info(f"返回metadata: market_regime={metadata.get('market_regime')}, funnel_data={metadata.get('funnel_data')}")
            # 确保funnel_data是字典格式
            if isinstance(metadata.get('funnel_data'), dict):
                logger.info(f"漏斗数据: total={metadata['funnel_data'].get('total')}, L1={metadata['funnel_data'].get('L1_pass')}, L2={metadata['funnel_data'].get('L2_pass')}, final={metadata['funnel_data'].get('final')}")
        else:
            logger.warning("metadata为空，可能未正确返回")
        logger.info(f"API返回: trade_date={response['trade_date']}, count={response['count']}")
        if diagnostic_info:
            logger.info(f"诊断信息: {diagnostic_info}")
        return response
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {str(e)}")
    except Exception as e:
        logger.error(f"获取推荐失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/model-k/history")
async def get_recommendation_history(
    run_date: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    model_version: Optional[str] = None,  # 按模型版本过滤
    current_user: dict = Depends(get_current_user)
):
    """获取推荐历史记录（自动验证早期推荐）"""
    try:
        from etl.trade_date_adapter import TradeDateAdapter
        from db.database import get_raw_connection
        import pandas as pd
        
        trade_date = None
        if run_date:
            trade_date = datetime.strptime(run_date, "%Y-%m-%d").date()
        
        # 只获取当前用户的推荐记录
        user_id = current_user.get('id', 0)
        recommendations = StrategyRecommendationRepository.get_recommendations(
            user_id=user_id,
            run_date=trade_date,
            limit=limit,
            offset=offset
        )
        
        # 如果指定了模型版本，过滤结果
        if model_version:
            if model_version.upper() == 'T7':
                recommendations = [r for r in recommendations if r.get('strategy_version', '').startswith('T7')]
        
        # 自动验证早期推荐（推荐日期距离今天超过5个交易日且未验证的）
        today = date.today()
        unverified_recommendations = [r for r in recommendations if not r.get('is_verified', False)]
        
        if unverified_recommendations:
            logger.info(f"发现 {len(unverified_recommendations)} 条未验证的推荐记录，开始自动验证...")
            
            for rec in unverified_recommendations:
                try:
                    rec_run_date = datetime.strptime(rec['run_date'], "%Y-%m-%d").date()
                    trading_days_between = TradeDateAdapter.get_trading_days_in_range(rec_run_date, today)
                    trading_days_count = len(trading_days_between)
                    
                    # 如果推荐日期距离今天已经超过5个交易日，可以验证
                    if trading_days_count >= 5:
                        sheep_code = rec['sheep_code']
                        entry_price = rec.get('entry_price')
                        
                        if not entry_price:
                            continue
                        
                        # 获取推荐日期后5个交易日的最高价和最终价格
                        date_5d_after = trading_days_between[4] if trading_days_count > 4 else trading_days_between[-1] if trading_days_between else None
                        
                        if date_5d_after:
                            with get_raw_connection() as conn:
                                # 获取5个交易日内的最高价
                                max_price_query = """
                                    SELECT MAX(high_price) as max_price
                                    FROM sheep_daily
                                    WHERE sheep_code = %s
                                      AND trade_date >= %s
                                      AND trade_date <= %s
                                """
                                max_price_df = pd.read_sql(
                                    max_price_query,
                                    conn,
                                    params=[sheep_code, rec_run_date, date_5d_after]
                                )
                                
                                # 获取第5个交易日的收盘价
                                final_price_query = """
                                    SELECT close_price
                                    FROM sheep_daily
                                    WHERE sheep_code = %s
                                      AND trade_date = %s
                                """
                                final_price_df = pd.read_sql(
                                    final_price_query,
                                    conn,
                                    params=[sheep_code, date_5d_after]
                                )
                                
                                if not max_price_df.empty and max_price_df.iloc[0]['max_price'] is not None:
                                    max_price = float(max_price_df.iloc[0]['max_price'])
                                    max_return_5d = ((max_price - entry_price) / entry_price) * 100
                                    
                                    if not final_price_df.empty and final_price_df.iloc[0]['close_price'] is not None:
                                        final_price = float(final_price_df.iloc[0]['close_price'])
                                        final_return_5d = ((final_price - entry_price) / entry_price) * 100
                                        
                                        # 判断结果：5日涨幅>5%为成功
                                        final_result = 'SUCCESS' if final_return_5d > 5 else 'FAIL'
                                        
                                        # 更新验证结果（仅更新当前用户的记录）
                                        StrategyRecommendationRepository.update_verification(
                                            user_id=user_id,
                                            run_date=rec_run_date,
                                            sheep_code=sheep_code,
                                            max_return_5d=max_return_5d,
                                            final_return_5d=final_return_5d,
                                            final_result=final_result
                                        )
                                        
                                        # 更新内存中的记录
                                        rec['is_verified'] = True
                                        rec['max_return_5d'] = max_return_5d
                                        rec['final_return_5d'] = final_return_5d
                                        rec['final_result'] = final_result
                                        
                                        logger.info(f"自动验证成功: {sheep_code} ({rec_run_date}), 5日涨幅: {final_return_5d:.2f}%")
                except Exception as e:
                    logger.error(f"自动验证失败 (sheep_code={rec.get('sheep_code')}, run_date={rec.get('run_date')}): {e}", exc_info=True)
                    continue
        
        return {
            "recommendations": recommendations,
            "count": len(recommendations)
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {str(e)}")
    except Exception as e:
        logger.error(f"获取历史记录失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/docs/{doc_name}")
async def get_documentation(doc_name: str):
    """获取文档内容"""
    import os
    from pathlib import Path
    
    try:
        # 文档目录路径
        docs_dir = Path(__file__).parent.parent.parent / "docs"
        doc_path = docs_dir / f"{doc_name}.md"
        
        # 安全检查：确保文件在docs目录内
        if not str(doc_path.resolve()).startswith(str(docs_dir.resolve())):
            raise HTTPException(status_code=400, detail="无效的文档名称")
        
        if not doc_path.exists():
            raise HTTPException(status_code=404, detail="文档不存在")
        
        # 读取文档内容
        with open(doc_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return {"content": content}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取文档失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="获取文档失败")

@app.delete("/api/model-k/history")
async def clear_recommendation_history(
    failed_only: bool = False,
    current_user: dict = Depends(get_current_user)
):
    """清空推荐历史记录（仅清空当前用户的记录）"""
    try:
        user_id = current_user.get('id', 0)
        if failed_only:
            count = StrategyRecommendationRepository.clear_failed_history(user_id)
            return {"message": f"已清空 {count} 条失败记录"}
        else:
            count = StrategyRecommendationRepository.clear_all_history(user_id)
            return {"message": f"已清空 {count} 条历史记录"}
    except Exception as e:
        logger.error(f"清空历史记录失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ========== 肥羊深度分析API ==========
from services.stock_analysis_service import StockAnalysisService

class StockAnalysisRequest(BaseModel):
    sheep_code: str
    trade_date: Optional[str] = None  # YYYY-MM-DD格式，默认最新交易日

@app.post("/api/sheep/{sheep_code}/deep-analysis")
async def deep_analysis_sheep(
    sheep_code: str,
    trade_date: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    肥羊深度分析 - 走势预判 + 形态识别 + 止盈止损
    
    返回：
    - predictions: 3日/5日/10日走势预判
    - pattern: 技术形态识别
    - stop_levels: 止盈止损位
    - factors: 技术因子详情
    - assessment: 综合评估和操作建议
    """
    try:
        # 解析日期
        analysis_date = None
        if trade_date:
            try:
                analysis_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(status_code=400, detail="日期格式错误，请使用YYYY-MM-DD格式")
        else:
            # 使用数据库中最新的交易日
            with get_db() as db:
                result = db.execute(text("""
                    SELECT MAX(trade_date) as max_date 
                    FROM sheep_daily 
                    WHERE sheep_code = :code
                """), {"code": sheep_code})
                row = result.fetchone()
                if row and row[0]:
                    analysis_date = row[0] if isinstance(row[0], date) else datetime.strptime(str(row[0]), "%Y-%m-%d").date()
                else:
                    raise HTTPException(status_code=404, detail=f"未找到肥羊 {sheep_code} 的数据")
        
        logger.info(f"开始深度分析: {sheep_code}, 交易日期: {analysis_date}")
        
        # 执行深度分析
        result = StockAnalysisService.analyze_stock(sheep_code, analysis_date)
        
        if not result.get('success'):
            raise HTTPException(
                status_code=400, 
                detail=result.get('message', '分析失败')
            )
        
        logger.info(f"深度分析完成: {sheep_code}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"深度分析失败: {sheep_code}, 错误: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"分析失败: {str(e)}")

@app.get("/api/sheep/{sheep_code}/prediction")
async def get_sheep_prediction(
    sheep_code: str,
    current_user: dict = Depends(get_current_user)
):
    """
    获取肥羊走势预判 (简化版API，仅返回预测结果)
    """
    try:
        result = StockAnalysisService.analyze_stock(sheep_code)
        
        if not result.get('success'):
            raise HTTPException(status_code=400, detail=result.get('message', '预测失败'))
        
        return {
            'sheep_code': sheep_code,
            'trade_date': result.get('trade_date'),
            'current_price': result.get('current_price'),
            'predictions': result.get('predictions'),
            'pattern': result.get('pattern'),
            'assessment': result.get('assessment')
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取预测失败: {sheep_code}, 错误: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ========== 下个交易日预测API ==========
from services.next_day_prediction_service import NextDayPredictionService

@app.get("/api/next-day-prediction")
async def get_next_day_prediction(current_user: dict = Depends(get_current_user)): 
    """
    获取下个交易日预测（板块热点+个股推荐）
    
    返回：
    - success: 是否成功
    - target_date: 预测目标日期
    - description: 预测描述文本
    - sector_predictions: 板块预测列表
    - stock_recommendations: 个股推荐列表
    """
    try:
        result = NextDayPredictionService.get_latest_prediction()
        return result
    except Exception as e:
        logger.error(f"获取下个交易日预测失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/next-day-prediction/refresh")
async def refresh_next_day_prediction(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(is_admin_user)
):
    """
    手动刷新下个交易日预测（仅管理员，后台异步执行）
    """
    try:
        def run_prediction():
            try:
                result = NextDayPredictionService.generate_prediction(force=True)
                logger.info(f"手动刷新预测完成: success={result.get('success')}")
            except Exception as e:
                logger.error(f"手动刷新预测失败: {e}", exc_info=True)
        
        background_tasks.add_task(run_prediction)
        return {"message": "预测刷新任务已启动，正在后台执行", "status": "running"}
    except Exception as e:
        logger.error(f"启动预测刷新任务失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
