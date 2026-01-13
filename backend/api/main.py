"""
FastAPI主应用 - 重构版
"""
import logging
from contextlib import asynccontextmanager
from typing import Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db.database import get_db
from auth.auth import authenticate_user, create_access_token, get_current_user, get_password_hash
from services.data_collection_service import DataCollectionService
from services.stock_service import StockService
from services.hot_rank_service import HotRankService
from services.concept_service import ConceptService
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

app = FastAPI(title="股票数据API", version="3.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

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
    try:
        ip_address = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")
        user = authenticate_user(login_data.username, login_data.password, ip_address, user_agent)
        
        if not user:
            raise HTTPException(status_code=401, detail="用户名或密码错误")
        
        access_token = create_access_token(data={"sub": user["username"]})
        return LoginResponse(access_token=access_token, username=user["username"])
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"登录失败: {e}")
        raise HTTPException(status_code=500, detail="登录失败")

class UserInfoResponse(BaseModel):
    id: int
    username: str
    can_use_ai_recommend: bool
    is_admin: bool

@app.get("/api/auth/me", response_model=UserInfoResponse)
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """获取当前用户信息（包含AI权限）"""
    username = current_user["username"]
    is_admin = username == "admin"
    can_use_ai_recommend = True if is_admin else UserService.can_user_use_ai_recommend(username)
    
    return UserInfoResponse(
        id=current_user["id"],
        username=username,
        can_use_ai_recommend=can_use_ai_recommend,
        is_admin=is_admin
    )

def is_admin_user(current_user: dict = Depends(get_current_user)) -> dict:
    """检查是否为admin用户"""
    if current_user.get("username") != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可访问此功能")
    return current_user

def can_use_ai_recommend(current_user: dict = Depends(get_current_user)) -> dict:
    """检查用户是否可以使用AI推荐功能"""
    username = current_user.get("username")
    if not UserService.can_user_use_ai_recommend(username):
        raise HTTPException(status_code=403, detail="您没有权限使用AI推荐功能，请联系管理员")
    return current_user

@app.post("/api/auth/logout")
async def logout(current_user: dict = Depends(get_current_user)):
    return {"message": "登出成功"}

# ========== 股票相关API ==========
@app.get("/api/stocks/search")
async def search_stocks(q: str = ""):
    """搜索股票"""
    if not q or not q.strip():
        return {"stocks": []}
    
    try:
        stocks = StockService.search_stocks(q.strip())
        return {"stocks": stocks}
    except Exception as e:
        logger.error(f"搜索失败: {e}")
        raise HTTPException(status_code=500, detail="搜索服务暂时不可用")

@app.get("/api/stocks/{stock_code}/daily")
async def get_stock_daily(stock_code: str, current_user: dict = Depends(get_current_user)):
    """获取股票日K数据"""
    try:
        data = StockService.get_stock_daily(stock_code)
        return {"data": data}
    except Exception as e:
        logger.error(f"获取股票日K数据失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stocks/{stock_code}/capital-flow")
async def get_stock_capital_flow(stock_code: str, current_user: dict = Depends(get_current_user)):
    """获取资金流入数据"""
    try:
        data = StockService.get_stock_capital_flow(stock_code)
        return {"data": data}
    except Exception as e:
        logger.error(f"获取资金流数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== 热度榜相关API ==========
@app.get("/api/hot-stocks")
async def get_hot_stocks(source: Optional[str] = None):
    """获取热度榜"""
    try:
        stocks = HotRankService.get_hot_stocks(source)
        return {"stocks": stocks}
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

@app.post("/api/refresh-hot-stocks")
async def refresh_hot_stocks(current_user: dict = Depends(get_current_user)):
    """手动刷新热度榜数据"""
    try:
        service = DataCollectionService()
        service.collect_hot_rank_data()
        return {"message": "热度榜数据刷新成功"}
    except Exception as e:
        logger.error(f"刷新热度榜失败: {e}")
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

@app.get("/api/sectors/{sector_name}/stocks")
async def get_sector_stocks(sector_name: str, current_user: dict = Depends(get_current_user)):
    """获取板块股票"""
    try:
        stocks = ConceptService.get_sector_stocks(sector_name)
        return {"stocks": stocks}
    except Exception as e:
        logger.error(f"获取板块股票失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== 数据采集管理API ==========
@app.post("/api/admin/collect-all-data")
async def collect_all_data(current_user: dict = Depends(get_current_user)):
    """
    手动触发全量数据采集（临时接口，用于首次运行）
    采集顺序：
    1. 概念板块数据
    2. 股票日K数据
    3. 资金流向数据（仅在交易日）
    4. 热度榜数据
    """
    try:
        logger.info("手动触发全量数据采集...")
        service = DataCollectionService()
        
        results = {
            "concept_data": False,
            "stock_daily_data": False,
            "money_flow_data": False,
            "hot_rank_data": False,
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
        
        # 2. 采集股票日K数据
        try:
            logger.info("开始采集股票日K数据...")
            service.collect_stock_daily_data()
            results["stock_daily_data"] = True
            results["messages"].append("股票日K数据采集完成")
        except Exception as e:
            logger.error(f"股票日K数据采集失败: {e}")
            results["messages"].append(f"股票日K数据采集失败: {str(e)}")
        
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
        
        return {
            "message": "全量数据采集任务已启动",
            "results": results
        }
        
    except Exception as e:
        logger.error(f"全量数据采集失败: {e}", exc_info=True)
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
    can_use_ai_recommend: bool = False

class UserUpdate(BaseModel):
    password: Optional[str] = None
    is_active: Optional[bool] = None
    can_use_ai_recommend: Optional[bool] = None

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
            user.password,
            user.can_use_ai_recommend
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
            user.is_active,
            user.can_use_ai_recommend
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
async def get_ai_prompts(current_user: dict = Depends(is_admin_user)):
    """获取所有Prompt模板"""
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

# ========== AI推荐和分析API（仅admin）==========
@app.post("/api/ai/recommend-stocks")
async def ai_recommend_stocks(current_user: dict = Depends(can_use_ai_recommend)):
    """AI推荐股票"""
    try:
        # 获取热门股票和板块数据
        hot_stocks = HotRankService.get_hot_stocks()
        hot_sectors = HotRankService.get_hot_sectors()
        
        # 调用AI服务
        recommendation = AIService.recommend_stocks(hot_stocks, hot_sectors)
        
        return {
            "recommendation": recommendation,
            "timestamp": datetime.now().isoformat()
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"AI推荐股票失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ai/analyze-stock/{stock_code}")
async def ai_analyze_stock(
    stock_code: str,
    current_user: dict = Depends(can_use_ai_recommend)
):
    """AI分析股票"""
    try:
        # 获取股票数据
        stock_daily = StockService.get_stock_daily(stock_code)
        capital_flow = StockService.get_stock_capital_flow(stock_code)
        
        # 获取股票基本信息
        hot_stocks = HotRankService.get_hot_stocks()
        stock_info = next((s for s in hot_stocks if s.get("stock_code") == stock_code), None)
        
        # stock_daily 和 capital_flow 返回的是 List[Dict]，不是 {"data": [...]}
        kline_data = stock_daily[-30:] if stock_daily else []  # 最近30天
        money_flow_data = capital_flow[-30:] if capital_flow else []  # 最近30天
        
        stock_data = {
            "current_price": stock_info.get("current_price") if stock_info else None,
            "change_pct": stock_info.get("change_pct") if stock_info else None,
            "volume": stock_info.get("volume") if stock_info else None,
            "sectors": stock_info.get("sectors", []) if stock_info else [],
            "kline": kline_data,
            "money_flow": money_flow_data
        }
        
        stock_name = stock_info.get("stock_name") if stock_info else stock_code
        
        # 调用AI服务
        analysis = AIService.analyze_stock(stock_code, stock_name, stock_data)
        
        return {
            "analysis": analysis,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "timestamp": datetime.now().isoformat()
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"AI分析股票失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
