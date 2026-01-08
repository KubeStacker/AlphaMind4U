"""
FastAPI主应用
"""
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from database import get_db
from datetime import datetime, timedelta
from typing import List, Optional
from pydantic import BaseModel
import logging
from scheduler import start_scheduler
from auth import authenticate_user, create_access_token, get_current_user, get_password_hash
from fastapi.security import HTTPBearer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="股票数据API", version="1.0.0")

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 安全认证
security = HTTPBearer()

# 启动定时任务
scheduler = start_scheduler()

# 数据模型
class StockDailyData(BaseModel):
    trade_date: str
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    volume: int
    amount: float
    ma5: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None
    ma30: Optional[float] = None
    ma60: Optional[float] = None

class CapitalFlowData(BaseModel):
    trade_date: str
    main_net_inflow: Optional[float] = None
    super_large_inflow: Optional[float] = None
    large_inflow: Optional[float] = None

class SectorDailyData(BaseModel):
    trade_date: str
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    volume: int
    amount: float
    change_pct: Optional[float] = None

class HotStock(BaseModel):
    stock_code: str
    stock_name: str
    source: str
    rank: int
    consecutive_days: int = 0

class SectorInfo(BaseModel):
    sector_code: str
    sector_name: str
    hot_count: int = 0

@app.get("/")
async def root():
    return {"message": "股票数据API服务运行中"}

# 登录相关模型
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

# 登录API（不需要认证）
@app.post("/api/auth/login", response_model=LoginResponse)
async def login(login_data: LoginRequest, request: Request):
    """用户登录"""
    try:
        # 获取客户端IP和User-Agent
        ip_address = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")
        
        # 验证用户
        user = authenticate_user(
            login_data.username, 
            login_data.password,
            ip_address=ip_address,
            user_agent=user_agent
        )
        
        if not user:
            raise HTTPException(
                status_code=401,
                detail="用户名或密码错误"
            )
        
        # 创建访问令牌
        access_token = create_access_token(data={"sub": user["username"]})
        
        return LoginResponse(
            access_token=access_token,
            username=user["username"]
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"登录失败: {e}")
        raise HTTPException(status_code=500, detail="登录失败")

# 获取当前用户信息（需要认证）
@app.get("/api/auth/me", response_model=UserInfo)
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """获取当前登录用户信息"""
    return UserInfo(
        id=current_user["id"],
        username=current_user["username"]
    )

# 登出API（客户端删除token即可）
@app.post("/api/auth/logout")
async def logout(current_user: dict = Depends(get_current_user)):
    """用户登出"""
    return {"message": "登出成功"}

@app.get("/api/stocks/search")
async def search_stocks(q: str = ""):
    """搜索股票（支持多种格式：300001, SZ300001, SH600000等）"""
    try:
        with get_db() as db:
            # 标准化搜索关键词
            search_key = q.strip().upper()
            # 移除SZ/SH前缀用于搜索
            if search_key.startswith('SZ') or search_key.startswith('SH'):
                search_key = search_key[2:]
            
            # 搜索股票代码或名称
            query = text("""
                SELECT stock_code, stock_name, sector 
                FROM stock_info 
                WHERE stock_name LIKE :q OR stock_code LIKE :q OR stock_code LIKE :q2
                LIMIT 20
            """)
            result = db.execute(query, {
                "q": f"%{search_key}%",
                "q2": f"%{q.strip()}%"
            })
            stocks = [{"code": row[0], "name": row[1], "sector": row[2]} for row in result]
            return {"stocks": stocks}
    except Exception as e:
        logger.error(f"搜索股票失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stocks/{stock_code}/daily")
async def get_stock_daily(stock_code: str, current_user: dict = Depends(get_current_user)):
    """获取股票最近3个月的日K数据（支持多种格式：300001, SZ300001等）"""
    try:
        # 标准化股票代码
        normalized_code = stock_code.strip().upper()
        if normalized_code.startswith('SZ') or normalized_code.startswith('SH'):
            normalized_code = normalized_code[2:]
        
        with get_db() as db:
            # 先尝试直接匹配，如果没找到再尝试带前缀的格式
            query = text("""
                SELECT trade_date, open_price, close_price, high_price, low_price,
                       volume, amount, ma5, ma10, ma20, ma30, ma60
                FROM stock_daily
                WHERE stock_code = :code OR stock_code = :code_sz OR stock_code = :code_sh
                ORDER BY trade_date ASC
            """)
            result = db.execute(query, {
                "code": normalized_code,
                "code_sz": f"SZ{normalized_code}",
                "code_sh": f"SH{normalized_code}"
            })
            
            # 如果没找到数据，尝试从stock_info表获取正确的代码格式
            if result.rowcount == 0:
                info_query = text("""
                    SELECT stock_code FROM stock_info 
                    WHERE stock_code = :code OR stock_code = :code_sz OR stock_code = :code_sh
                    LIMIT 1
                """)
                info_result = db.execute(info_query, {
                    "code": normalized_code,
                    "code_sz": f"SZ{normalized_code}",
                    "code_sh": f"SH{normalized_code}"
                })
                info_row = info_result.fetchone()
                if info_row:
                    actual_code = info_row[0]
                    query = text("""
                        SELECT trade_date, open_price, close_price, high_price, low_price,
                               volume, amount, ma5, ma10, ma20, ma30, ma60
                        FROM stock_daily
                        WHERE stock_code = :actual_code
                        ORDER BY trade_date ASC
                    """)
                    result = db.execute(query, {"actual_code": actual_code})
            data = []
            for row in result:
                data.append({
                    "trade_date": row[0].strftime("%Y-%m-%d") if row[0] else None,
                    "open_price": float(row[1]) if row[1] else 0,
                    "close_price": float(row[2]) if row[2] else 0,
                    "high_price": float(row[3]) if row[3] else 0,
                    "low_price": float(row[4]) if row[4] else 0,
                    "volume": int(row[5]) if row[5] else 0,
                    "amount": float(row[6]) if row[6] else 0,
                    "ma5": float(row[7]) if row[7] else None,
                    "ma10": float(row[8]) if row[8] else None,
                    "ma20": float(row[9]) if row[9] else None,
                    "ma30": float(row[10]) if row[10] else None,
                    "ma60": float(row[11]) if row[11] else None,
                })
            return {"data": data}
    except Exception as e:
        logger.error(f"获取股票日K数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stocks/{stock_code}/capital-flow")
async def get_stock_capital_flow(stock_code: str, current_user: dict = Depends(get_current_user)):
    """获取股票最近3个月的主力资金流入数据（支持多种格式）"""
    try:
        # 标准化股票代码
        normalized_code = stock_code.strip().upper()
        if normalized_code.startswith('SZ') or normalized_code.startswith('SH'):
            normalized_code = normalized_code[2:]
        
        with get_db() as db:
            query = text("""
                SELECT trade_date, main_net_inflow, super_large_inflow, large_inflow
                FROM stock_capital_flow
                WHERE stock_code = :code OR stock_code = :code_sz OR stock_code = :code_sh
                ORDER BY trade_date ASC
            """)
            result = db.execute(query, {
                "code": normalized_code,
                "code_sz": f"SZ{normalized_code}",
                "code_sh": f"SH{normalized_code}"
            })
            data = []
            for row in result:
                data.append({
                    "trade_date": row[0].strftime("%Y-%m-%d") if row[0] else None,
                    "main_net_inflow": float(row[1]) if row[1] else 0,
                    "super_large_inflow": float(row[2]) if row[2] else 0,
                    "large_inflow": float(row[3]) if row[3] else 0,
                })
            return {"data": data}
    except Exception as e:
        logger.error(f"获取资金流数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hot-stocks")
async def get_hot_stocks(source: Optional[str] = None, current_user: dict = Depends(get_current_user)):
    """获取热度榜数据，包含每只股票的所属板块（最多3个）"""
    try:
        with get_db() as db:
            if source:
                query = text("""
                    SELECT hs.stock_code, hs.stock_name, hs.source, hs.`rank`, hs.trade_date,
                           COALESCE(hs.volume, COALESCE(sd.volume, 0)) as volume,
                           si.sector
                    FROM hot_stocks hs
                    LEFT JOIN stock_daily sd ON hs.stock_code = sd.stock_code 
                        AND sd.trade_date = hs.trade_date
                    LEFT JOIN stock_info si ON hs.stock_code = si.stock_code
                    WHERE hs.source = :source AND hs.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    ORDER BY hs.`rank` ASC
                    LIMIT 100
                """)
                result = db.execute(query, {"source": source})
            else:
                query = text("""
                    SELECT hs.stock_code, hs.stock_name, hs.source, hs.`rank`, hs.trade_date,
                           COALESCE(hs.volume, COALESCE(sd.volume, 0)) as volume,
                           si.sector
                    FROM hot_stocks hs
                    LEFT JOIN stock_daily sd ON hs.stock_code = sd.stock_code 
                        AND sd.trade_date = hs.trade_date
                    LEFT JOIN stock_info si ON hs.stock_code = si.stock_code
                    WHERE hs.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    ORDER BY hs.source, hs.`rank` ASC
                    LIMIT 200
                """)
                result = db.execute(query)
            
            stocks = []
            for row in result:
                # 计算连续上榜天数
                consecutive_query = text("""
                    SELECT COUNT(DISTINCT trade_date) as days
                    FROM hot_stocks
                    WHERE stock_code = :code AND source = :source
                    AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                """)
                consecutive_result = db.execute(consecutive_query, {"code": row[0], "source": row[2]})
                consecutive_days = consecutive_result.fetchone()[0] if consecutive_result else 0
                
                # 获取当前价格
                price_query = text("""
                    SELECT close_price FROM stock_daily 
                    WHERE stock_code = :code 
                    ORDER BY trade_date DESC LIMIT 1
                """)
                price_result = db.execute(price_query, {"code": row[0]})
                price_row = price_result.fetchone()
                current_price = float(price_row[0]) if price_row and price_row[0] else None
                
                # 获取当日涨幅
                change_query = text("""
                    SELECT change_pct FROM gainers 
                    WHERE stock_code = :code AND trade_date = CURDATE()
                    LIMIT 1
                """)
                change_result = db.execute(change_query, {"code": row[0]})
                change_row = change_result.fetchone()
                change_pct = float(change_row[0]) if change_row and change_row[0] else None
                
                # 获取7天平均涨幅
                avg_change_query = text("""
                    SELECT AVG(change_pct) FROM gainers 
                    WHERE stock_code = :code 
                    AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                """)
                avg_change_result = db.execute(avg_change_query, {"code": row[0]})
                avg_change_row = avg_change_result.fetchone()
                avg_change_7d = float(avg_change_row[0]) if avg_change_row and avg_change_row[0] else None
                
                # 获取所属板块（最多3个）
                sectors = []
                if row[6]:  # sector字段
                    sectors.append(row[6])
                
                stocks.append({
                    "stock_code": row[0],
                    "stock_name": row[1],
                    "source": row[2],
                    "rank": row[3],
                    "trade_date": row[4].strftime("%Y-%m-%d") if row[4] else None,
                    "consecutive_days": consecutive_days,
                    "volume": int(row[5]) if row[5] else 0,
                    "sectors": sectors[:3],  # 最多返回3个板块
                    "current_price": current_price,
                    "change_pct": change_pct,
                    "avg_change_7d": avg_change_7d
                })
            
            return {"stocks": stocks}
    except Exception as e:
        logger.error(f"获取热度榜失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hot-sectors")
async def get_hot_sectors(current_user: dict = Depends(get_current_user)):
    """获取最热门的5个板块（基于热度榜前100和板块数据），包含每个板块的前10只热门股票"""
    try:
        with get_db() as db:
            # 方法1: 从热度榜股票的板块信息中提取
            query1 = text("""
                SELECT DISTINCT si.sector, COUNT(*) as hot_count
                FROM hot_stocks hs
                JOIN stock_info si ON hs.stock_code = si.stock_code
                WHERE hs.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                AND si.sector IS NOT NULL AND si.sector != ''
                GROUP BY si.sector
                ORDER BY hot_count DESC
                LIMIT 5
            """)
            result1 = db.execute(query1)
            sectors_from_stocks = []
            for row in result1:
                sectors_from_stocks.append({
                    "sector_name": row[0],
                    "hot_count": row[1]
                })
            
            # 方法2: 如果股票板块信息不足，从板块数据表中获取热门板块
            if len(sectors_from_stocks) < 5:
                try:
                    query2 = text("""
                        SELECT si.sector_name, 
                               AVG(sd.change_pct) as avg_change,
                               COUNT(*) as data_count
                        FROM sector_info si
                        JOIN sector_daily sd ON si.sector_code = sd.sector_code
                        WHERE sd.trade_date >= DATE_SUB(CURDATE(), INTERVAL 10 DAY)
                        GROUP BY si.sector_name
                        HAVING data_count >= 3
                        ORDER BY avg_change DESC
                        LIMIT 5
                    """)
                    result2 = db.execute(query2)
                    existing_names = {s['sector_name'] for s in sectors_from_stocks}
                    for row in result2:
                        # 避免重复
                        if row[0] not in existing_names and len(sectors_from_stocks) < 5:
                            sectors_from_stocks.append({
                                "sector_name": row[0],
                                "hot_count": int(row[2])
                            })
                except Exception as e:
                    logger.warning(f"从板块数据补充热门板块失败: {e}")
            
            # 如果还是不足，使用板块涨跌幅排序
            if len(sectors_from_stocks) < 5:
                query3 = text("""
                    SELECT DISTINCT si.sector_name, 
                           AVG(sd.change_pct) as avg_change
                    FROM sector_info si
                    JOIN sector_daily sd ON si.sector_code = sd.sector_code
                    WHERE sd.trade_date >= DATE_SUB(CURDATE(), INTERVAL 10 DAY)
                    GROUP BY si.sector_name
                    ORDER BY avg_change DESC
                    LIMIT 5
                """)
                result3 = db.execute(query3)
                for row in result3:
                    if not any(s['sector_name'] == row[0] for s in sectors_from_stocks):
                        sectors_from_stocks.append({
                            "sector_name": row[0],
                            "hot_count": 0
                        })
            
            # 为每个板块获取前10只热门股票
            sectors_with_stocks = []
            for sector in sectors_from_stocks[:5]:
                sector_name = sector["sector_name"]
                # 获取该板块下在热度榜中的股票，按排名排序
                stocks_query = text("""
                    SELECT hs.stock_code, hs.stock_name, 
                           MIN(hs.`rank`) as min_rank,
                           COUNT(DISTINCT hs.trade_date) as consecutive_days
                    FROM hot_stocks hs
                    JOIN stock_info si ON hs.stock_code = si.stock_code
                    WHERE si.sector = :sector
                    AND hs.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    GROUP BY hs.stock_code, hs.stock_name
                    ORDER BY min_rank ASC
                    LIMIT 10
                """)
                stocks_result = db.execute(stocks_query, {"sector": sector_name})
                hot_stocks = []
                for stock_row in stocks_result:
                    hot_stocks.append({
                        "stock_code": stock_row[0],
                        "stock_name": stock_row[1],
                        "rank": stock_row[2],
                        "consecutive_days": stock_row[3]
                    })
                
                sectors_with_stocks.append({
                    "sector_name": sector_name,
                    "hot_count": sector["hot_count"],
                    "hot_stocks": hot_stocks
                })
            
            return {"sectors": sectors_with_stocks}
    except Exception as e:
        logger.error(f"获取热门板块失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sectors/{sector_name}/daily")
async def get_sector_daily(sector_name: str, current_user: dict = Depends(get_current_user)):
    """获取板块最近10天的K线数据"""
    try:
        with get_db() as db:
            query = text("""
                SELECT trade_date, open_price, close_price, high_price, low_price,
                       volume, amount, change_pct
                FROM sector_daily
                WHERE sector_code = :sector
                ORDER BY trade_date ASC
                LIMIT 10
            """)
            result = db.execute(query, {"sector": sector_name})
            data = []
            for row in result:
                data.append({
                    "trade_date": row[0].strftime("%Y-%m-%d") if row[0] else None,
                    "open_price": float(row[1]) if row[1] else 0,
                    "close_price": float(row[2]) if row[2] else 0,
                    "high_price": float(row[3]) if row[3] else 0,
                    "low_price": float(row[4]) if row[4] else 0,
                    "volume": int(row[5]) if row[5] else 0,
                    "amount": float(row[6]) if row[6] else 0,
                    "change_pct": float(row[7]) if row[7] else 0,
                })
            return {"data": data}
    except Exception as e:
        logger.error(f"获取板块K线数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sectors/{sector_name}/stocks")
async def get_sector_stocks(sector_name: str, current_user: dict = Depends(get_current_user)):
    """获取板块下的股票（按热度排序）"""
    try:
        with get_db() as db:
            query = text("""
                SELECT si.stock_code, si.stock_name, 
                       COALESCE(MAX(hs.`rank`), 999) as min_rank,
                       COUNT(DISTINCT hs.trade_date) as consecutive_days
                FROM stock_info si
                LEFT JOIN hot_stocks hs ON si.stock_code = hs.stock_code
                WHERE si.sector = :sector
                AND (hs.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) OR hs.trade_date IS NULL)
                GROUP BY si.stock_code, si.stock_name
                ORDER BY min_rank ASC, consecutive_days DESC
            """)
            result = db.execute(query, {"sector": sector_name})
            stocks = []
            for row in result:
                stocks.append({
                    "stock_code": row[0],
                    "stock_name": row[1],
                    "rank": row[2] if row[2] != 999 else None,
                    "consecutive_days": row[3]
                })
            return {"stocks": stocks}
    except Exception as e:
        logger.error(f"获取板块股票失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/recommendations")
async def get_recommendations(current_user: dict = Depends(get_current_user)):
    """获取推荐数据（热门板块和股票，资金持续流入股票）"""
    try:
        with get_db() as db:
            # 获取热门板块（多种策略组合）
            # 策略1: 从热度榜股票的板块信息中提取
            hot_sectors_query1 = text("""
                SELECT si.sector, COUNT(*) as hot_count
                FROM hot_stocks hs
                JOIN stock_info si ON hs.stock_code = si.stock_code
                WHERE hs.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                AND si.sector IS NOT NULL AND si.sector != ''
                GROUP BY si.sector
                ORDER BY hot_count DESC
                LIMIT 5
            """)
            hot_sectors_result1 = db.execute(hot_sectors_query1)
            hot_sectors = [{"sector_name": row[0], "hot_count": row[1]} for row in hot_sectors_result1]
            
            # 策略2: 如果不足5个，从板块涨跌幅数据中补充
            if len(hot_sectors) < 5:
                try:
                    hot_sectors_query2 = text("""
                        SELECT si.sector_name, 
                               AVG(sd.change_pct) as avg_change,
                               COUNT(*) as data_count
                        FROM sector_info si
                        JOIN sector_daily sd ON si.sector_code = sd.sector_code
                        WHERE sd.trade_date >= DATE_SUB(CURDATE(), INTERVAL 10 DAY)
                        GROUP BY si.sector_name
                        HAVING data_count >= 3
                        ORDER BY avg_change DESC
                        LIMIT 5
                    """)
                    hot_sectors_result2 = db.execute(hot_sectors_query2)
                    existing_names = {s['sector_name'] for s in hot_sectors}
                    for row in hot_sectors_result2:
                        if row[0] not in existing_names and len(hot_sectors) < 5:
                            hot_sectors.append({
                                "sector_name": row[0],
                                "hot_count": int(row[2])
                            })
                except Exception as e:
                    logger.warning(f"从板块数据补充热门板块失败: {e}")
            
            # 为每个热门板块获取前10只热门股票
            hot_sectors_with_stocks = []
            for sector in hot_sectors[:5]:
                sector_name = sector["sector_name"]
                # 获取该板块下在热度榜中的股票，按排名排序
                stocks_query = text("""
                    SELECT hs.stock_code, hs.stock_name, 
                           MIN(hs.`rank`) as min_rank,
                           COUNT(DISTINCT hs.trade_date) as consecutive_days
                    FROM hot_stocks hs
                    JOIN stock_info si ON hs.stock_code = si.stock_code
                    WHERE si.sector = :sector
                    AND hs.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    GROUP BY hs.stock_code, hs.stock_name
                    ORDER BY min_rank ASC
                    LIMIT 10
                """)
                stocks_result = db.execute(stocks_query, {"sector": sector_name})
                hot_stocks_list = []
                for stock_row in stocks_result:
                    hot_stocks_list.append({
                        "stock_code": stock_row[0],
                        "stock_name": stock_row[1],
                        "rank": stock_row[2],
                        "consecutive_days": stock_row[3]
                    })
                
                hot_sectors_with_stocks.append({
                    "sector_name": sector_name,
                    "hot_count": sector["hot_count"],
                    "hot_stocks": hot_stocks_list
                })
            
            # 获取热门股票（结合涨幅榜和热度榜）
            # 策略1: 从涨幅榜获取（如果今天有数据）
            hot_stocks_query1 = text("""
                SELECT DISTINCT g.stock_code, g.stock_name, 
                       g.change_pct,
                       COALESCE(MIN(hs.`rank`), 999) as min_rank,
                       si.sector,
                       (SELECT close_price FROM stock_daily 
                        WHERE stock_code = g.stock_code 
                        ORDER BY trade_date DESC LIMIT 1) as current_price,
                       (SELECT AVG(change_pct) FROM gainers 
                        WHERE stock_code = g.stock_code 
                        AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)) as avg_change_7d,
                       (SELECT COUNT(*) FROM gainers 
                        WHERE stock_code = g.stock_code 
                        AND change_pct > 9.5 
                        AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)) as consecutive_boards
                FROM gainers g
                LEFT JOIN stock_info si ON g.stock_code = si.stock_code
                LEFT JOIN hot_stocks hs ON g.stock_code = hs.stock_code 
                    AND hs.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                WHERE g.trade_date = CURDATE()
                GROUP BY g.stock_code, g.stock_name, g.change_pct, si.sector
                ORDER BY g.change_pct DESC
                LIMIT 20
            """)
            hot_stocks_result1 = db.execute(hot_stocks_query1)
            hot_stocks = []
            existing_codes = set()
            for row in hot_stocks_result1:
                if row[0] not in existing_codes:
                    # 获取当前价格
                    price_query = text("""
                        SELECT close_price FROM stock_daily 
                        WHERE stock_code = :code 
                        ORDER BY trade_date DESC LIMIT 1
                    """)
                    price_result = db.execute(price_query, {"code": row[0]})
                    price_row = price_result.fetchone()
                    current_price = float(price_row[0]) if price_row and price_row[0] else None
                    
                    # 获取7天平均涨幅
                    avg_query = text("""
                        SELECT AVG(change_pct) FROM gainers 
                        WHERE stock_code = :code 
                        AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    """)
                    avg_result = db.execute(avg_query, {"code": row[0]})
                    avg_row = avg_result.fetchone()
                    avg_change_7d = float(avg_row[0]) if avg_row and avg_row[0] else None
                    
                    # 获取连板情况（连续涨停天数）
                    board_query = text("""
                        SELECT COUNT(*) FROM gainers 
                        WHERE stock_code = :code 
                        AND change_pct > 9.5 
                        AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    """)
                    board_result = db.execute(board_query, {"code": row[0]})
                    board_row = board_result.fetchone()
                    consecutive_boards = int(board_row[0]) if board_row and board_row[0] else 0
                    
                    hot_stocks.append({
                        "stock_code": row[0],
                        "stock_name": row[1] if row[1] else '',
                        "sector": row[4] if row[4] else None,
                        "change_pct": float(row[2]) if row[2] else None,
                        "rank": row[3] if row[3] != 999 else None,
                        "current_price": current_price,
                        "avg_change_7d": avg_change_7d,
                        "consecutive_boards": consecutive_boards
                    })
                    existing_codes.add(row[0])
            
            # 策略2: 如果不足，从热度榜补充（去重）
            if len(hot_stocks) < 20:
                hot_stocks_query2 = text("""
                    SELECT DISTINCT hs.stock_code, hs.stock_name,
                           COALESCE(g.change_pct, 0) as change_pct,
                           MIN(hs.`rank`) as min_rank,
                           si.sector
                    FROM hot_stocks hs
                    LEFT JOIN stock_info si ON hs.stock_code = si.stock_code
                    LEFT JOIN gainers g ON hs.stock_code = g.stock_code AND g.trade_date = CURDATE()
                    WHERE hs.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    GROUP BY hs.stock_code, hs.stock_name, g.change_pct, si.sector
                    ORDER BY min_rank ASC
                    LIMIT 30
                """)
                hot_stocks_result2 = db.execute(hot_stocks_query2)
                for row in hot_stocks_result2:
                    if row[0] not in existing_codes and len(hot_stocks) < 20:
                        # 获取当前价格
                        price_query = text("""
                            SELECT close_price FROM stock_daily 
                            WHERE stock_code = :code 
                            ORDER BY trade_date DESC LIMIT 1
                        """)
                        price_result = db.execute(price_query, {"code": row[0]})
                        price_row = price_result.fetchone()
                        current_price = float(price_row[0]) if price_row and price_row[0] else None
                        
                        # 获取7天平均涨幅
                        avg_query = text("""
                            SELECT AVG(change_pct) FROM gainers 
                            WHERE stock_code = :code 
                            AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                        """)
                        avg_result = db.execute(avg_query, {"code": row[0]})
                        avg_row = avg_result.fetchone()
                        avg_change_7d = float(avg_row[0]) if avg_row and avg_row[0] else None
                        
                        # 获取连板情况
                        board_query = text("""
                            SELECT COUNT(*) FROM gainers 
                            WHERE stock_code = :code 
                            AND change_pct > 9.5 
                            AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                        """)
                        board_result = db.execute(board_query, {"code": row[0]})
                        board_row = board_result.fetchone()
                        consecutive_boards = int(board_row[0]) if board_row and board_row[0] else 0
                        
                        hot_stocks.append({
                            "stock_code": row[0],
                            "stock_name": row[1] if row[1] else '',
                            "sector": row[4] if row[4] else None,
                            "change_pct": float(row[2]) if row[2] else None,
                            "rank": row[3] if row[3] else None,
                            "current_price": current_price,
                            "avg_change_7d": avg_change_7d,
                            "consecutive_boards": consecutive_boards
                        })
                        existing_codes.add(row[0])
            
            # 获取资金持续5天正流入的股票
            capital_flow_query = text("""
                SELECT scf.stock_code, si.stock_name,
                       AVG(scf.main_net_inflow) as avg_inflow,
                       COUNT(*) as positive_days
                FROM stock_capital_flow scf
                JOIN stock_info si ON scf.stock_code = si.stock_code
                WHERE scf.trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
                AND scf.main_net_inflow > 0
                GROUP BY scf.stock_code, si.stock_name
                HAVING positive_days >= 5
                ORDER BY avg_inflow DESC
                LIMIT 20
            """)
            capital_flow_result = db.execute(capital_flow_query)
            capital_flow_stocks = []
            for row in capital_flow_result:
                capital_flow_stocks.append({
                    "stock_code": row[0],
                    "stock_name": row[1],
                    "avg_inflow": float(row[2]) if row[2] else 0,
                    "positive_days": row[3]
                })
            
            return {
                "hot_sectors": hot_sectors_with_stocks,
                "hot_stocks": hot_stocks,
                "capital_flow_stocks": capital_flow_stocks
            }
    except Exception as e:
        logger.error(f"获取推荐数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/refresh-hot-stocks")
async def refresh_hot_stocks(current_user: dict = Depends(get_current_user)):
    """手动刷新热度榜数据"""
    try:
        from data_collector import DataCollector
        collector = DataCollector()
        collector.collect_hot_stocks()
        collector.close()
        return {"message": "热度榜数据刷新成功"}
    except Exception as e:
        logger.error(f"刷新热度榜失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
