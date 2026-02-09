# /backend/api/strategy.py

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from strategy.mainline import mainline_analyst
from strategy.sentiment import sentiment_analyst
from strategy.recommend import run_recommendation, get_plugin
from strategy.recommend.plugins.backtest_star50 import run_backtest as run_star50_backtest

router = APIRouter(
    prefix="/strategy",
    tags=["Strategy Engine"],
)

class StrategyParams(BaseModel):
    """
    策略推荐接口的查询参数。
    """
    target_date: str | None = None
    concept: str | None = None
    strategy_id: str | None = None

@router.get("/backtest/star50")
def get_star50_backtest():
    """ 
    获取或计算科创50情绪策略回测结果 
    """
    result = run_star50_backtest()
    if result:
        return result
    return {"status": "error", "message": "Backtest failed"}

@router.get("/recommendations")
def get_recommendations(params: StrategyParams = Depends()):
    """
    获取推荐结果，并自动触发结果持久化以便后续回测。
    """
    # 1. 获取推荐结果
    results = run_recommendation(
        strategy_id=params.strategy_id,
        target_date=params.target_date,
        concept=params.concept
    )
    
    # 2. 如果成功，自动保存结果到数据库供回测使用
    if results.get("status") == "success":
        backtester = get_plugin("backtester")
        if backtester:
            backtester.record_recommendations(
                date=results.get("date"),
                strategy_name=results.get("strategy", params.strategy_id or "alpha_momentum"),
                recommendations=results.get("data", [])
            )
            
    return results

@router.get("/mainline_history")
def get_mainline_history(days: int = 30):
    """ 获取主线演变历史图表数据 """
    return mainline_analyst.get_history(days=days)

@router.get("/hot_concepts")
def get_hot_concepts(limit: int = 10):
    """ 获取最近热门的概念名称列表 """
    results = mainline_analyst.analyze(days=7, limit=limit)
    return [r['name'] for r in results] if results else []

@router.get("/market_sentiment")
def get_market_sentiment(days: int = 30):
    """ 获取市场情绪与指数对比历史数据 """
    return sentiment_analyst.get_history(days=days)

@router.post("/sentiment/sync")
def sync_market_sentiment(days: int = 30):
    """ 
    手动/自动补全情绪历史数据。
    调用此接口将追溯计算过去 N 天的情绪指标与买卖信号。
    """
    try:
        sentiment_analyst.calculate(days=days)
        return {"status": "success", "message": f"Successfully synced sentiment for last {days} days."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/backtest")
def trigger_backtest():
    """ 触发一键收益验证计算 """
    return run_recommendation(strategy_id="backtester")

@router.get("/backtest/results")
def get_backtest_results(date: str):
    """ 获取特定日期的验证表现 """
    from db.connection import fetch_df
    import math
    query = "SELECT * FROM strategy_recommendations WHERE recommend_date = ? ORDER BY score DESC"
    df = fetch_df(query, params=[date])
    if df.empty:
        return []
        
    data = df.to_dict('records')
    for record in data:
        for key, value in record.items():
            if isinstance(value, float) and math.isnan(value):
                record[key] = None
    return data