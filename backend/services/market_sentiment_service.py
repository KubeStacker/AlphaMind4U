"""
市场情绪仪表盘服务层
量化市场的"体温"，防止逆势操作
"""
from typing import Dict, Optional
from datetime import date
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class MarketSentimentService:
    """市场情绪服务"""
    
    @staticmethod
    def get_market_sentiment(trade_date: date = None) -> Dict:
        """
        获取市场情绪数据
        
        Args:
            trade_date: 交易日期，如果为None则使用最新交易日
            
        Returns:
            包含市场情绪指标的字典
        """
        if trade_date is None:
            trade_date = MarketSentimentService._get_latest_trade_date()
        
        try:
            # 赚钱效应差值
            profit_effect = MarketSentimentService._calculate_profit_effect(trade_date)
            
            # 连板高度
            consecutive_limit_height = MarketSentimentService._get_consecutive_limit_height(trade_date)
            
            # 炸板率
            limit_up_failure_rate = MarketSentimentService._calculate_limit_up_failure_rate(trade_date)
            
            return {
                'trade_date': trade_date.isoformat() if hasattr(trade_date, 'isoformat') else str(trade_date),
                'profit_effect': profit_effect,
                'consecutive_limit_height': consecutive_limit_height,
                'limit_up_failure_rate': limit_up_failure_rate,
                'sentiment_level': MarketSentimentService._calculate_sentiment_level(
                    profit_effect, limit_up_failure_rate
                )
            }
        except Exception as e:
            logger.error(f"获取市场情绪数据失败: {e}", exc_info=True)
            return {
                'trade_date': trade_date.isoformat() if hasattr(trade_date, 'isoformat') else str(trade_date),
                'profit_effect': {'value': 0.0, 'level': 'neutral', 'message': '数据获取失败'},
                'consecutive_limit_height': 0,
                'limit_up_failure_rate': {'value': 0.0, 'level': 'neutral', 'message': '数据获取失败'},
                'sentiment_level': 'neutral'
            }
    
    @staticmethod
    def _get_latest_trade_date() -> date:
        """获取最新交易日期"""
        try:
            with get_db() as db:
                query = text("SELECT MAX(trade_date) FROM sheep_daily")
                result = db.execute(query)
                row = result.fetchone()
                if row and row[0]:
                    return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
        except Exception as e:
            logger.error(f"获取最新交易日期失败: {e}")
        
        return date.today()
    
    @staticmethod
    def _calculate_profit_effect(trade_date: date) -> Dict:
        """
        计算赚钱效应差值：(上涨家数 - 下跌家数) / 总家数
        
        Returns:
            包含value、level、message的字典
        """
        try:
            with get_db() as db:
                query = text("""
                    SELECT 
                        SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_count,
                        SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_count,
                        COUNT(*) AS total_count
                    FROM sheep_daily
                    WHERE trade_date = :trade_date
                    AND change_pct IS NOT NULL
                """)
                
                result = db.execute(query, {'trade_date': trade_date})
                row = result.fetchone()
                
                if row and row[2] and row[2] > 0:
                    up_count = int(row[0]) if row[0] else 0
                    down_count = int(row[1]) if row[1] else 0
                    total_count = int(row[2])
                    
                    profit_effect_value = (up_count - down_count) / total_count
                    
                    # 判断情绪级别
                    if profit_effect_value < -0.6:
                        level = 'extreme_cold'
                        message = '极度冰点，甚至可试错'
                    elif profit_effect_value < -0.3:
                        level = 'cold'
                        message = '情绪低迷，谨慎操作'
                    elif profit_effect_value < 0.3:
                        level = 'neutral'
                        message = '情绪中性，正常操作'
                    elif profit_effect_value < 0.8:
                        level = 'warm'
                        message = '情绪回暖，积极操作'
                    else:
                        level = 'extreme_hot'
                        message = '情绪高潮，注意风险'
                    
                    return {
                        'value': round(profit_effect_value, 4),
                        'up_count': up_count,
                        'down_count': down_count,
                        'total_count': total_count,
                        'level': level,
                        'message': message
                    }
        except Exception as e:
            logger.error(f"计算赚钱效应失败: {e}", exc_info=True)
        
        return {
            'value': 0.0,
            'up_count': 0,
            'down_count': 0,
            'total_count': 0,
            'level': 'neutral',
            'message': '数据获取失败'
        }
    
    @staticmethod
    def _get_consecutive_limit_height(trade_date: date) -> int:
        """
        获取市场最高连板高度
        
        Returns:
            最高连板高度（如7连板返回7）
        """
        try:
            with get_db() as db:
                # 查询当日涨停的个股
                query = text("""
                    SELECT sheep_code, change_pct
                    FROM sheep_daily
                    WHERE trade_date = :trade_date
                    AND change_pct >= 9.5
                """)
                
                result = db.execute(query, {'trade_date': trade_date})
                limit_up_stocks = [row[0] for row in result]
                
                if not limit_up_stocks:
                    return 0
                
                # 对每只涨停股，向前追溯连续涨停天数
                max_height = 0
                
                for stock_code in limit_up_stocks[:50]:  # 限制处理数量
                    height_query = text("""
                        SELECT trade_date, change_pct
                        FROM sheep_daily
                        WHERE sheep_code = :code
                        AND trade_date <= :trade_date
                        ORDER BY trade_date DESC
                        LIMIT 10
                    """)
                    
                    height_result = db.execute(height_query, {
                        'code': stock_code,
                        'trade_date': trade_date
                    })
                    
                    consecutive_days = 0
                    for row in height_result:
                        change_pct = float(row[1]) if row[1] else 0.0
                        if change_pct >= 9.5:
                            consecutive_days += 1
                        else:
                            break
                    
                    max_height = max(max_height, consecutive_days)
                
                return max_height
        except Exception as e:
            logger.error(f"获取连板高度失败: {e}", exc_info=True)
        
        return 0
    
    @staticmethod
    def _calculate_limit_up_failure_rate(trade_date: date) -> Dict:
        """
        计算炸板率：今日炸板数 / (今日涨停数 + 炸板数)
        
        Returns:
            包含value、level、message的字典
        """
        try:
            with get_db() as db:
                # 查询当日涨停的个股（涨幅 >= 9.5%）
                limit_up_query = text("""
                    SELECT COUNT(*)
                    FROM sheep_daily
                    WHERE trade_date = :trade_date
                    AND change_pct >= 9.5
                """)
                
                limit_up_result = db.execute(limit_up_query, {'trade_date': trade_date})
                limit_up_count = int(limit_up_result.scalar() or 0)
                
                # 查询当日炸板的个股（最高价触及涨停但收盘未封板）
                # 简化处理：查询最高价涨幅 >= 9.5% 但收盘涨幅 < 9.5% 的个股
                failure_query = text("""
                    SELECT COUNT(*)
                    FROM sheep_daily
                    WHERE trade_date = :trade_date
                    AND change_pct < 9.5
                    AND change_pct > 5.0
                    AND (
                        (high_price - LAG(close_price) OVER (PARTITION BY sheep_code ORDER BY trade_date)) / 
                        LAG(close_price) OVER (PARTITION BY sheep_code ORDER BY trade_date) * 100 >= 9.5
                        OR change_pct >= 7.0
                    )
                """)
                
                # 简化实现：使用更简单的逻辑
                # 查询涨幅在5%-9.5%之间的个股（可能是炸板）
                failure_query_simple = text("""
                    SELECT COUNT(*)
                    FROM sheep_daily
                    WHERE trade_date = :trade_date
                    AND change_pct >= 5.0
                    AND change_pct < 9.5
                """)
                
                failure_result = db.execute(failure_query_simple, {'trade_date': trade_date})
                failure_count = int(failure_result.scalar() or 0)
                
                total = limit_up_count + failure_count
                
                if total > 0:
                    failure_rate = failure_count / total
                    
                    # 判断级别
                    if failure_rate > 0.4:
                        level = 'high_risk'
                        message = '接力亏钱效应大'
                    elif failure_rate > 0.3:
                        level = 'medium_risk'
                        message = '炸板率偏高，注意风险'
                    else:
                        level = 'low_risk'
                        message = '封板质量较好'
                    
                    return {
                        'value': round(failure_rate, 4),
                        'limit_up_count': limit_up_count,
                        'failure_count': failure_count,
                        'total': total,
                        'level': level,
                        'message': message
                    }
        except Exception as e:
            logger.error(f"计算炸板率失败: {e}", exc_info=True)
        
        return {
            'value': 0.0,
            'limit_up_count': 0,
            'failure_count': 0,
            'total': 0,
            'level': 'neutral',
            'message': '数据获取失败'
        }
    
    @staticmethod
    def _calculate_sentiment_level(profit_effect: Dict, limit_up_failure_rate: Dict) -> str:
        """
        综合计算市场情绪级别
        
        Args:
            profit_effect: 赚钱效应数据
            limit_up_failure_rate: 炸板率数据
            
        Returns:
            情绪级别：extreme_cold / cold / neutral / warm / extreme_hot
        """
        profit_level = profit_effect.get('level', 'neutral')
        failure_level = limit_up_failure_rate.get('level', 'neutral')
        
        # 如果炸板率高，降低情绪级别
        if failure_level == 'high_risk':
            if profit_level == 'extreme_hot':
                return 'warm'
            elif profit_level == 'warm':
                return 'neutral'
            elif profit_level == 'neutral':
                return 'cold'
        
        return profit_level
