"""
智能资金矩阵服务层
捕捉长线资金（机构）与短线资金（游资）的动向
"""
from typing import List, Dict, Optional
from datetime import date
from db.money_flow_repository import MoneyFlowRepository
from db.sector_money_flow_repository import SectorMoneyFlowRepository
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class SmartMoneyMatrixService:
    """智能资金矩阵服务"""
    
    @staticmethod
    def get_smart_money_matrix(days: int = 1, limit: int = 100) -> Dict:
        """
        获取智能资金矩阵数据
        
        Args:
            days: 统计天数（1=当日，3=最近3天，5=最近5天）
            limit: 返回数量
            
        Returns:
            包含个股和板块资金流数据的字典
        """
        try:
            # 获取个股资金流数据
            stocks = SmartMoneyMatrixService._get_stock_money_flow(days, limit)
            
            # 获取板块资金流数据
            sectors = SmartMoneyMatrixService._get_sector_money_flow(days, limit)
            
            return {
                'stocks': stocks,
                'sectors': sectors,
                'days': days,
                'trade_date': SmartMoneyMatrixService._get_latest_trade_date().isoformat()
            }
        except Exception as e:
            logger.error(f"获取智能资金矩阵失败: {e}", exc_info=True)
            return {
                'stocks': [],
                'sectors': [],
                'days': days,
                'trade_date': date.today().isoformat()
            }
    
    @staticmethod
    def _get_latest_trade_date() -> date:
        """获取最新交易日期"""
        try:
            with get_db() as db:
                query = text("SELECT MAX(trade_date) FROM sheep_money_flow")
                result = db.execute(query)
                row = result.fetchone()
                if row and row[0]:
                    return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
        except Exception as e:
            logger.error(f"获取最新交易日期失败: {e}")
        
        return date.today()
    
    @staticmethod
    def _get_stock_money_flow(days: int, limit: int) -> List[Dict]:
        """
        获取个股资金流数据，并标记潜力标的
        
        Args:
            days: 统计天数
            limit: 返回数量
            
        Returns:
            个股列表，包含潜力标记
        """
        try:
            # 获取最近N天的资金流入Top标的
            stocks = MoneyFlowRepository.get_top_inflow_stocks(days=days, limit=limit * 2)
            
            # 获取最新交易日期
            latest_date = SmartMoneyMatrixService._get_latest_trade_date()
            
            # 如果是3天或5天视图，获取每只股票的每日资金流数据
            daily_data_map = {}
            if days > 1:
                daily_data_map = SmartMoneyMatrixService._get_stocks_daily_data(
                    [s['sheep_code'] for s in stocks], days
                )
            
            # 对每只股票进行潜力评分
            scored_stocks = []
            
            for stock in stocks:
                sheep_code = stock['sheep_code']
                
                # 获取5日涨幅和换手率
                stock_info = SmartMoneyMatrixService._get_stock_info(sheep_code, latest_date)
                
                # 计算潜力评分
                potential_score = SmartMoneyMatrixService._calculate_potential_score(
                    stock, stock_info, days
                )
                
                stock_result = {
                    **stock,
                    **stock_info,
                    'potential_score': potential_score['score'],
                    'is_high_potential': potential_score['is_high_potential'],
                    'potential_reason': potential_score['reason']
                }
                
                # 添加每日数据（用于趋势图）
                if days > 1 and sheep_code in daily_data_map:
                    stock_result['daily_data'] = daily_data_map[sheep_code]
                
                scored_stocks.append(stock_result)
            
            # 按潜力评分排序
            scored_stocks.sort(key=lambda x: x['potential_score'], reverse=True)
            
            return scored_stocks[:limit]
        except Exception as e:
            logger.error(f"获取个股资金流失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def _get_sector_money_flow(days: int, limit: int) -> List[Dict]:
        """
        获取板块资金流数据
        
        Args:
            days: 统计天数
            limit: 返回数量
            
        Returns:
            板块列表
        """
        try:
            sectors, metadata = SectorMoneyFlowRepository.get_top_sectors_by_inflow(
                days=days, limit=limit
            )
            
            return sectors
        except Exception as e:
            logger.error(f"获取板块资金流失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def _get_stocks_daily_data(sheep_codes: List[str], days: int) -> Dict[str, List[Dict]]:
        """
        获取多只股票的每日资金流数据
        
        Args:
            sheep_codes: 股票代码列表
            days: 统计天数
            
        Returns:
            股票代码 -> 每日数据列表的映射
        """
        try:
            with get_db() as db:
                # 获取最近N个交易日
                query = text("""
                    SELECT DISTINCT trade_date 
                    FROM sheep_money_flow 
                    ORDER BY trade_date DESC 
                    LIMIT :days
                """)
                result = db.execute(query, {'days': days})
                trade_dates = [row[0] for row in result]
                
                if not trade_dates:
                    return {}
                
                # 获取这些股票在这些日期的资金流数据
                placeholders = ','.join([f':code_{i}' for i in range(len(sheep_codes))])
                query = text(f"""
                    SELECT sheep_code, trade_date, main_net_inflow
                    FROM sheep_money_flow
                    WHERE sheep_code IN ({placeholders})
                    AND trade_date IN (
                        SELECT trade_date FROM (
                            SELECT DISTINCT trade_date 
                            FROM sheep_money_flow 
                            ORDER BY trade_date DESC 
                            LIMIT :days
                        ) d
                    )
                    ORDER BY sheep_code, trade_date ASC
                """)
                
                params = {'days': days}
                for i, code in enumerate(sheep_codes):
                    params[f'code_{i}'] = code
                
                result = db.execute(query, params)
                
                # 按股票代码分组
                daily_data_map = {}
                for row in result:
                    sheep_code = row[0]
                    trade_date = row[1]
                    main_net_inflow = float(row[2]) if row[2] else 0.0
                    
                    if sheep_code not in daily_data_map:
                        daily_data_map[sheep_code] = []
                    
                    daily_data_map[sheep_code].append({
                        'trade_date': trade_date.strftime('%Y-%m-%d') if hasattr(trade_date, 'strftime') else str(trade_date),
                        'main_net_inflow': round(main_net_inflow / 10000, 4)  # 转换为亿元
                    })
                
                return daily_data_map
        except Exception as e:
            logger.error(f"获取股票每日数据失败: {e}", exc_info=True)
            return {}
    
    @staticmethod
    def _get_stock_info(sheep_code: str, trade_date: date) -> Dict:
        """
        获取个股信息（5日涨幅、换手率、当前价格等）
        
        Args:
            sheep_code: 股票代码
            trade_date: 交易日期
            
        Returns:
            包含股票信息的字典
        """
        try:
            with get_db() as db:
                # 获取最近5天的日K数据
                query = text("""
                    SELECT trade_date, close_price, change_pct, turnover_rate, ma5
                    FROM sheep_daily
                    WHERE sheep_code = :code
                    AND trade_date <= :trade_date
                    ORDER BY trade_date DESC
                    LIMIT 5
                """)
                
                result = db.execute(query, {
                    'code': sheep_code,
                    'trade_date': trade_date
                })
                
                daily_data = list(result)
                
                if len(daily_data) < 2:
                    return {
                        'change_pct_5d': 0.0,
                        'turnover_rate': 0.0,
                        'ma5_price': 0.0,
                        'current_price': 0.0
                    }
                
                # 获取当前价格
                current_price = float(daily_data[0][1]) if daily_data[0][1] else 0.0
                
                # 计算5日涨幅
                latest_close = current_price
                five_days_ago_close = float(daily_data[-1][1]) if daily_data[-1][1] else 0.0
                
                change_pct_5d = 0.0
                if five_days_ago_close > 0:
                    change_pct_5d = ((latest_close - five_days_ago_close) / five_days_ago_close) * 100
                
                # 获取最新换手率
                latest_turnover = float(daily_data[0][3]) if daily_data[0][3] else 0.0
                
                # 获取MA5价格（主力成本线）
                ma5_price = float(daily_data[0][4]) if daily_data[0][4] else latest_close
                
                return {
                    'change_pct_5d': round(change_pct_5d, 2),
                    'turnover_rate': round(latest_turnover, 2),
                    'ma5_price': round(ma5_price, 2),
                    'current_price': round(current_price, 2)
                }
        except Exception as e:
            logger.error(f"获取股票信息失败（{sheep_code}）: {e}", exc_info=True)
            return {
                'change_pct_5d': 0.0,
                'turnover_rate': 0.0,
                'ma5_price': 0.0,
                'current_price': 0.0
            }
    
    @staticmethod
    def _calculate_potential_score(stock: Dict, stock_info: Dict, days: int) -> Dict:
        """
        计算潜力评分
        
        条件1 (低位潜伏)：5日资金流入排名 Top 50 且 5日涨幅 < 5%
        条件2 (趋势加速)：当日资金流入 > 1亿 且 换手率 < 8%（锁仓拉升）
        
        Args:
            stock: 股票资金流数据
            stock_info: 股票信息
            days: 统计天数
            
        Returns:
            包含评分和标记的字典
        """
        score = 0
        reasons = []
        is_high_potential = False
        
        change_pct_5d = stock_info.get('change_pct_5d', 0.0)
        turnover_rate = stock_info.get('turnover_rate', 0.0)
        total_inflow = stock.get('total_inflow', 0.0)  # 已经是亿元单位
        
        # 条件1：低位潜伏
        # 需要判断是否在Top 50（这里简化处理，假设传入的已经是排序后的数据）
        if change_pct_5d < 5.0 and total_inflow > 0:
            score += 30
            reasons.append('低位潜伏')
            is_high_potential = True
        
        # 条件2：趋势加速
        if days == 1 and total_inflow > 1.0 and turnover_rate < 8.0:
            score += 50
            reasons.append('趋势加速')
            is_high_potential = True
        
        # 资金流入加分
        if total_inflow > 2.0:
            score += 20
        elif total_inflow > 1.0:
            score += 10
        
        return {
            'score': score,
            'is_high_potential': is_high_potential,
            'reason': ' | '.join(reasons) if reasons else '正常资金流入'
        }
