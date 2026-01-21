"""
资金流向数据仓储层
"""
from typing import List, Dict
from datetime import date, timedelta
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class MoneyFlowRepository:
    """资金流向数据仓储"""
    
    @staticmethod
    def batch_upsert_money_flow(data_list: List[Dict]):
        """批量插入或更新资金流向数据"""
        if not data_list:
            return
        
        with get_db() as db:
            query = text("""
                INSERT INTO sheep_money_flow
                (sheep_code, trade_date, main_net_inflow, super_large_inflow, 
                 large_inflow, medium_inflow, small_inflow)
                VALUES 
                (:code, :date, :main, :super_large, :large, :medium, :small)
                ON DUPLICATE KEY UPDATE
                    main_net_inflow = VALUES(main_net_inflow),
                    super_large_inflow = VALUES(super_large_inflow),
                    large_inflow = VALUES(large_inflow),
                    medium_inflow = VALUES(medium_inflow),
                    small_inflow = VALUES(small_inflow)
            """)
            
            # 优化批量插入性能
            batch_size = 2000  # 增大批次大小
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                db.execute(query, batch)
                db.commit()  # 每批次提交
    
    @staticmethod
    def get_sheep_money_flow(sheep_code: str, limit: int = 60) -> List[Dict]:
        """
        获取肥羊资金流向数据（默认返回最新的数据）
        
        Args:
            sheep_code: 肥羊代码
            limit: 返回的记录数，默认60条（最新的60天）
            
        Returns:
            按日期升序排列的资金流数据列表（从旧到新）
        """
        with get_db() as db:
            # 直接按日期升序查询，避免在应用层排序
            query = text(f"""
                SELECT trade_date, main_net_inflow, super_large_inflow, large_inflow,
                       medium_inflow, small_inflow
                FROM (
                    SELECT trade_date, main_net_inflow, super_large_inflow, large_inflow,
                           medium_inflow, small_inflow
                    FROM sheep_money_flow
                    WHERE sheep_code = :code
                    ORDER BY trade_date DESC
                    LIMIT {limit}
                ) as subquery
                ORDER BY trade_date ASC
            """)
            result = db.execute(query, {'code': sheep_code})
            
            # 辅助函数：安全地格式化日期
            def safe_strftime(date_value, default=None):
                """安全地将日期对象转换为字符串"""
                if date_value is None:
                    return default
                if hasattr(date_value, 'strftime'):
                    try:
                        return date_value.strftime('%Y-%m-%d')
                    except (AttributeError, TypeError):
                        pass
                if isinstance(date_value, str):
                    return date_value
                return default
            
            # 获取数据并转换为列表
            data_list = [
                {
                    'trade_date': safe_strftime(row[0]),
                    'main_net_inflow': float(row[1]) if row[1] else 0,
                    'super_large_inflow': float(row[2]) if row[2] else 0,
                    'large_inflow': float(row[3]) if row[3] else 0,
                    'medium_inflow': float(row[4]) if row[4] else 0,
                    'small_inflow': float(row[5]) if row[5] else 0,
                }
                for row in result
            ]
            
            return data_list
    
    @staticmethod
    def get_continuous_inflow_stocks(days: int = 5, min_days: int = None) -> List[Dict]:
        """
        获取最近N个交易日资金持续流入的标的
        
        Args:
            days: 查询最近N个交易日的数据（实际查询范围会扩大到days*2天，确保能覆盖足够的交易日）
            min_days: 要求至少连续流入的交易日数（默认等于days，即要求全部交易日都流入）
        
        Returns:
            标的列表，包含sheep_code, sheep_name, continuous_days, total_inflow等信息
        """
        if min_days is None:
            min_days = days
        
        with get_db() as db:
            # 获取最近N*2天的所有资金流数据（扩大范围以确保覆盖足够的交易日）
            # 按日期倒序排列（最新的在前）
            query = text(f"""
                SELECT 
                    smf.sheep_code,
                    COALESCE(sb.sheep_name, smf.sheep_code) AS sheep_name,
                    smf.trade_date,
                    smf.main_net_inflow
                FROM sheep_money_flow smf
                LEFT JOIN sheep_basic sb ON smf.sheep_code = sb.sheep_code AND sb.is_active = 1
                WHERE smf.trade_date >= DATE_SUB(CURDATE(), INTERVAL :days * 2 DAY)
                  AND smf.trade_date <= CURDATE()
                ORDER BY smf.sheep_code, smf.trade_date DESC
            """)
            
            result = db.execute(query, {'days': days})
            
            # 按肥羊分组，存储每日数据
            stocks_data = {}
            for row in result:
                sheep_code = row[0]
                sheep_name = row[1] if row[1] and not str(row[1]).strip().isdigit() else sheep_code
                trade_date = row[2]
                main_net_inflow = float(row[3]) if row[3] else 0
                
                if sheep_code not in stocks_data:
                    stocks_data[sheep_code] = {
                        'sheep_code': sheep_code,
                        'sheep_name': sheep_name,
                        'daily_data': []  # 存储每日数据，已按日期倒序排列
                    }
                
                stocks_data[sheep_code]['daily_data'].append({
                    'trade_date': trade_date,
                    'main_net_inflow': main_net_inflow
                })
            
            # 计算连续流入天数
            filtered_stocks = []
            for sheep_code, data in stocks_data.items():
                daily_data = data['daily_data']
                if len(daily_data) < min_days:
                    continue
                
                # 检查最近min_days个交易日是否都流入（连续流入）
                # daily_data已经按日期倒序排列，最新的在前
                # 取前min_days个交易日（确保是连续的交易日）
                recent_days = daily_data[:min_days]
                
                # 确保有足够的数据，且都是流入
                if len(recent_days) < min_days:
                    continue
                
                all_inflow = all(d['main_net_inflow'] > 0 for d in recent_days)
                
                if all_inflow:
                    # 计算总流入、最大单日流入等
                    total_inflow = sum(d['main_net_inflow'] for d in recent_days)
                    max_single_day_inflow = max(d['main_net_inflow'] for d in recent_days)
                    avg_daily_inflow = total_inflow / min_days
                    
                    # 判断名称是否有效
                    name = data['sheep_name']
                    if not name or str(name).strip().isdigit() or len(str(name).strip()) == 6:
                        name = sheep_code
                    
                    filtered_stocks.append({
                        'sheep_code': sheep_code,
                        'sheep_name': name,
                        'continuous_days': min_days,
                        'total_inflow': round(total_inflow / 10000, 2),  # 转换为亿元
                        'max_single_day_inflow': round(max_single_day_inflow / 10000, 2),  # 转换为亿元
                        'avg_daily_inflow': round(avg_daily_inflow / 10000, 2)  # 转换为亿元
                    })
            
            # 按总流入金额排序
            filtered_stocks.sort(key=lambda x: x['total_inflow'], reverse=True)
            
            return filtered_stocks

    @staticmethod
    def get_top_inflow_stocks(days: int = 1, limit: int = 100) -> List[Dict]:
        """
        获取最近N个交易日净流入Top标的（按主力净流入合计降序）
            
        Args:
            days: 统计最近N个交易日（1/3/5）
            limit: 返回数量（默认100，最大200）
                
        Returns:
            标的列表：sheep_code, sheep_name, total_inflow(亿元), avg_daily_inflow(亿元), latest_trade_date, daily_data
        """
        if days <= 0:
            days = 1
        if limit <= 0:
            limit = 100
        if limit > 200:
            limit = 200
            
        with get_db() as db:
            # 取最近N个交易日（以表里实际日期为准，避免用自然日导致缺失）
            query = text("""
                SELECT 
                    smf.sheep_code,
                    COALESCE(sb.sheep_name, smf.sheep_code) AS sheep_name,
                    SUM(COALESCE(smf.main_net_inflow, 0)) AS total_main_net_inflow,
                    AVG(COALESCE(smf.main_net_inflow, 0)) AS avg_main_net_inflow,
                    MAX(smf.trade_date) AS latest_trade_date
                FROM sheep_money_flow smf
                LEFT JOIN sheep_basic sb 
                    ON smf.sheep_code = sb.sheep_code AND sb.is_active = 1
                WHERE smf.trade_date IN (
                    SELECT trade_date FROM (
                        SELECT DISTINCT trade_date
                        FROM sheep_money_flow
                        ORDER BY trade_date DESC
                        LIMIT :days
                    ) d
                )
                GROUP BY smf.sheep_code, sheep_name
                HAVING total_main_net_inflow > 0
                ORDER BY total_main_net_inflow DESC
                LIMIT :limit
            """)
                
            result = db.execute(query, {'days': days, 'limit': limit})
                
            stocks = []
            sheep_codes = []
            for row in result:
                sheep_code = row[0]
                sheep_name = row[1] if row[1] and not str(row[1]).strip().isdigit() else sheep_code
                total_main = float(row[2]) if row[2] is not None else 0.0
                avg_main = float(row[3]) if row[3] is not None else 0.0
                latest_date = row[4]
                    
                # 兆底：名称无效则用代码
                if not sheep_name or str(sheep_name).strip().isdigit() or len(str(sheep_name).strip()) == 6:
                    sheep_name = sheep_code
                    
                stocks.append({
                    'sheep_code': sheep_code,
                    'sheep_name': sheep_name,
                    # 与现有接口保持一致：转换为"亿元"
                    'total_inflow': round(total_main / 10000, 2),
                    'avg_daily_inflow': round(avg_main / 10000, 2),
                    'latest_trade_date': latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)
                })
                sheep_codes.append(sheep_code)
                
            # 如果是多天视图，获取每日详细数据
            if days > 1 and sheep_codes:
                daily_data_map = MoneyFlowRepository._get_stocks_daily_inflow(sheep_codes, days)
                for stock in stocks:
                    stock['daily_data'] = daily_data_map.get(stock['sheep_code'], [])
                
            return stocks
        
    @staticmethod
    def _get_stocks_daily_inflow(sheep_codes: List[str], days: int) -> Dict[str, List[Dict]]:
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
                # 获取这些股票在最近N天的资金流数据
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
                daily_data_map: Dict[str, List[Dict]] = {}
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
    def get_sheep_money_flow_count_for_date(trade_date: date) -> int:
        """
        获取指定日期的资金流数据数量
        
        Args:
            trade_date: 交易日期
            
        Returns:
            该日期的数据记录数
        """
        try:
            with get_db() as db:
                query = text("""
                    SELECT COUNT(*) 
                    FROM sheep_money_flow
                    WHERE trade_date = :trade_date
                """)
                result = db.execute(query, {'trade_date': trade_date})
                row = result.fetchone()
                return row[0] if row and row[0] is not None else 0
        except Exception as e:
            logger.error(f"获取日期 {trade_date} 的资金流数据数量失败: {e}")
            return 0
    
    @staticmethod
    def cleanup_old_data(retention_days: int = 1095):
        """
        清理超过保留天数的旧数据
        
        Args:
            retention_days: 保留天数，默认1095天（3年）
            
        Returns:
            删除的记录数
        """
        try:
            cutoff_date = date.today() - timedelta(days=retention_days)
            
            with get_db() as db:
                # 先查询要删除的记录数
                count_query = text("""
                    SELECT COUNT(*) AS count
                    FROM sheep_money_flow
                    WHERE trade_date < :cutoff_date
                """)
                count_result = db.execute(count_query, {'cutoff_date': cutoff_date})
                count_row = count_result.fetchone()
                deleted_count = count_row[0] if count_row else 0
                
                if deleted_count == 0:
                    logger.info(f"资金流数据清理：无需清理，所有数据都在保留期内（{retention_days}天）")
                    return 0
                
                # 删除旧数据
                delete_query = text("""
                    DELETE FROM sheep_money_flow
                    WHERE trade_date < :cutoff_date
                """)
                db.execute(delete_query, {'cutoff_date': cutoff_date})
                db.commit()
                
                logger.info(f"资金流数据清理完成：删除了 {deleted_count} 条 {cutoff_date} 之前的数据")
                return deleted_count
                
        except Exception as e:
            logger.error(f"清理资金流旧数据失败: {e}", exc_info=True)
            return 0
