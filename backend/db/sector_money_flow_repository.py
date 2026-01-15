"""
板块资金流向数据仓储层
"""
from typing import List, Dict, Optional
from datetime import date, timedelta
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class SectorMoneyFlowRepository:
    """板块资金流向数据仓储"""
    
    @staticmethod
    def batch_upsert_sector_money_flow(data_list: List[Dict]):
        """批量插入或更新板块资金流向数据"""
        if not data_list:
            return
        
        with get_db() as db:
            query = text("""
                INSERT INTO sector_money_flow
                (sector_name, trade_date, main_net_inflow, super_large_inflow, 
                 large_inflow, medium_inflow, small_inflow)
                VALUES 
                (:sector_name, :trade_date, :main_net_inflow, :super_large_inflow, 
                 :large_inflow, :medium_inflow, :small_inflow)
                ON DUPLICATE KEY UPDATE
                    main_net_inflow = VALUES(main_net_inflow),
                    super_large_inflow = VALUES(super_large_inflow),
                    large_inflow = VALUES(large_inflow),
                    medium_inflow = VALUES(medium_inflow),
                    small_inflow = VALUES(small_inflow),
                    updated_at = CURRENT_TIMESTAMP
            """)
            
            batch_size = 500
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                db.execute(query, batch)
                db.commit()
    
    @staticmethod
    def get_sector_money_flow(sector_name: str, limit: int = 90) -> List[Dict]:
        """
        获取板块资金流向数据（默认返回最新的数据）
        
        Args:
            sector_name: 板块名称
            limit: 返回的记录数，默认90条（3个月）
        """
        with get_db() as db:
            query = text("""
                SELECT trade_date, main_net_inflow, super_large_inflow, large_inflow,
                       medium_inflow, small_inflow
                FROM sector_money_flow
                WHERE sector_name = :sector_name
                ORDER BY trade_date DESC
                LIMIT :limit
            """)
            
            result = db.execute(query, {'sector_name': sector_name, 'limit': limit})
            data_list = []
            for row in result:
                data_list.append({
                    'trade_date': row[0],
                    'main_net_inflow': float(row[1]) if row[1] is not None else 0.0,
                    'super_large_inflow': float(row[2]) if row[2] is not None else 0.0,
                    'large_inflow': float(row[3]) if row[3] is not None else 0.0,
                    'medium_inflow': float(row[4]) if row[4] is not None else 0.0,
                    'small_inflow': float(row[5]) if row[5] is not None else 0.0,
                })
            
            # 按日期升序排序（用于图表显示）
            data_list.sort(key=lambda x: x['trade_date'])
            return data_list
    
    @staticmethod
    def get_top_sectors_by_inflow(days: int = 1, limit: int = 20) -> List[Dict]:
        """
        获取资金净流入最多的板块
        
        Args:
            days: 统计天数（1=当日，3=最近3天，5=最近5天）
            limit: 返回数量
        """
        with get_db() as db:
            if days == 1:
                # 当日
                query = text("""
                    SELECT 
                        sector_name,
                        trade_date,
                        main_net_inflow,
                        super_large_inflow,
                        large_inflow
                    FROM sector_money_flow
                    WHERE trade_date = CURDATE()
                    ORDER BY main_net_inflow DESC
                    LIMIT :limit
                """)
                result = db.execute(query, {'limit': limit})
                
                sectors = []
                for row in result:
                    sectors.append({
                        'sector_name': row[0],
                        'trade_date': row[1],
                        'main_net_inflow': float(row[2]) if row[2] is not None else 0.0,
                        'super_large_inflow': float(row[3]) if row[3] is not None else 0.0,
                        'large_inflow': float(row[4]) if row[4] is not None else 0.0,
                    })
                return sectors
            else:
                # 最近N天累计，同时返回每日详细数据
                # 先获取累计排名TOP的板块
                query = text("""
                    SELECT 
                        sector_name,
                        SUM(main_net_inflow) AS total_inflow,
                        SUM(super_large_inflow) AS total_super_large,
                        SUM(large_inflow) AS total_large,
                        MAX(trade_date) AS latest_date
                    FROM sector_money_flow
                    WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                    GROUP BY sector_name
                    ORDER BY total_inflow DESC
                    LIMIT :limit
                """)
                result = db.execute(query, {'days': days - 1, 'limit': limit})
                
                sectors = []
                sector_names = []
                for row in result:
                    sector_name = row[0]
                    sector_names.append(sector_name)
                    sectors.append({
                        'sector_name': sector_name,
                        'total_inflow': float(row[1]) if row[1] is not None else 0.0,
                        'total_super_large': float(row[2]) if row[2] is not None else 0.0,
                        'total_large': float(row[3]) if row[3] is not None else 0.0,
                        'latest_date': row[4],
                        'daily_data': []  # 每日详细数据
                    })
                
                # 获取这些板块的每日详细数据
                if sector_names:
                    # 构建IN子句的参数
                    placeholders = ','.join([f':name{i}' for i in range(len(sector_names))])
                    daily_query_str = f"""
                        SELECT 
                            sector_name,
                            trade_date,
                            main_net_inflow,
                            super_large_inflow,
                            large_inflow
                        FROM sector_money_flow
                        WHERE sector_name IN ({placeholders})
                        AND trade_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                        ORDER BY sector_name, trade_date DESC
                    """
                    daily_query = text(daily_query_str)
                    params = {f'name{i}': name for i, name in enumerate(sector_names)}
                    params['days'] = days - 1
                    daily_result = db.execute(daily_query, params)
                    
                    # 按板块分组每日数据
                    daily_data_map: Dict[str, List[Dict]] = {}
                    for row in daily_result:
                        sector_name = row[0]
                        if sector_name not in daily_data_map:
                            daily_data_map[sector_name] = []
                        daily_data_map[sector_name].append({
                            'trade_date': row[1],
                            'main_net_inflow': float(row[2]) if row[2] is not None else 0.0,
                            'super_large_inflow': float(row[3]) if row[3] is not None else 0.0,
                            'large_inflow': float(row[4]) if row[4] is not None else 0.0,
                        })
                    
                    # 将每日数据添加到对应的板块
                    for sector in sectors:
                        sector['daily_data'] = daily_data_map.get(sector['sector_name'], [])
                
                return sectors
    
    @staticmethod
    def cleanup_old_data(retention_days: int = 90):
        """
        清理旧数据（保留最近N天）
        
        Args:
            retention_days: 保留天数，默认90天（3个月）
        """
        cutoff_date = date.today() - timedelta(days=retention_days)
        
        with get_db() as db:
            # 先查询要删除的数量
            count_query = text("""
                SELECT COUNT(*) 
                FROM sector_money_flow 
                WHERE trade_date < :cutoff_date
            """)
            result = db.execute(count_query, {'cutoff_date': cutoff_date})
            count = result.scalar()
            
            if count > 0:
                # 删除旧数据
                delete_query = text("""
                    DELETE FROM sector_money_flow 
                    WHERE trade_date < :cutoff_date
                """)
                db.execute(delete_query, {'cutoff_date': cutoff_date})
                db.commit()
                logger.info(f"清理板块资金流旧数据完成，删除了 {count} 条记录（保留最近{retention_days}天）")
                return count
            else:
                logger.info(f"无需清理板块资金流数据（保留最近{retention_days}天）")
                return 0
