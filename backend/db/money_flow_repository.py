"""
资金流向数据仓储层
"""
from typing import List, Dict
from datetime import date
from sqlalchemy import text
from db.database import get_db

class MoneyFlowRepository:
    """资金流向数据仓储"""
    
    @staticmethod
    def batch_upsert_money_flow(data_list: List[Dict]):
        """批量插入或更新资金流向数据"""
        if not data_list:
            return
        
        with get_db() as db:
            query = text("""
                INSERT INTO stock_money_flow
                (stock_code, trade_date, main_net_inflow, super_large_inflow, 
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
            
            batch_size = 1000
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                db.execute(query, batch)
    
    @staticmethod
    def get_stock_money_flow(stock_code: str, limit: int = 90) -> List[Dict]:
        """获取股票资金流向数据"""
        with get_db() as db:
            query = text("""
                SELECT trade_date, main_net_inflow, super_large_inflow, large_inflow,
                       medium_inflow, small_inflow
                FROM stock_money_flow
                WHERE stock_code = :code
                ORDER BY trade_date ASC
                LIMIT :limit
            """)
            result = db.execute(query, {'code': stock_code, 'limit': limit})
            
            return [
                {
                    'trade_date': row[0].strftime('%Y-%m-%d') if row[0] else None,
                    'main_net_inflow': float(row[1]) if row[1] else 0,
                    'super_large_inflow': float(row[2]) if row[2] else 0,
                    'large_inflow': float(row[3]) if row[3] else 0,
                    'medium_inflow': float(row[4]) if row[4] else 0,
                    'small_inflow': float(row[5]) if row[5] else 0,
                }
                for row in result
            ]
