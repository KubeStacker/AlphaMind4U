"""
财务数据仓储层
"""
from typing import List, Dict, Optional
from datetime import date
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class FinancialRepository:
    """财务数据仓储"""
    
    @staticmethod
    def batch_upsert_financial_data(data_list: List[Dict]):
        """
        批量插入或更新财务数据
        
        Args:
            data_list: 财务数据列表
        """
        if not data_list:
            return
        
        try:
            with get_db() as db:
                for data in data_list:
                    query = text("""
                        INSERT INTO sheep_financials 
                        (sheep_code, report_date, rd_exp, net_profit, net_profit_growth, total_revenue)
                        VALUES 
                        (:sheep_code, :report_date, :rd_exp, :net_profit, :net_profit_growth, :total_revenue)
                        ON DUPLICATE KEY UPDATE
                            rd_exp = VALUES(rd_exp),
                            net_profit = VALUES(net_profit),
                            net_profit_growth = VALUES(net_profit_growth),
                            total_revenue = VALUES(total_revenue),
                            updated_at = CURRENT_TIMESTAMP
                    """)
                    db.execute(query, {
                        'sheep_code': data['sheep_code'],
                        'report_date': data['report_date'],
                        'rd_exp': data.get('rd_exp'),
                        'net_profit': data.get('net_profit'),
                        'net_profit_growth': data.get('net_profit_growth'),
                        'total_revenue': data.get('total_revenue')
                    })
                
                db.commit()
                # logger.info(f"批量保存财务数据完成，共 {len(data_list)} 条")
        except Exception as e:
            logger.error(f"批量保存财务数据失败: {e}", exc_info=True)
            raise
    
    @staticmethod
    def get_latest_report_date(sheep_code: str) -> Optional[date]:
        """
        获取肥羊最新的报告期日期
        
        Args:
            sheep_code: 肥羊代码
            
        Returns:
            最新报告期日期，如果没有数据返回None
        """
        try:
            with get_db() as db:
                query = text("""
                    SELECT MAX(report_date) 
                    FROM sheep_financials 
                    WHERE sheep_code = :sheep_code
                """)
                result = db.execute(query, {'sheep_code': sheep_code})
                row = result.fetchone()
                if row and row[0]:
                    return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
        except Exception as e:
            logger.error(f"获取最新报告期失败: {e}")
        return None
    
    @staticmethod
    def cleanup_old_data(retention_days: int = 1825):
        """
        清理旧数据（默认保留5年）
        
        Args:
            retention_days: 保留天数
            
        Returns:
            删除的记录数
        """
        try:
            with get_db() as db:
                from datetime import timedelta
                cutoff_date = date.today() - timedelta(days=retention_days)
                
                delete_query = text("""
                    DELETE FROM sheep_financials 
                    WHERE report_date < :cutoff_date
                """)
                result = db.execute(delete_query, {'cutoff_date': cutoff_date})
                db.commit()
                
                count = result.rowcount
                if count > 0:
                    logger.info(f"清理财务数据旧数据完成，删除了 {count} 条记录（保留最近{retention_days}天）")
                else:
                    logger.info(f"无需清理财务数据（保留最近{retention_days}天）")
                
                return count
        except Exception as e:
            logger.error(f"清理财务数据失败: {e}", exc_info=True)
            return 0
