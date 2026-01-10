"""
股票数据仓储层
"""
from typing import List, Dict, Optional
from datetime import date
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class StockRepository:
    """股票数据仓储"""
    
    @staticmethod
    def upsert_stock_basic(stock_code: str, stock_name: str, market: Optional[str] = None, 
                          industry: Optional[str] = None, list_date: Optional[date] = None):
        """插入或更新股票基本信息"""
        with get_db() as db:
            query = text("""
                INSERT INTO stock_basic (stock_code, stock_name, market, industry, list_date)
                VALUES (:code, :name, :market, :industry, :list_date)
                ON DUPLICATE KEY UPDATE
                    stock_name = VALUES(stock_name),
                    market = VALUES(market),
                    industry = VALUES(industry),
                    list_date = VALUES(list_date)
            """)
            db.execute(query, {
                'code': stock_code,
                'name': stock_name,
                'market': market,
                'industry': industry,
                'list_date': list_date
            })
    
    @staticmethod
    def batch_upsert_stock_daily(data_list: List[Dict]):
        """批量插入或更新股票日K数据"""
        if not data_list:
            return
        
        with get_db() as db:
            query = text("""
                INSERT INTO stock_daily 
                (stock_code, trade_date, open_price, close_price, high_price, low_price,
                 volume, amount, turnover_rate, change_pct, ma5, ma10, ma20, ma30, ma60)
                VALUES 
                (:code, :date, :open, :close, :high, :low, :volume, :amount, 
                 :turnover_rate, :change_pct, :ma5, :ma10, :ma20, :ma30, :ma60)
                ON DUPLICATE KEY UPDATE
                    open_price = VALUES(open_price),
                    close_price = VALUES(close_price),
                    high_price = VALUES(high_price),
                    low_price = VALUES(low_price),
                    volume = VALUES(volume),
                    amount = VALUES(amount),
                    turnover_rate = VALUES(turnover_rate),
                    change_pct = VALUES(change_pct),
                    ma5 = VALUES(ma5),
                    ma10 = VALUES(ma10),
                    ma20 = VALUES(ma20),
                    ma30 = VALUES(ma30),
                    ma60 = VALUES(ma60)
            """)
            
            # 分批插入
            batch_size = 1000
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                db.execute(query, batch)
    
    @staticmethod
    def search_stocks(keyword: str, limit: int = 20) -> List[Dict]:
        """搜索股票"""
        with get_db() as db:
            keyword_clean = keyword.strip().upper().replace("SZ", "").replace("SH", "")
            
            if keyword_clean.isdigit():
                # 代码搜索
                query = text("""
                    SELECT stock_code, stock_name, industry
                    FROM stock_basic
                    WHERE stock_code LIKE :kw AND is_active = 1
                    ORDER BY 
                        CASE WHEN stock_code = :full_kw THEN 0 ELSE 1 END,
                        stock_code ASC
                    LIMIT :limit
                """)
                result = db.execute(query, {
                    'kw': f"{keyword_clean}%",
                    'full_kw': keyword_clean,
                    'limit': limit
                })
            else:
                # 名称搜索
                query = text("""
                    SELECT stock_code, stock_name, industry
                    FROM stock_basic
                    WHERE stock_name LIKE :kw AND is_active = 1
                    ORDER BY 
                        CASE WHEN stock_name = :full_kw THEN 0 
                             WHEN stock_name LIKE :prefix_kw THEN 1 
                             ELSE 2 END,
                        stock_code
                    LIMIT :limit
                """)
                result = db.execute(query, {
                    'kw': f"%{keyword_clean}%",
                    'full_kw': keyword_clean,
                    'prefix_kw': f"{keyword_clean}%",
                    'limit': limit
                })
            
            return [
                {'code': row[0], 'name': row[1], 'industry': row[2]}
                for row in result
            ]
    
    @staticmethod
    def get_stock_daily(stock_code: str, limit: int = 90) -> List[Dict]:
        """获取股票日K数据"""
        with get_db() as db:
            query = text("""
                SELECT trade_date, open_price, close_price, high_price, low_price,
                       volume, amount, ma5, ma10, ma20, ma30, ma60
                FROM stock_daily
                WHERE stock_code = :code
                ORDER BY trade_date ASC
                LIMIT :limit
            """)
            result = db.execute(query, {'code': stock_code, 'limit': limit})
            
            return [
                {
                    'trade_date': row[0].strftime('%Y-%m-%d') if row[0] else None,
                    'open_price': float(row[1]) if row[1] else None,
                    'close_price': float(row[2]) if row[2] else None,
                    'high_price': float(row[3]) if row[3] else None,
                    'low_price': float(row[4]) if row[4] else None,
                    'volume': int(row[5]) if row[5] else 0,
                    'amount': float(row[6]) if row[6] else 0,
                    'ma5': float(row[7]) if row[7] else None,
                    'ma10': float(row[8]) if row[8] else None,
                    'ma20': float(row[9]) if row[9] else None,
                    'ma30': float(row[10]) if row[10] else None,
                    'ma60': float(row[11]) if row[11] else None,
                }
                for row in result
            ]
    
    @staticmethod
    def get_stock_concepts(stock_code: str) -> List[Dict]:
        """获取股票的概念列表"""
        with get_db() as db:
            query = text("""
                SELECT ct.concept_name, scm.weight
                FROM stock_concept_mapping scm
                INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                WHERE scm.stock_code = :code
                  AND ct.is_active = 1
                ORDER BY scm.weight DESC, ct.concept_name ASC
            """)
            result = db.execute(query, {'code': stock_code})
            
            return [
                {
                    'concept_name': row[0],
                    'weight': float(row[1]) if row[1] else 1.0
                }
                for row in result
            ]
