"""
大盘指数数据仓储层
"""
from typing import List, Dict, Optional
from datetime import date
from sqlalchemy import text
from db.database import get_db, get_raw_connection
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class IndexRepository:
    """大盘指数数据仓储"""
    
    @staticmethod
    def save_index_daily_data(df: pd.DataFrame, index_code: str = 'CSI1000'):
        """
        保存指数日K数据到数据库
        
        Args:
            df: 包含指数数据的DataFrame，必须包含以下列：
                - trade_date: 交易日期
                - open_price: 开盘价
                - close_price: 收盘价
                - high_price: 最高价
                - low_price: 最低价
                - volume: 成交量（可选）
                - amount: 成交额（可选）
            index_code: 指数代码，默认CSI1000
        """
        if df.empty:
            logger.warning(f"指数数据为空，跳过保存: {index_code}")
            return
        
        try:
            with get_raw_connection() as conn:
                saved_count = 0
                updated_count = 0
                
                for _, row in df.iterrows():
                    try:
                        trade_date = row['trade_date']
                        if pd.isna(trade_date):
                            continue
                        
                        # 确保日期为date类型
                        if isinstance(trade_date, str):
                            from datetime import datetime
                            trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
                        elif hasattr(trade_date, 'date'):
                            trade_date = trade_date.date()
                        
                        # 获取指数名称
                        index_name = row.get('index_name', '中证1000')
                        
                        # 准备数据
                        open_price = round(float(row.get('open_price', 0)), 1) if pd.notna(row.get('open_price')) else None
                        close_price = round(float(row.get('close_price', 0)), 1) if pd.notna(row.get('close_price')) else None
                        high_price = round(float(row.get('high_price', 0)), 1) if pd.notna(row.get('high_price')) else None
                        low_price = round(float(row.get('low_price', 0)), 1) if pd.notna(row.get('low_price')) else None
                        volume = int(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0
                        amount = round(float(row.get('amount', 0)), 2) if pd.notna(row.get('amount')) else 0
                        
                        # 计算涨跌幅（如果有前一日数据）
                        change_pct = None
                        if close_price and open_price:
                            change_pct = round(((close_price - open_price) / open_price) * 100, 1)
                        
                        # 使用INSERT ... ON DUPLICATE KEY UPDATE
                        query = text("""
                            INSERT INTO market_index_daily
                            (index_code, index_name, trade_date, open_price, close_price, 
                             high_price, low_price, volume, amount, change_pct)
                            VALUES
                            (:index_code, :index_name, :trade_date, :open_price, :close_price,
                             :high_price, :low_price, :volume, :amount, :change_pct)
                            ON DUPLICATE KEY UPDATE
                                open_price = VALUES(open_price),
                                close_price = VALUES(close_price),
                                high_price = VALUES(high_price),
                                low_price = VALUES(low_price),
                                volume = VALUES(volume),
                                amount = VALUES(amount),
                                change_pct = VALUES(change_pct),
                                updated_at = CURRENT_TIMESTAMP
                        """)
                        
                        with get_db() as db:
                            db.execute(query, {
                                'index_code': index_code,
                                'index_name': index_name,
                                'trade_date': trade_date,
                                'open_price': open_price,
                                'close_price': close_price,
                                'high_price': high_price,
                                'low_price': low_price,
                                'volume': volume,
                                'amount': amount,
                                'change_pct': change_pct
                            })
                            db.commit()
                        
                        # 判断是新增还是更新
                        # 简化处理：假设都是更新（因为使用了ON DUPLICATE KEY UPDATE）
                        updated_count += 1
                        
                    except Exception as e:
                        logger.error(f"保存指数数据失败 (trade_date={row.get('trade_date')}): {e}")
                        continue
                
                logger.info(f"指数数据保存完成: {index_code}, 处理 {len(df)} 条，更新 {updated_count} 条")
                
        except Exception as e:
            logger.error(f"保存指数数据失败: {index_code}, 错误: {e}", exc_info=True)
            raise
    
    @staticmethod
    def get_index_daily_data(index_code: str = 'CSI1000', 
                            start_date: Optional[date] = None,
                            end_date: Optional[date] = None) -> pd.DataFrame:
        """
        从数据库获取指数日K数据
        
        Args:
            index_code: 指数代码，默认CSI1000
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DataFrame，包含指数数据
        """
        try:
            with get_raw_connection() as conn:
                conditions = ["index_code = %s"]
                params = [index_code]
                
                if start_date:
                    conditions.append("trade_date >= %s")
                    params.append(start_date)
                
                if end_date:
                    conditions.append("trade_date <= %s")
                    params.append(end_date)
                
                where_clause = "WHERE " + " AND ".join(conditions)
                
                query = f"""
                    SELECT 
                        index_code, index_name, trade_date,
                        open_price, close_price, high_price, low_price,
                        volume, amount, change_pct
                    FROM market_index_daily
                    {where_clause}
                    ORDER BY trade_date ASC
                """
                
                df = pd.read_sql(query, conn, params=params)
                
                if not df.empty:
                    # 转换日期格式
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                
                return df
                
        except Exception as e:
            logger.error(f"获取指数数据失败: {index_code}, 错误: {e}", exc_info=True)
            return pd.DataFrame()
    
    @staticmethod
    def get_latest_trade_date(index_code: str = 'CSI1000') -> Optional[date]:
        """获取最新的交易日期"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT MAX(trade_date) as max_date
                    FROM market_index_daily
                    WHERE index_code = :index_code
                """)
                result = db.execute(query, {'index_code': index_code})
                row = result.fetchone()
                if row and row[0]:
                    return row[0] if isinstance(row[0], date) else None
                return None
        except Exception as e:
            logger.error(f"获取最新交易日期失败: {index_code}, 错误: {e}")
            return None
    
    @staticmethod
    def get_index_data_count_for_date(trade_date: date, index_code: str = 'CSI1000') -> int:
        """获取某个交易日的指数数据数量（通常为1或0）"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT COUNT(*) as count
                    FROM market_index_daily
                    WHERE index_code = :index_code AND trade_date = :trade_date
                """)
                result = db.execute(query, {'index_code': index_code, 'trade_date': trade_date})
                row = result.fetchone()
                if row and row[0]:
                    return int(row[0])
                return 0
        except Exception as e:
            logger.error(f"获取交易日 {trade_date} 的指数数据数量失败: {e}")
            return 0
