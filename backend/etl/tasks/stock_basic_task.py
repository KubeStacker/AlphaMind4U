import logging
logger = logging.getLogger(__name__)
from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection
import pandas as pd

class StockBasicTask(BaseTask):
    def sync(self) -> int:
        """ 同步股票基础信息 """
        logger.info(f"正在使用 {self.provider.provider_name} 同步股票基础信息...")
        df = self.provider.stock_basic()
        
        if not df.empty:
            # list_date, delist_date 格式清洗
            if 'list_date' in df.columns:
                df['list_date'] = pd.to_datetime(df['list_date'], errors='coerce').dt.date
            if 'delist_date' in df.columns:
                df['delist_date'] = pd.to_datetime(df['delist_date'], errors='coerce').dt.date

            # 确保列存在
            required_cols = [
                'ts_code', 'symbol', 'name', 'area', 'industry', 'market', 'list_date',
                'fullname', 'enname', 'curr_type', 'list_status', 'is_hs',
                'delist_date', 'exchange', 'cnspell'
            ]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = None

            df = df[required_cols]

            with get_db_connection() as con:
                # DuckDB upsert
                con.execute("""
                    INSERT INTO stock_basic (
                        ts_code, symbol, name, area, industry, market, list_date,
                        fullname, enname, curr_type, list_status, is_hs,
                        delist_date, exchange, cnspell
                    )
                    SELECT ts_code, symbol, name, area, industry, market, list_date,
                           fullname, enname, curr_type, list_status, is_hs,
                           delist_date, exchange, cnspell
                    FROM df
                    ON CONFLICT (ts_code) DO UPDATE SET 
                        symbol=excluded.symbol, 
                        name=excluded.name, 
                        area=excluded.area, 
                        industry=excluded.industry, 
                        market=excluded.market, 
                        list_date=excluded.list_date, 
                        fullname=excluded.fullname, 
                        enname=excluded.enname, 
                        curr_type=excluded.curr_type, 
                        list_status=excluded.list_status, 
                        is_hs=excluded.is_hs,
                        delist_date=excluded.delist_date,
                        exchange=excluded.exchange,
                        cnspell=excluded.cnspell
                """)

            logger.info(f"成功同步 {len(df)} 条股票基础信息")
            return len(df)
        else:
            logger.warning("未获取到股票基础信息")
            return 0
