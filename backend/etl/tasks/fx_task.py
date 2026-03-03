from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class FxTask(BaseTask):
    def sync(self):
        """同步外汇/宏观数据 - 只保留最近7天"""
        self.logger.info("开始同步外汇数据 (最近7天)...")
        
        try:
            df = self.provider.fx_daily()
            if df.empty:
                self.logger.warning("API返回空数据")
                return
            
            target_codes = ['USDOLLAR', 'USDCNH.FXCM']
            df = df[df['ts_code'].isin(target_codes)]
            
            if df.empty:
                self.logger.warning("没有找到目标外汇数据")
                return
            
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            
            # 只保留最近7天的数据
            cutoff_date = (datetime.now() - timedelta(days=7)).date()
            df = df[df['trade_date'] >= cutoff_date]
            
            if df.empty:
                self.logger.info("没有最近7天的数据")
                return
            
            target_cols = ['ts_code', 'trade_date', 'bid_open', 'bid_close', 'bid_high', 'bid_low',
                          'ask_open', 'ask_close', 'ask_high', 'ask_low']
            for col in target_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[[c for c in target_cols if c in df.columns]]
            
            with get_db_connection() as con:
                # 先删除旧数据
                for code in target_codes:
                    con.execute(f"DELETE FROM fx_daily WHERE ts_code = '{code}' AND trade_date < '{cutoff_date}'")
                
                # 插入新数据
                con.register('df_view', df)
                cols = df.columns.tolist()
                col_str = ','.join(cols)
                con.execute(f"INSERT INTO fx_daily ({col_str}) SELECT {col_str} FROM df_view ON CONFLICT DO NOTHING")
            
            self.logger.info(f"外汇数据同步完成: +{len(df)} 条")
            
        except Exception as e:
            self.logger.error(f"外汇数据同步失败: {str(e)[:100]}")
