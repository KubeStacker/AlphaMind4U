from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class ForexDataTask(BaseTask):
    """外汇数据同步任务
    
    负责同步外汇/宏观数据，只保留最近7天数据
    """
    
    def sync_forex_data(self):
        """同步外汇/宏观数据 - 只保留最近7天"""
        logger.info("开始同步外汇数据 (最近7天)...")
        
        try:
            df = self.provider.fx_daily()
            if df.empty:
                logger.warning("API返回空数据")
                return
            
            target_codes = ['USDOLLAR', 'USDCNH.FXCM']
            df = df[df['ts_code'].isin(target_codes)]
            
            if df.empty:
                logger.warning("没有找到目标外汇数据")
                return
            
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            
            # 只保留最近7天的数据
            cutoff_date = (datetime.now() - timedelta(days=7)).date()
            df = df[df['trade_date'] >= cutoff_date]
            
            if df.empty:
                logger.info("没有最近7天的数据")
                return
            
            target_cols = ['ts_code', 'trade_date', 'bid_open', 'bid_close', 'bid_high', 'bid_low',
                          'ask_open', 'ask_close', 'ask_high', 'ask_low']
            for col in target_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[[c for c in target_cols if c in df.columns]]
            
            self._upsert_forex_data(df, target_codes, cutoff_date)
            
            logger.info(f"外汇数据同步完成: +{len(df)} 条")
            
        except Exception as e:
            logger.error(f"外汇数据同步失败: {str(e)[:100]}")

    def _upsert_forex_data(self, df: pd.DataFrame, target_codes: list, cutoff_date):
        """更新或插入外汇数据
        
        Args:
            df: 包含外汇数据的DataFrame
            target_codes: 目标外汇代码列表
            cutoff_date: 截止日期，早于此日期的数据将被删除
        """
        with get_db_connection() as con:
            try:
                # 开始事务
                con.begin()
                
                # 先删除旧数据（使用参数化查询）
                for code in target_codes:
                    con.execute("DELETE FROM fx_daily WHERE ts_code = ? AND trade_date < ?", [code, cutoff_date])
                
                # 插入新数据
                con.register('df_view', df)
                cols = df.columns.tolist()
                col_str = ','.join(cols)
                con.execute(f"INSERT INTO fx_daily ({col_str}) SELECT {col_str} FROM df_view ON CONFLICT DO NOTHING")
                
                # 提交事务
                con.commit()
                
            except Exception as e:
                # 回滚事务
                con.rollback()
                logger.error(f"外汇数据同步事务失败，已回滚: {e}")
                raise
