from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class MarginTask(BaseTask):
    def sync(self, days: int = 90):
        """同步融资融券数据
        
        融资融券数据是日频数据，需要每个交易日同步
        调用 margin_detail 不带日期参数获取最新数据
        根据 TUSHARE_TOKEN_TYPE 环境变量选择使用 short 或 long token
        """
        from etl.calendar import trading_calendar
        
        self.logger.info(f"开始同步融资融券数据 (最近{days}天)...")
        
        # 使用 provider (会自动根据环境变量选择 token 类型)
        try:
            df = self.provider.margin_detail()
            if df.empty:
                self.logger.warning("融资融券数据为空")
                return
            
            # 获取返回数据中的所有日期 - 先转换为 YYYY-MM-DD 格式
            df['_orig_date'] = df['trade_date']
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            unique_dates = df['trade_date'].unique()
            self.logger.info(f"获取到融资融券数据: {len(unique_dates)} 个交易日, {len(df)} 条")
            
            # 过滤ts_code
            df = df[df['ts_code'].notna()]
            if df.empty:
                self.logger.warning("没有有效数据")
                return
            
            # 转换日期格式 (已经转换过了)
            target_cols = ['ts_code', 'trade_date', 'rzye', 'rzmre', 'rzche', 'rqye', 'rqmcl', 'rzrqye', 'rqyl']
            for col in target_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[[c for c in target_cols if c in df.columns]]
            
            # 保存到数据库 - 先删除所有涉及到的日期数据再插入
            dates_to_delete = list(unique_dates)
            with get_db_connection() as con:
                for db_date in dates_to_delete:
                    con.execute(f"DELETE FROM stock_margin WHERE trade_date = '{db_date}'")
                # 插入新数据
                con.register('df_view', df)
                cols = df.columns.tolist()
                col_str = ','.join(cols)
                con.execute(f"INSERT INTO stock_margin ({col_str}) SELECT {col_str} FROM df_view")
                # 验证
                for db_date in dates_to_delete:
                    verify = con.execute(f"SELECT COUNT(*) FROM stock_margin WHERE trade_date = '{db_date}'").fetchone()
                    self.logger.info(f"融资融券 {db_date} 同步完成: {verify[0]} 条")
            
            self.logger.info(f"融资融券同步完成")
            
        except Exception as e:
            self.logger.error(f"融资融券同步失败: {e}")
