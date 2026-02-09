from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df
import pandas as pd
import time

class FinancialsTask(BaseTask):
    def sync(self, limit: int = 1000):
        self.logger.info(f"开始同步财务指标 (Limit: {limit})...")
        
        stocks = fetch_df("SELECT ts_code FROM stock_basic")['ts_code'].tolist()
        if not stocks: 
            return
        
        count = 0
        for ts_code in stocks[:limit]:
            try:
                df = self.provider.fina_indicator(ts_code=ts_code)
                if not df.empty:
                    df['ann_date'] = pd.to_datetime(df['ann_date']).dt.date
                    df['end_date'] = pd.to_datetime(df['end_date']).dt.date
                    
                    fields = [
                        'ts_code','ann_date','end_date','eps','dt_eps','total_revenue_ps',
                        'revenue_ps','capital_rese_ps','surplus_rese_ps','undist_profit_ps',
                        'extra_item','profit_dedt','gross_margin','net_profit_margin','roe',
                        'roa','debt_to_assets'
                    ]
                    # 确保字段存在
                    for f in fields:
                        if f not in df.columns:
                            df[f] = None
                            
                    processed = df[fields]
                    
                    with get_db_connection() as con:
                        con.execute("INSERT INTO stock_financials SELECT * FROM processed ON CONFLICT DO NOTHING")
                
                count += 1
                if count % 10 == 0:
                    time.sleep(1)
                else:
                    time.sleep(0.2)
            except Exception as e:
                self.logger.error(f"同步财务数据 {ts_code} 失败: {e}")
