import time
import pandas as pd
import arrow
from etl.sync import sync_engine
from db.connection import fetch_df, get_db_connection

def safe_backfill(days=3):
    print(f"开始安全补全最近 {days} 天的数据...")
    
    # 1. 获取所有股票
    stocks_df = fetch_df("SELECT ts_code FROM stock_basic")
    if stocks_df.empty:
        print("未发现股票基础信息，请先运行 sync_stock_basic")
        return
    
    all_stocks = stocks_df['ts_code'].tolist()
    
    # 2. 确定日期范围
    end_date = arrow.now().format("YYYYMMDD")
    start_date = arrow.now().shift(days=-days).format("YYYYMMDD")
    
    print(f"同步范围: {start_date} 至 {end_date}, 共 {len(all_stocks)} 只股票")
    
    count = 0
    success = 0
    for ts_code in all_stocks:
        count += 1
        try:
            # 获取该股最近几日的行情
            # 这里调用 provider.daily, 它会自动使用 ak.stock_zh_a_hist (因为指定了 ts_code)
            df = sync_engine.provider.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            if not df.empty:
                # 处理数据格式并保存
                df['factors'] = '{}'
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                if 'adj_factor' not in df.columns:
                    df['adj_factor'] = 1.0
                
                # 保存到数据库
                cols = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'factors', 'adj_factor']
                df_to_save = df[cols]
                with get_db_connection() as con:
                    con.execute("INSERT INTO daily_price SELECT * FROM df_to_save ON CONFLICT (trade_date, ts_code) DO NOTHING")
                
                success += 1
            
            if count % 10 == 0:
                print(f"进度: {count}/{len(all_stocks)}, 成功: {success}")
            
            # 关键：延时避免封 IP
            time.sleep(0.5)
            
        except Exception as e:
            print(f"同步 {ts_code} 失败: {e}")
            time.sleep(2)

if __name__ == "__main__":
    safe_backfill()
