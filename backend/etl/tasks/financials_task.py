from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df
import pandas as pd
import time
import logging

logger = logging.getLogger(__name__)

class FinancialsTask(BaseTask):
    def sync_quarterly_income(self, ts_code: str = None, force_sync: bool = False):
        """同步季度利润表数据
        
        策略:
        - 首次全量同步: 获取所有历史季度数据
        - 增量同步: 只获取最新季度数据
        - Long token: 串行获取，有限流保护
        - Short token: 批量获取，速度快
        
        Args:
            ts_code: 指定股票代码，不指定则同步全部
            force_sync: 强制重新同步已存在的季度
        """
        from datetime import datetime
        from etl.config import settings
        
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        is_short = settings.tushare_token_type == "short"
        
        self.logger.info(f"开始季度利润表同步 (token: {'short' if is_short else 'long'}, force: {force_sync})")
        
        if ts_code:
            stocks = [ts_code]
        else:
            stocks = fetch_df("SELECT ts_code FROM stock_basic")['ts_code'].tolist()
        
        if not stocks:
            self.logger.warning("没有股票数据")
            return
        
        self.logger.info(f"待同步股票数: {len(stocks)}")
        
        target_cols = self._get_income_columns()
        
        if is_short:
            self._sync_short_token(stocks, force_sync, target_cols)
        else:
            self._sync_long_token(stocks, force_sync, target_cols, current_year, current_month)
        
        self.logger.info("季度利润表同步完成")

    def _get_income_columns(self):
        return [
            'ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 
            'comp_type', 'basic_eps', 'diluted_eps', 'total_revenue', 'revenue',
            'int_income', 'prem_earned', 'comm_income', 'n_commis_income',
            'n_oth_income', 'n_oth_b_income', 'prem_income', 'total_cogs', 'oper_cost',
            'int_exp', 'comm_exp', 'biz_tax_surchg', 'sell_exp', 'admin_exp', 
            'fin_exp', 'assets_impair_loss', 'operate_profit', 'total_profit',
            'income_tax', 'n_income', 'n_income_attr_p', 'minority_gain'
        ]

    def _get_existing_quarters(self):
        """获取已存在的季度列表"""
        try:
            df = fetch_df("SELECT DISTINCT end_date FROM stock_income")
            return set(
                df['end_date'].astype(str).str.replace('-', '', regex=False).tolist()
            )
        except:
            return set()

    def _get_quarters_to_sync(self, existing_quarters: set, force_sync: bool, year: int, month: int) -> list:
        """获取需要同步的季度列表
        
        财报发布规律:
        - Q1 (0331): 当年4月发布
        - Q2 (0630): 当年8月发布
        - Q3 (0930): 当年11月发布  
        - Q4 (1231): 次年3月发布(年报)
        """
        all_quarters = []
        
        for y in range(2015, year):
            all_quarters.extend([f"{y}0331", f"{y}0630", f"{y}0930", f"{y}1231"])
        
        if month >= 4:
            all_quarters.append(f"{year}0331")
        if month >= 8:
            all_quarters.append(f"{year}0630")
        if month >= 11:
            all_quarters.append(f"{year}0930")
        
        if force_sync:
            return all_quarters
        
        return [q for q in all_quarters if q not in existing_quarters]

    def _sync_short_token(self, stocks: list, force_sync: bool, target_cols: list):
        """Short token: 批量快速获取"""
        self.logger.info("使用 Short Token 批量同步...")
        
        existing_quarters = self._get_existing_quarters()
        self.logger.info(f"已存在季度: {len(existing_quarters)} 个")
        
        if not force_sync and len(existing_quarters) >= 40:
            self.logger.info("已有完整历史数据，跳过同步")
            return
        
        success_count = 0
        batch_size = 100
        
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            ts_codes = ','.join(batch)
            
            try:
                df = self.provider.income_vip(ts_code=ts_codes)
                if df.empty:
                    continue
                
                df['ann_date'] = pd.to_datetime(df['ann_date'], errors='coerce').dt.date
                df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date
                df['f_ann_date'] = pd.to_datetime(df['f_ann_date'], errors='coerce').dt.date
                
                if not force_sync:
                    existing = set()
                    for _, row in df.iterrows():
                        key = str(row['end_date']).replace('-', '')
                        existing.add((row['ts_code'], key))
                    
                    existing_db = self._get_existing_with_stock([s for s in batch if s in df['ts_code'].values])
                    df = df[~df.apply(lambda x: (x['ts_code'], str(x['end_date']).replace('-', '')) in existing_db, axis=1)]
                
                if df.empty:
                    continue
                
                available = [c for c in target_cols if c in df.columns]
                df = df[available]
                
                with get_db_connection() as con:
                    con.register('df_view', df)
                    cols = df.columns.tolist()
                    col_str = ','.join(cols)
                    update_set = ','.join([f"{c}=EXCLUDED.{c}" for c in cols])
                    con.execute(f"INSERT INTO stock_income ({col_str}) SELECT {col_str} FROM df_view ON CONFLICT (ts_code, end_date, report_type) DO UPDATE SET {update_set}")
                    success_count += len(df)
                
                self.logger.info(f"批次 {i//batch_size+1}: +{len(df)} 条")
                
            except Exception as e:
                self.logger.warning(f"批次 {i//batch_size+1} 失败: {str(e)[:50]}")
            
            if (i // batch_size + 1) % 10 == 0:
                self.logger.info(f"进度: {min(i+batch_size, len(stocks))}/{len(stocks)}, 总计 +{success_count}")
        
        self.logger.info(f"Short Token 同步完成: +{success_count} 条")

    def _get_existing_with_stock(self, stock_codes: list) -> set:
        """获取指定股票已存在的季度"""
        if not stock_codes:
            return set()
        try:
            codes_str = ','.join(stock_codes)
            df = fetch_df(f"SELECT ts_code, end_date FROM stock_income WHERE ts_code IN ({codes_str})")
            return set(zip(df['ts_code'], df['end_date'].astype(str).str.replace('-', '')))
        except:
            return set()

    def _sync_long_token(self, stocks: list, force_sync: bool, target_cols: list, year: int, month: int):
        """Long token: 串行获取，有限流保护
        
        分批策略:
        - 每天只同步500条记录
        - 记录已同步的股票，次日起接着同步剩余股票
        """
        self.logger.info("使用 Long Token 串行同步...")
        
        # 每日限制: 500条记录
        DAILY_LIMIT = 500
        
        existing_quarters = self._get_existing_quarters()
        quarters_to_sync = self._get_quarters_to_sync(existing_quarters, force_sync, year, month)
        
        self.logger.info(f"已存在: {len(existing_quarters)} 个季度, 需同步: {len(quarters_to_sync)} 个季度")
        
        if not quarters_to_sync:
            self.logger.info("没有需要同步的季度")
            return
        
        latest_quarter = quarters_to_sync[-1]
        self.logger.info(f"同步最新季度: {latest_quarter}")
        
        # 获取最新季度已同步的股票
        synced_stocks = self._get_synced_stocks_for_quarter(latest_quarter)
        total_stocks = len(stocks)
        unsynced_count = total_stocks - len(synced_stocks)
        self.logger.info(f"待同步: {unsynced_count}/{total_stocks} 只股票")
        
        success_count = 0
        synced_count = 0
        
        for idx, stock_code in enumerate(stocks):
            # 检查是否已达每日限制
            if success_count >= DAILY_LIMIT:
                self.logger.info(f"已达到每日限制 {DAILY_LIMIT} 条，停止同步")
                break
            
            # 跳过已同步的
            if stock_code in synced_stocks:
                continue
            
            try:
                df = self.provider.income(ts_code=stock_code, end_date=latest_quarter)
                if df.empty:
                    continue
                
                df['ann_date'] = pd.to_datetime(df['ann_date'], errors='coerce').dt.date
                df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date
                df['f_ann_date'] = pd.to_datetime(df['f_ann_date'], errors='coerce').dt.date
                
                available = [c for c in target_cols if c in df.columns]
                df = df[available]
                
                with get_db_connection() as con:
                    con.register('df_view', df)
                    cols = df.columns.tolist()
                    col_str = ','.join(cols)
                    update_set = ','.join([f"{c}=EXCLUDED.{c}" for c in cols])
                    con.execute(f"INSERT INTO stock_income ({col_str}) SELECT {col_str} FROM df_view ON CONFLICT (ts_code, end_date, report_type) DO UPDATE SET {update_set}")
                    success_count += len(df)
                    synced_count += 1
                    
            except Exception as e:
                if "权限" in str(e) or "无权限" in str(e):
                    self.logger.warning(f"权限不足，停止同步")
                    break
                if "限流" in str(e) or "频繁" in str(e):
                    self.logger.warning(f"触发限流，暂停 60秒")
                    time.sleep(60)
            
            if (idx + 1) % 100 == 0:
                self.logger.info(f"进度: {idx+1}/{total_stocks}, +{success_count} 条")
                time.sleep(2)
        
        self.logger.info(f"Long Token 同步完成: +{success_count} 条 ({synced_count} 只股票)")

    def _get_synced_stocks_for_quarter(self, quarter: str) -> set:
        """获取指定季度已同步的股票代码"""
        try:
            quarter_date = f"{quarter[:4]}-{quarter[4:6]}-{quarter[6:8]}"
            df = fetch_df("""
                SELECT DISTINCT ts_code FROM stock_income 
                WHERE end_date = ?
            """, params=[quarter_date])
            return set(df['ts_code'].tolist() if not df.empty else [])
        except:
            return set()

    def sync_fina_indicator(self, ts_code: str = None):
        """同步财务指标"""
        self.logger.info("开始同步财务指标...")
        
        stocks = [ts_code] if ts_code else fetch_df("SELECT ts_code FROM stock_basic")['ts_code'].tolist()
        
        if not stocks:
            return
        
        target_cols = [
            'ts_code', 'ann_date', 'end_date', 'eps', 'eps_yoy', 'bvps',
            'roe', 'roe_yoy', 'net_profit_margin', 'net_profit_margin_yoy',
            'gross_profit_margin', 'gross_profit_margin_yoy',
            'total_rev', 'total_rev_yoy', 'rev_ps', 'profit', 'profit_yoy', 'profit_ps'
        ]
        
        success_count = 0
        for idx, ts_code in enumerate(stocks):
            try:
                df = self.provider.fina_indicator(ts_code=ts_code)
                if df.empty:
                    continue
                
                # 列名映射: Tushare API 返回的列名可能不带下划线
                col_mapping = {
                    'grossprofit_margin': 'gross_profit_margin',
                    'netprofit_margin': 'net_profit_margin',
                    'grossprofit_margin_yoy': 'gross_profit_margin_yoy',
                    'netprofit_margin_yoy': 'net_profit_margin_yoy'
                }
                for api_col, db_col in col_mapping.items():
                    if api_col in df.columns and db_col not in df.columns:
                        df[db_col] = df[api_col]
                
                df['ann_date'] = pd.to_datetime(df['ann_date'], errors='coerce').dt.date
                df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date
                
                for f in target_cols:
                    if f not in df.columns:
                        df[f] = None
                
                df = df[[f for f in target_cols if f in df.columns]]
                
                with get_db_connection() as con:
                    con.register('df_view', df)
                    cols = df.columns.tolist()
                    col_str = ','.join(cols)
                    con.execute(f"INSERT INTO stock_fina_indicator ({col_str}) SELECT {col_str} FROM df_view ON CONFLICT DO NOTHING")
                    success_count += len(df)
                    
            except Exception as e:
                if "权限" in str(e):
                    self.logger.warning(f"权限不足: {str(e)[:50]}")
                    break
            
            if (idx + 1) % 500 == 0:
                self.logger.info(f"进度: {idx+1}/{len(stocks)}")
                time.sleep(1)
        
        self.logger.info(f"财务指标同步完成: +{success_count} 条")
