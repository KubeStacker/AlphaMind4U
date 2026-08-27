from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df
from etl.utils.art_safe_write import dedupe_by_key, update_then_insert_missing
import pandas as pd
import time
import arrow
import logging
from etl.calendar import trading_calendar

logger = logging.getLogger(__name__)


class FinancialsTask(BaseTask):
    def sync_quarterly_income(self, ts_code: str = None, force_sync: bool = False):
        """同步季度利润表数据"""
        from datetime import datetime
        from core.config import settings

        current_year = datetime.now().year
        current_month = datetime.now().month
        is_short = settings.tushare_token_type == "short"

        logger.info(f"开始季度利润表同步 (token: {'short' if is_short else 'long'}, force: {force_sync})")
        stocks = [ts_code] if ts_code else fetch_df("SELECT ts_code FROM stock_basic")['ts_code'].tolist()
        if not stocks:
            logger.warning("没有股票数据")
            return

        target_cols = self._get_income_columns()
        if is_short:
            self._sync_short_token(stocks, force_sync, target_cols, "income", "stock_income")
        else:
            self._sync_long_token(stocks, force_sync, target_cols, current_year, current_month, "income", "stock_income")
        logger.info("季度利润表同步完成")

    def sync_balance_sheet(self, ts_code: str = None, force_sync: bool = False):
        """同步资产负债表数据"""
        from datetime import datetime
        from core.config import settings

        current_year = datetime.now().year
        current_month = datetime.now().month
        is_short = settings.tushare_token_type == "short"

        logger.info(f"开始资产负债表同步 (token: {'short' if is_short else 'long'}, force: {force_sync})")
        stocks = [ts_code] if ts_code else fetch_df("SELECT ts_code FROM stock_basic")['ts_code'].tolist()
        if not stocks:
            logger.warning("没有股票数据")
            return

        target_cols = self._get_balance_sheet_columns()
        if is_short:
            self._sync_short_token(stocks, force_sync, target_cols, "balancesheet", "stock_balance_sheet")
        else:
            self._sync_long_token(stocks, force_sync, target_cols, current_year, current_month, "balancesheet", "stock_balance_sheet")
        logger.info("资产负债表同步完成")

    def sync_cashflow(self, ts_code: str = None, force_sync: bool = False):
        """同步现金流量表数据"""
        from datetime import datetime
        from core.config import settings

        current_year = datetime.now().year
        current_month = datetime.now().month
        is_short = settings.tushare_token_type == "short"

        logger.info(f"开始现金流量表同步 (token: {'short' if is_short else 'long'}, force: {force_sync})")
        stocks = [ts_code] if ts_code else fetch_df("SELECT ts_code FROM stock_basic")['ts_code'].tolist()
        if not stocks:
            logger.warning("没有股票数据")
            return

        target_cols = self._get_cashflow_columns()
        if is_short:
            self._sync_short_token(stocks, force_sync, target_cols, "cashflow", "stock_cashflow")
        else:
            self._sync_long_token(stocks, force_sync, target_cols, current_year, current_month, "cashflow", "stock_cashflow")
        logger.info("现金流量表同步完成")

    def _get_income_columns(self):
        return [
            'ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 
            'comp_type', 'end_type', 'basic_eps', 'diluted_eps', 'total_revenue', 'revenue',
            'int_income', 'prem_earned', 'comm_income', 'n_commis_income',
            'n_oth_income', 'n_oth_b_income', 'prem_income', 'total_cogs', 'oper_cost',
            'int_exp', 'comm_exp', 'biz_tax_surchg', 'sell_exp', 'admin_exp', 
            'fin_exp', 'assets_impair_loss', 'operate_profit', 'total_profit',
            'income_tax', 'n_income', 'n_income_attr_p', 'minority_gain',
            'total_operate_income', 'operate_exp', 'total_operate_cost', 'rd_exp'
        ]

    def _get_balance_sheet_columns(self):
        return [
            'ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 'comp_type',
            'end_type', 'total_share', 'money_cap', 'trad_asset', 'notes_receiv', 'accounts_receiv',
            'oth_receiv', 'prepayment', 'inventories', 'total_cur_assets', 'fix_assets', 'cip',
            'intan_assets', 'goodwill', 'lt_rec', 'lt_eqt_invest', 'total_assets', 'st_borr',
            'notes_payable', 'acct_payable', 'adv_receipts', 'contract_liab', 'payroll_payable',
            'taxes_payable', 'total_cur_liab', 'lt_borr', 'bond_payable', 'total_liab',
            'cap_rese', 'surplus_rese', 'undistr_porfit', 'total_hldr_eqy_exc_min_int',
            'minority_int', 'total_hldr_eqy_inc_min_int'
        ]

    def _get_cashflow_columns(self):
        return [
            'ts_code', 'ann_date', 'f_ann_date', 'end_date', 'comp_type', 'report_type',
            'end_type', 'net_profit', 'n_cashflow_act', 'c_fr_sale_sg', 'recp_tax_rends',
            'n_depos_incr_fi', 'c_paid_goods_s', 'c_paid_to_for_empl', 'c_paid_for_taxes',
            'n_cashflow_inv_act', 'c_disp_withdrwl_invest', 'c_recp_return_invest',
            'c_pay_acq_const_fi_and_ot_la', 'c_paid_invest', 'n_cash_flows_fnc_act',
            'c_recp_borrow', 'c_pay_dist_dpcp_int_exp', 'n_incr_cash_cash_equ',
            'c_cash_equ_beg_period', 'c_cash_equ_end_period', 'free_cashflow'
        ]

    def _get_existing_quarters(self, table_name: str = "stock_income"):
        try:
            df = fetch_df(f"SELECT DISTINCT end_date FROM {table_name}")
            return set(
                df['end_date'].astype(str).str.replace('-', '', regex=False).tolist()
            )
        except:
            return set()

    def _get_quarters_to_sync(self, existing_quarters: set, force_sync: bool, year: int, month: int) -> list:
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

    def _fetch_provider_df(self, func_type: str, ts_code: str, end_date: str = None):
        if func_type == "income":
            return self.provider.income(ts_code=ts_code, end_date=end_date)
        elif func_type == "balancesheet":
            return self.provider.balance_sheet(ts_code=ts_code, end_date=end_date)
        elif func_type == "cashflow":
            return self.provider.cashflow(ts_code=ts_code, end_date=end_date)
        return pd.DataFrame()

    def _sync_short_token(self, stocks: list, force_sync: bool, target_cols: list, func_type: str, table_name: str):
        logger.info(f"使用 Short Token 批量同步 {table_name}...")
        existing_quarters = self._get_existing_quarters(table_name)
        if not force_sync and len(existing_quarters) >= 40:
            logger.info(f"{table_name} 已有完整历史数据，跳过同步")
            return

        success_count = 0
        batch_size = 100

        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            ts_codes = ','.join(batch)
            try:
                if func_type == "income":
                    df = self.provider.income_vip(ts_code=ts_codes)
                else:
                    df = self._fetch_provider_df(func_type, ts_code=ts_codes)
                if df.empty:
                    continue

                for dcol in ('ann_date', 'end_date', 'f_ann_date'):
                    if dcol in df.columns:
                        df[dcol] = pd.to_datetime(df[dcol], errors='coerce').dt.date

                if func_type == "cashflow" and 'c_pay_acq_const_fiolta' in df.columns and 'c_pay_acq_const_fi_and_ot_la' not in df.columns:
                    df['c_pay_acq_const_fi_and_ot_la'] = df['c_pay_acq_const_fiolta']

                available = [c for c in target_cols if c in df.columns]
                df_to_save = df[available].copy()
                df_to_save = dedupe_by_key(
                    df_to_save,
                    key_cols=['ts_code', 'end_date', 'report_type'],
                    table_name=table_name,
                    logger=logger,
                )

                with get_db_connection() as con:
                    update_then_insert_missing(
                        con,
                        df=df_to_save,
                        table_name=table_name,
                        view_name=f"{table_name}_view",
                        columns=available,
                        key_cols=['ts_code', 'end_date', 'report_type'],
                        logger=logger,
                    )
                success_count += len(df_to_save)
            except Exception as e:
                logger.warning(f"{table_name} 批次 {i//batch_size+1} 失败: {str(e)[:50]}")

        logger.info(f"{table_name} Short Token 同步完成: +{success_count} 条")

    def _sync_long_token(self, stocks: list, force_sync: bool, target_cols: list, year: int, month: int, func_type: str, table_name: str):
        logger.info(f"使用 Long Token 串行同步 {table_name}...")
        DAILY_LIMIT = 500
        existing_quarters = self._get_existing_quarters(table_name)
        quarters_to_sync = self._get_quarters_to_sync(existing_quarters, force_sync, year, month)

        if not quarters_to_sync:
            logger.info(f"{table_name} 没有需要同步的季度")
            return

        latest_quarter = quarters_to_sync[-1]
        logger.info(f"同步最新季度: {latest_quarter}")

        synced_stocks = self._get_synced_stocks_for_quarter(latest_quarter, table_name)
        total_stocks = len(stocks)
        unsynced_count = total_stocks - len(synced_stocks)
        logger.info(f"待同步: {unsynced_count}/{total_stocks} 只股票")

        success_count = 0
        synced_count = 0

        for idx, stock_code in enumerate(stocks):
            if success_count >= DAILY_LIMIT:
                logger.info(f"已达到每日限制 {DAILY_LIMIT} 条，停止同步")
                break
            if stock_code in synced_stocks:
                continue

            try:
                df = self._fetch_provider_df(func_type, ts_code=stock_code, end_date=latest_quarter)
                if df.empty:
                    continue

                for dcol in ('ann_date', 'end_date', 'f_ann_date'):
                    if dcol in df.columns:
                        df[dcol] = pd.to_datetime(df[dcol], errors='coerce').dt.date

                if func_type == "cashflow" and 'c_pay_acq_const_fiolta' in df.columns and 'c_pay_acq_const_fi_and_ot_la' not in df.columns:
                    df['c_pay_acq_const_fi_and_ot_la'] = df['c_pay_acq_const_fiolta']

                available = [c for c in target_cols if c in df.columns]
                df_to_save = df[available].copy()
                df_to_save = dedupe_by_key(
                    df_to_save,
                    key_cols=['ts_code', 'end_date', 'report_type'],
                    table_name=table_name,
                    logger=logger,
                )

                with get_db_connection() as con:
                    update_then_insert_missing(
                        con,
                        df=df_to_save,
                        table_name=table_name,
                        view_name=f"{table_name}_view",
                        columns=available,
                        key_cols=['ts_code', 'end_date', 'report_type'],
                        logger=logger,
                    )
                success_count += len(df_to_save)
                synced_count += 1
            except Exception as e:
                if "权限" in str(e) or "无权限" in str(e):
                    logger.warning(f"权限不足，停止同步 {table_name}")
                    break
                if "限流" in str(e) or "频繁" in str(e):
                    logger.warning("触发限流，暂停 60秒")
                    time.sleep(60)

            if (idx + 1) % 100 == 0:
                logger.info(f"进度: {idx+1}/{total_stocks}, +{success_count} 条")
                time.sleep(2)

        logger.info(f"{table_name} Long Token 同步完成: +{success_count} 条 ({synced_count} 只股票)")

    def _get_synced_stocks_for_quarter(self, quarter: str, table_name: str = "stock_income") -> set:
        try:
            quarter_date = f"{quarter[:4]}-{quarter[4:6]}-{quarter[6:8]}"
            df = fetch_df(f"""
                SELECT DISTINCT ts_code FROM {table_name} 
                WHERE end_date = ?
            """, params=[quarter_date])
            return set(df['ts_code'].tolist() if not df.empty else [])
        except:
            return set()

    def sync_fina_indicator(self, ts_code: str = None):
        """同步财务指标 (含25+核心指标)"""
        logger.info("开始同步财务指标...")
        stocks = [ts_code] if ts_code else fetch_df("SELECT ts_code FROM stock_basic")['ts_code'].tolist()
        if not stocks:
            return

        target_cols = [
            'ts_code', 'ann_date', 'end_date', 'eps', 'dt_eps', 'eps_yoy', 'bvps',
            'roe', 'roe_waa', 'roe_dt', 'roe_yoy', 'roa', 'debt_to_assets',
            'current_ratio', 'quick_ratio', 'net_profit_margin', 'net_profit_margin_yoy',
            'gross_profit_margin', 'gross_profit_margin_yoy', 'total_rev', 'total_rev_yoy',
            'rev_ps', 'profit', 'profit_yoy', 'profit_ps', 'ocfps', 'cfps',
            'or_yoy', 'op_yoy', 'ebt_yoy', 'rd_exp', 'update_flag'
        ]

        col_mapping = {
            'grossprofit_margin': 'gross_profit_margin',
            'netprofit_margin': 'net_profit_margin',
            'grossprofit_margin_yoy': 'gross_profit_margin_yoy',
            'netprofit_margin_yoy': 'net_profit_margin_yoy',
        }

        success_count = 0
        for idx, s_code in enumerate(stocks):
            try:
                df = self.provider.fina_indicator(ts_code=s_code)
                if df.empty:
                    continue

                for api_col, db_col in col_mapping.items():
                    if api_col in df.columns and db_col not in df.columns:
                        df[db_col] = df[api_col]

                for dcol in ('ann_date', 'end_date'):
                    if dcol in df.columns:
                        df[dcol] = pd.to_datetime(df[dcol], errors='coerce').dt.date

                for f in target_cols:
                    if f not in df.columns:
                        df[f] = None

                df_to_save = df[target_cols].copy()
                df_to_save = dedupe_by_key(
                    df_to_save,
                    key_cols=['ts_code', 'end_date'],
                    table_name='stock_fina_indicator',
                    logger=logger,
                )

                with get_db_connection() as con:
                    update_then_insert_missing(
                        con,
                        df=df_to_save,
                        table_name='stock_fina_indicator',
                        view_name='fina_indicator_view',
                        columns=target_cols,
                        key_cols=['ts_code', 'end_date'],
                        logger=logger,
                    )
                success_count += len(df_to_save)
            except Exception as e:
                if "权限" in str(e):
                    logger.warning(f"权限不足: {str(e)[:50]}")
                    break

            if (idx + 1) % 500 == 0:
                logger.info(f"进度: {idx+1}/{len(stocks)}")
                time.sleep(1)

        logger.info(f"财务指标同步完成: +{success_count} 条")

    def sync_dividend(self, ts_code: str = None):
        """同步分红送股数据"""
        logger.info("开始同步分红送股数据...")
        stocks = [ts_code] if ts_code else fetch_df("SELECT ts_code FROM stock_basic")['ts_code'].tolist()
        if not stocks:
            return

        target_cols = [
            'ts_code', 'end_date', 'ann_date', 'div_proc', 'stk_div', 'stk_bo_rate', 'stk_co_rate',
            'cash_div', 'cash_div_tax', 'record_date', 'ex_date', 'pay_date', 'div_listdate',
            'imp_ann_date', 'base_date', 'base_share'
        ]

        success_count = 0
        for idx, s_code in enumerate(stocks):
            try:
                df = self.provider.dividend(ts_code=s_code)
                if df.empty:
                    continue

                for dcol in ('ann_date', 'end_date', 'record_date', 'ex_date', 'pay_date', 'div_listdate', 'imp_ann_date', 'base_date'):
                    if dcol in df.columns:
                        df[dcol] = pd.to_datetime(df[dcol], errors='coerce').dt.date

                for col in target_cols:
                    if col not in df.columns:
                        df[col] = None

                df_to_save = df[target_cols].copy()
                df_to_save = dedupe_by_key(
                    df_to_save,
                    key_cols=['ts_code', 'end_date', 'ann_date'],
                    table_name='stock_dividend',
                    logger=logger,
                )

                with get_db_connection() as con:
                    update_then_insert_missing(
                        con,
                        df=df_to_save,
                        table_name='stock_dividend',
                        view_name='dividend_view',
                        columns=target_cols,
                        key_cols=['ts_code', 'end_date', 'ann_date'],
                        logger=logger,
                    )
                success_count += len(df_to_save)
            except Exception as e:
                if "权限" in str(e):
                    logger.warning(f"分红数据权限不足: {str(e)[:50]}")
                    break

            if (idx + 1) % 500 == 0:
                logger.info(f"分红同步进度: {idx+1}/{len(stocks)}")
                time.sleep(1)

        logger.info(f"分红送股同步完成: +{success_count} 条")

    def sync_forecast(
        self,
        days: int = 120,
        start_date: str | None = None,
        end_date: str | None = None,
        ts_code: str | None = None,
    ):
        """同步业绩预告数据 (按日期或股票代码)"""
        target_cols = [
            'ts_code', 'ann_date', 'end_date', 'type', 'p_change_min', 'p_change_max',
            'net_profit_min', 'net_profit_max', 'last_parent_net', 'first_ann_date',
            'summary', 'change_reason'
        ]

        if ts_code:
            logger.info(f"同步个股业绩预告: {ts_code}")
            df = self.provider.forecast(ts_code=ts_code)
            if not df.empty:
                self._save_forecast_df(df, target_cols)
            return

        if not end_date:
            end_date = arrow.get(trading_calendar.get_latest_sync_date()).format("YYYY-MM-DD")
        if not start_date:
            start_date = arrow.get(end_date).shift(days=-days).format("YYYY-MM-DD")

        logger.info(f"开始按公告日期同步业绩预告: {start_date} ~ {end_date}")
        start_dt = arrow.get(start_date)
        end_dt = arrow.get(end_date)

        for dt in arrow.Arrow.range("day", start_dt, end_dt):
            date_str = dt.format("YYYYMMDD")
            try:
                df = self.provider.forecast(ann_date=date_str)
                if not df.empty:
                    self._save_forecast_df(df, target_cols)
                time.sleep(0.3)
            except Exception as exc:
                logger.warning(f"同步 {date_str} 业绩预告失败: {exc}")

        logger.info(f"业绩预告范围同步完成: {start_date} ~ {end_date}")

    def _save_forecast_df(self, df: pd.DataFrame, target_cols: list[str]):
        for dcol in ('ann_date', 'end_date', 'first_ann_date'):
            if dcol in df.columns:
                df[dcol] = pd.to_datetime(df[dcol], errors='coerce').dt.date

        for col in target_cols:
            if col not in df.columns:
                df[col] = None

        df_to_save = df[target_cols].copy()
        df_to_save = dedupe_by_key(
            df_to_save,
            key_cols=['ts_code', 'end_date', 'ann_date'],
            table_name='stock_forecast',
            logger=logger,
        )

        with get_db_connection() as con:
            update_then_insert_missing(
                con,
                df=df_to_save,
                table_name='stock_forecast',
                view_name='forecast_view',
                columns=target_cols,
                key_cols=['ts_code', 'end_date', 'ann_date'],
                logger=logger,
            )

