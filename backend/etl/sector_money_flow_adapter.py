"""
板块资金流向数据采集适配器
"""
import akshare as ak
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from datetime import date, datetime, timedelta
import logging
import time
from scipy.stats import percentileofscore

logger = logging.getLogger(__name__)


class SectorMetricsCalculator:
    """板块指标计算服务 - 计算所有衍生指标"""
    
    def __init__(self):
        from db.sector_money_flow_repository import SectorMoneyFlowRepository
        self.repo = SectorMoneyFlowRepository()
    
    def calculate_comprehensive_sector_metrics(self, trade_date: date, lookback_periods: List[int] = [20, 50]) -> bool:
        """
        计算指定日期的所有板块衍生指标
        
        Args:
            trade_date: 交易日期
            lookback_periods: 回看周期列表，默认[20, 50]
        
        Returns:
            bool: 是否成功计算
        """
        try:
            # 获取当天所有板块数据
            sectors = self.repo.get_all_sectors_for_rps_calculation(trade_date)
            if not sectors:
                logger.warning(f"没有找到 {trade_date} 的板块数据，跳过指标计算")
                return False
            
            # 计算所有指标
            rps_success = self._calculate_rps_scores(sectors, trade_date, lookback_periods)
            ma_success = self._calculate_ma_status(sectors, trade_date)
            other_fields_success = self._calculate_other_fields(sectors, trade_date)
            
            total_updates = sum([rps_success, ma_success, other_fields_success])
            logger.info(f"{trade_date} 板块指标计算完成，更新了 {total_updates} 项指标")
            return total_updates > 0
            
        except Exception as e:
            logger.error(f"计算板块指标时发生错误: {e}", exc_info=True)
            return False
    
    def _calculate_rps_scores(self, sectors: List[Dict], trade_date: date, lookback_periods: List[int]) -> bool:
        """计算RPS分数 - 优化版本，避免O(n²)复杂度"""
        try:
            logger.info(f"开始计算 {len(sectors)} 个板块的RPS分数...")

            # 获取历史数据用于RPS计算
            # 简化版本：只使用数据库中已有的历史数据，避免从股票数据计算（太慢）
            sector_performance_history = {}
            for sector in sectors:
                sector_name = sector['sector_name']
                history = self.repo.get_sector_performance_history(sector_name, max(lookback_periods) + 10)
                sector_performance_history[sector_name] = history

            # 预先计算所有板块的累积收益率，避免重复计算
            logger.info("预计算所有板块的累积收益率...")
            sector_cumulative_returns = {}
            for period in lookback_periods:
                sector_cumulative_returns[period] = {}
                for sector in sectors:
                    sector_name = sector['sector_name']
                    history = sector_performance_history.get(sector_name, [])

                    if len(history) >= period:
                        recent_history = history[-period:]
                        cumulative_return = 1.0
                        for record in recent_history:
                            if record.get('change_pct') is not None:
                                cumulative_return *= (1 + record['change_pct'] / 100.0)
                        cumulative_return = (cumulative_return - 1) * 100  # 转换为百分比
                        sector_cumulative_returns[period][sector_name] = cumulative_return

            # 计算每个板块的RPS分数
            update_results = []
            for sector in sectors:
                sector_name = sector['sector_name']

                # 计算RPS百分位数
                final_rps_20 = None
                final_rps_50 = None

                # 计算20日RPS
                if 20 in sector_cumulative_returns and sector_name in sector_cumulative_returns[20]:
                    current_return_20 = sector_cumulative_returns[20][sector_name]
                    all_returns_20 = list(sector_cumulative_returns[20].values())

                    if all_returns_20:
                        rps_20_percentile = percentileofscore(all_returns_20, current_return_20)
                        final_rps_20 = round(rps_20_percentile, 2)

                # 计算50日RPS
                if 50 in sector_cumulative_returns and sector_name in sector_cumulative_returns[50]:
                    current_return_50 = sector_cumulative_returns[50][sector_name]
                    all_returns_50 = list(sector_cumulative_returns[50].values())

                    if all_returns_50:
                        rps_50_percentile = percentileofscore(all_returns_50, current_return_50)
                        final_rps_50 = round(rps_50_percentile, 2)

                # 如果无法计算，使用默认值0.0
                if final_rps_20 is None:
                    final_rps_20 = 0.0
                if final_rps_50 is None:
                    final_rps_50 = 0.0

                # 更新数据库
                update_success = self.repo.update_sector_rps_scores(
                    sector_name, trade_date, final_rps_20, final_rps_50
                )
                if update_success:
                    update_results.append((sector_name, final_rps_20, final_rps_50))

            logger.info(f"{trade_date} RPS计算完成，更新了 {len(update_results)} 个板块")
            return len(update_results) > 0

        except Exception as e:
            logger.error(f"计算RPS分数时发生错误: {e}", exc_info=True)
            return False
    
    def _calculate_ma_status(self, sectors: List[Dict], trade_date: date) -> bool:
        """计算并更新板块均线状态"""
        try:
            update_count = 0
            for sector in sectors:
                sector_name = sector['sector_name']
                
                # 获取最近的涨跌幅数据用于均线判断
                history = self.repo.get_sector_performance_history(sector_name, 60)
                
                # 如果历史数据不足，尝试从股票数据计算
                if len(history) < 20:
                    calculated_history = self._calculate_sector_history_from_stocks(sector_name, trade_date, 60)
                    if calculated_history and len(calculated_history) > len(history):
                        history = calculated_history
                
                # 如果仍然不足20天，使用默认值0（震荡状态）
                if len(history) < 20:
                    ma_status = 0  # 默认震荡状态
                else:
                
                    # 计算简单的移动平均线状态
                    close_prices = []
                    current_price = 100  # 假设基期价格为100
                    for record in history:
                        change_pct = record.get('change_pct', 0) or 0
                        current_price = current_price * (1 + change_pct / 100.0)
                        close_prices.append(current_price)
                    
                    # 计算不同周期的移动平均线
                    prices_array = np.array(close_prices)
                    ma5 = np.mean(prices_array[-5:]) if len(prices_array) >= 5 else np.nan
                    ma10 = np.mean(prices_array[-10:]) if len(prices_array) >= 10 else np.nan
                    ma20 = np.mean(prices_array[-20:]) if len(prices_array) >= 20 else np.nan
                    
                    # 判断均线状态
                    ma_status = 0  # 默认震荡
                    if not np.isnan(ma5) and not np.isnan(ma10) and not np.isnan(ma20):
                        # 多头排列：MA5 > MA10 > MA20
                        if ma5 > ma10 > ma20:
                            ma_status = 1  # 多头
                        # 空头排列：MA5 < MA10 < MA20
                        elif ma5 < ma10 < ma20:
                            ma_status = -1  # 空头
                
                # 更新数据库（所有板块都更新，即使值为0）
                update_success = self.repo.update_sector_ma_status(sector_name, trade_date, ma_status)
                if update_success:
                    update_count += 1
            
            logger.info(f"{trade_date} 均线状态计算完成，更新了 {update_count} 个板块")
            return update_count > 0
            
        except Exception as e:
            logger.error(f"计算均线状态时发生错误: {e}", exc_info=True)
            return False
    
    def _calculate_other_fields(self, sectors: List[Dict], trade_date: date) -> bool:
        """计算其他字段（change_pct, avg_turnover, limit_up_count, top_weight_stocks）"""
        try:
            update_count = 0
            for sector in sectors:
                sector_name = sector['sector_name']
                
                # 获取板块的历史数据来计算各种指标
                history = self.repo.get_sector_performance_history(sector_name, 10)
                
                # 计算当前涨跌幅（优先从历史数据获取，否则从股票数据计算）
                current_change_pct = self._get_current_change_pct_from_history(history, trade_date)
                if current_change_pct is None:
                    current_change_pct = self._calculate_change_pct_from_stocks(sector_name, trade_date)
                # 如果仍然无法计算，使用0.0作为默认值（表示无涨跌）
                if current_change_pct is None:
                    current_change_pct = 0.0
                
                # 计算平均换手率（优先从历史数据获取，否则从股票数据计算）
                avg_turnover = self._calculate_avg_turnover_from_history(history)
                if avg_turnover is None:
                    avg_turnover = self._calculate_avg_turnover_from_stocks(sector_name, trade_date)
                # 如果仍然无法计算，使用0.0作为默认值
                if avg_turnover is None:
                    avg_turnover = 0.0
                
                # 获取涨停家数（可以从外部API获取，这里先用历史数据估算）
                limit_up_count = self._estimate_limit_up_count_for_sector(sector_name, trade_date)
                # 确保limit_up_count不为None
                if limit_up_count is None:
                    limit_up_count = 0
                
                # 获取前5大权重股（从概念-股票映射表中获取）
                top_weight_stocks = self._get_top_weight_stocks_for_sector(sector_name, trade_date)
                # 确保top_weight_stocks不为None
                if top_weight_stocks is None:
                    top_weight_stocks = []
                
                # 更新数据库
                update_success = self.repo.update_sector_additional_fields(
                    sector_name, trade_date, current_change_pct, avg_turnover, limit_up_count, top_weight_stocks
                )
                if update_success:
                    update_count += 1
            
            logger.info(f"{trade_date} 其他字段计算完成，更新了 {update_count} 个板块")
            return update_count > 0
            
        except Exception as e:
            logger.error(f"计算其他字段时发生错误: {e}", exc_info=True)
            return False
    
    def _get_current_change_pct_from_history(self, history: List[Dict], trade_date: date) -> Optional[float]:
        """从历史数据中获取当前涨跌幅"""
        for record in history:
            if record['trade_date'] == trade_date:
                return record.get('change_pct')
        return None
    
    def _calculate_change_pct_from_stocks(self, sector_name: str, trade_date: date) -> Optional[float]:
        """从板块相关股票计算加权涨跌幅"""
        try:
            from db.database import get_db
            from sqlalchemy import text
            
            with get_db() as db:
                # 获取概念ID
                concept_query = text("""
                    SELECT concept_id FROM concept_theme 
                    WHERE concept_name = :name AND is_active = 1
                    LIMIT 1
                """)
                result = db.execute(concept_query, {'name': sector_name})
                row = result.fetchone()
                if not row:
                    return None
                concept_id = row[0]
                
                # 获取该概念下的股票代码
                stock_query = text("""
                    SELECT sheep_code 
                    FROM sheep_concept_mapping 
                    WHERE concept_id = :concept_id
                """)
                result = db.execute(stock_query, {'concept_id': concept_id})
                stock_codes = [row[0] for row in result]
                
                if not stock_codes:
                    return None
                
                # 计算板块加权涨跌幅（按成交额加权）
                placeholders = ','.join([f':code{i}' for i in range(len(stock_codes))])
                change_query = text(f"""
                    SELECT 
                        SUM(sd.change_pct * sd.amount) / NULLIF(SUM(sd.amount), 0) as weighted_change_pct
                    FROM sheep_daily sd
                    WHERE sd.sheep_code IN ({placeholders})
                      AND sd.trade_date = :trade_date
                      AND sd.change_pct IS NOT NULL
                      AND sd.amount > 0
                """)
                params = {f'code{i}': code for i, code in enumerate(stock_codes)}
                params['trade_date'] = trade_date
                
                result = db.execute(change_query, params)
                row = result.fetchone()
                if row and row[0] is not None:
                    return round(float(row[0]), 4)
                
                return None
        except Exception as e:
            logger.warning(f"计算板块 {sector_name} 涨跌幅失败: {e}")
            return None
    
    def _calculate_avg_turnover_from_history(self, history: List[Dict]) -> Optional[float]:
        """基于历史数据计算平均换手率"""
        if not history:
            return None
        
        # 从历史数据中获取换手率信息（如果存在）
        # 如果没有现成的换手率数据，可以使用一些估算方法
        turnovers = [record.get('avg_turnover') for record in history if record.get('avg_turnover') is not None]
        if turnovers:
            return round(sum(turnovers) / len(turnovers), 2)
        
        # 如果没有历史换手率数据，返回None，表示需要从其他来源获取
        return None
    
    def _calculate_avg_turnover_from_stocks(self, sector_name: str, trade_date: date) -> Optional[float]:
        """从板块相关股票计算平均换手率"""
        try:
            from db.database import get_db
            from sqlalchemy import text
            
            with get_db() as db:
                # 获取概念ID
                concept_query = text("""
                    SELECT concept_id FROM concept_theme 
                    WHERE concept_name = :name AND is_active = 1
                    LIMIT 1
                """)
                result = db.execute(concept_query, {'name': sector_name})
                row = result.fetchone()
                if not row:
                    return None
                concept_id = row[0]
                
                # 获取该概念下的股票代码
                stock_query = text("""
                    SELECT sheep_code 
                    FROM sheep_concept_mapping 
                    WHERE concept_id = :concept_id
                """)
                result = db.execute(stock_query, {'concept_id': concept_id})
                stock_codes = [row[0] for row in result]
                
                if not stock_codes:
                    return None
                
                # 计算板块平均换手率
                placeholders = ','.join([f':code{i}' for i in range(len(stock_codes))])
                turnover_query = text(f"""
                    SELECT AVG(sd.turnover_rate) as avg_turnover
                    FROM sheep_daily sd
                    WHERE sd.sheep_code IN ({placeholders})
                      AND sd.trade_date = :trade_date
                      AND sd.turnover_rate IS NOT NULL
                      AND sd.turnover_rate > 0
                """)
                params = {f'code{i}': code for i, code in enumerate(stock_codes)}
                params['trade_date'] = trade_date
                
                result = db.execute(turnover_query, params)
                row = result.fetchone()
                if row and row[0] is not None:
                    return round(float(row[0]), 4)
                
                return None
        except Exception as e:
            logger.warning(f"计算板块 {sector_name} 平均换手率失败: {e}")
            return None
    
    def _calculate_sector_history_from_stocks(self, sector_name: str, trade_date: date, days: int) -> List[Dict]:
        """从股票数据计算板块历史涨跌幅（用于RPS计算）"""
        try:
            from db.database import get_db
            from sqlalchemy import text
            from datetime import timedelta
            from etl.trade_date_adapter import TradeDateAdapter
            
            # 获取概念ID
            with get_db() as db:
                concept_query = text("""
                    SELECT concept_id FROM concept_theme 
                    WHERE concept_name = :name AND is_active = 1
                    LIMIT 1
                """)
                result = db.execute(concept_query, {'name': sector_name})
                row = result.fetchone()
                if not row:
                    return []
                concept_id = row[0]
                
                # 获取该概念下的股票代码
                stock_query = text("""
                    SELECT sheep_code 
                    FROM sheep_concept_mapping 
                    WHERE concept_id = :concept_id
                """)
                result = db.execute(stock_query, {'concept_id': concept_id})
                stock_codes = [row[0] for row in result]
                
                if not stock_codes:
                    return []
                
                # 获取最近N个交易日
                trade_date_adapter = TradeDateAdapter()
                trading_days = trade_date_adapter.get_trading_days_in_range(
                    trade_date - timedelta(days=days * 2),  # 多取一些日期，确保有足够的交易日
                    trade_date
                )
                
                if len(trading_days) < days:
                    return []
                
                # 只取最近days个交易日
                trading_days = trading_days[-days:]
                
                # 计算每个交易日的板块加权涨跌幅
                history = []
                placeholders = ','.join([f':code{i}' for i in range(len(stock_codes))])
                
                for day in trading_days:
                    change_query = text(f"""
                        SELECT 
                            SUM(sd.change_pct * sd.amount) / NULLIF(SUM(sd.amount), 0) as weighted_change_pct
                        FROM sheep_daily sd
                        WHERE sd.sheep_code IN ({placeholders})
                          AND sd.trade_date = :trade_date
                          AND sd.change_pct IS NOT NULL
                          AND sd.amount > 0
                    """)
                    params = {f'code{i}': code for i, code in enumerate(stock_codes)}
                    params['trade_date'] = day
                    
                    result = db.execute(change_query, params)
                    row = result.fetchone()
                    if row and row[0] is not None:
                        history.append({
                            'trade_date': day,
                            'change_pct': float(row[0])
                        })
                
                return history
        except Exception as e:
            logger.warning(f"从股票数据计算板块 {sector_name} 历史涨跌幅失败: {e}")
            return []
    
    def _estimate_limit_up_count_for_sector(self, sector_name: str, trade_date: date) -> int:
        """估算板块涨停家数"""
        # 实际实现中应该从真实的涨停数据中获取
        # 这里提供一个基础实现，可以通过查询板块相关股票的状态
        try:
            from db.database import get_db
            from sqlalchemy import text
            
            # 从概念-股票映射表中获取该板块下的股票
            with get_db() as db:
                # 首先获取概念ID
                concept_query = text("""
                    SELECT concept_id FROM concept_theme 
                    WHERE concept_name = :name AND is_active = 1
                    LIMIT 1
                """)
                result = db.execute(concept_query, {'name': sector_name})
                row = result.fetchone()
                if not row:
                    return 0
                concept_id = row[0]
                
                # 获取该概念下的股票代码
                stock_query = text("""
                    SELECT sheep_code 
                    FROM sheep_concept_mapping 
                    WHERE concept_id = :concept_id
                """)
                result = db.execute(stock_query, {'concept_id': concept_id})
                stock_codes = [row[0] for row in result]
                
                if not stock_codes:
                    return 0
                
                # 检查这些股票中涨停的数量（涨跌幅 >= 9.8%）
                placeholders = ','.join([f':code{i}' for i in range(len(stock_codes))])
                limit_up_query = text(f"""
                    SELECT COUNT(*) FROM sheep_daily 
                    WHERE sheep_code IN ({placeholders}) 
                    AND trade_date = :trade_date
                    AND change_pct >= 9.8
                """)
                params = {f'code{i}': code for i, code in enumerate(stock_codes)}
                params['trade_date'] = trade_date
                
                result = db.execute(limit_up_query, params)
                limit_up_count = result.fetchone()[0]
                
                return int(limit_up_count)
        except Exception as e:
            logger.warning(f"估算板块 {sector_name} 涨停家数失败: {e}")
            return 0
    
    def _get_top_weight_stocks_for_sector(self, sector_name: str, trade_date: date = None) -> Optional[List[str]]:
        """获取板块前5大权重股"""
        # 实际实现中应该从真实的权重数据中获取
        # 这里通过查询板块相关股票的市值或其他权重指标
        try:
            from db.database import get_db
            from sqlalchemy import text
            
            # 从概念-股票映射表中获取该板块下的股票
            with get_db() as db:
                # 首先获取概念ID
                concept_query = text("""
                    SELECT concept_id FROM concept_theme 
                    WHERE concept_name = :name AND is_active = 1
                    LIMIT 1
                """)
                result = db.execute(concept_query, {'name': sector_name})
                row = result.fetchone()
                if not row:
                    return []
                concept_id = row[0]
                
                # 获取该概念下的股票代码（按某种权重排序，比如按成交额或流通市值）
                if trade_date is None:
                    trade_date = date.today()
                
                stock_query = text("""
                    SELECT sm.sheep_code, sb.sheep_name, sd.amount
                    FROM sheep_concept_mapping sm
                    JOIN sheep_basic sb ON sm.sheep_code = sb.sheep_code
                    LEFT JOIN sheep_daily sd ON sm.sheep_code = sd.sheep_code AND sd.trade_date = :trade_date
                    WHERE sm.concept_id = :concept_id
                    ORDER BY COALESCE(sd.amount, 0) DESC
                    LIMIT 5
                """)
                result = db.execute(stock_query, {'concept_id': concept_id, 'trade_date': trade_date})
                
                top_stocks = []
                for row in result:
                    stock_code = row[0]
                    stock_name = row[1] if row[1] else stock_code
                    top_stocks.append(stock_code)  # 只保存股票代码，不保存名称
                
                return top_stocks if top_stocks else []
        except Exception as e:
            logger.warning(f"获取板块 {sector_name} 前几大权重股失败: {e}")
            return None

class SectorMoneyFlowAdapter:
    """板块资金流向数据采集适配器"""
    
    @staticmethod
    def get_sector_money_flow_today(sector_type: str = '概念资金流', max_retries: int = 3) -> Optional[pd.DataFrame]:
        """
        获取今日板块资金流向数据
        
        Args:
            sector_type: 板块类型（'行业资金流'、'概念资金流'、'地域资金流'）
            max_retries: 最大重试次数
            
        Returns:
            板块资金流向DataFrame
        """
        import requests
        import warnings
        
        # 方法1: 尝试使用 stock_sector_fund_flow_rank (带重试)
        last_exception = None
        for attempt in range(max_retries):
            try:
                df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type=sector_type)
                
                if df is not None and not df.empty:
                    logger.debug(f"获取到 {len(df)} 条{sector_type}资金流数据 (stock_sector_fund_flow_rank)")
                    return df
                    
            except (requests.exceptions.ConnectionError, 
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError) as e:
                last_exception = e
                wait_time = 2 ** attempt
                logger.warning(f"获取{sector_type}资金流数据网络错误 (尝试 {attempt + 1}/{max_retries}): {type(e).__name__}，{wait_time}秒后重试")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
            except Exception as e:
                last_exception = e
                logger.warning(f"stock_sector_fund_flow_rank 失败: {type(e).__name__}")
                break
        
        # 方法2: 使用备用接口 stock_fund_flow_concept (仅概念资金流)
        if sector_type == '概念资金流':
            logger.info("尝试使用备用接口 stock_fund_flow_concept...")
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    df = ak.stock_fund_flow_concept()
                
                if df is not None and not df.empty:
                    # 转换列名以匹配预期格式
                    df = df.rename(columns={
                        '行业': '名称',
                        '净额': '今日主力净流入-净额',
                        '行业-涨跌幅': '涨跌幅'
                    })
                    # 净额单位是亿元，转换为元以保持一致
                    if '今日主力净流入-净额' in df.columns:
                        df['今日主力净流入-净额'] = df['今日主力净流入-净额'] * 100000000
                    logger.info(f"备用接口获取到 {len(df)} 条概念资金流数据")
                    return df
            except Exception as e:
                logger.error(f"备用接口 stock_fund_flow_concept 也失败: {e}")
        
        logger.error(f"获取{sector_type}资金流数据失败，所有方法均已尝试: {last_exception}")
        return None

    @staticmethod
    def get_sector_price_data_today() -> Optional[pd.DataFrame]:
        """
        获取今日板块价格数据（用于计算change_pct）

        Returns:
            板块价格DataFrame
        """
        try:
            # 使用akshare获取今日概念板块价格数据
            df = ak.stock_board_concept_name_em()

            if df is None or df.empty:
                logger.warning("获取板块价格数据为空")
                return None

            logger.debug(f"获取到 {len(df)} 条板块价格数据")
            return df

        except Exception as e:
            logger.error(f"获取板块价格数据失败: {e}", exc_info=True)
            return None

    @staticmethod
    def normalize_sector_money_flow(df: pd.DataFrame, sector_type: str = '概念资金流', price_dict: Optional[Dict[str, float]] = None) -> List[Dict]:
        """
        标准化板块资金流向数据
        
        Args:
            df: 原始DataFrame
            sector_type: 板块类型
            
        Returns:
            标准化后的数据列表
        """
        if df is None or df.empty:
            return []
        
        data_list = []
        
        # 列名映射（akshare返回的列名可能是中文，且带有"今日"前缀）
        column_mapping = {
            '名称': 'sector_name',
            '板块名称': 'sector_name',
            # 带"今日"前缀的列名（akshare新版本）
            '今日主力净流入-净额': 'main_net_inflow',
            '今日超大单净流入-净额': 'super_large_inflow',
            '今日大单净流入-净额': 'large_inflow',
            '今日中单净流入-净额': 'medium_inflow',
            '今日小单净流入-净额': 'small_inflow',
            # 不带前缀的列名（兼容旧版本）
            '主力净流入-净额': 'main_net_inflow',
            '主力净流入': 'main_net_inflow',
            '超大单净流入-净额': 'super_large_inflow',
            '超大单净流入': 'super_large_inflow',
            '大单净流入-净额': 'large_inflow',
            '大单净流入': 'large_inflow',
            '中单净流入-净额': 'medium_inflow',
            '中单净流入': 'medium_inflow',
            '小单净流入-净额': 'small_inflow',
            '小单净流入': 'small_inflow',
        }
        
        # 标准化列名
        normalized_df = df.copy()
        for old_col, new_col in column_mapping.items():
            if old_col in normalized_df.columns:
                normalized_df[new_col] = normalized_df[old_col]
        
        # 提取数据
        for _, row in normalized_df.iterrows():
            try:
                # 获取板块名称
                sector_name = None
                for col in ['sector_name', '名称', '板块名称']:
                    if col in row.index and pd.notna(row[col]):
                        sector_name = str(row[col]).strip()
                        break
                
                if not sector_name:
                    continue
                
                # 获取资金流数据（转换为万元）
                main_net_inflow = 0.0
                super_large_inflow = 0.0
                large_inflow = 0.0
                medium_inflow = 0.0
                small_inflow = 0.0
                
                # 主力净流入（可能是元，需要转换为万元）
                if 'main_net_inflow' in row.index and pd.notna(row['main_net_inflow']):
                    try:
                        main_net_inflow = float(row['main_net_inflow']) / 10000  # 转换为万元
                    except (ValueError, TypeError):
                        pass
                
                # 超大单
                if 'super_large_inflow' in row.index and pd.notna(row['super_large_inflow']):
                    try:
                        super_large_inflow = float(row['super_large_inflow']) / 10000
                    except (ValueError, TypeError):
                        pass
                
                # 大单
                if 'large_inflow' in row.index and pd.notna(row['large_inflow']):
                    try:
                        large_inflow = float(row['large_inflow']) / 10000
                    except (ValueError, TypeError):
                        pass
                
                # 中单
                if 'medium_inflow' in row.index and pd.notna(row['medium_inflow']):
                    try:
                        medium_inflow = float(row['medium_inflow']) / 10000
                    except (ValueError, TypeError):
                        pass
                
                # 小单
                if 'small_inflow' in row.index and pd.notna(row['small_inflow']):
                    try:
                        small_inflow = float(row['small_inflow']) / 10000
                    except (ValueError, TypeError):
                        pass
                
                # 获取涨跌幅数据
                change_pct = None
                if price_dict and sector_name in price_dict:
                    change_pct = price_dict[sector_name]

                data_list.append({
                    'sector_name': sector_name,
                    'trade_date': date.today(),
                    'main_net_inflow': main_net_inflow,
                    'super_large_inflow': super_large_inflow,
                    'large_inflow': large_inflow,
                    'medium_inflow': medium_inflow,
                    'small_inflow': small_inflow,
                    'change_pct': change_pct,
                })
                
            except Exception as e:
                logger.debug(f"处理板块资金流数据行失败: {e}")
                continue
        
        return data_list
    
    @staticmethod
    def get_all_sector_money_flow_today() -> List[Dict]:
        """
        获取所有类型的板块资金流向数据（今日）
        包含资金流数据和价格数据

        Returns:
            所有板块资金流向数据列表
        """
        all_data = []

        # 获取板块价格数据（用于change_pct）
        price_df = SectorMoneyFlowAdapter.get_sector_price_data_today()
        price_dict = {}
        if price_df is not None:
            # 创建板块名称到涨跌幅的映射
            for _, row in price_df.iterrows():
                sector_name = str(row.get('板块名称', '')).strip()
                change_pct = row.get('涨跌幅')
                if sector_name and pd.notna(change_pct):
                    price_dict[sector_name] = float(change_pct)

        # 获取概念资金流
        concept_df = SectorMoneyFlowAdapter.get_sector_money_flow_today('概念资金流')
        if concept_df is not None:
            concept_data = SectorMoneyFlowAdapter.normalize_sector_money_flow(concept_df, '概念资金流', price_dict)
            all_data.extend(concept_data)
            logger.debug(f"获取到 {len(concept_data)} 条概念资金流数据")

        # 延迟，避免请求过快
        time.sleep(0.5)

        # 获取行业资金流
        industry_df = SectorMoneyFlowAdapter.get_sector_money_flow_today('行业资金流')
        if industry_df is not None:
            industry_data = SectorMoneyFlowAdapter.normalize_sector_money_flow(industry_df, '行业资金流', price_dict)
            all_data.extend(industry_data)
            logger.debug(f"获取到 {len(industry_data)} 条行业资金流数据")

        return all_data
    
    @staticmethod
    def get_concept_money_flow_hist(concept_name: str) -> List[Dict]:
        """
        获取单个概念的历史资金流数据
        通过概念下的肥羊聚合计算
        
        Args:
            concept_name: 概念名称（如：人工智能、新能源车等）
            
        Returns:
            历史资金流数据列表
        """
        try:
            from db.database import get_db
            from sqlalchemy import text
            from datetime import date, timedelta
            
            # 从数据库获取概念ID
            with get_db() as db:
                concept_query = text("""
                    SELECT concept_id FROM concept_theme 
                    WHERE concept_name = :name AND is_active = 1
                    LIMIT 1
                """)
                result = db.execute(concept_query, {'name': concept_name})
                row = result.fetchone()
                if not row:
                    logger.debug(f"概念 {concept_name} 不存在于数据库中")
                    return []
                concept_id = row[0]
            
            # 获取该概念下的所有肥羊代码
            with get_db() as db:
                sheep_query = text("""
                    SELECT DISTINCT sheep_code 
                    FROM sheep_concept_mapping 
                    WHERE concept_id = :concept_id
                """)
                result = db.execute(sheep_query, {'concept_id': concept_id})
                sheep_codes = [row[0] for row in result]
                
                if not sheep_codes:
                    logger.debug(f"概念 {concept_name} 下没有肥羊")
                    return []
            
            # 获取最近120天的数据
            cutoff_date = date.today() - timedelta(days=120)
            
            # 查询这些肥羊的资金流数据并聚合
            with get_db() as db:
                # 构建IN子句
                placeholders = ','.join([f':code{i}' for i in range(len(sheep_codes))])
                money_flow_query = text(f"""
                    SELECT 
                        trade_date,
                        SUM(main_net_inflow) AS total_main_net_inflow,
                        SUM(super_large_inflow) AS total_super_large_inflow,
                        SUM(large_inflow) AS total_large_inflow,
                        SUM(medium_inflow) AS total_medium_inflow,
                        SUM(small_inflow) AS total_small_inflow
                    FROM sheep_money_flow
                    WHERE sheep_code IN ({placeholders})
                      AND trade_date >= :cutoff_date
                    GROUP BY trade_date
                    ORDER BY trade_date ASC
                """)
                
                params = {f'code{i}': code for i, code in enumerate(sheep_codes)}
                params['cutoff_date'] = cutoff_date
                
                result = db.execute(money_flow_query, params)
                
                data_list = []
                for row in result:
                    data_list.append({
                        'sector_name': concept_name,
                        'trade_date': row[0],
                        'main_net_inflow': float(row[1]) if row[1] is not None else 0.0,  # 已经是万元
                        'super_large_inflow': float(row[2]) if row[2] is not None else 0.0,
                        'large_inflow': float(row[3]) if row[3] is not None else 0.0,
                        'medium_inflow': float(row[4]) if row[4] is not None else 0.0,
                        'small_inflow': float(row[5]) if row[5] is not None else 0.0,
                    })
                
                logger.debug(f"概念 {concept_name}: 从 {len(sheep_codes)} 只肥羊聚合得到 {len(data_list)} 天的数据")
                return data_list
                
        except Exception as e:
            logger.warning(f"获取概念 {concept_name} 历史数据失败: {e}")
            return []
    
    @staticmethod
    def get_industry_money_flow_hist(industry_name: str) -> List[Dict]:
        """
        获取单个行业的历史资金流数据（约120天）
        
        Args:
            industry_name: 行业名称（如：半导体、消费电子等）
            
        Returns:
            历史资金流数据列表
        """
        try:
            df = ak.stock_sector_fund_flow_hist(symbol=industry_name)
            
            if df is None or df.empty:
                logger.warning(f"获取行业 {industry_name} 历史数据为空")
                return []
            
            data_list = []
            for _, row in df.iterrows():
                try:
                    trade_date = pd.to_datetime(row['日期']).date()
                    
                    # 获取资金流数据（转换为万元）
                    main_net_inflow = float(row.get('主力净流入-净额', 0)) / 10000 if pd.notna(row.get('主力净流入-净额')) else 0.0
                    super_large_inflow = float(row.get('超大单净流入-净额', 0)) / 10000 if pd.notna(row.get('超大单净流入-净额')) else 0.0
                    large_inflow = float(row.get('大单净流入-净额', 0)) / 10000 if pd.notna(row.get('大单净流入-净额')) else 0.0
                    medium_inflow = float(row.get('中单净流入-净额', 0)) / 10000 if pd.notna(row.get('中单净流入-净额')) else 0.0
                    small_inflow = float(row.get('小单净流入-净额', 0)) / 10000 if pd.notna(row.get('小单净流入-净额')) else 0.0
                    
                    data_list.append({
                        'sector_name': industry_name,
                        'trade_date': trade_date,
                        'main_net_inflow': main_net_inflow,
                        'super_large_inflow': super_large_inflow,
                        'large_inflow': large_inflow,
                        'medium_inflow': medium_inflow,
                        'small_inflow': small_inflow,
                    })
                except Exception as e:
                    logger.debug(f"处理行业 {industry_name} 历史数据行失败: {e}")
                    continue
            
            return data_list
            
        except Exception as e:
            logger.error(f"获取行业 {industry_name} 历史数据失败: {e}")
            return []
    
    @staticmethod
    def get_all_industry_money_flow_hist(days: int = 90) -> List[Dict]:
        """
        获取所有行业的历史资金流数据
        
        Args:
            days: 获取最近N天的数据，默认90天
            
        Returns:
            所有行业的历史资金流数据列表
        """
        from datetime import date, timedelta
        
        all_data = []
        cutoff_date = date.today() - timedelta(days=days)
        
        # 先获取行业列表
        industry_df = SectorMoneyFlowAdapter.get_sector_money_flow_today('行业资金流')
        if industry_df is None or industry_df.empty:
            logger.warning("无法获取行业列表")
            return []
        
        # 获取行业名称列表
        industry_names = industry_df['名称'].tolist() if '名称' in industry_df.columns else []
        
        logger.info(f"开始采集 {len(industry_names)} 个行业的历史资金流数据（最近{days}天）...")
        
        for idx, industry_name in enumerate(industry_names):
            try:
                hist_data = SectorMoneyFlowAdapter.get_industry_money_flow_hist(industry_name)
                
                # 只保留最近N天的数据
                filtered_data = [d for d in hist_data if d['trade_date'] >= cutoff_date]
                all_data.extend(filtered_data)
                
                if (idx + 1) % 10 == 0:
                    logger.info(f"进度: {idx + 1}/{len(industry_names)}，累计 {len(all_data)} 条数据")
                
                # 延迟，避免请求过快
                time.sleep(0.3)
                
            except Exception as e:
                logger.warning(f"获取行业 {industry_name} 历史数据失败: {e}")
                continue
        
        logger.info(f"行业历史资金流数据采集完成，共 {len(all_data)} 条")
        return all_data
    
    @staticmethod
    def get_all_concept_money_flow_hist(days: int = 30) -> List[Dict]:
        """
        获取所有概念的历史资金流数据
        
        Args:
            days: 获取最近N天的数据，默认30天
            
        Returns:
            所有概念的历史资金流数据列表
        """
        from datetime import date, timedelta
        
        all_data = []
        cutoff_date = date.today() - timedelta(days=days)
        
        # 先获取概念列表
        concept_df = SectorMoneyFlowAdapter.get_sector_money_flow_today('概念资金流')
        if concept_df is None or concept_df.empty:
            logger.warning("无法获取概念列表")
            return []
        
        # 获取概念名称列表
        concept_names = concept_df['名称'].tolist() if '名称' in concept_df.columns else []
        
        logger.info(f"开始采集 {len(concept_names)} 个概念的历史资金流数据（最近{days}天）...")
        
        success_count = 0
        fail_count = 0
        
        for idx, concept_name in enumerate(concept_names):
            try:
                hist_data = SectorMoneyFlowAdapter.get_concept_money_flow_hist(concept_name)
                
                # 只保留最近N天的数据
                filtered_data = [d for d in hist_data if d['trade_date'] >= cutoff_date]
                if filtered_data:
                    all_data.extend(filtered_data)
                    success_count += 1
                
                # 每10个概念输出一次进度
                if (idx + 1) % 10 == 0:
                    logger.info(f"进度: {idx + 1}/{len(concept_names)}，成功: {success_count}，失败: {fail_count}，累计 {len(all_data)} 条数据")
                
                # 延迟，避免请求过快
                time.sleep(0.3)
                
            except Exception as e:
                logger.debug(f"获取概念 {concept_name} 历史数据失败: {e}")
                fail_count += 1
                continue
        
        logger.info(f"概念历史资金流数据采集完成，共 {len(all_data)} 条，成功 {success_count} 个概念，失败 {fail_count} 个概念")
        return all_data
