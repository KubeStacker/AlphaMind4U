"""
AlphaModel_T6_Resonance - 自适应多策略系统
实现RSRS市场状态识别、板块共振引擎、自适应动态过滤和微观结构过滤
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import date
import logging
from db.database import get_raw_connection
from etl.trade_date_adapter import TradeDateAdapter
import statsmodels.api as sm

logger = logging.getLogger(__name__)


class AlphaModelT6Resonance:
    """T6共振模型 - 自适应多策略系统"""
    
    # 常量定义
    MIN_HISTORY_DAYS = 90  # 最少需要90天历史数据
    HISTORY_DAYS_FOR_FACTORS = 90  # 用于计算因子的历史数据天数
    STOP_LOSS_RATIO = 0.93  # 止损比例（93%）
    DEFAULT_TOP_N = 5  # 默认返回肥羊数量
    
    # RSRS参数
    RSRS_LOOKBACK_DAYS = 18  # RSRS回归窗口（18日）
    RSRS_ATTACK_THRESHOLD = 0.7  # 进攻模式阈值
    RSRS_DEFENSE_THRESHOLD = -0.7  # 防守模式阈值
    
    # 板块共振参数
    SECTOR_MAIN_TREND_AVG_CHG = 1.5  # 主线板块平均涨幅阈值（%）
    SECTOR_MAIN_TREND_BREADTH = 0.20  # 主线板块广度阈值（20%）
    SECTOR_SOLO_RUN_PENALTY_CHG = 6.0  # 孤军深入涨幅阈值（%）
    SECTOR_SOLO_RUN_MAX_AVG_CHG = 0.5  # 孤军深入时板块平均涨幅上限（%）
    SECTOR_LEADER_CHG = 9.8  # 板块领头羊涨幅阈值（%）
    
    def __init__(self):
        self.model_version = "T6_Resonance"
        self.regime = "Balance"  # 市场状态：Attack/Defense/Balance
        self.rsrs_zscore = 0.0  # RSRS标准分
    
    def detect_market_regime(self, trade_date: date) -> Dict[str, any]:
        """
        Module 1: RSRS 市场状态识别（牛熊市判断）
        
        算法：
        1. 合成全市场指数（基于所有肥羊的加权平均）
        2. 对过去N=18日的大盘最高价和最低价进行OLS线性回归
        3. 计算斜率β和RSRS标准分
        4. 定义市场状态：Attack (RSRS_Zscore > 0.7) / Defense (< -0.7) / Balance
        
        Returns:
            {
                'regime': 'Attack'/'Defense'/'Balance',
                'rsrs_zscore': float,
                'rsrs_beta': float,
                'index_high': float,
                'index_low': float
            }
        """
        try:
            with get_raw_connection() as conn:
                # 步骤1：获取全市场指数数据（最近18+30天，用于计算RSRS和标准分）
                # 优先从指数表读取，如果没有则降级为实时计算加权平均指数
                lookback_days = self.RSRS_LOOKBACK_DAYS + 30  # 多取30天用于计算标准分
                
                # 优先从指数表读取（使用中证1000作为全市场指数）
                query_index = """
                    SELECT 
                        trade_date,
                        close_price as index_close,
                        high_price as index_high,
                        low_price as index_low
                    FROM market_index_daily
                    WHERE index_code = 'CSI1000'
                      AND trade_date <= %s
                      AND trade_date >= DATE_SUB(%s, INTERVAL %s DAY)
                    ORDER BY trade_date DESC
                    LIMIT %s
                """
                
                df_index = pd.read_sql(
                    query_index, 
                    conn, 
                    params=[trade_date, trade_date, lookback_days, lookback_days]
                )
                
                # 如果指数表没有数据，降级为实时计算加权平均指数
                if df_index.empty or len(df_index) < self.RSRS_LOOKBACK_DAYS:
                    logger.warning(f"指数表数据不足，降级为实时计算加权平均指数")
                    query_fallback = """
                        SELECT 
                            sd.trade_date,
                            SUM(sd.close_price * sd.amount) / NULLIF(SUM(sd.amount), 0) as index_close,
                            MAX(sd.high_price) as index_high,
                            MIN(sd.low_price) as index_low
                        FROM sheep_daily sd
                        INNER JOIN sheep_basic sb ON sd.sheep_code = sb.sheep_code
                        WHERE sd.trade_date <= %s
                          AND sd.trade_date >= DATE_SUB(%s, INTERVAL %s DAY)
                          AND sb.is_active = 1
                          AND sd.amount > 0
                        GROUP BY sd.trade_date
                        ORDER BY sd.trade_date DESC
                        LIMIT %s
                    """
                    
                    df_index = pd.read_sql(
                        query_fallback, 
                        conn, 
                        params=[trade_date, trade_date, lookback_days, lookback_days]
                    )
                
                if df_index.empty or len(df_index) < self.RSRS_LOOKBACK_DAYS:
                    logger.warning(f"RSRS: 无法获取足够的指数数据（需要至少{self.RSRS_LOOKBACK_DAYS}天）")
                    self.regime = "Balance"
                    self.rsrs_zscore = 0.0
                    return {
                        'regime': self.regime,
                        'rsrs_zscore': self.rsrs_zscore,
                        'rsrs_beta': 0.0,
                        'index_high': 0.0,
                        'index_low': 0.0
                    }
                
                # 按日期升序排列（用于回归）
                df_index = df_index.sort_values('trade_date').reset_index(drop=True)
                
                # 步骤2：取最近N=18日的数据进行OLS回归
                recent_data = df_index.tail(self.RSRS_LOOKBACK_DAYS).copy()
                
                if len(recent_data) < self.RSRS_LOOKBACK_DAYS:
                    logger.warning(f"RSRS: 数据不足（仅{len(recent_data)}天）")
                    self.regime = "Balance"
                    self.rsrs_zscore = 0.0
                    return {
                        'regime': self.regime,
                        'rsrs_zscore': self.rsrs_zscore,
                        'rsrs_beta': 0.0,
                        'index_high': recent_data['index_high'].iloc[-1] if not recent_data.empty else 0.0,
                        'index_low': recent_data['index_low'].iloc[-1] if not recent_data.empty else 0.0
                    }
                
                # 提取最高价和最低价序列
                high_prices = recent_data['index_high'].values
                low_prices = recent_data['index_low'].values
                
                # 步骤3：OLS线性回归：High = α + β * Low
                # 使用statsmodels进行回归
                X = sm.add_constant(low_prices)  # 添加常数项
                y = high_prices
                
                try:
                    model = sm.OLS(y, X).fit()
                    beta = model.params[1]  # 斜率β
                    alpha = model.params[0]  # 截距α
                except Exception as e:
                    logger.error(f"RSRS回归计算失败: {e}")
                    beta = 1.0
                    alpha = 0.0
                
                # 步骤4：计算RSRS标准分
                # 需要历史β序列来计算均值和标准差
                # 性能优化：限制计算窗口数量，避免过多循环
                if len(df_index) >= self.RSRS_LOOKBACK_DAYS * 2:
                    # 计算滚动β序列（用于计算标准分）
                    # 优化：只计算最近30个窗口，避免过多计算
                    max_windows = 30
                    start_idx = max(self.RSRS_LOOKBACK_DAYS, len(df_index) - max_windows)
                    beta_history = []
                    
                    for i in range(start_idx, len(df_index)):
                        window_data = df_index.iloc[i - self.RSRS_LOOKBACK_DAYS:i]
                        if len(window_data) >= self.RSRS_LOOKBACK_DAYS:
                            try:
                                window_high = window_data['index_high'].values
                                window_low = window_data['index_low'].values
                                X_window = sm.add_constant(window_low)
                                y_window = window_high
                                model_window = sm.OLS(y_window, X_window).fit()
                                beta_history.append(model_window.params[1])
                            except:
                                continue
                    
                    if len(beta_history) > 0:
                        beta_mean = np.mean(beta_history)
                        beta_std = np.std(beta_history)
                        if beta_std > 0:
                            rsrs_zscore = (beta - beta_mean) / beta_std
                        else:
                            rsrs_zscore = 0.0
                    else:
                        # 如果无法计算历史β，使用简化方法
                        rsrs_zscore = (beta - 1.0) * 10  # 简化标准分
                else:
                    # 数据不足，使用简化方法
                    rsrs_zscore = (beta - 1.0) * 10
                
                # 步骤5：定义市场状态
                if rsrs_zscore > self.RSRS_ATTACK_THRESHOLD:
                    regime = "Attack"
                elif rsrs_zscore < self.RSRS_DEFENSE_THRESHOLD:
                    regime = "Defense"
                else:
                    regime = "Balance"
                
                self.regime = regime
                self.rsrs_zscore = rsrs_zscore
                
                logger.info(f"RSRS市场状态: {regime} (Z-score: {rsrs_zscore:.3f}, Beta: {beta:.3f})")
                
                return {
                    'regime': regime,
                    'rsrs_zscore': rsrs_zscore,
                    'rsrs_beta': beta,
                    'index_high': recent_data['index_high'].iloc[-1],
                    'index_low': recent_data['index_low'].iloc[-1]
                }
                
        except Exception as e:
            logger.error(f"RSRS市场状态识别失败: {e}", exc_info=True)
            self.regime = "Balance"
            self.rsrs_zscore = 0.0
            return {
                'regime': self.regime,
                'rsrs_zscore': self.rsrs_zscore,
                'rsrs_beta': 0.0,
                'index_high': 0.0,
                'index_low': 0.0
            }
    
    def calculate_sector_resonance(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Module 2: 板块共振引擎
        
        基于内存数据实时计算板块指数，捕捉"板块共振"
        
        逻辑步骤（使用groupby + transform向量化实现）：
        1. 板块指数合成：基于industry字段，计算每个板块当日的：
           - Sector_Avg_Chg: 板块内肥羊平均涨跌幅
           - Sector_Breadth: 板块内change_pct > 3%的肥羊占比
           - Sector_Max_Chg: 板块内涨幅最大的肥羊（寻找领头羊）
        2. 共振判定：
           - 主线确认：Sector_Avg_Chg > 1.5% 且 Sector_Breadth > 20%
           - 孤军深入惩罚：若肥羊change_pct > 6% 但所属板块Sector_Avg_Chg < 0.5%，强制扣分
           - 同伴验证：若同板块中有肥羊涨停（Sector_Max_Chg > 9.8%），给予同板块其他跟风肥羊额外加分
        
        Args:
            df: 包含industry和change_pct字段的DataFrame
            
        Returns:
            添加了板块共振相关字段的DataFrame
        """
        if df.empty or 'industry' not in df.columns:
            logger.warning("板块共振计算：数据为空或缺少industry字段")
            df['sector_avg_chg'] = 0.0
            df['sector_breadth'] = 0.0
            df['sector_max_chg'] = 0.0
            df['is_main_sector'] = 0
            df['sector_resonance_score'] = 0.0
            return df
        
        # 填充缺失的industry字段（原地修改，避免复制）
        if 'industry' not in df.columns:
            df['industry'] = '未知板块'
        else:
            df['industry'] = df['industry'].fillna('未知板块')
        
        # 确保change_pct为数值类型（原地转换）
        if 'change_pct' in df.columns:
            df['change_pct'] = pd.to_numeric(df['change_pct'], errors='coerce').fillna(0.0)
        else:
            df['change_pct'] = 0.0
        
        # 步骤1：使用groupby + transform向量化计算板块指标
        # Sector_Avg_Chg: 板块内肥羊平均涨跌幅
        df['sector_avg_chg'] = df.groupby('industry')['change_pct'].transform('mean')
        
        # Sector_Breadth: 板块内change_pct > 3%的肥羊占比
        df['is_strong'] = (df['change_pct'] > 3.0).astype(int)
        sector_strong_count = df.groupby('industry')['is_strong'].transform('sum')
        sector_total_count = df.groupby('industry')['change_pct'].transform('count')
        df['sector_breadth'] = np.where(
            sector_total_count > 0,
            sector_strong_count / sector_total_count,
            0.0
        )
        
        # Sector_Max_Chg: 板块内涨幅最大的肥羊
        df['sector_max_chg'] = df.groupby('industry')['change_pct'].transform('max')
        
        # 步骤2：共振判定（向量化操作）
        # 主线确认：Sector_Avg_Chg > 1.5% 且 Sector_Breadth > 20%
        df['is_main_sector'] = (
            (df['sector_avg_chg'] > self.SECTOR_MAIN_TREND_AVG_CHG) &
            (df['sector_breadth'] > self.SECTOR_MAIN_TREND_BREADTH)
        ).astype(int)
        
        # 孤军深入惩罚：若肥羊change_pct > 6% 但所属板块Sector_Avg_Chg < 0.5%
        solo_run_penalty = (
            (df['change_pct'] > self.SECTOR_SOLO_RUN_PENALTY_CHG) &
            (df['sector_avg_chg'] < self.SECTOR_SOLO_RUN_MAX_AVG_CHG)
        ).astype(int) * -50  # 扣50分
        
        # 同伴验证：若同板块中有肥羊涨停（Sector_Max_Chg > 9.8%），给予同板块其他跟风肥羊额外加分
        # 注意：领头羊本身不额外加分，只给跟风肥羊加分
        has_leader = (df['sector_max_chg'] > self.SECTOR_LEADER_CHG).astype(int)
        is_not_leader = (df['change_pct'] < self.SECTOR_LEADER_CHG).astype(int)
        peer_validation_bonus = (has_leader & is_not_leader).astype(int) * 30  # 加30分
        
        # 主线板块加分
        main_sector_bonus = df['is_main_sector'] * 40  # 主线板块加40分
        
        # 计算板块共振总分
        df['sector_resonance_score'] = (
            main_sector_bonus +
            peer_validation_bonus +
            solo_run_penalty
        )
        
        # 清理计算过程中的临时列
        df = df.drop(columns=['is_strong'], errors='ignore')
        
        main_sector_count = df['is_main_sector'].sum()
        solo_penalty_count = (solo_run_penalty < 0).sum()
        resonance_positive_count = (df['sector_resonance_score'] > 0).sum()
        resonance_negative_count = (df['sector_resonance_score'] < 0).sum()
        
        logger.info(f"板块共振计算完成：")
        logger.info(f"  - 主线板块: {main_sector_count} 个")
        logger.info(f"  - 共振加分肥羊: {resonance_positive_count} 只")
        logger.info(f"  - 孤军深入扣分: {solo_penalty_count} 只")
        logger.info(f"  - 共振分数范围: {df['sector_resonance_score'].min():.0f} ~ {df['sector_resonance_score'].max():.0f}")
        
        return df
    
    def level1_extract_features(self, trade_date: date) -> pd.DataFrame:
        """
        第1级：离线特征提取（ETL Layer）
        计算高维因子：VCP、均线粘合度、主力潜伏、套牢盘等
        集成板块共振计算
        """
        try:
            with get_raw_connection() as conn:
                # 步骤1：快速获取每个肥羊的最新交易日数据
                query1 = """
                    SELECT 
                        sd.sheep_code,
                        sd.trade_date,
                        sd.close_price,
                        sd.high_price,
                        sd.low_price,
                        sd.open_price,
                        sd.volume,
                        sd.amount,
                        sd.turnover_rate,
                        sd.change_pct,
                        sd.ma5,
                        sd.ma10,
                        sd.ma20,
                        sd.ma30,
                        sd.ma60,
                        sb.sheep_name,
                        sb.list_date,
                        sb.industry
                    FROM (
                        SELECT sheep_code, MAX(trade_date) as max_date
                        FROM sheep_daily USE INDEX (idx_stock_code_date)
                        WHERE trade_date <= %s
                        GROUP BY sheep_code
                        HAVING COUNT(*) >= %s
                    ) latest
                    INNER JOIN sheep_daily sd 
                        ON sd.sheep_code = latest.sheep_code AND sd.trade_date = latest.max_date
                    INNER JOIN sheep_basic sb ON sd.sheep_code = sb.sheep_code
                    WHERE sb.is_active = 1
                """
                
                df_latest = pd.read_sql(query1, conn, params=[trade_date, self.MIN_HISTORY_DAYS])
                
                if df_latest.empty:
                    logger.warning(f"Level 1: 无法获取最新数据 (trade_date={trade_date})")
                    return pd.DataFrame()
                
                logger.info(f"Level 1: 获取到 {len(df_latest)} 只肥羊的最新数据")
                
                # 步骤2：获取历史数据用于计算因子
                valid_sheep = df_latest['sheep_code'].tolist()
                if not valid_sheep:
                    return pd.DataFrame()
                
                batch_size = 500
                df_history_list = []
                
                for i in range(0, len(valid_sheep), batch_size):
                    batch_sheep = valid_sheep[i:i + batch_size]
                    sheep_placeholders = ','.join(['%s'] * len(batch_sheep))
                    query2 = f"""
                        SELECT 
                            sheep_code,
                            trade_date,
                            close_price,
                            high_price,
                            low_price,
                            volume,
                            amount
                        FROM sheep_daily USE INDEX (idx_stock_code_date)
                        WHERE sheep_code IN ({sheep_placeholders})
                          AND trade_date <= %s
                          AND trade_date >= DATE_SUB(%s, INTERVAL %s DAY)
                        ORDER BY sheep_code, trade_date
                    """
                    
                    batch_history = pd.read_sql(query2, conn, params=batch_sheep + [trade_date, trade_date, self.HISTORY_DAYS_FOR_FACTORS])
                    if not batch_history.empty:
                        df_history_list.append(batch_history)
                
                if df_history_list:
                    df_history = pd.concat(df_history_list, ignore_index=True)
                else:
                    df_history = pd.DataFrame()
                
                # 步骤3：使用pandas向量化计算因子
                if df_history.empty:
                    logger.warning("Level 1: 无法获取历史数据，因子计算将使用默认值")
                    df_latest['vol_ma_5'] = df_latest['volume']
                    df_latest['vol_ma_20'] = df_latest['volume']
                    df_latest['vcp_factor'] = None
                    df_latest['rps_250'] = None
                else:
                    df_history = df_history.sort_values(['sheep_code', 'trade_date'])
                    
                    unique_stocks = df_history['sheep_code'].nunique()
                    logger.info(f"Level 1: 开始计算 {unique_stocks} 只肥羊的因子")
                    
                    factors_list = []
                    processed_count = 0
                    
                    for sheep_code, group in df_history.groupby('sheep_code'):
                        processed_count += 1
                        if processed_count % 100 == 0:
                            logger.info(f"Level 1: 因子计算进度 {processed_count}/{unique_stocks}")
                        
                        group = group.sort_values('trade_date').reset_index(drop=True)
                        factor_dict = {'sheep_code': sheep_code}
                        
                        # 计算5日均量
                        if len(group) >= 5:
                            factor_dict['vol_ma_5'] = group['volume'].tail(5).mean()
                        elif len(group) > 0:
                            factor_dict['vol_ma_5'] = group['volume'].mean()
                        else:
                            factor_dict['vol_ma_5'] = None
                        
                        # 计算20日均量
                        if len(group) >= 20:
                            factor_dict['vol_ma_20'] = group['volume'].tail(20).mean()
                        elif len(group) > 0:
                            factor_dict['vol_ma_20'] = group['volume'].mean()
                        else:
                            factor_dict['vol_ma_20'] = None
                        
                        # 计算VCP（最近20日）
                        if len(group) >= 20:
                            recent_20 = group.tail(20)
                            high_max = recent_20['high_price'].max()
                            low_min = recent_20['low_price'].min()
                            close_mean = recent_20['close_price'].mean()
                            factor_dict['vcp_factor'] = (high_max - low_min) / close_mean if close_mean > 0 else None
                        else:
                            factor_dict['vcp_factor'] = None
                        
                        # RPS需要250天数据
                        if len(group) >= 250:
                            price_250d_ago = group.iloc[0]['close_price']
                            current_price = group.iloc[-1]['close_price']
                            factor_dict['rps_250'] = (current_price / price_250d_ago - 1) * 100 if price_250d_ago and price_250d_ago > 0 else None
                        else:
                            factor_dict['rps_250'] = None
                        
                        factors_list.append(factor_dict)
                    
                    logger.info(f"Level 1: 因子计算完成，共处理 {processed_count} 只肥羊")
                    
                    factors = pd.DataFrame(factors_list)
                    df_latest = df_latest.merge(factors, on='sheep_code', how='left')
                
                # 填充缺失值
                df_latest['vol_ma_5'] = df_latest['vol_ma_5'].fillna(df_latest['volume'])
                df_latest['vol_ma_20'] = df_latest['vol_ma_20'].fillna(df_latest['volume'])
                df_latest['vcp_factor'] = df_latest['vcp_factor'].fillna(1.0)
                df_latest['rps_250'] = df_latest['rps_250'].fillna(0)
                
                # 计算量比因子
                df_latest['vol_ratio_ma20'] = np.where(
                    (df_latest['vol_ma_20'].notna()) & (df_latest['vol_ma_20'] > 0),
                    df_latest['volume'] / df_latest['vol_ma_20'],
                    1.0
                )
                df_latest['vol_ratio_ma20'] = df_latest['vol_ratio_ma20'].fillna(1.0)
                
                # 计算上影线比例
                price_range = df_latest['high_price'] - df_latest['low_price']
                upper_shadow = df_latest['high_price'] - df_latest['close_price']
                df_latest['upper_shadow_ratio'] = np.where(
                    price_range > 0,
                    upper_shadow / price_range,
                    0
                )
                
                # 识别板块类型
                df_latest['is_star_market'] = df_latest['sheep_code'].str.startswith('688').astype(int)
                df_latest['is_gem'] = df_latest['sheep_code'].str.startswith('300').astype(int)
                
                # 步骤4：计算VWAP（成交量加权平均价）
                df_latest['vwap'] = np.where(
                    df_latest['volume'] > 0,
                    df_latest['amount'] / (df_latest['volume'] * 100),  # amount是元，volume是手，需要转换
                    df_latest['close_price']
                )
                
                # 步骤5：计算ATR（真实波幅）- 简化版，使用当日高低价差
                df_latest['atr'] = df_latest['high_price'] - df_latest['low_price']
                
                # 步骤6：计算RSI_6（需要历史数据，这里简化处理）
                # 实际应该使用rolling计算，但为了性能，这里先设为None，后续可以优化
                df_latest['rsi_6'] = None
                
                # 步骤7：计算Bias_20（20日乖离率）- 需要历史数据，这里简化
                df_latest['bias_20'] = None
                if 'ma20' in df_latest.columns:
                    df_latest['bias_20'] = np.where(
                        df_latest['ma20'].notna() & (df_latest['ma20'] > 0),
                        ((df_latest['close_price'] - df_latest['ma20']) / df_latest['ma20']) * 100,
                        None
                    )
                
                # 步骤8：集成板块共振计算（Module 2）
                df_latest = self.calculate_sector_resonance(df_latest)
                
                logger.info(f"Level 1: 成功提取 {len(df_latest)} 只肥羊的特征（含板块共振）")
                
                return df_latest.reset_index(drop=True)
                
        except Exception as e:
            logger.error(f"Level 1特征提取失败: {e}", exc_info=True)
            import traceback
            logger.error(f"Level 1异常详情:\n{traceback.format_exc()}")
            return pd.DataFrame()
    
    def level2_adaptive_filter(self, df: pd.DataFrame, params: Dict, regime_info: Dict) -> pd.DataFrame:
        """
        Module 3: 自适应动态过滤器（Level 2）
        
        根据市场状态（Module 1）和板块共振（Module 2）动态调整筛选标准：
        
        1. 进攻模式 (Attack)：
           - 允许追高：放宽涨幅限制至 3% ~ 9%
           - 优先：Sector_Resonance（共振肥羊）> RPS_250 > 85（强势肥羊）
        
        2. 防守模式 (Defense) - 解决熊市亏损：
           - 禁止追高：剔除当日涨幅 > 4% 的肥羊
           - 低吸策略：筛选Bias_20 < -8%（乖离率超跌）或RSI_6 < 25的肥羊
           - 低波红利：剔除ATR（真实波幅）过大的肥羊，寻找抗跌标的
        
        3. 震荡模式 (Balance)：
           - 使用默认阈值
        
        Args:
            df: 包含特征和板块共振数据的DataFrame
            params: 用户参数
            regime_info: 市场状态信息（来自detect_market_regime）
        """
        if df.empty:
            return df
        
        filtered_df = df.copy()
        regime = regime_info.get('regime', 'Balance')
        
        logger.info(f"Level 2: 自适应过滤开始，市场状态: {regime}")
        
        # 根据市场状态调整筛选标准
        if regime == "Attack":
            # 进攻模式：允许追高，放宽涨幅限制
            min_change_pct = params.get('min_change_pct_attack', 3.0)  # 默认3%
            max_change_pct = params.get('max_change_pct_attack', 9.0)  # 默认9%
            
            if 'change_pct' in filtered_df.columns:
                change_mask = filtered_df['change_pct'].notna()
                filtered_df = filtered_df[
                    change_mask & 
                    (filtered_df['change_pct'] >= min_change_pct) & 
                    (filtered_df['change_pct'] <= max_change_pct)
                ]
                logger.info(f"Level 2 (Attack): 涨幅筛选 ({min_change_pct}% ~ {max_change_pct}%)，剩余 {len(filtered_df)} 只肥羊")
            
            # 优先选择板块共振肥羊或RPS强势肥羊（仅在启用板块共振时）
            sector_boost = params.get('sector_boost', True)
            if sector_boost and 'sector_resonance_score' in filtered_df.columns and 'rps_250' in filtered_df.columns:
                # 保留共振肥羊或RPS > 85的肥羊
                resonance_mask = filtered_df['sector_resonance_score'] > 0
                rps_mask = filtered_df['rps_250'].fillna(0) > 85
                priority_mask = resonance_mask | rps_mask
                before_count = len(filtered_df)
                filtered_df = filtered_df[priority_mask]
                resonance_selected = resonance_mask.sum()
                rps_selected = rps_mask.sum()
                logger.info(f"Level 2 (Attack): 优先选择共振/RPS强势肥羊，{before_count} -> {len(filtered_df)} 只（共振: {resonance_selected}, RPS: {rps_selected}）")
            elif not sector_boost:
                logger.info("Level 2 (Attack): 板块共振已关闭，跳过共振优先筛选")
        
        elif regime == "Defense":
            # 防守模式：禁止追高，低吸策略
            max_change_pct = params.get('max_change_pct_defense', 4.0)  # 默认4%
            
            if 'change_pct' in filtered_df.columns:
                change_mask = filtered_df['change_pct'].notna()
                filtered_df = filtered_df[change_mask & (filtered_df['change_pct'] <= max_change_pct)]
                logger.info(f"Level 2 (Defense): 禁止追高（涨幅 <= {max_change_pct}%），剩余 {len(filtered_df)} 只肥羊")
            
            # 低吸策略：筛选Bias_20 < -8% 或 RSI_6 < 25
            low_buy_mask = pd.Series([False] * len(filtered_df), index=filtered_df.index)
            
            if 'bias_20' in filtered_df.columns:
                bias_mask = filtered_df['bias_20'].notna() & (filtered_df['bias_20'] < -8.0)
                low_buy_mask = low_buy_mask | bias_mask
            
            if 'rsi_6' in filtered_df.columns:
                rsi_mask = filtered_df['rsi_6'].notna() & (filtered_df['rsi_6'] < 25)
                low_buy_mask = low_buy_mask | rsi_mask
            
            # 如果bias_20和rsi_6都不可用，则放宽条件（仅要求涨幅不大）
            if not low_buy_mask.any() and 'bias_20' not in filtered_df.columns and 'rsi_6' not in filtered_df.columns:
                logger.warning("Level 2 (Defense): Bias_20和RSI_6不可用，仅使用涨幅限制")
            else:
                filtered_df = filtered_df[low_buy_mask]
                logger.info(f"Level 2 (Defense): 低吸策略筛选，剩余 {len(filtered_df)} 只肥羊")
            
            # 低波红利：剔除ATR过大的肥羊
            if 'atr' in filtered_df.columns and 'close_price' in filtered_df.columns:
                # 计算ATR相对比例
                atr_ratio = filtered_df['atr'] / filtered_df['close_price']
                atr_threshold = params.get('max_atr_ratio_defense', 0.05)  # 默认5%
                filtered_df = filtered_df[atr_ratio <= atr_threshold]
                logger.info(f"Level 2 (Defense): 低波筛选（ATR比例 <= {atr_threshold}），剩余 {len(filtered_df)} 只肥羊")
        
        else:
            # 震荡模式：使用默认阈值
            min_change_pct = params.get('min_change_pct', 7.0)  # 默认7%
            if 'change_pct' in filtered_df.columns:
                change_mask = filtered_df['change_pct'].notna()
                filtered_df = filtered_df[change_mask & (filtered_df['change_pct'] >= min_change_pct)]
                logger.info(f"Level 2 (Balance): 涨幅筛选 (>= {min_change_pct}%)，剩余 {len(filtered_df)} 只肥羊")
        
        # 通用过滤条件（所有模式都适用）
        # 1. 量比因子筛选
        vol_ratio_threshold = params.get('vol_ratio_ma20_threshold', 2.5)
        if 'vol_ratio_ma20' in filtered_df.columns:
            vol_ratio_mask = filtered_df['vol_ratio_ma20'].notna()
            filtered_df = filtered_df[vol_ratio_mask & (filtered_df['vol_ratio_ma20'] > vol_ratio_threshold)]
            logger.info(f"Level 2: 量比因子筛选 (Volume/MA20 > {vol_ratio_threshold})，剩余 {len(filtered_df)} 只肥羊")
        
        # 2. K线形态约束：上影线比例（根据市场状态调整）
        if regime == "Attack":
            max_upper_shadow_ratio = params.get('max_upper_shadow_ratio_attack', 0.2)  # 进攻模式放宽
        elif regime == "Defense":
            max_upper_shadow_ratio = params.get('max_upper_shadow_ratio_defense', 0.05)  # 防守模式严格
        else:
            max_upper_shadow_ratio = params.get('max_upper_shadow_ratio', 0.1)
        
        if 'upper_shadow_ratio' in filtered_df.columns:
            shadow_mask = filtered_df['upper_shadow_ratio'].notna()
            filtered_df = filtered_df[shadow_mask & (filtered_df['upper_shadow_ratio'] <= max_upper_shadow_ratio)]
            logger.info(f"Level 2: K线形态筛选 (上影线比例 <= {max_upper_shadow_ratio})，剩余 {len(filtered_df)} 只肥羊")
        
        # 3. VWAP避雷针过滤（Module 4的一部分）：收盘价低于VWAP，说明尾盘主力出货，一票否决
        if 'vwap' in filtered_df.columns and 'close_price' in filtered_df.columns:
            vwap_mask = filtered_df['close_price'] >= filtered_df['vwap']
            before_vwap = len(filtered_df)
            filtered_df = filtered_df[vwap_mask]
            after_vwap = len(filtered_df)
            logger.info(f"Level 2: VWAP避雷针过滤（收盘 >= VWAP），{before_vwap} -> {after_vwap} 只肥羊")
        
        # 4. 趋势护城河：股价 > MA(N)
        ma_support = params.get('ma_support', 'MA60')
        if ma_support == 'MA20' and 'ma20' in filtered_df.columns:
            ma20_mask = filtered_df['ma20'].notna()
            filtered_df = filtered_df[ma20_mask & (filtered_df['close_price'] > filtered_df['ma20'])]
        elif ma_support == 'MA60' and 'ma60' in filtered_df.columns:
            ma60_mask = filtered_df['ma60'].notna()
            filtered_df = filtered_df[ma60_mask & (filtered_df['close_price'] > filtered_df['ma60'])]
        
        # 5. 负面剔除：上市<60天
        if 'list_date' in filtered_df.columns:
            has_list_date = filtered_df['list_date'].notna()
            filtered_df['days_since_list'] = None
            
            if has_list_date.any():
                trade_date_col = pd.to_datetime(filtered_df.loc[has_list_date, 'trade_date'], errors='coerce')
                list_date_col = pd.to_datetime(filtered_df.loc[has_list_date, 'list_date'], errors='coerce')
                date_diff = trade_date_col - list_date_col
                filtered_df.loc[has_list_date, 'days_since_list'] = date_diff.dt.days
            
            filtered_df = filtered_df[
                filtered_df['days_since_list'].isna() | (filtered_df['days_since_list'] >= 60)
            ]
        
        # 6. 剔除ST肥羊
        if 'sheep_name' in filtered_df.columns:
            filtered_df = filtered_df[~filtered_df['sheep_name'].str.contains('ST', na=False)]
        
        # 7. 剔除涨停的肥羊
        if 'change_pct' in filtered_df.columns and 'is_star_market' in filtered_df.columns and 'is_gem' in filtered_df.columns:
            change_mask = filtered_df['change_pct'].notna()
            main_board_limit_up = (filtered_df['is_star_market'] == 0) & (filtered_df['is_gem'] == 0) & (filtered_df['change_pct'] >= 9.95)
            gem_star_limit_up = ((filtered_df['is_star_market'] == 1) | (filtered_df['is_gem'] == 1)) & (filtered_df['change_pct'] >= 19.95)
            limit_up_mask = main_board_limit_up | gem_star_limit_up
            filtered_df = filtered_df[~limit_up_mask]
            logger.info(f"Level 2: 剔除涨停肥羊，剩余 {len(filtered_df)} 只肥羊")
        
        return filtered_df
    
    def level3_scoring_engine(self, df: pd.DataFrame, params: Dict, regime_info: Dict) -> pd.DataFrame:
        """
        Module 4: 评分和微观结构（Level 3 & 4）
        
        1. 板块Beta弹性打分：
           - 针对科创板(688)和创业板(300)，使用动态公式
           - Score_Final = Score_Base × (1 + Is_Gem/Star × Factor_Regime × β_Stock)
           - 逻辑：进攻时Factor为正（奖励高Beta），防守时Factor为负（惩罚高波动）
        
        2. 拥挤度风控：
           - 若Turnover_Rate > 20% 且股价未涨停，且(High - Close) / (High - Low) > 0.4（长上影），视为出货形态，大幅扣分
        
        Args:
            df: 包含特征和板块共振数据的DataFrame
            params: 用户参数
            regime_info: 市场状态信息
        """
        if df.empty:
            return df
        
        scored_df = df.copy()
        regime = regime_info.get('regime', 'Balance')
        
        # 权重参数
        w_tech = params.get('w_tech', 0.4)
        w_trend = params.get('w_trend', 0.4)
        w_hot = params.get('w_hot', 0.2)
        
        # 1. 爆发力得分 (S_Explosion)
        vol_ratio = params.get('vol_ratio', 2.0)
        vol_threshold = params.get('vol_threshold', 2.0)
        
        scored_df['vol_ma_5'] = scored_df['vol_ma_5'].fillna(scored_df['volume'])
        scored_df['vol_ma_5'] = scored_df['vol_ma_5'].mask(scored_df['vol_ma_5'] == 0, scored_df['volume'])
        scored_df['vol_ratio'] = scored_df['volume'] / scored_df['vol_ma_5']
        scored_df['vol_ratio'] = scored_df['vol_ratio'].fillna(1.0)
        scored_df['vol_explosion'] = (scored_df['vol_ratio'] >= vol_threshold).astype(int) * 50
        
        s_explosion = scored_df['vol_explosion']
        
        # 2. 结构美感得分 (S_Structure)
        rps_threshold = params.get('rps_threshold', 90)
        if 'rps_250' in scored_df.columns:
            scored_df['rps_250'] = scored_df['rps_250'].fillna(0)
            scored_df['rps_score'] = (scored_df['rps_250'] >= rps_threshold).astype(int) * 30
        else:
            scored_df['rps_score'] = 0
        
        if 'vcp_factor' in scored_df.columns:
            scored_df['vcp_factor'] = scored_df['vcp_factor'].fillna(1.0)
            vcp_median = scored_df['vcp_factor'].median()
            if vcp_median > 0:
                scored_df['vcp_score'] = (1 - scored_df['vcp_factor'] / (vcp_median * 2)).clip(0, 1) * 20
            else:
                scored_df['vcp_score'] = 10
        else:
            scored_df['vcp_score'] = 0
        
        s_structure = scored_df['rps_score'] + scored_df['vcp_score']
        
        # 3. 板块协同得分 (S_Sector) - 使用板块共振分数
        # 检查是否启用板块共振（通过sector_boost参数控制）
        sector_boost = params.get('sector_boost', True)
        if sector_boost and 'sector_resonance_score' in scored_df.columns:
            scored_df['sector_score'] = scored_df['sector_resonance_score'].fillna(0)
            # 记录板块共振统计
            resonance_count = (scored_df['sector_score'] > 0).sum()
            penalty_count = (scored_df['sector_score'] < 0).sum()
            logger.info(f"Level 3: 板块共振启用，共振加分 {resonance_count} 只，孤军深入扣分 {penalty_count} 只")
        else:
            scored_df['sector_score'] = 0
            if not sector_boost:
                logger.info("Level 3: 板块共振已关闭（sector_boost=false）")
            else:
                logger.warning("Level 3: 板块共振数据不可用")
        
        s_sector = scored_df['sector_score']
        
        # 计算基础总分
        scored_df['total_score'] = (
            w_tech * s_explosion +
            w_trend * s_structure +
            w_hot * s_sector
        )
        
        # 板块Beta弹性打分（Module 4.1）
        if 'is_star_market' in scored_df.columns and 'is_gem' in scored_df.columns:
            # 根据市场状态设置Factor
            if regime == "Attack":
                factor_regime = params.get('beta_factor_attack', 0.15)  # 进攻时奖励高Beta
            elif regime == "Defense":
                factor_regime = params.get('beta_factor_defense', -0.15)  # 防守时惩罚高波动
            else:
                factor_regime = params.get('beta_factor_balance', 0.0)  # 震荡时中性
            
            # 计算肥羊的Beta（简化：使用ATR/Close作为波动率代理）
            if 'atr' in scored_df.columns and 'close_price' in scored_df.columns:
                scored_df['beta_proxy'] = scored_df['atr'] / scored_df['close_price']
            else:
                scored_df['beta_proxy'] = 0.0
            
            # 识别创业板和科创板
            gem_star_mask = (scored_df['is_star_market'] == 1) | (scored_df['is_gem'] == 1)
            
            # 应用Beta弹性公式：Score_Final = Score_Base × (1 + Is_Gem/Star × Factor_Regime × β_Stock)
            beta_adjustment = 1 + (gem_star_mask.astype(int) * factor_regime * scored_df['beta_proxy'])
            scored_df.loc[gem_star_mask, 'total_score'] = scored_df.loc[gem_star_mask, 'total_score'] * beta_adjustment[gem_star_mask]
            
            gem_star_count = gem_star_mask.sum()
            if gem_star_count > 0:
                logger.info(f"Level 3: 板块Beta弹性打分（{regime}模式，Factor={factor_regime}），共 {gem_star_count} 只肥羊获得调整")
        
        # 拥挤度风控（Module 4.2）
        if 'turnover_rate' in scored_df.columns and 'change_pct' in scored_df.columns:
            scored_df['turnover_rate'] = scored_df['turnover_rate'].fillna(0)
            
            # 判断是否涨停（主板>=9.95%，创业板/科创板>=19.95%）
            is_limit_up = pd.Series([False] * len(scored_df), index=scored_df.index)
            if 'is_star_market' in scored_df.columns and 'is_gem' in scored_df.columns:
                main_board_limit = (scored_df['is_star_market'] == 0) & (scored_df['is_gem'] == 0) & (scored_df['change_pct'] >= 9.95)
                gem_star_limit = ((scored_df['is_star_market'] == 1) | (scored_df['is_gem'] == 1)) & (scored_df['change_pct'] >= 19.95)
                is_limit_up = main_board_limit | gem_star_limit
            
            # 拥挤度条件：Turnover_Rate > 20% 且未涨停 且 长上影（upper_shadow_ratio > 0.4）
            if 'upper_shadow_ratio' in scored_df.columns:
                crowded_mask = (
                    (scored_df['turnover_rate'] > 20.0) &
                    (~is_limit_up) &
                    (scored_df['upper_shadow_ratio'] > 0.4)
                )
                
                # 大幅扣分（扣50分）
                scored_df.loc[crowded_mask, 'total_score'] = scored_df.loc[crowded_mask, 'total_score'] - 50
                
                crowded_count = crowded_mask.sum()
                if crowded_count > 0:
                    logger.info(f"Level 3: 拥挤度风控（扣分），共 {crowded_count} 只肥羊被扣分")
        
        # 排序
        scored_df = scored_df.sort_values('total_score', ascending=False)
        
        return scored_df
    
    def level4_ai_enhancement(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        """
        第4级：AI概率修正
        使用规则过滤假突破
        """
        if df.empty:
            return df
        
        ai_filter = params.get('ai_filter', True)
        enhanced_df = df.copy()
        
        # 计算胜率（简化规则，向量化操作）
        enhanced_df['turnover_rate'] = enhanced_df['turnover_rate'].fillna(0)
        enhanced_df['vol_ratio'] = enhanced_df['vol_ratio'].fillna(1.0)
        
        conditions = (
            (enhanced_df['turnover_rate'] > 1.0) &
            (enhanced_df['turnover_rate'] < 20.0) &
            (enhanced_df['vol_ratio'] >= 1.5)
        )
        
        if 'vcp_factor' in enhanced_df.columns:
            enhanced_df['vcp_factor'] = enhanced_df['vcp_factor'].fillna(1.0)
            conditions = conditions & (enhanced_df['vcp_factor'] < 0.3)
        
        enhanced_df['win_probability'] = np.where(conditions, 70, 40)
        
        if ai_filter:
            before_count = len(enhanced_df)
            enhanced_df = enhanced_df[enhanced_df['win_probability'] >= 60]
            after_count = len(enhanced_df)
            logger.info(f"Level 4 AI过滤: {before_count} -> {after_count} (胜率 >= 60%)")
            
            if enhanced_df.empty and before_count > 0:
                logger.warning("Level 4: 胜率>=60%过滤后无数据，放宽到>=50%")
                enhanced_df = df.copy()
                enhanced_df['turnover_rate'] = enhanced_df['turnover_rate'].fillna(0)
                enhanced_df['vol_ratio'] = enhanced_df['vol_ratio'].fillna(1.0)
                conditions = (
                    (enhanced_df['turnover_rate'] > 1.0) &
                    (enhanced_df['turnover_rate'] < 20.0) &
                    (enhanced_df['vol_ratio'] >= 1.5)
                )
                if 'vcp_factor' in enhanced_df.columns:
                    enhanced_df['vcp_factor'] = enhanced_df['vcp_factor'].fillna(1.0)
                    conditions = conditions & (enhanced_df['vcp_factor'] < 0.3)
                enhanced_df['win_probability'] = np.where(conditions, 70, 40)
                enhanced_df = enhanced_df[enhanced_df['win_probability'] >= 50]
        
        return enhanced_df
    
    def run_full_pipeline(self, trade_date: date, params: Dict, top_n: Optional[int] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        运行完整的T6共振流程
        
        Returns:
            (推荐结果列表, 诊断信息)
        """
        diagnostic_info = []
        
        try:
            # Module 1: RSRS市场状态识别（在Level 1之前执行）
            logger.info(f"Module 1: RSRS市场状态识别 (trade_date={trade_date})")
            regime_info = self.detect_market_regime(trade_date)
            diagnostic_info.append(f"市场状态: {regime_info['regime']} (RSRS Z-score: {regime_info['rsrs_zscore']:.3f})")
            
            # Level 1: 特征提取（集成板块共振）
            logger.info(f"Level 1: 特征提取 (trade_date={trade_date})")
            df = self.level1_extract_features(trade_date)
            
            if df.empty:
                msg = "Level 1返回空数据 - 可能原因：数据库中没有足够的历史数据（需要至少90天）"
                logger.warning(msg)
                diagnostic_info.append(f"Level 1: 提取失败 - 数据库中没有 {trade_date} 之前至少90天的肥羊数据")
                return [], " | ".join(diagnostic_info)
            
            logger.info(f"Level 1: 成功提取 {len(df)} 只肥羊的特征")
            diagnostic_info.append(f"Level 1: 提取了 {len(df)} 只肥羊")
            
            # Level 2: 自适应动态过滤（Module 3）
            logger.info(f"Level 2: 自适应动态过滤 (过滤前: {len(df)} 只肥羊)")
            before_level2 = len(df)
            df = self.level2_adaptive_filter(df, params, regime_info)
            after_level2 = len(df)
            logger.info(f"Level 2: 过滤后: {len(df)} 只肥羊")
            diagnostic_info.append(f"Level 2: {before_level2} -> {after_level2} 只肥羊 (市场状态: {regime_info['regime']})")
            
            if df.empty:
                msg = f"Level 2过滤后无数据 - 可能原因：过滤条件太严格或市场状态不适合"
                logger.warning(msg)
                diagnostic_info.append(f"Level 2: 所有肥羊被过滤 - 建议：根据市场状态调整参数")
                return [], " | ".join(diagnostic_info)
            
            # Level 3: 打分排序（Module 4）
            logger.info(f"Level 3: 打分排序 (当前: {len(df)} 只肥羊)")
            df = self.level3_scoring_engine(df, params, regime_info)
            max_score = df['total_score'].max() if not df.empty else 0
            logger.info(f"Level 3: 打分完成，最高分: {max_score:.2f}")
            diagnostic_info.append(f"Level 3: {len(df)} 只肥羊，最高分 {max_score:.2f}")
            
            # Level 4: AI修正
            logger.info(f"Level 4: AI修正 (过滤前: {len(df)} 只肥羊)")
            before_level4 = len(df)
            df = self.level4_ai_enhancement(df, params)
            after_level4 = len(df)
            logger.info(f"Level 4: AI修正后: {len(df)} 只肥羊")
            ai_filter_enabled = params.get('ai_filter', True)
            diagnostic_info.append(f"Level 4: {before_level4} -> {after_level4} 只肥羊 (AI过滤: {'开启' if ai_filter_enabled else '关闭'})")
            
            if df.empty:
                msg = f"Level 4过滤后无数据 - 可能原因：AI过滤条件太严格（胜率要求 >= 60%）"
                logger.warning(msg)
                diagnostic_info.append(f"Level 4: 所有肥羊被过滤 - 建议：关闭AI过滤或降低胜率要求")
                return [], " | ".join(diagnostic_info)
            
            # 确保数据按total_score降序排序
            df = df.sort_values('total_score', ascending=False).reset_index(drop=True)
            
            # 取Top N
            if top_n is not None and top_n > 0:
                top_stocks = df.head(top_n).copy()
                logger.info(f"最终推荐: {len(top_stocks)} 只肥羊（限制Top {top_n}）")
            else:
                top_stocks = df.copy()
                logger.info(f"最终推荐: {len(top_stocks)} 只肥羊（全部符合条件的肥羊）")
            
            if top_stocks.empty:
                logger.warning("Top N肥羊为空，无法生成推荐结果")
                return [], " | ".join(diagnostic_info)
            
            # 检查必要字段
            required_fields = ['sheep_code', 'sheep_name', 'close_price', 'total_score', 'win_probability', 'vol_ratio']
            missing_fields = [f for f in required_fields if f not in top_stocks.columns]
            if missing_fields:
                logger.error(f"缺少必要字段: {missing_fields}")
                logger.info(f"可用字段: {list(top_stocks.columns)}")
                return [], " | ".join(diagnostic_info)
            
            # 准备数据列
            top_stocks = top_stocks.copy()
            top_stocks['entry_price'] = top_stocks['close_price'].astype(float)
            top_stocks['stop_loss_price'] = (top_stocks['entry_price'] * self.STOP_LOSS_RATIO).round(2)
            
            # 填充缺失值
            vol_ratio_col = top_stocks.get('vol_ratio', pd.Series([0.0] * len(top_stocks), index=top_stocks.index)).fillna(0.0).astype(float)
            rps_250_col = top_stocks.get('rps_250', pd.Series([0.0] * len(top_stocks), index=top_stocks.index)).fillna(0.0).astype(float)
            vcp_factor_col = top_stocks.get('vcp_factor', pd.Series([1.0] * len(top_stocks), index=top_stocks.index)).fillna(1.0).astype(float)
            
            # 生成推荐理由
            def build_reasons(idx):
                reasons = []
                vol_ratio = vol_ratio_col.loc[idx] if idx in vol_ratio_col.index else 0.0
                rps_250 = rps_250_col.loc[idx] if idx in rps_250_col.index else 0.0
                vcp_factor = vcp_factor_col.loc[idx] if idx in vcp_factor_col.index else 1.0
                
                if vol_ratio >= params.get('vol_threshold', 2.0):
                    reasons.append(f"倍量{vol_ratio:.1f}倍")
                if rps_250 >= params.get('rps_threshold', 90):
                    reasons.append("RPS强势")
                if vcp_factor < 0.3:
                    reasons.append("VCP收敛")
                
                # 添加板块共振信息
                if 'is_main_sector' in top_stocks.columns and top_stocks.loc[idx, 'is_main_sector'] == 1:
                    reasons.append("板块共振")
                
                return " + ".join(reasons) if reasons else "技术突破"
            
            top_stocks['reason_tags'] = [build_reasons(idx) for idx in top_stocks.index]
            
            # 转换为字典列表
            result_df = top_stocks[[
                'sheep_code', 'sheep_name', 'entry_price', 'total_score',
                'win_probability', 'reason_tags', 'stop_loss_price'
            ]].copy()
            
            # 添加可选列
            if 'vol_ratio' in top_stocks.columns:
                result_df['vol_ratio'] = top_stocks['vol_ratio'].fillna(0.0).astype(float)
            else:
                result_df['vol_ratio'] = 0.0
            
            if 'rps_250' in top_stocks.columns:
                result_df['rps_250'] = top_stocks['rps_250'].where(pd.notna(top_stocks['rps_250']), None)
            else:
                result_df['rps_250'] = None
            
            if 'vcp_factor' in top_stocks.columns:
                result_df['vcp_factor'] = top_stocks['vcp_factor'].where(pd.notna(top_stocks['vcp_factor']), None)
            else:
                result_df['vcp_factor'] = None
            
            # 添加市场状态和板块信息
            result_df['market_regime'] = regime_info['regime']
            if 'sector_resonance_score' in top_stocks.columns:
                result_df['resonance_score'] = top_stocks['sector_resonance_score'].fillna(0.0).astype(float)
            else:
                result_df['resonance_score'] = 0.0
            
            if 'industry' in top_stocks.columns:
                result_df['sector_trend'] = top_stocks['industry'].fillna('未知')
            else:
                result_df['sector_trend'] = '未知'
            
            # 重命名列
            result_df = result_df.rename(columns={'total_score': 'ai_score'})
            
            # 计算涨幅信息（简化处理，与原版保持一致）
            today = date.today()
            trading_days_between = TradeDateAdapter.get_trading_days_in_range(trade_date, today)
            trading_days_count = len(trading_days_between)
            
            sheep_codes_list = result_df['sheep_code'].tolist()
            return_data = {}
            
            if sheep_codes_list and trading_days_count > 0:
                try:
                    with get_raw_connection() as conn:
                        sheep_placeholders = ','.join(['%s'] * len(sheep_codes_list))
                        latest_prices_query = f"""
                            SELECT sheep_code, close_price, trade_date
                            FROM sheep_daily
                            WHERE sheep_code IN ({sheep_placeholders})
                              AND trade_date <= %s
                              AND (sheep_code, trade_date) IN (
                                  SELECT sheep_code, MAX(trade_date)
                                  FROM sheep_daily
                                  WHERE sheep_code IN ({sheep_placeholders})
                                    AND trade_date <= %s
                                  GROUP BY sheep_code
                              )
                        """
                        latest_prices_df = pd.read_sql(
                            latest_prices_query, 
                            conn, 
                            params=sheep_codes_list + [today] + sheep_codes_list + [today]
                        )
                        
                        if trading_days_count >= 10:
                            date_5d_ago = trading_days_between[trading_days_count - 5] if trading_days_count >= 5 else None
                            date_10d_ago = trading_days_between[trading_days_count - 10] if trading_days_count >= 10 else None
                            
                            if date_5d_ago:
                                prices_5d_query = f"""
                                    SELECT sheep_code, close_price
                                    FROM sheep_daily
                                    WHERE sheep_code IN ({sheep_placeholders})
                                      AND trade_date = %s
                                """
                                prices_5d_df = pd.read_sql(prices_5d_query, conn, params=sheep_codes_list + [date_5d_ago])
                                prices_5d_dict = dict(zip(prices_5d_df['sheep_code'], prices_5d_df['close_price']))
                            else:
                                prices_5d_dict = {}
                            
                            if date_10d_ago:
                                prices_10d_query = f"""
                                    SELECT sheep_code, close_price
                                    FROM sheep_daily
                                    WHERE sheep_code IN ({sheep_placeholders})
                                      AND trade_date = %s
                                """
                                prices_10d_df = pd.read_sql(prices_10d_query, conn, params=sheep_codes_list + [date_10d_ago])
                                prices_10d_dict = dict(zip(prices_10d_df['sheep_code'], prices_10d_df['close_price']))
                            else:
                                prices_10d_dict = {}
                            
                            latest_prices_dict = dict(zip(latest_prices_df['sheep_code'], latest_prices_df['close_price']))
                            for sheep_code in sheep_codes_list:
                                return_data[sheep_code] = {'return_5d': None, 'return_10d': None, 'return_nd': None}
                                latest_price = latest_prices_dict.get(sheep_code)
                                if latest_price is None or pd.isna(latest_price):
                                    continue
                                latest_price = float(latest_price)
                                
                                if sheep_code in prices_5d_dict and prices_5d_dict[sheep_code] is not None and not pd.isna(prices_5d_dict[sheep_code]):
                                    price_5d = float(prices_5d_dict[sheep_code])
                                    if price_5d > 0:
                                        return_data[sheep_code]['return_5d'] = ((latest_price - price_5d) / price_5d) * 100
                                
                                if sheep_code in prices_10d_dict and prices_10d_dict[sheep_code] is not None and not pd.isna(prices_10d_dict[sheep_code]):
                                    price_10d = float(prices_10d_dict[sheep_code])
                                    if price_10d > 0:
                                        return_data[sheep_code]['return_10d'] = ((latest_price - price_10d) / price_10d) * 100
                        else:
                            date_5d_ago = trading_days_between[trading_days_count - 5] if trading_days_count >= 5 else None
                            
                            if date_5d_ago:
                                prices_5d_query = f"""
                                    SELECT sheep_code, close_price
                                    FROM sheep_daily
                                    WHERE sheep_code IN ({sheep_placeholders})
                                      AND trade_date = %s
                                """
                                prices_5d_df = pd.read_sql(prices_5d_query, conn, params=sheep_codes_list + [date_5d_ago])
                                prices_5d_dict = dict(zip(prices_5d_df['sheep_code'], prices_5d_df['close_price']))
                            else:
                                prices_5d_dict = {}
                            
                            latest_prices_dict = dict(zip(latest_prices_df['sheep_code'], latest_prices_df['close_price']))
                            entry_prices_dict = dict(zip(result_df['sheep_code'], result_df['entry_price']))
                            
                            for sheep_code in sheep_codes_list:
                                return_data[sheep_code] = {'return_5d': None, 'return_10d': None, 'return_nd': None}
                                latest_price = latest_prices_dict.get(sheep_code)
                                entry_price = entry_prices_dict.get(sheep_code)
                                
                                if latest_price is None or pd.isna(latest_price) or entry_price is None or pd.isna(entry_price):
                                    continue
                                
                                latest_price = float(latest_price)
                                entry_price = float(entry_price)
                                
                                if sheep_code in prices_5d_dict and prices_5d_dict[sheep_code] is not None and not pd.isna(prices_5d_dict[sheep_code]):
                                    price_5d = float(prices_5d_dict[sheep_code])
                                    if price_5d > 0:
                                        return_data[sheep_code]['return_5d'] = ((latest_price - price_5d) / price_5d) * 100
                                
                                if entry_price > 0:
                                    return_data[sheep_code]['return_nd'] = ((latest_price - entry_price) / entry_price) * 100
                                
                except Exception as e:
                    logger.warning(f"批量计算涨幅信息失败: {e}", exc_info=True)
                    for sheep_code in sheep_codes_list:
                        return_data[sheep_code] = {'return_5d': None, 'return_10d': None, 'return_nd': None}
            
            # 转换为字典列表
            results = result_df.to_dict('records')
            
            # 确保类型正确并添加涨幅信息
            for r in results:
                r['sheep_code'] = str(r['sheep_code'])
                r['sheep_name'] = str(r['sheep_name'])
                r['entry_price'] = float(r['entry_price'])
                r['ai_score'] = float(r['ai_score'])
                r['win_probability'] = float(r['win_probability'])
                r['reason_tags'] = str(r['reason_tags'])
                r['stop_loss_price'] = float(r['stop_loss_price'])
                r['vol_ratio'] = float(r['vol_ratio']) if r['vol_ratio'] is not None else 0.0
                if r['rps_250'] is not None:
                    r['rps_250'] = float(r['rps_250'])
                if r['vcp_factor'] is not None:
                    r['vcp_factor'] = float(r['vcp_factor'])
                
                # 添加涨幅信息
                sheep_code = r['sheep_code']
                return_info = return_data.get(sheep_code, {'return_5d': None, 'return_10d': None, 'return_nd': None})
                r['return_5d'] = return_info['return_5d']
                r['return_10d'] = return_info['return_10d']
                r['return_nd'] = return_info['return_nd']
                
                # 添加市场状态和板块信息
                r['market_regime'] = str(r.get('market_regime', 'Balance'))
                r['resonance_score'] = float(r.get('resonance_score', 0.0))
                r['sector_trend'] = str(r.get('sector_trend', '未知'))
            
            logger.info(f"成功生成 {len(results)} 条推荐结果")
            diagnostic_info.append(f"最终: 生成 {len(results)} 条推荐")
            return results, " | ".join(diagnostic_info)
            
        except Exception as e:
            logger.error(f"T6共振模型运行失败: {e}", exc_info=True)
            diagnostic_info.append(f"异常: {str(e)}")
            return [], " | ".join(diagnostic_info)
