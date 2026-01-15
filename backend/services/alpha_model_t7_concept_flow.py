"""
AlphaModel_T7_Concept_Flow - 概念资金双驱模型 v2.0

简化使用说明：
================
1. 默认参数已优化，直接调用即可：
   model = AlphaModelT7ConceptFlow()
   results, info = model.run_full_pipeline(trade_date)

2. 如需调整，只需关注4个核心参数：
   params = {
       'min_change_pct': 2.0,    # 最低涨幅(%)
       'max_change_pct': 9.5,    # 最高涨幅(%)
       'vol_threshold': 1.5,     # 量比阈值
       'concept_boost': True,    # 是否启用概念共振
   }
   results, info = model.run_full_pipeline(trade_date, params)

3. 市场状态会自动识别并调整策略：
   - Attack(进攻): 追涨，重技术因子
   - Defense(防守): 低吸，重概念因子
   - Balance(震荡): 均衡策略

核心特性：
- 概念竞速引擎：精准捕捉最强概念驱动力
- 主力资金验证：结合资金流数据验证主力意图
- 动态权重调整：根据市场状态自动优化评分权重
- 多因子胜率模型：综合技术、资金、概念多维度评估
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import date, timedelta
import logging
import warnings
from db.database import get_raw_connection, get_sqlalchemy_engine
from etl.trade_date_adapter import TradeDateAdapter
import statsmodels.api as sm
from scipy.stats import zscore
from collections import defaultdict

# 抑制pandas关于DBAPI2连接的警告（我们使用的是pymysql，功能正常）
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy')

logger = logging.getLogger(__name__)


class AlphaModelT7ConceptFlow:
    """
    T7概念资金双驱模型 v2.0 - 概念竞速+资金验证
    
    简化配置说明：
    - 所有参数已收敛到 DEFAULT_PARAMS，用户只需调整少量核心参数
    - 默认参数已优化，适合大多数市场环境
    - 市场状态会自动识别并调整策略
    """
    
    # ============================================
    # 核心常量（一般不需要修改）
    # ============================================
    MIN_HISTORY_DAYS = 60          # 最少历史数据天数
    HISTORY_DAYS_FOR_FACTORS = 90  # 因子计算历史天数
    STOP_LOSS_RATIO = 0.93         # 止损比例（7%止损）
    DEFAULT_TOP_N = 5              # 默认推荐数量
    
    # RSRS市场状态识别参数
    RSRS_LOOKBACK_DAYS = 18        # RSRS回归窗口
    RSRS_ATTACK_THRESHOLD = 0.7    # 进攻模式阈值
    RSRS_DEFENSE_THRESHOLD = -0.7  # 防守模式阈值
    
    # 概念竞速参数
    CONCEPT_MAIN_TREND_AVG_CHG = 1.0   # 主线概念平均涨幅阈值(%)
    CONCEPT_MAIN_TREND_BREADTH = 0.15  # 主线概念广度阈值(15%)
    CONCEPT_LEADER_CHG = 7.0           # 概念领头羊涨幅阈值(%)
    CONCEPT_LEADER_TOP_N = 5           # 概念内领涨股数量
    
    # ============================================
    # 默认参数配置（简化版，只保留核心参数）
    # ============================================
    DEFAULT_PARAMS = {
        # 核心筛选参数
        'min_change_pct': 2.0,       # 最低涨幅(%)
        'max_change_pct': 9.5,       # 最高涨幅(%)
        'vol_threshold': 1.5,        # 量比阈值
        'rps_threshold': 80,         # RPS强度阈值
        
        # 功能开关
        'concept_boost': True,       # 概念共振加分
        'ai_filter': True,           # AI胜率过滤
        'min_win_probability': 45,   # 最低胜率(%)
    }
    
    @classmethod
    def get_default_params(cls) -> Dict:
        """获取默认参数配置（用户可基于此修改）"""
        return cls.DEFAULT_PARAMS.copy()
    
    @classmethod
    def merge_params(cls, user_params: Dict = None) -> Dict:
        """合并用户参数和默认参数"""
        params = cls.DEFAULT_PARAMS.copy()
        if user_params:
            params.update(user_params)
        return params
    
    def __init__(self):
        self.model_version = "T7_Concept_Flow_v2"
        self.regime = "Balance"  # 市场状态：Attack/Defense/Balance
        self.rsrs_zscore = 0.0   # RSRS标准分
    
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
    
    def calculate_concept_resonance(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """
        Module 1: 概念竞速引擎（核心升级）
        
        原理：一只股票属于多个板块（行业+多个概念）。必须找出当前表现最强的那个板块作为它的"真实身份"。
        
        实现步骤（向量化，严禁循环）：
        1. 数据准备：从数据库获取每只股票的概念列表（通过sheep_concept_mapping表）
        2. 维度展开 (Explode)：将一只股票裂变为多行（每行对应一个概念+行业）
        3. 全市场竞速：按tag分组，计算每个概念/行业的指标
        4. 最强身份确认：对于每只股票，在它所属的所有tag中，选取Tag_Avg_Chg最大的那个作为Resonance_Base
        
        Args:
            df: 包含sheep_code, industry, change_pct等字段的DataFrame
            trade_date: 交易日期
            
        Returns:
            添加了概念共振相关字段的DataFrame
        """
        if df.empty:
            logger.warning("概念竞速计算：数据为空")
            df['concept_resonance_score'] = 0.0
            df['resonance_base_tag'] = df.get('industry', '未知').fillna('未知')
            df['is_main_concept'] = 0
            df['is_concept_leader'] = 0
            return df
        
        try:
            # 步骤1：获取所有股票的概念列表
            sheep_codes = df['sheep_code'].unique().tolist()
            if not sheep_codes:
                logger.warning("概念竞速计算：没有股票代码")
                df['concept_resonance_score'] = 0.0
                df['resonance_base_tag'] = df.get('industry', '未知').fillna('未知')
                df['is_main_concept'] = 0
                df['is_concept_leader'] = 0
                return df
            
            # 从数据库批量获取概念数据
            with get_raw_connection() as conn:
                # 构建参数化查询
                placeholders = ','.join(['%s'] * len(sheep_codes))
                query_concepts = f"""
                    SELECT 
                        scm.sheep_code,
                        ct.concept_name,
                        scm.weight
                    FROM sheep_concept_mapping scm
                    INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                    WHERE scm.sheep_code IN ({placeholders})
                      AND ct.is_active = 1
                    ORDER BY scm.sheep_code, scm.weight DESC
                """
                
                # 使用SQLAlchemy引擎避免pandas警告
                df_concepts = pd.read_sql(query_concepts, get_sqlalchemy_engine(), params=sheep_codes)
            
            # 如果数据库中没有概念数据，降级使用industry字段
            if df_concepts.empty:
                logger.warning("概念竞速计算：数据库中没有概念数据，降级使用industry字段")
                # 降级逻辑：直接使用industry作为tag
                df['concept_resonance_score'] = 0.0
                df['resonance_base_tag'] = df.get('industry', '未知').fillna('未知')
                df['is_main_concept'] = 0
                df['is_concept_leader'] = 0
                return df
            
            # 步骤2：维度展开 - 将每只股票的概念列表展开为多行
            # 为每只股票创建tag列表（概念+行业）
            df_tags = []
            for sheep_code in sheep_codes:
                # 获取该股票的所有概念
                concepts = df_concepts[df_concepts['sheep_code'] == sheep_code]['concept_name'].tolist()
                # 获取该股票的行业
                industry = df[df['sheep_code'] == sheep_code]['industry'].iloc[0] if len(df[df['sheep_code'] == sheep_code]) > 0 else None
                
                # 创建tag列表：概念 + 行业
                tags = concepts.copy()
                if industry and pd.notna(industry) and industry != '未知':
                    tags.append(industry)
                
                # 如果没有任何tag，使用"未知"作为默认tag
                if not tags:
                    tags = ['未知']
                
                # 为每个tag创建一行
                for tag in tags:
                    df_tags.append({
                        'sheep_code': sheep_code,
                        'tag': tag,
                        'tag_type': 'concept' if tag in concepts else 'industry'
                    })
            
            df_tags = pd.DataFrame(df_tags)
            
            if df_tags.empty:
                logger.warning("概念竞速计算：展开后没有tag数据")
                df['concept_resonance_score'] = 0.0
                df['resonance_base_tag'] = df.get('industry', '未知').fillna('未知')
                df['is_main_concept'] = 0
                df['is_concept_leader'] = 0
                return df
            
            # 将股票数据merge到tag数据中
            df_merge = df_tags.merge(
                df[['sheep_code', 'change_pct']],
                on='sheep_code',
                how='left'
            )
            
            # 填充缺失值
            df_merge['change_pct'] = pd.to_numeric(df_merge['change_pct'], errors='coerce').fillna(0.0)
            
            # 步骤3：全市场竞速 - 按tag分组，计算每个tag的指标（向量化）
            tag_stats = df_merge.groupby('tag').agg({
                'change_pct': ['mean', 'count', 'std'],
                'sheep_code': lambda x: x.tolist()  # 保存该tag下的所有股票代码
            }).reset_index()
            
            # 扁平化列名
            tag_stats.columns = ['tag', 'tag_avg_chg', 'tag_count', 'tag_std', 'tag_sheep_codes']
            
            # 计算Tag_Breadth（上涨比例）：change_pct > 3%的股票占比
            tag_breadth = df_merge.groupby('tag').apply(
                lambda g: (g['change_pct'] > 3.0).sum() / len(g) if len(g) > 0 else 0.0
            ).reset_index(name='tag_breadth')
            
            tag_stats = tag_stats.merge(tag_breadth, on='tag', how='left')
            tag_stats['tag_breadth'] = tag_stats['tag_breadth'].fillna(0.0)
            
            # 计算Tag_Zscore（热度标准分）：基于tag_avg_chg的zscore
            if len(tag_stats) > 1:
                tag_stats['tag_zscore'] = zscore(tag_stats['tag_avg_chg'].fillna(0.0))
            else:
                tag_stats['tag_zscore'] = 0.0
            
            # 填充缺失值
            tag_stats['tag_avg_chg'] = tag_stats['tag_avg_chg'].fillna(0.0)
            tag_stats['tag_count'] = tag_stats['tag_count'].fillna(0).astype(int)
            tag_stats['tag_std'] = tag_stats['tag_std'].fillna(0.0)
            tag_stats['tag_zscore'] = tag_stats['tag_zscore'].fillna(0.0)
            
            # 步骤4：最强身份确认 - 将tag指标merge回原股票数据
            # 对于每只股票，找出它所属的所有tag中，tag_avg_chg最大的那个
            df_with_tags = df_tags.merge(tag_stats[['tag', 'tag_avg_chg', 'tag_breadth', 'tag_zscore', 'tag_count']], 
                                         on='tag', how='left')
            
            # 按sheep_code分组，找出tag_avg_chg最大的tag（最强身份）
            best_tag = df_with_tags.loc[
                df_with_tags.groupby('sheep_code')['tag_avg_chg'].idxmax()
            ][['sheep_code', 'tag', 'tag_avg_chg', 'tag_breadth', 'tag_zscore']].copy()
            best_tag.columns = ['sheep_code', 'resonance_base_tag', 'resonance_base_avg_chg', 
                               'resonance_base_breadth', 'resonance_base_zscore']
            
            # Merge回原数据
            df = df.merge(best_tag, on='sheep_code', how='left')
            
            # 填充缺失值
            df['resonance_base_tag'] = df['resonance_base_tag'].fillna(df.get('industry', '未知')).fillna('未知')
            df['resonance_base_avg_chg'] = df['resonance_base_avg_chg'].fillna(0.0)
            df['resonance_base_breadth'] = df['resonance_base_breadth'].fillna(0.0)
            df['resonance_base_zscore'] = df['resonance_base_zscore'].fillna(0.0)
            
            # 计算主线概念判定：resonance_base_avg_chg > 1.5% 且 resonance_base_breadth > 20%
            df['is_main_concept'] = (
                (df['resonance_base_avg_chg'] > self.CONCEPT_MAIN_TREND_AVG_CHG) &
                (df['resonance_base_breadth'] > self.CONCEPT_MAIN_TREND_BREADTH)
            ).astype(int)
            
            # 计算概念领头羊判定：在该概念中涨幅排名前3
            # 需要找出每只股票在其resonance_base_tag中的排名
            df['is_concept_leader'] = 0
            for tag_name in df['resonance_base_tag'].unique():
                if tag_name == '未知':
                    continue
                tag_stocks = df[df['resonance_base_tag'] == tag_name].copy()
                if len(tag_stocks) > 0:
                    tag_stocks = tag_stocks.sort_values('change_pct', ascending=False)
                    top_n = min(self.CONCEPT_LEADER_TOP_N, len(tag_stocks))
                    leader_codes = tag_stocks.head(top_n)['sheep_code'].tolist()
                    df.loc[df['sheep_code'].isin(leader_codes), 'is_concept_leader'] = 1
            
            # 计算概念共振分数 - v2.0优化版
            # 主线概念加分（使用放宽后的阈值）
            main_concept_bonus = df['is_main_concept'] * 35  # 从40降低到35
            
            # 概念领头羊加分（使用放宽后的TOP_N）
            leader_bonus = df['is_concept_leader'] * 25  # 从30降低到25
            
            # v2.0新增：概念热度加分（基于resonance_base_zscore）
            # zscore > 1 加20分，zscore > 0.5 加10分
            concept_heat_bonus = np.where(
                df['resonance_base_zscore'] > 1.0, 
                20,
                np.where(df['resonance_base_zscore'] > 0.5, 10, 0)
            )
            
            # v2.0新增：概念广度加分（基于resonance_base_breadth）
            # breadth > 30% 加15分，breadth > 20% 加10分
            concept_breadth_bonus = np.where(
                df['resonance_base_breadth'] > 0.30, 
                15,
                np.where(df['resonance_base_breadth'] > 0.20, 10, 0)
            )
            
            # 孤军深入惩罚：放宽条件，只有当板块表现极差且个股涨幅很大时才扣分
            # 逻辑：如果resonance_base_avg_chg < 0.3% 且 change_pct > 7%，则扣分（从6%提高到7%）
            solo_penalty = (
                (df['resonance_base_avg_chg'] < 0.3) &  # 从0.5降低到0.3
                (df['change_pct'] > 7.0)  # 从6%提高到7%
            ).astype(int) * -30  # 从-50降低到-30
            
            df['concept_resonance_score'] = (
                main_concept_bonus + 
                leader_bonus + 
                concept_heat_bonus + 
                concept_breadth_bonus + 
                solo_penalty
            )
            
            # 统计信息
            main_concept_count = df['is_main_concept'].sum()
            leader_count = df['is_concept_leader'].sum()
            solo_penalty_count = (solo_penalty < 0).sum()
            
            logger.info(f"概念竞速计算完成：")
            logger.info(f"  - 主线概念: {main_concept_count} 只股票")
            logger.info(f"  - 概念领头羊: {leader_count} 只股票")
            logger.info(f"  - 孤军深入扣分: {solo_penalty_count} 只股票")
            logger.info(f"  - 共振分数范围: {df['concept_resonance_score'].min():.0f} ~ {df['concept_resonance_score'].max():.0f}")
            
            return df
            
        except Exception as e:
            logger.error(f"概念竞速计算失败: {e}", exc_info=True)
            import traceback
            logger.error(traceback.format_exc())
            # 降级处理：使用industry字段
            df['concept_resonance_score'] = 0.0
            df['resonance_base_tag'] = df.get('industry', '未知').fillna('未知')
            df['is_main_concept'] = 0
            df['is_concept_leader'] = 0
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
                
                # 步骤8：计算资金流因子（Module 2: Smart Money Flow）
                # 8.1 Intraday Intensity (II%): 识别资金是在日内高点买入（抢筹）还是低点买入（护盘）
                # II = (2*Close - High - Low) / (High - Low) * Volume
                # 需要5日历史数据计算标准化II%
                if not df_history.empty:
                    df_history = df_history.sort_values(['sheep_code', 'trade_date'])
                    for sheep_code, group in df_history.groupby('sheep_code'):
                        group = group.sort_values('trade_date').reset_index(drop=True)
                        if len(group) >= 5:
                            recent_5 = group.tail(5)
                            # 计算每日II
                            price_range = recent_5['high_price'] - recent_5['low_price']
                            ii_raw = np.where(
                                price_range > 0,
                                (2 * recent_5['close_price'] - recent_5['high_price'] - recent_5['low_price']) / price_range * recent_5['volume'],
                                0
                            )
                            # 标准化：II% = sum(II) / sum(Volume)
                            ii_sum = np.sum(ii_raw)
                            vol_sum = recent_5['volume'].sum()
                            ii_pct = (ii_sum / vol_sum * 100) if vol_sum > 0 else 0.0
                            
                            idx = df_latest[df_latest['sheep_code'] == sheep_code].index
                            if len(idx) > 0:
                                df_latest.loc[idx, 'intraday_intensity_pct'] = ii_pct
                    
                    # 计算量价相关性（过去10日Volume和Close的相关系数）
                    for sheep_code, group in df_history.groupby('sheep_code'):
                        group = group.sort_values('trade_date').reset_index(drop=True)
                        if len(group) >= 10:
                            recent_10 = group.tail(10)
                            if len(recent_10) > 1:
                                # 检查数据是否有变化，避免除以0的警告
                                vol_std = recent_10['volume'].std()
                                price_std = recent_10['close_price'].std()
                                if vol_std > 0 and price_std > 0 and not pd.isna(vol_std) and not pd.isna(price_std):
                                    try:
                                        vol_corr = recent_10['volume'].corr(recent_10['close_price'])
                                        if pd.isna(vol_corr):
                                            vol_corr = 0.0
                                    except Exception:
                                        vol_corr = 0.0
                                else:
                                    vol_corr = 0.0
                                idx = df_latest[df_latest['sheep_code'] == sheep_code].index
                                if len(idx) > 0:
                                    df_latest.loc[idx, 'volume_price_correlation'] = vol_corr
                    
                    # 8.2 计算RSI_6（相对强弱指标）
                    for sheep_code, group in df_history.groupby('sheep_code'):
                        group = group.sort_values('trade_date').reset_index(drop=True)
                        if len(group) >= 7:  # 需要至少7天数据计算RSI_6
                            recent_7 = group.tail(7)
                            price_changes = recent_7['close_price'].diff().dropna()
                            if len(price_changes) >= 6:
                                gains = price_changes.clip(lower=0)
                                losses = (-price_changes.clip(upper=0))
                                avg_gain = gains.mean()
                                avg_loss = losses.mean()
                                if avg_loss > 0:
                                    rs = avg_gain / avg_loss
                                    rsi_6 = 100 - (100 / (1 + rs))
                                elif avg_gain > 0:
                                    rsi_6 = 100.0
                                else:
                                    rsi_6 = 50.0
                                idx = df_latest[df_latest['sheep_code'] == sheep_code].index
                                if len(idx) > 0:
                                    df_latest.loc[idx, 'rsi_6'] = rsi_6
                
                # 填充缺失值
                df_latest['intraday_intensity_pct'] = df_latest.get('intraday_intensity_pct', pd.Series([0.0] * len(df_latest), index=df_latest.index)).fillna(0.0)
                df_latest['volume_price_correlation'] = df_latest.get('volume_price_correlation', pd.Series([0.0] * len(df_latest), index=df_latest.index)).fillna(0.0)
                df_latest['rsi_6'] = df_latest.get('rsi_6', pd.Series([50.0] * len(df_latest), index=df_latest.index)).fillna(50.0)
                
                # 8.3 从sheep_money_flow表获取主力资金净流入数据（新增v2.0）
                try:
                    sheep_codes = df_latest['sheep_code'].tolist()
                    if sheep_codes:
                        sheep_placeholders = ','.join(['%s'] * len(sheep_codes))
                        money_flow_query = f"""
                            SELECT 
                                sheep_code,
                                main_net_inflow,
                                super_large_inflow,
                                large_inflow
                            FROM sheep_money_flow
                            WHERE sheep_code IN ({sheep_placeholders})
                              AND trade_date = %s
                        """
                        df_money_flow = pd.read_sql(
                            money_flow_query, 
                            conn, 
                            params=sheep_codes + [trade_date]
                        )
                        
                        if not df_money_flow.empty:
                            # 合并主力资金数据
                            df_latest = df_latest.merge(
                                df_money_flow[['sheep_code', 'main_net_inflow', 'super_large_inflow', 'large_inflow']],
                                on='sheep_code',
                                how='left'
                            )
                            logger.info(f"Level 1: 成功获取 {len(df_money_flow)} 只肥羊的主力资金数据")
                        else:
                            df_latest['main_net_inflow'] = 0.0
                            df_latest['super_large_inflow'] = 0.0
                            df_latest['large_inflow'] = 0.0
                            logger.warning(f"Level 1: 未找到 {trade_date} 的主力资金数据")
                except Exception as e:
                    logger.warning(f"Level 1: 获取主力资金数据失败: {e}")
                    df_latest['main_net_inflow'] = 0.0
                    df_latest['super_large_inflow'] = 0.0
                    df_latest['large_inflow'] = 0.0
                
                # 填充主力资金缺失值
                df_latest['main_net_inflow'] = df_latest.get('main_net_inflow', pd.Series([0.0] * len(df_latest), index=df_latest.index)).fillna(0.0)
                df_latest['super_large_inflow'] = df_latest.get('super_large_inflow', pd.Series([0.0] * len(df_latest), index=df_latest.index)).fillna(0.0)
                df_latest['large_inflow'] = df_latest.get('large_inflow', pd.Series([0.0] * len(df_latest), index=df_latest.index)).fillna(0.0)
                
                # 步骤9：计算筹码获利盘（Module 4: Cost Structure）
                # ASR (Average Supply Ratio)：利用换手率衰减模拟持仓成本
                # 简化实现：使用最近20日的平均价格作为成本价
                if not df_history.empty:
                    df_history = df_history.sort_values(['sheep_code', 'trade_date'])
                    for sheep_code, group in df_history.groupby('sheep_code'):
                        group = group.sort_values('trade_date').reset_index(drop=True)
                        if len(group) >= 20:
                            recent_20 = group.tail(20)
                            cost_avg = recent_20['close_price'].mean()
                            idx = df_latest[df_latest['sheep_code'] == sheep_code].index
                            if len(idx) > 0:
                                current_price = df_latest.loc[idx, 'close_price'].iloc[0]
                                # 真空区判断：若 Close > Cost_Avg * 1.15，视为突破密集套牢区，上方无阻力
                                df_latest.loc[idx, 'cost_avg'] = cost_avg
                                df_latest.loc[idx, 'is_vacuum_zone'] = (current_price > cost_avg * 1.15).astype(int) if cost_avg > 0 else 0
                
                # 填充缺失值
                df_latest['cost_avg'] = df_latest.get('cost_avg', df_latest['close_price']).fillna(df_latest['close_price'])
                df_latest['is_vacuum_zone'] = df_latest.get('is_vacuum_zone', pd.Series([0] * len(df_latest), index=df_latest.index)).fillna(0)
                
                # 步骤10：集成概念竞速计算（Module 1: Concept Resonance Engine）
                df_latest = self.calculate_concept_resonance(df_latest, trade_date)
                
                logger.info(f"Level 1: 成功提取 {len(df_latest)} 只肥羊的特征（含概念竞速、资金流、筹码分析）")
                
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
        
        # 获取核心参数（使用统一的参数名，简化配置）
        min_change_pct = params.get('min_change_pct', 2.0)
        max_change_pct = params.get('max_change_pct', 9.5)
        concept_boost = params.get('concept_boost', True)
        rps_threshold = params.get('rps_threshold', 80)
        
        # 根据市场状态自动调整涨幅范围
        if regime == "Attack":
            # 进攻模式：允许追高
            min_change_pct = max(min_change_pct, 3.0)  # 至少3%
            max_change_pct = min(max_change_pct, 9.5)  # 最高9.5%
            logger.info(f"Level 2 (Attack): 自动调整涨幅范围 {min_change_pct}% ~ {max_change_pct}%")
        elif regime == "Defense":
            # 防守模式：禁止追高
            min_change_pct = 0.0  # 允许低涨幅
            max_change_pct = min(max_change_pct, 4.0)  # 最高4%
            logger.info(f"Level 2 (Defense): 自动调整涨幅范围 {min_change_pct}% ~ {max_change_pct}%")
        
        # 涨幅筛选（所有模式通用）
        if 'change_pct' in filtered_df.columns:
            change_mask = filtered_df['change_pct'].notna()
            
            # 防守模式：概念领头羊豁免涨幅上限
            if regime == "Defense" and 'is_concept_leader' in filtered_df.columns:
                leader_exempt = filtered_df['is_concept_leader'] == 1
                filtered_df = filtered_df[
                    change_mask & (
                        ((filtered_df['change_pct'] >= min_change_pct) & 
                         (filtered_df['change_pct'] <= max_change_pct)) | 
                        leader_exempt
                    )
                ]
                logger.info(f"Level 2 ({regime}): 涨幅筛选，概念龙头豁免，剩余 {len(filtered_df)} 只")
            else:
                filtered_df = filtered_df[
                    change_mask & 
                    (filtered_df['change_pct'] >= min_change_pct) & 
                    (filtered_df['change_pct'] <= max_change_pct)
                ]
                logger.info(f"Level 2 ({regime}): 涨幅筛选 ({min_change_pct}% ~ {max_change_pct}%)，剩余 {len(filtered_df)} 只")
        
        # 进攻模式：优先概念共振或RPS强势
        if regime == "Attack" and concept_boost:
            if 'concept_resonance_score' in filtered_df.columns and 'rps_250' in filtered_df.columns:
                resonance_mask = filtered_df['concept_resonance_score'] > 0
                rps_mask = filtered_df['rps_250'].fillna(0) > rps_threshold
                priority_mask = resonance_mask | rps_mask
                if priority_mask.any():
                    before_count = len(filtered_df)
                    filtered_df = filtered_df[priority_mask]
                    logger.info(f"Level 2 (Attack): 概念共振/RPS优先，{before_count} -> {len(filtered_df)} 只")
        
        # 防守模式：低吸策略
        if regime == "Defense" and len(filtered_df) > 0:
            low_buy_mask = pd.Series([True] * len(filtered_df), index=filtered_df.index)
            
            # RSI超卖或乖离率超跌
            if 'rsi_6' in filtered_df.columns:
                rsi_oversold = filtered_df['rsi_6'].fillna(50) < 30
                if rsi_oversold.any():
                    low_buy_mask = low_buy_mask | rsi_oversold
            
            if 'bias_20' in filtered_df.columns:
                bias_oversold = filtered_df['bias_20'].fillna(0) < -5
                if bias_oversold.any():
                    low_buy_mask = low_buy_mask | bias_oversold
            
            # 不强制过滤，只记录日志
            oversold_count = low_buy_mask.sum()
            logger.info(f"Level 2 (Defense): 超卖/超跌肥羊 {oversold_count} 只")
        
        # ============================================
        # 通用过滤条件（默认开启，不需要用户配置）
        # ============================================
        
        before_common = len(filtered_df)
        
        # 1. 剔除新股（上市<60天）
        if 'list_date' in filtered_df.columns:
            has_list_date = filtered_df['list_date'].notna()
            filtered_df['days_since_list'] = None
            if has_list_date.any():
                trade_date_col = pd.to_datetime(filtered_df.loc[has_list_date, 'trade_date'], errors='coerce')
                list_date_col = pd.to_datetime(filtered_df.loc[has_list_date, 'list_date'], errors='coerce')
                filtered_df.loc[has_list_date, 'days_since_list'] = (trade_date_col - list_date_col).dt.days
            filtered_df = filtered_df[filtered_df['days_since_list'].isna() | (filtered_df['days_since_list'] >= 60)]
        
        # 2. 剔除ST肥羊
        if 'sheep_name' in filtered_df.columns:
            filtered_df = filtered_df[~filtered_df['sheep_name'].str.contains('ST', na=False)]
        
        # 3. 剔除涨停肥羊
        if 'change_pct' in filtered_df.columns and 'is_star_market' in filtered_df.columns and 'is_gem' in filtered_df.columns:
            main_board_limit = (filtered_df['is_star_market'] == 0) & (filtered_df['is_gem'] == 0) & (filtered_df['change_pct'] >= 9.95)
            gem_star_limit = ((filtered_df['is_star_market'] == 1) | (filtered_df['is_gem'] == 1)) & (filtered_df['change_pct'] >= 19.95)
            filtered_df = filtered_df[~(main_board_limit | gem_star_limit)]
        
        logger.info(f"Level 2: 通用过滤（新股/ST/涨停），{before_common} -> {len(filtered_df)} 只")
        
        return filtered_df
    
    def level3_scoring_engine(self, df: pd.DataFrame, params: Dict, regime_info: Dict) -> pd.DataFrame:
        """
        Module 4: 评分和微观结构（Level 3 & 4）- v2.0优化版
        
        1. 动态权重调整：根据市场状态调整评分权重
           - 进攻模式：提高技术因子权重，降低概念权重（追求爆发力）
           - 防守模式：提高概念权重，降低技术因子权重（追求安全性）
           - 震荡模式：均衡权重
        
        2. 板块Beta弹性打分：
           - 针对科创板(688)和创业板(300)，使用动态公式
           - Score_Final = Score_Base × (1 + Is_Gem/Star × Factor_Regime × β_Stock)
        
        3. 主力资金因子加分（新增v2.0）：
           - 主力净流入 > 0 加分
           - 超大单净流入 > 0 额外加分
        
        4. 拥挤度风控：
           - 若Turnover_Rate > 20% 且股价未涨停，且长上影，视为出货形态，大幅扣分
        
        Args:
            df: 包含特征和板块共振数据的DataFrame
            params: 用户参数
            regime_info: 市场状态信息
        """
        if df.empty:
            return df
        
        scored_df = df.copy()
        regime = regime_info.get('regime', 'Balance')
        
        # 动态权重（内置配置，根据市场状态自动调整）
        WEIGHT_CONFIG = {
            'Attack':  {'tech': 0.50, 'trend': 0.30, 'hot': 0.20},  # 进攻：重技术
            'Defense': {'tech': 0.25, 'trend': 0.35, 'hot': 0.40},  # 防守：重概念
            'Balance': {'tech': 0.35, 'trend': 0.35, 'hot': 0.30},  # 震荡：均衡
        }
        weights = WEIGHT_CONFIG.get(regime, WEIGHT_CONFIG['Balance'])
        w_tech, w_trend, w_hot = weights['tech'], weights['trend'], weights['hot']
        logger.info(f"Level 3: {regime}模式权重 - 技术:{w_tech}, 趋势:{w_trend}, 热度:{w_hot}")
        
        # 获取核心评分参数
        vol_threshold = params.get('vol_threshold', 1.5)
        rps_threshold = params.get('rps_threshold', 80)
        concept_boost = params.get('concept_boost', True)
        
        # 1. 爆发力得分 (S_Explosion)
        scored_df['vol_ma_5'] = scored_df['vol_ma_5'].fillna(scored_df['volume'])
        scored_df['vol_ma_5'] = scored_df['vol_ma_5'].mask(scored_df['vol_ma_5'] == 0, scored_df['volume'])
        scored_df['vol_ratio'] = scored_df['volume'] / scored_df['vol_ma_5']
        scored_df['vol_ratio'] = scored_df['vol_ratio'].fillna(1.0)
        scored_df['vol_explosion'] = (scored_df['vol_ratio'] >= vol_threshold).astype(int) * 50
        
        s_explosion = scored_df['vol_explosion']
        
        # 2. 结构美感得分 (S_Structure)
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
        
        # 3. 概念协同得分 (S_Concept)
        if concept_boost and 'concept_resonance_score' in scored_df.columns:
            scored_df['concept_score'] = scored_df['concept_resonance_score'].fillna(0)
            resonance_count = (scored_df['concept_score'] > 0).sum()
            logger.info(f"Level 3: 概念共振加分 {resonance_count} 只")
        else:
            scored_df['concept_score'] = 0
        
        s_sector = scored_df['concept_score']
        
        # 4. 资金流因子加分（Module 2）
        # Intraday Intensity > 0 表示抢筹，加分
        if 'intraday_intensity_pct' in scored_df.columns:
            scored_df['ii_bonus'] = np.where(scored_df['intraday_intensity_pct'] > 0, 10, 0)
        else:
            scored_df['ii_bonus'] = 0
        
        # 量价相关性 > 0.5 表示正相关强，加分
        if 'volume_price_correlation' in scored_df.columns:
            scored_df['vpc_bonus'] = np.where(scored_df['volume_price_correlation'] > 0.5, 15, 0)
        else:
            scored_df['vpc_bonus'] = 0
        
        # 5. 筹码获利盘加分（Module 4）
        # 真空区判断：突破密集套牢区，上方无阻力，加分
        if 'is_vacuum_zone' in scored_df.columns:
            scored_df['vacuum_bonus'] = scored_df['is_vacuum_zone'] * 20
        else:
            scored_df['vacuum_bonus'] = 0
        
        # 6. 主力资金净流入加分（新增v2.0）
        # 主力净流入 > 0 加分，超大单净流入 > 0 额外加分
        if 'main_net_inflow' in scored_df.columns:
            # 主力净流入 > 0: 加15分
            scored_df['main_inflow_bonus'] = np.where(
                scored_df['main_net_inflow'] > self.MONEY_FLOW_STRONG_THRESHOLD, 
                15, 
                0
            )
            # 主力净流入 > 500万: 额外加10分
            scored_df['main_inflow_bonus'] += np.where(
                scored_df['main_net_inflow'] > 500, 
                10, 
                0
            )
            # 主力净流出 < -500万: 扣15分
            scored_df['main_inflow_bonus'] += np.where(
                scored_df['main_net_inflow'] < -500, 
                -15, 
                0
            )
        else:
            scored_df['main_inflow_bonus'] = 0
        
        if 'super_large_inflow' in scored_df.columns:
            # 超大单净流入 > 0: 加10分
            scored_df['super_large_bonus'] = np.where(
                scored_df['super_large_inflow'] > 0, 
                10, 
                0
            )
            # 超大单净流入 > 300万: 额外加10分
            scored_df['super_large_bonus'] += np.where(
                scored_df['super_large_inflow'] > 300, 
                10, 
                0
            )
        else:
            scored_df['super_large_bonus'] = 0
        
        # 记录资金流统计
        main_inflow_positive = (scored_df.get('main_net_inflow', pd.Series([0])) > 0).sum()
        super_large_positive = (scored_df.get('super_large_inflow', pd.Series([0])) > 0).sum()
        logger.info(f"Level 3: 主力资金净流入为正: {main_inflow_positive} 只，超大单净流入为正: {super_large_positive} 只")
        
        # 计算基础总分（包含资金流和筹码因子）
        scored_df['total_score'] = (
            w_tech * s_explosion +
            w_trend * s_structure +
            w_hot * s_sector +
            scored_df['ii_bonus'] +
            scored_df['vpc_bonus'] +
            scored_df['vacuum_bonus'] +
            scored_df['main_inflow_bonus'] +
            scored_df['super_large_bonus']
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
        第4级：AI概率修正 - v2.0优化版
        使用多因子规则计算胜率，增加更多正向因子提高胜率
        
        胜率计算因子：
        1. 基础因子：换手率、量比、VCP
        2. 资金流因子：主力净流入、Intraday Intensity
        3. 概念因子：概念共振、概念龙头
        4. 趋势因子：RSI、真空区
        """
        if df.empty:
            return df
        
        ai_filter = params.get('ai_filter', True)
        enhanced_df = df.copy()
        
        # 填充缺失值
        enhanced_df['turnover_rate'] = enhanced_df['turnover_rate'].fillna(0)
        enhanced_df['vol_ratio'] = enhanced_df['vol_ratio'].fillna(1.0)
        
        # v2.0优化：多因子胜率计算
        # 基础胜率 = 40%
        base_probability = 40
        
        # 计算胜率加分项（每个因子最多加10-15分）
        probability_bonus = pd.Series([0.0] * len(enhanced_df), index=enhanced_df.index)
        
        # 1. 换手率因子：1% < turnover_rate < 15% 加10分
        turnover_ok = (enhanced_df['turnover_rate'] > 1.0) & (enhanced_df['turnover_rate'] < 15.0)
        probability_bonus += turnover_ok.astype(int) * 10
        
        # 2. 量比因子：vol_ratio >= 1.5 加10分
        vol_ratio_min = params.get('ai_vol_ratio_min', 1.2)
        vol_ratio_ok = enhanced_df['vol_ratio'] >= vol_ratio_min
        probability_bonus += vol_ratio_ok.astype(int) * 10
        
        # 3. VCP因子：vcp_factor < 0.5 加5分
        if 'vcp_factor' in enhanced_df.columns:
            enhanced_df['vcp_factor'] = enhanced_df['vcp_factor'].fillna(1.0)
            vcp_factor_max = params.get('ai_vcp_factor_max', 0.5)  # 放宽到0.5
            vcp_ok = enhanced_df['vcp_factor'] < vcp_factor_max
            probability_bonus += vcp_ok.astype(int) * 5
        
        # 4. 主力资金因子：main_net_inflow > 0 加10分
        if 'main_net_inflow' in enhanced_df.columns:
            main_inflow_ok = enhanced_df['main_net_inflow'] > 0
            probability_bonus += main_inflow_ok.astype(int) * 10
        
        # 5. Intraday Intensity因子：ii_pct > 0 加5分
        if 'intraday_intensity_pct' in enhanced_df.columns:
            ii_ok = enhanced_df['intraday_intensity_pct'] > 0
            probability_bonus += ii_ok.astype(int) * 5
        
        # 6. 概念共振因子：is_main_concept = 1 加10分
        if 'is_main_concept' in enhanced_df.columns:
            concept_ok = enhanced_df['is_main_concept'] == 1
            probability_bonus += concept_ok.astype(int) * 10
        
        # 7. 概念龙头因子：is_concept_leader = 1 加10分
        if 'is_concept_leader' in enhanced_df.columns:
            leader_ok = enhanced_df['is_concept_leader'] == 1
            probability_bonus += leader_ok.astype(int) * 10
        
        # 8. RSI因子：30 < RSI < 70 加5分（非超买超卖区）
        if 'rsi_6' in enhanced_df.columns:
            rsi_ok = (enhanced_df['rsi_6'] > 30) & (enhanced_df['rsi_6'] < 70)
            probability_bonus += rsi_ok.astype(int) * 5
        
        # 9. 真空区因子：is_vacuum_zone = 1 加5分
        if 'is_vacuum_zone' in enhanced_df.columns:
            vacuum_ok = enhanced_df['is_vacuum_zone'] == 1
            probability_bonus += vacuum_ok.astype(int) * 5
        
        # 计算最终胜率（最高80%，最低40%）
        enhanced_df['win_probability'] = (base_probability + probability_bonus).clip(40, 80)
        
        # 记录胜率分布
        high_prob_count = (enhanced_df['win_probability'] >= 60).sum()
        mid_prob_count = ((enhanced_df['win_probability'] >= 50) & (enhanced_df['win_probability'] < 60)).sum()
        low_prob_count = (enhanced_df['win_probability'] < 50).sum()
        logger.info(f"Level 4: 胜率分布 - 高(>=60%): {high_prob_count}, 中(50-60%): {mid_prob_count}, 低(<50%): {low_prob_count}")
        
        if ai_filter:
            # 放宽胜率要求：默认45%
            min_win_probability = params.get('min_win_probability', 45)
            before_count = len(enhanced_df)
            enhanced_df = enhanced_df[enhanced_df['win_probability'] >= min_win_probability]
            after_count = len(enhanced_df)
            logger.info(f"Level 4 AI过滤: {before_count} -> {after_count} (胜率 >= {min_win_probability}%)")
            
            # 如果过滤后为空，进一步放宽到40%
            if enhanced_df.empty and before_count > 0:
                logger.warning(f"Level 4: 胜率>={min_win_probability}%过滤后无数据，放宽到>=40%")
                enhanced_df = df.copy()
                # 重新计算（简化版）
                enhanced_df['turnover_rate'] = enhanced_df['turnover_rate'].fillna(0)
                enhanced_df['vol_ratio'] = enhanced_df['vol_ratio'].fillna(1.0)
                # 只要换手率合理，就给基础胜率
                basic_ok = (enhanced_df['turnover_rate'] > 0.5) & (enhanced_df['turnover_rate'] < 25.0)
                enhanced_df['win_probability'] = np.where(basic_ok, 45, 40)
                enhanced_df = enhanced_df[enhanced_df['win_probability'] >= 40]
        
        return enhanced_df
    
    def run_full_pipeline(self, trade_date: date, params: Dict = None, top_n: Optional[int] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        运行完整的T7概念资金双驱流程
        
        Args:
            trade_date: 交易日期
            params: 用户参数（可选，会与默认参数合并）
            top_n: 返回数量（可选，默认返回所有符合条件的）
        
        Returns:
            (推荐结果列表, 诊断信息)
        """
        # 合并用户参数和默认参数
        params = self.merge_params(params)
        
        diagnostic_info = []
        
        try:
            # Module 1: RSRS市场状态识别（在Level 1之前执行）
            logger.info(f"Module 1: RSRS市场状态识别 (trade_date={trade_date})")
            regime_info = self.detect_market_regime(trade_date)
            diagnostic_info.append(f"市场状态: {regime_info['regime']} (RSRS Z-score: {regime_info['rsrs_zscore']:.3f})")
            
            # Level 1: 特征提取（集成概念竞速、资金流、筹码分析）
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
            
            # 生成推荐理由 - v2.0优化版
            def build_reasons(idx):
                reasons = []
                vol_ratio = vol_ratio_col.loc[idx] if idx in vol_ratio_col.index else 0.0
                rps_250 = rps_250_col.loc[idx] if idx in rps_250_col.index else 0.0
                vcp_factor = vcp_factor_col.loc[idx] if idx in vcp_factor_col.index else 1.0
                
                # 技术因子（放宽阈值）
                if vol_ratio >= params.get('vol_threshold', 1.5):
                    reasons.append(f"倍量{vol_ratio:.1f}x")
                if rps_250 >= params.get('rps_threshold', 80):
                    reasons.append("RPS强势")
                if vcp_factor < 0.4:
                    reasons.append("VCP收敛")
                
                # 概念共振信息
                if 'is_main_concept' in top_stocks.columns and top_stocks.loc[idx, 'is_main_concept'] == 1:
                    reasons.append("概念共振")
                if 'is_concept_leader' in top_stocks.columns and top_stocks.loc[idx, 'is_concept_leader'] == 1:
                    reasons.append("概念龙头")
                if 'is_vacuum_zone' in top_stocks.columns and top_stocks.loc[idx, 'is_vacuum_zone'] == 1:
                    reasons.append("真空区突破")
                
                # v2.0新增：资金流信息
                if 'main_net_inflow' in top_stocks.columns:
                    main_inflow = top_stocks.loc[idx, 'main_net_inflow']
                    if pd.notna(main_inflow) and main_inflow > 500:
                        reasons.append("主力流入")
                    elif pd.notna(main_inflow) and main_inflow > 0:
                        reasons.append("资金净流入")
                
                if 'super_large_inflow' in top_stocks.columns:
                    super_inflow = top_stocks.loc[idx, 'super_large_inflow']
                    if pd.notna(super_inflow) and super_inflow > 300:
                        reasons.append("超大单进场")
                
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
            
            # 添加市场状态和概念信息
            result_df['market_regime'] = regime_info['regime']
            if 'concept_resonance_score' in top_stocks.columns:
                result_df['resonance_score'] = top_stocks['concept_resonance_score'].fillna(0.0).astype(float)
            else:
                result_df['resonance_score'] = 0.0
            
            if 'resonance_base_tag' in top_stocks.columns:
                result_df['concept_trend'] = top_stocks['resonance_base_tag'].fillna('未知')
            elif 'industry' in top_stocks.columns:
                result_df['concept_trend'] = top_stocks['industry'].fillna('未知')
            else:
                result_df['concept_trend'] = '未知'
            
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
                
                # 添加市场状态和概念信息
                r['market_regime'] = str(r.get('market_regime', 'Balance'))
                r['resonance_score'] = float(r.get('resonance_score', 0.0))
                r['concept_trend'] = str(r.get('concept_trend', '未知'))
            
            logger.info(f"成功生成 {len(results)} 条推荐结果")
            diagnostic_info.append(f"最终: 生成 {len(results)} 条推荐")
            return results, " | ".join(diagnostic_info)
            
        except Exception as e:
            logger.error(f"T7概念资金双驱模型运行失败: {e}", exc_info=True)
            diagnostic_info.append(f"异常: {str(e)}")
            return [], " | ".join(diagnostic_info)
