"""
AlphaModelT10StructuralSniper - 结构狙击者模型 v1.0

设计哲学（A肥羊微观结构专家）：
================
核心信仰：
- L1数据（价量）是唯一不会撒谎的指标
- "极致缩量"和"板块效应"无法伪造
- 只做"主升浪中的缩量回踩"（黄金坑/龙回头）

四层漏斗架构（The T10 Protocol）：
================
Layer 1: 战场选择 (Battlefield Selection)
  - 板块锚定：只选板块MA20向上的个肥羊
  - 基本面排雷：剔除ST、极端PE

Layer 2: 肥羊性基因 (Active Gene)
  - 拒绝死肥羊：近20日内必须有涨停或>6%大阳线
  - 流动性门槛：日均成交额>1亿

Layer 3: 狙击形态 (Sniper Setup)
  - 极致缩量：Vol_Ratio < 0.6
  - 黄金坑：小阴线回调 + 站稳MA20
  - 阴线低吸：优先推荐当日收跌的肥羊票

Layer 4: 筹码评分 (Scoring Engine)
  - F1: 缩量极致分 (40%)
  - F2: 换手率健康分 (30%)
  - F3: RPS护盘分 (30%)

使用说明：
================
1. 默认参数已优化，直接调用即可：
   model = AlphaModelT10StructuralSniper()
   results, info, metadata = model.run_full_pipeline(trade_date)

2. 如需调整核心参数：
   params = {
       'vol_ratio_max': 0.6,       # 缩量阈值（越小越好）
       'turnover_min': 2.0,        # 换手率下限
       'turnover_max': 8.0,        # 换手率上限
   }
   results, info, metadata = model.run_full_pipeline(trade_date, params)
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import date, timedelta
import logging
import warnings
from dataclasses import dataclass, field
from db.database import get_raw_connection, get_sqlalchemy_engine, get_db
from etl.trade_date_adapter import TradeDateAdapter
from sqlalchemy import text
from collections import defaultdict

warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy')
logger = logging.getLogger(__name__)


# ============================================
# T10 统一配置管理 - 结构狙击者参数
# ============================================
@dataclass
class T10Config:
    """T10模型配置 - 纯L1数据驱动"""
    
    # === 基础常量 ===
    MIN_HISTORY_DAYS: int = 60           # 最少历史交易日
    LOOKBACK_DAYS: int = 20              # 回看天数（涨停基因检测）
    STOP_LOSS_RATIO: float = 0.93        # 止损比例
    DEFAULT_TOP_N: int = 10              # 默认推荐数量
    NEW_STOCK_DAYS: int = 60             # 新肥羊排除天数
    
    # === Layer 1: 战场选择 (Battlefield) ===
    # 板块MA20向上才入选
    SECTOR_MA_STATUS_BULLISH: int = 1    # ma_status=1 表示多头排列
    MIN_MV: float = 10                   # 最小市值（亿）
    MAX_MV: float = 500                  # 最大市值（亿）
    
    # === Layer 2: 肥羊性基因 (Active Gene) ===
    LIMIT_UP_THRESHOLD: float = 9.8      # 涨停判定阈值
    BIG_YANG_THRESHOLD: float = 6.0      # 大阳线判定阈值
    MIN_DAILY_AMOUNT_SMALL: float = 10000  # 小盘肥羊日均成交额下限（万）= 1亿
    MIN_DAILY_AMOUNT_LARGE: float = 30000  # 中大盘日均成交额下限（万）= 3亿
    MV_THRESHOLD_LARGE: float = 100      # 中大盘市值界限（亿）
    
    # === Layer 3: 狙击形态 (Sniper Setup) - 核心 ===
    VOL_RATIO_MAX: float = 0.6           # 极致缩量阈值（Vol/MA5 < 0.6）
    VOL_RATIO_EXTREME: float = 0.55      # 更严苛的缩量阈值
    GOLDEN_PIT_CHANGE_MIN: float = -3.0  # 黄金坑涨跌幅下限
    GOLDEN_PIT_CHANGE_MAX: float = 1.0   # 黄金坑涨跌幅上限（小阴线或十字星）
    MA20_TOLERANCE: float = 0.99         # MA20支撑容忍度（Close >= MA20 * 0.99）
    
    # === Layer 4: 评分权重 (Scoring) ===
    WEIGHT_VOL_CONTRACTION: float = 0.40  # 缩量极致分权重
    WEIGHT_TURNOVER_HEALTH: float = 0.30  # 换手率健康分权重
    WEIGHT_RPS_PROTECTION: float = 0.30   # RPS护盘分权重
    
    # 换手率健康区间
    TURNOVER_OPTIMAL_MIN: float = 2.0     # 最优换手率下限
    TURNOVER_OPTIMAL_MAX: float = 8.0     # 最优换手率上限
    TURNOVER_ACCEPTABLE_MIN: float = 1.0  # 可接受换手率下限
    TURNOVER_ACCEPTABLE_MAX: float = 15.0 # 可接受换手率上限
    
    # === 最终筛选 ===
    MIN_SCORE: float = 50                 # 最低评分门槛
    MAX_RECOMMENDATIONS: int = 20         # 最大推荐数量
    PREFER_NEGATIVE_CHANGE: bool = True   # 优先阴线低吸


class AlphaModelT10StructuralSniper:
    """
    T10结构狙击者模型 v1.0 - 纯L1数据驱动
    
    核心设计原则：
    1. 只用L1数据（OHLCV），不依赖资金流向
    2. 板块驱动：个肥羊必须有板块掩护
    3. 缩量回踩：只做主升浪中的黄金坑
    4. 阴线低吸：不追涨当日翻红的肥羊票
    """
    
    def __init__(self, config: T10Config = None):
        self.config = config or T10Config()
        self.model_version = "T10_Structural_Sniper_v1.0"
        self.trade_date_adapter = TradeDateAdapter()
    
    # ============================================
    # 默认参数（T10结构狙击者特化）
    # ============================================
    DEFAULT_PARAMS = {
        'vol_ratio_max': 0.6,           # 缩量阈值（越小越好）
        'turnover_min': 2.0,            # 换手率下限
        'turnover_max': 8.0,            # 换手率上限
        'golden_pit_change_min': -3.0,  # 黄金坑涨跌幅下限
        'golden_pit_change_max': 1.0,   # 黄金坑涨跌幅上限
        'min_score': 50,                # 最低评分门槛
        'max_recommendations': 20,      # 最大推荐数量
        'prefer_negative_change': True, # 优先阴线低吸
        'require_sector_bullish': True, # 要求板块多头
    }
    
    @classmethod
    def get_default_params(cls) -> Dict:
        return cls.DEFAULT_PARAMS.copy()
    
    @classmethod
    def merge_params(cls, user_params: Dict = None) -> Dict:
        params = cls.DEFAULT_PARAMS.copy()
        if user_params:
            params.update(user_params)
        return params
    
   # ============================================
    # Layer 1: 战场选择 (Battlefield Selection)
    # ============================================
    def level1_sector_filter(self, trade_date: date, params: Dict = None) -> pd.DataFrame:
        """
        Layer 1: 战场选择 - 板块锚定 + 基本面排雷
        
        规则：
        1. 板块MA20向上（ma_status=1）的个肥羊才入选
        2. 剔除ST、新肥羊
        3. 市值在合理范围内
        """
        params = params or self.DEFAULT_PARAMS
        
        try:
            with get_db() as db:
                # Step 1: 获取板块MA状态为多头的板块列表
                bullish_sectors = self._get_bullish_sectors(trade_date)
                logger.info(f"Layer 1: 找到 {len(bullish_sectors)} 个多头板块")
                
                # Step 2: 获取符合基本条件的肥羊票
                query = text("""
                    SELECT 
                        sd.sheep_code, sd.trade_date, sd.close_price, sd.high_price,
                        sd.low_price, sd.open_price, sd.volume, sd.amount,
                        sd.turnover_rate, sd.change_pct, sd.ma5, sd.ma10, sd.ma20,
                        sd.ma30, sd.ma60, sd.vol_ma_5, sd.rps_250,
                        sb.sheep_name, sb.list_date, sb.industry,
                        CASE WHEN sd.turnover_rate > 0 
                             THEN (sd.amount / (sd.turnover_rate / 100)) / 100000000 
                             ELSE NULL END as estimated_mv
                    FROM sheep_daily sd
                    INNER JOIN sheep_basic sb ON sd.sheep_code = sb.sheep_code
                    WHERE sd.trade_date = :trade_date
                      AND sb.is_active = 1
                      AND sb.sheep_name NOT LIKE '%%ST%%'
                      AND (sb.list_date IS NULL OR DATEDIFF(:trade_date, sb.list_date) >= :new_stock_days)
                      AND sd.ma20 IS NOT NULL
                      AND sd.ma20 > 0
                """)
                
                result = db.execute(query, {
                    'trade_date': trade_date,
                    'new_stock_days': self.config.NEW_STOCK_DAYS
                })
                
                rows = result.fetchall()
                if not rows:
                    logger.warning("Layer 1: 没有找到符合基本条件的肥羊票")
                    return pd.DataFrame()
                
                df = pd.DataFrame(rows, columns=[
                    'sheep_code', 'trade_date', 'close_price', 'high_price',
                    'low_price', 'open_price', 'volume', 'amount',
                    'turnover_rate', 'change_pct', 'ma5', 'ma10', 'ma20',
                    'ma30', 'ma60', 'vol_ma_5', 'rps_250',
                    'sheep_name', 'list_date', 'industry', 'estimated_mv'
                ])
                
                # 填充缺失值
                df['vol_ma_5'] = df['vol_ma_5'].fillna(df['volume'])
                df['rps_250'] = df['rps_250'].fillna(0.0)
                df['estimated_mv'] = df['estimated_mv'].fillna(50.0)  # 默认50亿
                
                # Step 3: 市值过滤
                mv_mask = (df['estimated_mv'] >= self.config.MIN_MV) & (df['estimated_mv'] <= self.config.MAX_MV)
                df = df[mv_mask]
                logger.info(f"Layer 1: 市值过滤后剩余 {len(df)} 只")
                
                # Step 4: 板块过滤（只保留属于多头板块的肥羊票）
                if params.get('require_sector_bullish', True) and bullish_sectors:
                    df = self._filter_by_bullish_sectors(df, bullish_sectors)
                    logger.info(f"Layer 1: 板块过滤后剩余 {len(df)} 只")
                
                logger.info(f"Layer 1 完成: 筛选出 {len(df)} 只肥羊票")
                return df
                
        except Exception as e:
            logger.error(f"Layer 1 失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    def _get_bullish_sectors(self, trade_date: date) -> List[str]:
        """"获取MA20向上的板块列表"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT DISTINCT sector_name 
                    FROM sector_money_flow 
                    WHERE trade_date = :trade_date
                      AND ma_status = :ma_status
                """)
                result = db.execute(query, {
                    'trade_date': trade_date,
                    'ma_status': self.config.SECTOR_MA_STATUS_BULLISH
                })
                return [row[0] for row in result]
        except Exception as e:
            logger.warning(f"获取多头板块失败: {e}")
            return []
    
    def _filter_by_bullish_sectors(self, df: pd.DataFrame, bullish_sectors: List[str]) -> pd.DataFrame:
        """筛选属于多头板块的肥羊票"""
        if df.empty or not bullish_sectors:
            return df
        
        try:
            sheep_codes = df['sheep_code'].tolist()
            
            with get_db() as db:
                # 查询肥羊票所属板块
                placeholders = ','.join([f':code{i}' for i in range(len(sheep_codes))])
                sector_placeholders = ','.join([f':sector{i}' for i in range(len(bullish_sectors))])
                
                query = text(f"""
                    SELECT DISTINCT scm.sheep_code
                    FROM sheep_concept_mapping scm
                    INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                    WHERE scm.sheep_code IN ({placeholders})
                      AND ct.concept_name IN ({sector_placeholders})
                      AND ct.is_active = 1
                """)
                
                params = {f'code{i}': code for i, code in enumerate(sheep_codes)}
                params.update({f'sector{i}': sector for i, sector in enumerate(bullish_sectors)})
                
                result = db.execute(query, params)
                bullish_sheep = set(row[0] for row in result)
                
                # 同时考虑行业字段
                industry_match = df['industry'].isin(bullish_sectors)
                concept_match = df['sheep_code'].isin(bullish_sheep)
                
                return df[industry_match | concept_match]
                
        except Exception as e:
            logger.warning(f"板块筛选失败: {e}")
            return df
    
    # ============================================
    # Layer 2: 肥羊性基因 (Active Gene)
    # ============================================
    def level2_gene_filter(self, df: pd.DataFrame, trade_date: date, params: Dict = None) -> pd.DataFrame:
        """
        Layer 2: 肥羊性基因筛选 - 拒绝死肥羊
        
        规则：
        1. 近20个交易日内，必须出现过涨停或>6%大阳线
        2. 日均成交额门槛：小盘肥羊>1亿，中大盘>3亿
        
        逻辑：没有涨停基因的肥羊票，主力实力弱或意愿差
        """
        if df.empty:
            return df
        
        params = params or self.DEFAULT_PARAMS
        df = df.copy()
        
        try:
            sheep_codes = df['sheep_code'].tolist()
            if not sheep_codes:
                return df
            
            # 计算回看日期
            lookback_dates = self._get_lookback_dates(trade_date, self.config.LOOKBACK_DAYS)
            if not lookback_dates:
                logger.warning("Layer 2: 无法获取回看日期")
                return df
            
            start_date = lookback_dates[-1]
            
            with get_db() as db:
                # 查询近20日内有涨停或大阳线的肥羊票
                placeholders = ','.join([f':code{i}' for i in range(len(sheep_codes))])
                
                query = text(f"""
                    SELECT sheep_code, 
                           MAX(change_pct) as max_change,
                           SUM(CASE WHEN change_pct >= :limit_up THEN 1 ELSE 0 END) as limit_up_count,
                           SUM(CASE WHEN change_pct >= :big_yang THEN 1 ELSE 0 END) as big_yang_count,
                           AVG(amount) as avg_amount
                    FROM sheep_daily
                    WHERE sheep_code IN ({placeholders})
                      AND trade_date BETWEEN :start_date AND :end_date
                    GROUP BY sheep_code
                """)
                
                params_dict = {f'code{i}': code for i, code in enumerate(sheep_codes)}
                params_dict.update({
                    'limit_up': self.config.LIMIT_UP_THRESHOLD,
                    'big_yang': self.config.BIG_YANG_THRESHOLD,
                    'start_date': start_date,
                    'end_date': trade_date
                })
                
                result = db.execute(query, params_dict)
                history_df = pd.DataFrame(result.fetchall(), columns=[
                    'sheep_code', 'max_change', 'limit_up_count', 'big_yang_count', 'avg_amount'
                ])
                
                if history_df.empty:
                    logger.warning("Layer 2: 无法获取历史数据")
                    return df
                
                # 合并历史数据
                df = df.merge(history_df, on='sheep_code', how='left')
                df['limit_up_count'] = df['limit_up_count'].fillna(0)
                df['big_yang_count'] = df['big_yang_count'].fillna(0)
                df['avg_amount'] = df['avg_amount'].fillna(0)
                
                # 筛选条件1: 必须有涨停或大阳线基因
                has_gene = (df['limit_up_count'] > 0) | (df['big_yang_count'] > 0)
                logger.info(f"Layer 2: 涨停/大阳基因筛选 {has_gene.sum()}/{len(df)} 通过")
                
                # 筛选条件2: 流动性门槛
                # 小盘肥羊(<100亿): 日均成交额>1亿
                # 中大盘(>=100亿): 日均成交额>3亿
                is_large_cap = df['estimated_mv'] >= self.config.MV_THRESHOLD_LARGE
                liquidity_ok_small = (~is_large_cap) & (df['avg_amount'] >= self.config.MIN_DAILY_AMOUNT_SMALL)
                liquidity_ok_large = is_large_cap & (df['avg_amount'] >= self.config.MIN_DAILY_AMOUNT_LARGE)
                has_liquidity = liquidity_ok_small | liquidity_ok_large
                logger.info(f"Layer 2: 流动性门槛筛选 {has_liquidity.sum()}/{len(df)} 通过")
                
                # 综合筛选
                mask = has_gene & has_liquidity
                df = df[mask]
                
                logger.info(f"Layer 2 完成: 筛选出 {len(df)} 只肥羊性活跃肥羊")
                return df
                
        except Exception as e:
            logger.error(f"Layer 2 失败: {e}", exc_info=True)
            return df
    
    def _get_lookback_dates(self, trade_date: date, days: int) -> List[date]:
        """获取回看的交易日列表"""
        try:
            # 计算开始日期（大约 days*2 天前，确保有足够的交易日）
            start_date = trade_date - timedelta(days=days*2)
            all_dates = self.trade_date_adapter.get_trading_days_in_range(start_date, trade_date)
            # 返回最近的 days 个交易日
            return all_dates[-days:] if len(all_dates) >= days else all_dates
        except Exception as e:
            logger.warning(f"获取回看日期失败: {e}")
            return []
    
    # ============================================
    # Layer 3: 狙击形态 (Sniper Setup) - 核心筛选
    # ============================================
    def level3_technical_setup(self, df: pd.DataFrame, params: Dict = None) -> pd.DataFrame:
        """
        Layer 3: 狙击形态 - 洗盘识别
        
        规则（核心中的核心）：
        1. 极致缩量: Vol_Ratio = Volume / MA5_Volume < 0.6
        2. 黄金坑: 小阴线回调（-3% ~ 1%）+ 成交量缩小
        3. 生命线支撑: Close >= MA20 * 0.99
        4. 阴线低吸: 优先推荐change_pct < 0的肥羊票
        
        逻辑：缩量到极致代表浮筹清洗完毕，主力锁仓
        """
        if df.empty:
            return df
        
        params = params or self.DEFAULT_PARAMS
        df = df.copy()
        
        try:
            # 计算量比 Vol_Ratio = Volume / MA5_Volume
            df['vol_ma_5'] = df['vol_ma_5'].fillna(df['volume'])
            df['vol_ratio'] = np.where(df['vol_ma_5'] > 0, df['volume'] / df['vol_ma_5'], 1.0)
            
            # 标记板块类型
            df['is_star_market'] = df['sheep_code'].str.startswith('688').astype(int)
            df['is_gem'] = df['sheep_code'].str.startswith('300').astype(int)
            
            # === 极致缩量筛选 ===
            vol_ratio_max = params.get('vol_ratio_max', self.config.VOL_RATIO_MAX)
            extreme_shrink = df['vol_ratio'] < vol_ratio_max
            logger.info(f"Layer 3: 极致缩量(<{vol_ratio_max}) {extreme_shrink.sum()}/{len(df)} 通过")
            
            # === 黄金坑形态 ===
            golden_pit_min = params.get('golden_pit_change_min', self.config.GOLDEN_PIT_CHANGE_MIN)
            golden_pit_max = params.get('golden_pit_change_max', self.config.GOLDEN_PIT_CHANGE_MAX)
            golden_pit = (df['change_pct'] >= golden_pit_min) & (df['change_pct'] <= golden_pit_max)
            logger.info(f"Layer 3: 黄金坑形态({golden_pit_min}%~{golden_pit_max}%) {golden_pit.sum()}/{len(df)} 通过")
            
            # === MA20支撑 ===
            ma20_tolerance = self.config.MA20_TOLERANCE
            # 确保 ma20 是数值类型，避免 Decimal 和 float 运算错误
            ma20_values = df['ma20'].astype(float)
            ma20_support = df['close_price'] >= (ma20_values * ma20_tolerance)
            logger.info(f"Layer 3: MA20支撑(>={ma20_tolerance*100:.0f}%) {ma20_support.sum()}/{len(df)} 通过")
            
            # 综合筛选: 极致缩量 + 黄金坑 + MA20支撑
            mask = extreme_shrink & golden_pit & ma20_support
            df_filtered = df[mask].copy()
            
            # 添加狙击形态标记
            df_filtered['sniper_setup'] = True
            df_filtered['is_extreme_shrink'] = df_filtered['vol_ratio'] < self.config.VOL_RATIO_EXTREME
            
            # 阴线低吸优先标记
            df_filtered['is_negative_day'] = df_filtered['change_pct'] < 0
            
            logger.info(f"Layer 3 完成: 筛选出 {len(df_filtered)} 只狙击形态肥羊")
            logger.info(f"Layer 3: 其中极致缩量(<{self.config.VOL_RATIO_EXTREME}) {df_filtered['is_extreme_shrink'].sum()} 只")
            logger.info(f"Layer 3: 其中阴线低吸 {df_filtered['is_negative_day'].sum()} 只")
            
            return df_filtered
            
        except Exception as e:
            logger.error(f"Layer 3 失败: {e}", exc_info=True)
            return df
    
    # ============================================
    # Layer 4: 筹码与评分 (Scoring Engine)
    # ============================================
    def score_and_rank(self, df: pd.DataFrame, trade_date: date, params: Dict = None) -> pd.DataFrame:
        """
        Layer 4: 筹码评分引擎 (0-100分)
        
        评分公式：
        F1: 缩量极致分 (40%) - Vol_Ratio越小分数越高
        F2: 换手率健康分 (30%) - 2%~8%区间得高分
        F3: RPS护盘分 (30%) - 抗跌肥羊得高分
        
        特殊加分：阴线低吸优先
        """
        if df.empty:
            return df
        
        params = params or self.DEFAULT_PARAMS
        df = df.copy()
        
        try:
            # === F1: 缩量极致分 (0-40) ===
            # Vol_Ratio越小越好
            vol_ratio_max = self.config.VOL_RATIO_MAX
            vol_ratio_extreme = self.config.VOL_RATIO_EXTREME
            
            f1_score = np.where(
                df['vol_ratio'] < vol_ratio_extreme, 40,  # 极致缩量，满分
                np.where(
                    df['vol_ratio'] < vol_ratio_max, 30,  # 较好缩量
                    np.where(
                        df['vol_ratio'] < 0.7, 20,  # 一般缩量
                        np.where(df['vol_ratio'] < 0.8, 10, 0)  # 轻微缩量
                    )
                )
            )
            df['f1_vol_score'] = f1_score
            
            # === F2: 换手率健康分 (0-30) ===
            # 最优区间 2%~8%
            turnover_min = self.config.TURNOVER_OPTIMAL_MIN
            turnover_max = self.config.TURNOVER_OPTIMAL_MAX
            
            # 在最优区间内得满分
            in_optimal = (df['turnover_rate'] >= turnover_min) & (df['turnover_rate'] <= turnover_max)
            # 可接受区间 1%~15%
            in_acceptable = (df['turnover_rate'] >= 1.0) & (df['turnover_rate'] <= 15.0)
            
            f2_score = np.where(in_optimal, 30, np.where(in_acceptable, 15, 0))
            df['f2_turnover_score'] = f2_score
            
            # === F3: RPS护盘分 (0-30) ===
            # 计算护盘能力：大盘跌时个肥羊护盘得高分
            # 获取大盘当日涨跌幅
            market_change = self._get_market_change(trade_date)
            
            if market_change is not None and market_change < 0:
                # 大盘下跌时，计算护盘分
                # 护盘程度 = 个肥羊涨跌幅 - 大盘涨跌幅
                # 确保 change_pct 是数值类型，避免 Decimal 和 float 运算错误
                change_pct_values = df['change_pct'].astype(float)
                protection = change_pct_values - market_change
                f3_score = np.where(
                    protection > 2.0, 30,  # 强势护盘
                    np.where(
                        protection > 1.0, 25,
                        np.where(
                            protection > 0, 20,  # 轻微护盘
                            np.where(protection > -1.0, 10, 0)  # 跟随下跌
                        )
                    )
                )
            else:
                # 大盘上涨时，使用RPS_250评分
                rps = df['rps_250'].fillna(0)
                f3_score = np.where(
                    rps >= 90, 30,
                    np.where(
                        rps >= 80, 25,
                        np.where(
                            rps >= 70, 20,
                            np.where(rps >= 60, 15, np.where(rps >= 50, 10, 5))
                        )
                    )
                )
            df['f3_rps_score'] = f3_score
            
            # === 综合评分 ===
            weight_vol = self.config.WEIGHT_VOL_CONTRACTION
            weight_turnover = self.config.WEIGHT_TURNOVER_HEALTH
            weight_rps = self.config.WEIGHT_RPS_PROTECTION
            
            df['total_score'] = (
                df['f1_vol_score'] * (weight_vol / 0.4) +
                df['f2_turnover_score'] * (weight_turnover / 0.3) +
                df['f3_rps_score'] * (weight_rps / 0.3)
            ).clip(0, 100)
            
            # === 阴线低吸加分 ===
            if params.get('prefer_negative_change', self.config.PREFER_NEGATIVE_CHANGE):
                negative_bonus = np.where(df['change_pct'] < 0, 5, 0)
                df['total_score'] = (df['total_score'] + negative_bonus).clip(0, 100)

            # === 添加阴线标识 ===
            df['is_negative_day'] = df['change_pct'] < 0
            
            # === 生成推荐理由 ===
            df['reason_tags'] = df.apply(self._build_reason_tags, axis=1)
            
            # 排序
            df = df.sort_values('total_score', ascending=False)
            
            # 最低分数筛选
            min_score = params.get('min_score', self.config.MIN_SCORE)
            df = df[df['total_score'] >= min_score]
            
            logger.info(f"Layer 4 完成: 评分后剩余 {len(df)} 只肥羊票")
            if not df.empty:
                logger.info(f"Layer 4: 分数范围 {df['total_score'].min():.1f}~{df['total_score'].max():.1f}")
                logger.info(f"Layer 4: F1缩量分平均 {df['f1_vol_score'].mean():.1f}, F2换手分平均 {df['f2_turnover_score'].mean():.1f}, F3护盘分平均 {df['f3_rps_score'].mean():.1f}")
            
            return df
            
        except Exception as e:
            logger.error(f"Layer 4 失败: {e}", exc_info=True)
            return df
    
    def _get_market_change(self, trade_date: date) -> Optional[float]:
        """获取大盘当日涨跌幅"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT change_pct FROM market_index_daily
                    WHERE index_code = '000001.SH' AND trade_date = :trade_date
                """)
                result = db.execute(query, {'trade_date': trade_date})
                row = result.fetchone()
                return float(row[0]) if row else None
        except Exception as e:
            logger.warning(f"获取大盘涨跌幅失败: {e}")
            return None
    
    def _build_reason_tags(self, row) -> str:
        """构建推荐理由标签"""
        tags = []
        
        # 板块状态
        tags.append("板块向上")
        
        # 缩量描述
        vol_ratio = row.get('vol_ratio', 1.0)
        if vol_ratio < 0.5:
            tags.append(f"缩量{vol_ratio:.2f}倍")
        elif vol_ratio < 0.6:
            tags.append(f"缩量{vol_ratio:.2f}倍")
        else:
            tags.append(f"量比{vol_ratio:.2f}")
        
        # MA20支撑
        close = row.get('close_price', 0)
        ma20 = row.get('ma20', 0)
        # 确保 ma20 是数值类型，避免 Decimal 和 float 运算错误
        if ma20 is not None:
            ma20 = float(ma20)
        else:
            ma20 = 0.0
        if ma20 > 0 and close >= ma20 * 0.99:
            tags.append("回踩MA20")
        
        # 阴线低吸
        if row.get('change_pct', 0) < 0:
            tags.append("阴线低吸")
        
        # 换手率状态
        turnover = row.get('turnover_rate', 0)
        if 2 <= turnover <= 8:
            tags.append(f"换手{turnover:.1f}%")
        
        return "+".join(tags)
    
    # ============================================
    # 主Pipeline - T10结构狙击者流程
    # ============================================
    def run_full_pipeline(self, trade_date: date, params: Dict = None, top_n: Optional[int] = None) -> Tuple[List[Dict], Optional[str], Optional[Dict]]:
        """
        T10结构狙击者完整流程
        
        Pipeline: Layer1(板块) -> Layer2(基因) -> Layer3(狙击) -> Layer4(评分) -> Output
        
        核心理念：
        - 纯L1数据驱动，不依赖资金流
        - 极致缩量 + 板块效应
        - 阴线低吸，不追涨
        """
        params = self.merge_params(params)
        diagnostic_info = []
        funnel_data = {'total': 0, 'L1_pass': 0, 'L2_pass': 0, 'L3_pass': 0, 'L4_pass': 0, 'final': 0}
        
        try:
            logger.info(f"T10 Pipeline Start: trade_date={trade_date}")
            logger.info(f"T10 模型版本: {self.model_version}")
            
            # 获取全市场数量
            try:
                with get_db() as db:
                    query = text("SELECT COUNT(DISTINCT sd.sheep_code) as total FROM sheep_daily sd INNER JOIN sheep_basic sb ON sd.sheep_code = sb.sheep_code WHERE sd.trade_date = :trade_date AND sb.is_active = 1")
                    result = db.execute(query, {'trade_date': trade_date})
                    row = result.fetchone()
                    funnel_data['total'] = int(row[0]) if row else 0
            except Exception as e:
                logger.warning(f"获取全市场数量失败: {e}")
                funnel_data['total'] = 0
            
            # === Layer 1: 战场选择 ===
            df = self.level1_sector_filter(trade_date, params)
            if df.empty:
                return self._empty_result(diagnostic_info, funnel_data, "Layer 1 板块筛选后无数据")
            funnel_data['L1_pass'] = len(df)
            diagnostic_info.append(f"L1(板块): {funnel_data['total']} -> {len(df)}")
            
            # === Layer 2: 肥羊性基因 ===
            df = self.level2_gene_filter(df, trade_date, params)
            if df.empty:
                return self._empty_result(diagnostic_info, funnel_data, "Layer 2 基因筛选后无数据")
            funnel_data['L2_pass'] = len(df)
            diagnostic_info.append(f"L2(基因): {funnel_data['L1_pass']} -> {len(df)}")
            
            # === Layer 3: 狙击形态 ===
            df = self.level3_technical_setup(df, params)
            if df.empty:
                return self._empty_result(diagnostic_info, funnel_data, "Layer 3 狙击筛选后无数据")
            funnel_data['L3_pass'] = len(df)
            diagnostic_info.append(f"L3(狙击): {funnel_data['L2_pass']} -> {len(df)}")
            
            # === Layer 4: 评分排序 ===
            df = self.score_and_rank(df, trade_date, params)
            if df.empty:
                return self._empty_result(diagnostic_info, funnel_data, "Layer 4 评分后无数据")
            funnel_data['L4_pass'] = len(df)
            diagnostic_info.append(f"L4(评分): {funnel_data['L3_pass']} -> {len(df)}")
            
            # === 最终输出 ===
            max_recommendations = params.get('max_recommendations', self.config.MAX_RECOMMENDATIONS)
            actual_limit = min(top_n or max_recommendations, max_recommendations)
            
            # 阴线低吸优先排序
            if params.get('prefer_negative_change', self.config.PREFER_NEGATIVE_CHANGE):
                df = df.sort_values(['is_negative_day', 'total_score'], ascending=[False, False])
            
            top_stocks = df.head(actual_limit).copy()
            funnel_data['final'] = len(top_stocks)
            
            # 构建结果
            results = self._build_t10_results(top_stocks, params, trade_date)
            metadata = self._build_t10_metadata(funnel_data, df, params)
            
            logger.info(f"T10 Pipeline Complete: 生成 {len(results)} 条推荐")
            diagnostic_info.append(f"推荐: {len(results)}只")
            
            return results, " | ".join(diagnostic_info), metadata
            
        except Exception as e:
            logger.error(f"T10 Pipeline失败: {e}", exc_info=True)
            diagnostic_info.append(f"异常: {str(e)}")
            return [], " | ".join(diagnostic_info), {'model_version': self.model_version, 'funnel_data': funnel_data}
    
    # ============================================
    # Helper Methods - T10结构狙击者
    # ============================================
    def _empty_result(self, diagnostic_info: List[str], funnel_data: Dict, msg: str) -> Tuple[List[Dict], str, Dict]:
        """返回空结果"""
        diagnostic_info.append(msg)
        metadata = {
            'model_version': self.model_version,
            'funnel_data': funnel_data
        }
        return [], " | ".join(diagnostic_info), metadata
    
    def _build_t10_results(self, top_stocks: pd.DataFrame, params: Dict, trade_date: date) -> List[Dict]:
        """T10构建推荐结果"""
        if top_stocks.empty:
            return []
        
        top_stocks = top_stocks.copy()
        top_stocks['entry_price'] = top_stocks['close_price'].astype(float)
        top_stocks['stop_loss_price'] = (top_stocks['entry_price'] * self.config.STOP_LOSS_RATIO).round(2)
        
        results = []
        for _, row in top_stocks.iterrows():
            result = {
                'sheep_code': str(row['sheep_code']),
                'sheep_name': str(row['sheep_name']),
                'entry_price': float(row['entry_price']),
                'ai_score': float(row['total_score']),
                'reason_tags': str(row.get('reason_tags', '板块向上+缩量回踩')),
                'stop_loss_price': float(row['stop_loss_price']),
                'vol_ratio': float(row.get('vol_ratio', 0.0)),
                'turnover_rate': float(row.get('turnover_rate', 0.0)),
                'change_pct': float(row.get('change_pct', 0.0)),
                'rps_250': float(row.get('rps_250', 0)) if pd.notna(row.get('rps_250')) else None,
                'is_star_market': bool(row.get('is_star_market', False)),
                'is_gem': bool(row.get('is_gem', False)),
                'estimated_mv': float(row.get('estimated_mv', 0.0)) if pd.notna(row.get('estimated_mv')) else 0.0,
                'industry': str(row.get('industry', '未知')),
                # T10特有字段
                'f1_vol_score': float(row.get('f1_vol_score', 0)),
                'f2_turnover_score': float(row.get('f2_turnover_score', 0)),
                'f3_rps_score': float(row.get('f3_rps_score', 0)),
                'is_negative_day': bool(row.get('is_negative_day', False)),
                'is_extreme_shrink': bool(row.get('is_extreme_shrink', False)),
                'sniper_setup': bool(row.get('sniper_setup', False)),
                'model_version': self.model_version,
            }
            results.append(result)
        
        return results
    
    def _build_t10_metadata(self, funnel_data: Dict, df: pd.DataFrame, params: Dict) -> Dict:
        """T10构建元数据"""
        return {
            'model_version': self.model_version,
            'funnel_data': {k: int(v) if isinstance(v, (int, np.integer)) else v for k, v in funnel_data.items()},
            'filter_stats': {
                'level1_pass': int(funnel_data.get('L1_pass', 0)),
                'level2_pass': int(funnel_data.get('L2_pass', 0)),
                'level3_pass': int(funnel_data.get('L3_pass', 0)),
                'level4_pass': int(funnel_data.get('L4_pass', 0)),
                'final': int(funnel_data.get('final', 0)),
            },
            'score_stats': {
                'avg_f1_vol_score': float(df['f1_vol_score'].mean()) if 'f1_vol_score' in df.columns else 0,
                'avg_f2_turnover_score': float(df['f2_turnover_score'].mean()) if 'f2_turnover_score' in df.columns else 0,
                'avg_f3_rps_score': float(df['f3_rps_score'].mean()) if 'f3_rps_score' in df.columns else 0,
                'avg_total_score': float(df['total_score'].mean()) if 'total_score' in df.columns else 0,
                'negative_day_count': int(df['is_negative_day'].sum()) if 'is_negative_day' in df.columns else 0,
            },
            'params_used': {
                'vol_ratio_max': float(params.get('vol_ratio_max', self.config.VOL_RATIO_MAX)),
                'turnover_min': float(params.get('turnover_min', self.config.TURNOVER_OPTIMAL_MIN)),
                'turnover_max': float(params.get('turnover_max', self.config.TURNOVER_OPTIMAL_MAX)),
                'min_score': float(params.get('min_score', self.config.MIN_SCORE)),
                'prefer_negative_change': bool(params.get('prefer_negative_change', self.config.PREFER_NEGATIVE_CHANGE)),
            }
        }
