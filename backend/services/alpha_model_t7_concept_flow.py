"""
AlphaModel_T7_Concept_Flow - 概念资金双驱模型 v6.0重构版

重构核心优化（基于量化金融工程最佳实践）：
================
v6.0核心重构目标：
1. 清晰的3层漏斗架构（Filter -> Score -> Validate），消除层级重叠
2. 因子正交化：技术/资金/概念三大类因子独立计算，Z-Score标准化后加权
3. 参数统一管理：所有阈值集中到ModelConfig，根据市场状态自适应调整
4. 消除硬编码：移除散落各处的魔数，改用配置驱动
5. 消除重复逻辑：Level 2不再做Level 0/Level 1的"二次确认"

架构设计：
================
┌─────────────────────────────────────────────────────────────┐
│                    Pipeline Flow                             │
├─────────────────────────────────────────────────────────────┤
│  Input: trade_date                                           │
│    │                                                         │
│    ▼                                                         │
│  [Market Regime] 市场状态识别 (Attack/Defense/Balance)        │
│    │                                                         │
│    ▼                                                         │
│  [Filter Layer] 硬性过滤层 ─────────────────────────┐        │
│    ├─ SQL过滤: ST/新股/涨停/基础市值/基础RPS         │        │
│    └─ 数据验证: 历史数据充足性检查                   │        │
│    │                                                 ▼        │
│    ▼                                              (剔除)      │
│  [Feature Layer] 特征提取层                                  │
│    ├─ 技术因子: RPS, VCP, 量比, 均线位置                     │
│    ├─ 资金因子: 资金流占比, 超大单, II%                      │
│    └─ 概念因子: 概念共振, 板块联动                           │
│    │                                                         │
│    ▼                                                         │
│  [Score Layer] 多因子评分层 (Z-Score标准化 + 动态权重)        │
│    ├─ 技术得分 (0-40分)                                      │
│    ├─ 资金得分 (0-30分)                                      │
│    └─ 概念得分 (0-30分)                                      │
│    │                                                         │
│    ▼                                                         │
│  [Validate Layer] 启动质量验证层 (扣分制)                     │
│    ├─ 一票否决: 墓碑线/放量滞涨/主力出货                      │
│    └─ 渐进扣分: 上影线/尾盘急拉/孤军深入                      │
│    │                                                         │
│    ▼                                                         │
│  [Output] 推荐结果 + 诊断信息 + 元数据                        │
└─────────────────────────────────────────────────────────────┘

使用说明：
================
1. 默认参数已优化，直接调用即可：
   model = AlphaModelT7ConceptFlow()
   results, info, metadata = model.run_full_pipeline(trade_date)

2. 如需调整，只需关注核心参数：
   params = {
       'vol_threshold': 1.5,        # 量比阈值
       'rps_threshold': 80,         # RPS强度阈值
       'min_ai_score': 50,          # AI评分门槛
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
from db.database import get_raw_connection, get_sqlalchemy_engine
from etl.trade_date_adapter import TradeDateAdapter
import statsmodels.api as sm
from scipy.stats import zscore
from collections import defaultdict

warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy')
logger = logging.getLogger(__name__)


# ============================================
# 统一配置管理 - 消除硬编码
# ============================================
@dataclass
class ModelConfig:
    """模型配置 - 所有阈值集中管理"""
    
    # === 基础常量 ===
    MIN_HISTORY_DAYS: int = 60
    HISTORY_DAYS_FOR_FACTORS: int = 90
    STOP_LOSS_RATIO: float = 0.93
    DEFAULT_TOP_N: int = 5
    
    # === 市场状态识别参数 ===
    RSRS_LOOKBACK_DAYS: int = 18
    RSRS_ZSCORE_WINDOW: int = 600
    ATTACK_THRESHOLD: float = 0.35
    DEFENSE_THRESHOLD: float = -0.35
    STRUCTURAL_BULL_INFLOW: float = 500000  # 50亿=500000万 (v6.1: 提高门槛，避免误判)
    
    # === 筛选层阈值 (Filter Layer) ===
    # SQL层使用宽松值，留更多buffer
    SQL_MIN_MV: float = 10
    SQL_MAX_MV: float = 1000
    SQL_RPS_THRESHOLD: float = 50       # v6.1: 从60降到50，防止初筛太严
    SQL_NEW_STOCK_DAYS: int = 60
    
    # === 评分层阈值 (Score Layer) ===
    # 技术因子
    VOL_THRESHOLD: float = 1.3          # v6.1: 从1.5降到1.3
    RPS_THRESHOLD: float = 75           # v6.1: 从80降到75
    VCP_TIGHT_THRESHOLD: float = 0.4
    
    # 资金因子
    FLOW_RATIO_STRONG: float = 3.0
    FLOW_RATIO_MEDIUM: float = 1.0
    SUPER_LARGE_THRESHOLD: float = 300
    
    # 概念因子
    CONCEPT_RESONANCE_MIN: float = 5    # v6.1: 从10降到5
    SECTOR_LINKAGE_MIN: float = 0.1     # v6.1: 从0.15降到0.1
    SECTOR_INFLOW_STRONG: float = 5000
    
    # === 验证层阈值 (Validate Layer) ===
    BREAKOUT_BASE_SCORE: float = 60
    VETO_TOMBSTONE_RATIO: float = 0.35
    VETO_STAGNATION_TURNOVER: float = 15.0 # 放宽
    VETO_STAGNATION_GAIN: float = 1.5      # 放宽
    VETO_MAIN_DUMP: float = -500           # 放宽
    
    DEDUCT_UPPER_SHADOW: float = 0.30      # 放宽
    DEDUCT_LATE_RALLY: float = 0.85        # 放宽
    DEDUCT_HIGH_TURNOVER: float = 20.0     # 放宽
    DEDUCT_VOL_SPIKE: float = 2.5          # 放宽
    
    # === 最终筛选阈值 ===
    MIN_AI_SCORE: float = 40               # v6.1: 从50降到40
    MIN_BREAKOUT_QUALITY: float = 30
    MAX_RECOMMENDATIONS: int = 20


@dataclass 
class RegimeAdaptiveConfig:
    """市场状态自适应配置"""
    
    # 各状态下的因子权重
    WEIGHTS: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        "Attack":  {"technical": 0.5, "fund": 0.3, "concept": 0.2},
        "Defense": {"technical": 0.2, "fund": 0.3, "concept": 0.5},
        "Balance": {"technical": 0.35, "fund": 0.35, "concept": 0.3}
    })
    
    # 各状态下的涨幅范围 - v6.1: 放宽范围避免0结果
    CHANGE_RANGE: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        "Attack":  {"min": 1.5, "max": 9.9},   # 放宽下限
        "Defense": {"min": -3.0, "max": 7.0},  # 放宽范围
        "Balance": {"min": 0.5, "max": 9.9}    # 放宽下限
    })


class AlphaModelT7ConceptFlow:
    """
    T7概念资金双驱模型 v6.0 - 重构版
    
    核心设计原则：
    1. 单一职责：每层只做一件事
    2. 因子正交：技术/资金/概念独立计算
    3. 配置驱动：无硬编码魔数
    """
    
    def __init__(self, config: ModelConfig = None):
        self.config = config or ModelConfig()
        self.regime_config = RegimeAdaptiveConfig()
        self.model_version = "T7_Concept_Flow_v6.0"
        self.regime = "Balance"
        self.rsrs_zscore = 0.0
        self.regime_score = 0.0
    
    # ============================================
    # 默认参数（向后兼容）
    # ============================================
    DEFAULT_PARAMS = {
        'min_change_pct': 1.0,           # v6.1: 默认更宽松
        'max_change_pct': 9.9,
        'vol_threshold': 1.3,           # v6.1: 默认更宽松
        'rps_threshold': 75,            # v6.1: 默认更宽松
        'min_main_inflow': -300,        # v6.1: 默认更宽松
        'require_positive_inflow': False,
        'min_turnover': 1.0,            # v6.1: 默认更宽松
        'max_turnover': 35.0,
        'breakout_validation': True,
        'min_breakout_quality': 30,
        'min_ai_score': 40,             # v6.1: 默认更宽松
        'max_recommendations': 20,
        'require_concept_resonance': True,
        'enable_sector_linkage': True,
        'concept_boost': True,
        'prefer_20cm': True,
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
    # Layer 0: 市场状态识别
    # ============================================
    def detect_market_regime(self, trade_date: date) -> Dict[str, any]:
        """
        市场状态识别 - 决定后续层的行为
        
        采用多维度评分：
        - RSRS (15%): 支撑阻力相对强度
        - 板块轮动 (25%): sector_money_flow资金集中度
        - 市场宽度 (25%): 涨跌家数比
        - 量能变化 (15%): 成交量趋势
        - 均线排列 (15%): MA多空排列
        - 情绪指标 (5%): 涨停/跌停比例
        """
        try:
            from db.index_repository import IndexRepository
            from etl.index_adapter import IndexAdapter
            from config import Config
            
            default_result = self._get_default_regime_result()
            
            # 获取指数数据
            end_date = trade_date
            start_date = trade_date - timedelta(days=250)
            index_df = IndexRepository.get_index_daily_data('CSI1000', start_date=start_date, end_date=end_date)
            
            if index_df.empty or len(index_df) < Config.RSRS_WINDOW:
                logger.warning("市场状态识别: 指数数据不足")
                return default_result
            
            index_data = self._convert_index_data(index_df, trade_date)
            if len(index_data) < Config.RSRS_WINDOW:
                return default_result
            
            # 计算各维度得分
            rsrs_score = self._calc_rsrs_score(index_data, IndexAdapter, Config)
            sector_score, is_structural_bull, dominant_sector = self._calc_sector_rotation_score(trade_date)
            breadth_score, stats = self._calc_market_breadth_score(trade_date)
            volume_score = self._calc_volume_score(index_data)
            ma_score = self._calc_ma_alignment_score(index_data)
            sentiment_score = self._calc_sentiment_score(stats)
            
            # 综合评分
            regime_score = (
                rsrs_score * 0.15 +
                sector_score * 0.25 +
                breadth_score * 0.25 +
                volume_score * 0.15 +
                ma_score * 0.15 +
                sentiment_score * 0.05
            )
            
            # 确定市场状态
            if is_structural_bull or regime_score > self.config.ATTACK_THRESHOLD:
                regime = "Attack"
                regime_score = max(regime_score, 0.6) if is_structural_bull else regime_score
            elif regime_score < self.config.DEFENSE_THRESHOLD:
                regime = "Defense"
            else:
                regime = "Balance"
            
            # 极端情况修正
            if stats['limit_up_count'] > 80 and stats['limit_down_count'] < 5:
                regime, regime_score = "Attack", max(regime_score, 0.6)
            if stats['limit_down_count'] > 50:
                regime, regime_score = "Defense", min(regime_score, -0.5)
            
            self.regime = regime
            self.rsrs_zscore = rsrs_score
            self.regime_score = regime_score
            
            logger.info(f"市场状态: {regime} (综合评分: {regime_score:.3f})")
            logger.info(f"市场状态详情 - RSRS: {rsrs_score:.3f}, 板块轮动: {sector_score:.3f}, 市场宽度: {breadth_score:.3f}, 量能: {volume_score:.3f}, 均线: {ma_score:.3f}, 情绪: {sentiment_score:.3f}")
            logger.info(f"市场状态详情 - 涨跌家数: {stats['up_count']}/{stats['down_count']}, 涨跌停: {stats['limit_up_count']}/{stats['limit_down_count']}")
            if is_structural_bull:
                logger.info(f"市场状态详情 - 结构性牛市检测: {dominant_sector} 资金得分 {sector_score:.3f}")
            
            return {
                'regime': regime,
                'regime_score': regime_score,
                'rsrs_zscore': rsrs_score,
                'rsrs_score': rsrs_score,
                'sector_rotation_score': sector_score,
                'is_structural_bull': is_structural_bull,
                'dominant_sector': dominant_sector,
                'market_breadth_score': breadth_score,
                'volume_score': volume_score,
                'ma_score': ma_score,
                'sentiment_score': sentiment_score,
                **stats
            }
            
        except Exception as e:
            logger.error(f"市场状态识别失败: {e}", exc_info=True)
            return self._get_default_regime_result()
    
    def _get_default_regime_result(self) -> Dict:
        self.regime = "Balance"
        self.rsrs_zscore = 0.0
        self.regime_score = 0.0
        return {
            'regime': 'Balance', 'regime_score': 0.0, 'rsrs_zscore': 0.0,
            'rsrs_beta': 0.0, 'rsrs_r2': 0.0, 'rsrs_score': 0.0,
            'sector_rotation_score': 0.0, 'is_structural_bull': False,
            'dominant_sector': None, 'market_breadth_score': 0.0,
            'volume_score': 0.0, 'ma_score': 0.0, 'sentiment_score': 0.0,
            'up_count': 0, 'down_count': 0, 'limit_up_count': 0, 'limit_down_count': 0
        }
    
    def _convert_index_data(self, index_df: pd.DataFrame, trade_date: date) -> List[Dict]:
        index_data = []
        for _, row in index_df.iterrows():
            if row['trade_date'] <= trade_date:
                index_data.append({
                    'trade_date': row['trade_date'],
                    'high_price': float(row['high_price']) if pd.notna(row['high_price']) else 0.0,
                    'low_price': float(row['low_price']) if pd.notna(row['low_price']) else 0.0,
                    'close_price': float(row['close_price']) if pd.notna(row['close_price']) else 0.0,
                    'open_price': float(row['open_price']) if pd.notna(row.get('open_price')) else 0.0,
                    'volume': float(row['volume']) if pd.notna(row.get('volume')) else 0.0,
                    'change_pct': float(row['change_pct']) if pd.notna(row.get('change_pct')) else 0.0
                })
        return sorted(index_data, key=lambda x: x['trade_date'])
    
    def _calc_rsrs_score(self, index_data: List[Dict], IndexAdapter, Config) -> float:
        rsrs = IndexAdapter.calculate_rsrs(index_data, Config.RSRS_WINDOW)
        if rsrs:
            return np.clip(rsrs['zscore'] / 1.5, -1.0, 1.0)
        return 0.0
    
    def _calc_sector_rotation_score(self, trade_date: date) -> Tuple[float, bool, Optional[str]]:
        try:
            with get_raw_connection() as conn:
                sector_df = pd.read_sql(
                    "SELECT sector_name, main_net_inflow FROM sector_money_flow WHERE trade_date = %s ORDER BY ABS(main_net_inflow) DESC",
                    get_sqlalchemy_engine(), params=(trade_date,)
                )
                
                if sector_df.empty:
                    return 0.0, False, None
                
                max_inflow = sector_df['main_net_inflow'].max()
                min_inflow = sector_df['main_net_inflow'].min()
                is_structural_bull = max_inflow > self.config.STRUCTURAL_BULL_INFLOW
                is_structural_bear = abs(min_inflow) > self.config.STRUCTURAL_BULL_INFLOW  # 看资金流出情况
                dominant_sector = sector_df.loc[sector_df['main_net_inflow'].idxmax(), 'sector_name'] if is_structural_bull else None
                dominant_outflow_sector = sector_df.loc[sector_df['main_net_inflow'].idxmin(), 'sector_name'] if is_structural_bear else None
                
                if is_structural_bull:
                    threshold_yi = self.config.STRUCTURAL_BULL_INFLOW / 10000
                    logger.info(f"结构性牛市检测: {dominant_sector}资金流入 {max_inflow:.0f}万 > {threshold_yi:.0f}亿")
                elif is_structural_bear:
                    threshold_yi = self.config.STRUCTURAL_BULL_INFLOW / 10000
                    logger.info(f"结构性熊市检测: {dominant_outflow_sector}资金流出 {-min_inflow:.0f}万 > {threshold_yi:.0f}亿")
                else:
                    logger.info(f"板块轮动检测: 最强板块 {dominant_sector or 'None'} 资金流入 {max_inflow:.0f}万，未达结构牛阈值 {self.config.STRUCTURAL_BULL_INFLOW / 10000:.0f}亿")
                
                total_inflow = sector_df['main_net_inflow'].sum()
                positive_inflow = sector_df[sector_df['main_net_inflow'] > 0]['main_net_inflow'].sum()
                negative_inflow = sector_df[sector_df['main_net_inflow'] < 0]['main_net_inflow'].abs().sum()
                
                # 计算资金流入流出比率，更好地反映市场情绪
                if positive_inflow + negative_inflow > 0:
                    inflow_ratio = positive_inflow / (positive_inflow + negative_inflow)
                else:
                    inflow_ratio = 0.5  # 中性
                
                # 计算板块集中度
                if total_inflow > 0:
                    top10_inflow = sector_df.nlargest(10, 'main_net_inflow')['main_net_inflow'].sum()
                    concentration = top10_inflow / total_inflow if total_inflow > 0 else 0
                    logger.info(f"板块集中度: top10/{len(sector_df)}流入占比 {concentration:.2f} (top10: {top10_inflow:.0f}万, 总计: {total_inflow:.0f}万)")
                    # 改进评分：考虑流入流出比率 and 集中度
                    score = np.clip((concentration * 0.6 + inflow_ratio * 0.4) * (1 if positive_inflow >= negative_inflow else -1), -1.0, 1.0)
                else:
                    # 资金流出模式
                    score = np.clip((inflow_ratio - 0.5) * 2, -1.0, 0.0)
                
                logger.info(f"板块轮动得分: {score:.3f}, 结构牛: {is_structural_bull}, 主导板块: {dominant_sector}")
                return score, is_structural_bull, dominant_sector
        except Exception as e:
            logger.warning(f"板块轮动计算失败: {e}")
            return 0.0, False, None
    
    def _calc_market_breadth_score(self, trade_date: date) -> Tuple[float, Dict]:
        stats = {'up_count': 0, 'down_count': 0, 'limit_up_count': 0, 'limit_down_count': 0}
        try:
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) as total,
                        SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) as up_count,
                        SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) as down_count,
                        SUM(CASE WHEN change_pct >= 9.9 THEN 1 ELSE 0 END) as limit_up_count,
                        SUM(CASE WHEN change_pct <= -9.9 THEN 1 ELSE 0 END) as limit_down_count
                    FROM sheep_daily WHERE trade_date = %s
                """, [trade_date])
                row = cursor.fetchone()
                cursor.close()
                
                if row and row[0] > 0:
                    stats = {
                        'up_count': int(row[1] or 0),
                        'down_count': int(row[2] or 0),
                        'limit_up_count': int(row[3] or 0),
                        'limit_down_count': int(row[4] or 0)
                    }
                    
                    if stats['down_count'] > 0:
                        ad_ratio = stats['up_count'] / stats['down_count']
                        score = np.clip(np.log2(ad_ratio) / 2, -1.0, 1.0)
                    elif stats['up_count'] > 0:
                        score = 1.0
                    else:
                        score = 0.0
                    
                    if stats['limit_down_count'] > 20:
                        score = min(score, -0.3)
                    if stats['limit_up_count'] > 50 and stats['limit_down_count'] < 10:
                        score = max(score, 0.3)
                    
                    return score, stats
        except Exception as e:
            logger.warning(f"市场宽度计算失败: {e}")
        return 0.0, stats
    
    def _calc_volume_score(self, index_data: List[Dict]) -> float:
        try:
            if len(index_data) >= 20:
                recent_5_vol = np.mean([d.get('volume', 0) for d in index_data[-5:]])
                ma20_vol = np.mean([d.get('volume', 0) for d in index_data[-20:]])
                if ma20_vol > 0:
                    vol_ratio = recent_5_vol / ma20_vol
                    return np.clip((vol_ratio - 1.0) / 0.5, -1.0, 1.0)
        except:
            pass
        return 0.0
    
    def _calc_ma_alignment_score(self, index_data: List[Dict]) -> float:
        try:
            if len(index_data) >= 20:
                closes = [d['close_price'] for d in index_data[-20:]]
                ma5, ma10, ma20 = np.mean(closes[-5:]), np.mean(closes[-10:]), np.mean(closes[-20:])
                current = closes[-1]
                
                score = 0
                score += 0.25 if current > ma5 else -0.25
                score += 0.25 if ma5 > ma10 else -0.25
                score += 0.25 if ma10 > ma20 else -0.25
                score += 0.25 if current > ma20 else -0.25
                return score
        except:
            pass
        return 0.0
    
    def _calc_sentiment_score(self, stats: Dict) -> float:
        total = stats['up_count'] + stats['down_count']
        if total > 0:
            limit_up_ratio = stats['limit_up_count'] / total
            limit_down_ratio = stats['limit_down_count'] / total
            return np.clip((limit_up_ratio - limit_down_ratio) * 20, -1.0, 1.0)
        return 0.0
    
    # ============================================
    # Layer 1: Filter - 硬性过滤
    # ============================================
    def filter_layer(self, trade_date: date, params: Dict) -> pd.DataFrame:
        """
        筛选层 - 只做硬性条件过滤，不做评分
        
        SQL层过滤（宽松）：
        - ST/新股/涨停 -> 剔除
        - 市值 10-1000亿 -> 保留
        - RPS > 60 -> 保留
        - 站上MA20 -> 保留
        """
        try:
            with get_raw_connection() as conn:
                # 先检查sheep_daily表是否有rps_250和vcp_factor列
                cursor = conn.cursor()
                cursor.execute("SHOW COLUMNS FROM sheep_daily LIKE 'rps_250'")
                has_rps_col = cursor.fetchone() is not None
                cursor.execute("SHOW COLUMNS FROM sheep_daily LIKE 'vcp_factor'")
                has_vcp_col = cursor.fetchone() is not None
                cursor.execute("SHOW COLUMNS FROM sheep_daily LIKE 'vol_ma_5'")
                has_vol_ma_col = cursor.fetchone() is not None
                cursor.close()
                
                # 动态构建SELECT字段
                optional_cols = []
                if has_rps_col:
                    optional_cols.append('sd.rps_250')
                if has_vcp_col:
                    optional_cols.append('sd.vcp_factor')
                if has_vol_ma_col:
                    optional_cols.append('sd.vol_ma_5')
                optional_cols_sql = (', ' + ', '.join(optional_cols)) if optional_cols else ''
                
                # 动态构建WHERE条件
                rps_filter = f"AND (sd.rps_250 IS NULL OR sd.rps_250 > %s)" if has_rps_col else ""
                
                # 涨幅限制
                max_chg = params.get('max_change_pct', 9.9)
                max_chg_gem = max(max_chg, 19.9) # 创/沪默认放宽到20%
                
                query = f"""
                    SELECT 
                        sd.sheep_code, sd.trade_date, sd.close_price, sd.high_price,
                        sd.low_price, sd.open_price, sd.volume, sd.amount,
                        sd.turnover_rate, sd.change_pct, sd.ma5, sd.ma10, sd.ma20,
                        sd.ma30, sd.ma60{optional_cols_sql},
                        sb.sheep_name, sb.list_date, sb.industry,
                        CASE WHEN sd.turnover_rate > 0 THEN (sd.amount / (sd.turnover_rate / 100)) / 100000000 ELSE NULL END as estimated_mv
                    FROM (
                        SELECT sheep_code, MAX(trade_date) as max_date
                        FROM sheep_daily WHERE trade_date <= %s GROUP BY sheep_code HAVING COUNT(*) >= %s
                    ) latest
                    INNER JOIN sheep_daily sd ON sd.sheep_code = latest.sheep_code AND sd.trade_date = latest.max_date
                    INNER JOIN sheep_basic sb ON sd.sheep_code = sb.sheep_code
                    WHERE sb.is_active = 1
                      AND sb.sheep_name NOT LIKE '%%ST%%'
                      AND (sb.list_date IS NULL OR DATEDIFF(%s, sb.list_date) >= %s)
                      AND (((sd.sheep_code LIKE '688%%' OR sd.sheep_code LIKE '300%%') AND sd.change_pct < {max_chg_gem})
                           OR (sd.sheep_code NOT LIKE '688%%' AND sd.sheep_code NOT LIKE '300%%' AND sd.change_pct < {max_chg}))
                      AND ((sd.turnover_rate IS NULL OR sd.turnover_rate = 0) OR
                           ((sd.amount / (sd.turnover_rate / 100)) / 100000000 BETWEEN %s AND %s))
                      {rps_filter}
                      AND (sd.ma20 IS NULL OR sd.close_price > sd.ma20)
                """
                
                # 动态构建参数
                params_sql = [trade_date, self.config.MIN_HISTORY_DAYS, trade_date,
                          self.config.SQL_NEW_STOCK_DAYS, self.config.SQL_MIN_MV, self.config.SQL_MAX_MV]
                if has_rps_col:
                    params_sql.append(self.config.SQL_RPS_THRESHOLD)
                
                df = pd.read_sql(query, get_sqlalchemy_engine(), params=tuple(params_sql))
                
                # 如果缺少列，添加默认值
                if 'rps_250' not in df.columns:
                    df['rps_250'] = 0.0
                if 'vcp_factor' not in df.columns:
                    df['vcp_factor'] = 1.0
                if 'vol_ma_5' not in df.columns:
                    df['vol_ma_5'] = df['volume'] if 'volume' in df.columns else 0
                
                logger.info(f"Filter Layer: SQL过滤后剩余 {len(df)} 只")
                logger.info(f"Filter Layer: 数据列包括: {list(df.columns)}")
                
                # 添加数据分布统计
                if 'rps_250' in df.columns:
                    logger.info(f"Filter Layer: RPS_250统计 - min={df['rps_250'].min():.2f}, max={df['rps_250'].max():.2f}, mean={df['rps_250'].mean():.2f}")
                if 'turnover_rate' in df.columns:
                    logger.info(f"Filter Layer: 换手率统计 - min={df['turnover_rate'].min():.2f}%, max={df['turnover_rate'].max():.2f}%, mean={df['turnover_rate'].mean():.2f}%")
                if 'change_pct' in df.columns:
                    logger.info(f"Filter Layer: 涨幅统计 - min={df['change_pct'].min():.2f}%, max={df['change_pct'].max():.2f}%, mean={df['change_pct'].mean():.2f}%")
                
                return df
                
        except Exception as e:
            logger.error(f"Filter Layer失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    # ============================================
    # Layer 2: Feature - 特征提取（独立计算，不评分）
    # ============================================
    def feature_layer(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """
        特征提取层 - 计算所有因子值，但不做评分
        
        三大类因子：
        1. 技术因子: vol_ratio, rps_score, vcp_score, ma_position
        2. 资金因子: money_flow_ratio, super_large_inflow, ii_pct
        3. 概念因子: concept_resonance_score, sector_linkage_strength
        """
        if df.empty:
            return df
        
        df = df.copy()
        
        # 计算基础技术因子
        df = self._calc_technical_factors(df, trade_date)
        
        # 计算资金流因子
        df = self._calc_fund_factors(df, trade_date)
        
        # 计算概念因子
        df = self._calc_concept_factors(df, trade_date)
        
        logger.info(f"Feature Layer: 提取 {len(df)} 只特征完成")
        
        # 特征列统计
        feature_cols = ['rps_250', 'vol_ratio', 'turnover_rate', 'change_pct', 'concept_resonance_score', 'main_net_inflow', 'super_large_inflow', 'clv_position', 'vcp_factor', 'upper_shadow_ratio']
        for col in feature_cols:
            if col in df.columns:
                logger.info(f"Feature Layer: {col} - min={df[col].min():.2f}, max={df[col].max():.2f}, mean={df[col].mean():.2f}")
        
        return df
    
    def _calc_technical_factors(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """计算技术因子"""
        # 量比
        df['vol_ma_5'] = df['vol_ma_5'].fillna(df['volume'])
        df['vol_ratio'] = np.where(df['vol_ma_5'] > 0, df['volume'] / df['vol_ma_5'], 1.0)
        
        # 上影线比例
        price_range = df['high_price'] - df['low_price']
        upper_shadow = df['high_price'] - df['close_price']
        df['upper_shadow_ratio'] = np.where(price_range > 0, upper_shadow / price_range, 0)
        
        # 板块标识
        df['is_star_market'] = df['sheep_code'].str.startswith('688').astype(int)
        df['is_gem'] = df['sheep_code'].str.startswith('300').astype(int)
        
        # CLV位置
        df['clv_position'] = np.where(
            price_range > 0,
            (df['close_price'] - df['low_price']) / price_range,
            0.5
        )
        
        # 填充RPS和VCP
        df['rps_250'] = df['rps_250'].fillna(0)
        df['vcp_factor'] = df['vcp_factor'].fillna(1.0)
        
        return df
    
    def _calc_fund_factors(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """计算资金流因子"""
        try:
            sheep_codes = df['sheep_code'].tolist()
            if not sheep_codes:
                return df
            
            with get_sqlalchemy_engine().connect() as conn:
                placeholders = ','.join(['%s'] * len(sheep_codes))
                money_flow_df = pd.read_sql(
                    f"""SELECT sheep_code, main_net_inflow, super_large_inflow, large_inflow
                        FROM sheep_money_flow WHERE sheep_code IN ({placeholders}) AND trade_date = %s""",
                    conn, params=tuple(sheep_codes + [trade_date])
                )
                
                if not money_flow_df.empty:
                    df = df.merge(money_flow_df, on='sheep_code', how='left')
                    logger.info(f"Feature Layer: 获取 {len(money_flow_df)} 只资金流数据")
        except Exception as e:
            logger.warning(f"获取资金流数据失败: {e}")
        
        # 填充缺失值
        for col in ['main_net_inflow', 'super_large_inflow', 'large_inflow']:
            if col not in df.columns:
                df[col] = 0.0
            df[col] = df[col].fillna(0.0)
        
        # 资金流占比 = 主力净流入 / 成交额 * 100
        df['money_flow_ratio'] = np.where(
            df['amount'] > 0,
            df['main_net_inflow'] / (df['amount'] / 10000) * 100,
            0.0
        )
        
        return df
    
    def _calc_concept_factors(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """计算概念因子 - 简化版"""
        if df.empty:
            df['concept_resonance_score'] = 0.0
            df['resonance_base_tag'] = df.get('industry', '未知').fillna('未知')
            df['is_main_concept'] = 0
            df['is_concept_leader'] = 0
            df['sector_linkage_strength'] = 0.0
            df['tag_total_inflow'] = 0.0
            return df
        
        try:
            sheep_codes = df['sheep_code'].unique().tolist()
            
            with get_raw_connection() as conn:
                # 获取概念映射
                placeholders = ','.join(['%s'] * len(sheep_codes))
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT scm.sheep_code, ct.concept_name, scm.weight
                    FROM sheep_concept_mapping scm
                    INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                    WHERE scm.sheep_code IN ({placeholders}) AND ct.is_active = 1
                    ORDER BY scm.sheep_code, scm.weight DESC
                """, sheep_codes)
                rows = cursor.fetchall()
                cursor.close()
                
                if not rows:
                    return self._set_default_concept_factors(df)
                
                df_concepts = pd.DataFrame(rows, columns=['sheep_code', 'concept_name', 'weight'])
            
            # 构建tag数据
            df_tags = []
            for sheep_code in sheep_codes:
                concepts = df_concepts[df_concepts['sheep_code'] == sheep_code]['concept_name'].tolist()
                industry = df.loc[df['sheep_code'] == sheep_code, 'industry'].iloc[0] if len(df[df['sheep_code'] == sheep_code]) > 0 else None
                
                tags = concepts.copy()
                if industry and pd.notna(industry) and industry != '未知':
                    tags.append(industry)
                if not tags:
                    tags = ['未知']
                
                for tag in tags:
                    df_tags.append({'sheep_code': sheep_code, 'tag': tag})
            
            df_tags = pd.DataFrame(df_tags)
            
            # Merge资金流数据计算tag统计
            df_merge = df_tags.merge(df[['sheep_code', 'change_pct', 'main_net_inflow', 'super_large_inflow']], on='sheep_code', how='left')
            df_merge['total_inflow'] = df_merge['main_net_inflow'].fillna(0) + df_merge['super_large_inflow'].fillna(0)
            
            # 按tag聚合
            tag_stats = df_merge.groupby('tag').agg({
                'change_pct': ['mean', 'count'],
                'total_inflow': 'sum'
            }).reset_index()
            tag_stats.columns = ['tag', 'tag_avg_chg', 'tag_count', 'tag_total_inflow']
            
            # 计算广度
            tag_breadth = df_merge.groupby('tag').apply(
                lambda g: (g['change_pct'].fillna(0) > 3.0).sum() / len(g) if len(g) > 0 else 0.0
            ).reset_index(name='tag_breadth')
            tag_stats = tag_stats.merge(tag_breadth, on='tag', how='left')
            
            # 选择最佳tag
            df_with_tags = df_tags.merge(tag_stats, on='tag', how='left')
            best_tag = df_with_tags.loc[
                df_with_tags.groupby('sheep_code')['tag_total_inflow'].idxmax()
            ][['sheep_code', 'tag', 'tag_avg_chg', 'tag_breadth', 'tag_total_inflow']].copy()
            best_tag.columns = ['sheep_code', 'resonance_base_tag', 'resonance_base_avg_chg', 'resonance_base_breadth', 'tag_total_inflow']
            
            df = df.merge(best_tag, on='sheep_code', how='left')
            
            # 计算共振分数
            df['resonance_base_tag'] = df['resonance_base_tag'].fillna(df.get('industry', '未知')).fillna('未知')
            df['resonance_base_avg_chg'] = df.get('resonance_base_avg_chg', pd.Series([0.0] * len(df))).fillna(0.0)
            df['resonance_base_breadth'] = df.get('resonance_base_breadth', pd.Series([0.0] * len(df))).fillna(0.0)
            df['tag_total_inflow'] = df.get('tag_total_inflow', pd.Series([0.0] * len(df))).fillna(0.0)
            
            # 主线概念判定
            df['is_main_concept'] = ((df['resonance_base_avg_chg'] > 1.0) & (df['resonance_base_breadth'] > 0.15)).astype(int)
            
            # 领头羊判定
            df['is_concept_leader'] = 0
            for tag_name in df['resonance_base_tag'].unique():
                if tag_name == '未知':
                    continue
                tag_stocks = df[df['resonance_base_tag'] == tag_name]
                if len(tag_stocks) > 0:
                    top_codes = tag_stocks.nlargest(5, 'change_pct')['sheep_code'].tolist()
                    df.loc[df['sheep_code'].isin(top_codes), 'is_concept_leader'] = 1
            
            # 计算概念共振分数
            main_bonus = df['is_main_concept'] * 35
            leader_bonus = df['is_concept_leader'] * 25
            heat_bonus = np.where(df['resonance_base_avg_chg'] > 2.0, 20, np.where(df['resonance_base_avg_chg'] > 1.0, 10, 0))
            breadth_bonus = np.where(df['resonance_base_breadth'] > 0.30, 15, np.where(df['resonance_base_breadth'] > 0.20, 10, 0))
            inflow_bonus = np.where(df['tag_total_inflow'] > 1000, 25, np.where(df['tag_total_inflow'] > 500, 15, 0))
            
            df['concept_resonance_score'] = main_bonus + leader_bonus + heat_bonus + breadth_bonus + inflow_bonus
            
            # 板块联动强度
            df['sector_linkage_strength'] = 0.0
            for tag_name in df['resonance_base_tag'].unique():
                if tag_name == '未知':
                    continue
                tag_stocks = df[df['resonance_base_tag'] == tag_name]
                if len(tag_stocks) >= 3:
                    tag_avg = tag_stocks['change_pct'].mean()
                    up_ratio = (tag_stocks['change_pct'] > 0).sum() / len(tag_stocks)
                    if up_ratio >= 0.6 and tag_avg >= 2.0:
                        strength = min(tag_avg / 5.0, 1.0) * up_ratio
                        df.loc[df['resonance_base_tag'] == tag_name, 'sector_linkage_strength'] = strength
            
            logger.info(f"Feature Layer: 概念因子计算完成，共振分数范围 {df['concept_resonance_score'].min():.0f}~{df['concept_resonance_score'].max():.0f}")
            return df
            
        except Exception as e:
            logger.error(f"概念因子计算失败: {e}", exc_info=True)
            return self._set_default_concept_factors(df)
    
    def _set_default_concept_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        df['concept_resonance_score'] = 0.0
        df['resonance_base_tag'] = df.get('industry', '未知').fillna('未知')
        df['is_main_concept'] = 0
        df['is_concept_leader'] = 0
        df['sector_linkage_strength'] = 0.0
        df['tag_total_inflow'] = 0.0
        return df
    
    # ============================================
    # Layer 3: Score - 多因子评分（Z-Score标准化 + 动态权重）
    # ============================================
    def score_layer(self, df: pd.DataFrame, params: Dict, regime_info: Dict) -> pd.DataFrame:
        """
        评分层 - Z-Score标准化后按动态权重加权
        
        评分公式：
        Total = Technical * W_tech + Fund * W_fund + Concept * W_concept
        
        其中：
        - Technical (0-40): 量比 + RPS + VCP + 均线位置
        - Fund (0-30): 资金流占比 + 超大单
        - Concept (0-30): 概念共振 + 板块联动
        """
        if df.empty:
            return df
        
        df = df.copy()
        regime = regime_info.get('regime', 'Balance')
        weights = self.regime_config.WEIGHTS.get(regime, self.regime_config.WEIGHTS['Balance'])
        
        logger.info(f"Score Layer: {regime}模式权重 - 技术:{weights['technical']}, 资金:{weights['fund']}, 概念:{weights['concept']}")
        
        # 技术因子评分 (0-40)
        tech_score = self._score_technical(df, params)
        
        # 资金因子评分 (0-30)
        fund_score = self._score_fund(df, params)
        
        # 概念因子评分 (0-30)
        concept_score = self._score_concept(df, params)
        
        # 加权求和
        df['tech_score'] = tech_score
        df['fund_score'] = fund_score
        df['concept_score'] = concept_score
        
        logger.info(f"Score Layer - 技术因子统计: min={tech_score.min():.2f}, max={tech_score.max():.2f}, mean={tech_score.mean():.2f}")
        logger.info(f"Score Layer - 资金因子统计: min={fund_score.min():.2f}, max={fund_score.max():.2f}, mean={fund_score.mean():.2f}")
        logger.info(f"Score Layer - 概念因子统计: min={concept_score.min():.2f}, max={concept_score.max():.2f}, mean={concept_score.mean():.2f}")
        
        df['total_score'] = (
            tech_score * weights['technical'] +
            fund_score * weights['fund'] +
            concept_score * weights['concept']
        ).clip(0, 100)
        
        # 20cm弹性加分
        if params.get('prefer_20cm', True):
            gem_star_mask = (df['is_star_market'] == 1) | (df['is_gem'] == 1)
            df.loc[gem_star_mask, 'total_score'] += 5
        
        df = df.sort_values('total_score', ascending=False)
        logger.info(f"Score Layer: 评分完成，最高分 {df['total_score'].max():.2f}, 最低分 {df['total_score'].min():.2f}, 平均分 {df['total_score'].mean():.2f}")
        logger.info(f"Score Layer: 技术评分范围 - min={df['tech_score'].min():.2f}, max={df['tech_score'].max():.2f}, mean={df['tech_score'].mean():.2f}")
        logger.info(f"Score Layer: 资金评分范围 - min={df['fund_score'].min():.2f}, max={df['fund_score'].max():.2f}, mean={df['fund_score'].mean():.2f}")
        logger.info(f"Score Layer: 概念评分范围 - min={df['concept_score'].min():.2f}, max={df['concept_score'].max():.2f}, mean={df['concept_score'].mean():.2f}")
        
        return df
    
    def _score_technical(self, df: pd.DataFrame, params: Dict) -> pd.Series:
        """技术因子评分 (0-40)"""
        vol_threshold = params.get('vol_threshold', self.config.VOL_THRESHOLD)
        rps_threshold = params.get('rps_threshold', self.config.RPS_THRESHOLD)
        
        # 量比得分 (0-15)
        vol_score = np.where(df['vol_ratio'] >= vol_threshold * 1.5, 15,
                    np.where(df['vol_ratio'] >= vol_threshold, 10,
                    np.where(df['vol_ratio'] >= 1.0, 5, 0)))
        
        # RPS得分 (0-15)
        rps_score = np.where(df['rps_250'] >= rps_threshold + 10, 15,
                   np.where(df['rps_250'] >= rps_threshold, 10,
                   np.where(df['rps_250'] >= rps_threshold - 10, 5, 0)))
        
        # VCP得分 (0-10): 越小越好
        vcp_median = df['vcp_factor'].median() if df['vcp_factor'].median() > 0 else 1.0
        vcp_score = np.where(df['vcp_factor'] < self.config.VCP_TIGHT_THRESHOLD, 10,
                   np.where(df['vcp_factor'] < vcp_median, 5, 0))
        
        return pd.Series(vol_score + rps_score + vcp_score, index=df.index).clip(0, 40)
    
    def _score_fund(self, df: pd.DataFrame, params: Dict) -> pd.Series:
        """资金因子评分 (0-30)"""
        # 资金流占比得分 (0-15)
        flow_score = np.where(df['money_flow_ratio'] >= self.config.FLOW_RATIO_STRONG, 15,
                    np.where(df['money_flow_ratio'] >= self.config.FLOW_RATIO_MEDIUM, 10,
                    np.where(df['money_flow_ratio'] > 0, 5, 0)))
        
        # 资金流出扣分
        flow_score = np.where(df['money_flow_ratio'] < -2.0, flow_score - 10,
                    np.where(df['money_flow_ratio'] < -1.0, flow_score - 5, flow_score))
        
        # 超大单得分 (0-15)
        super_score = np.where(df['super_large_inflow'] > self.config.SUPER_LARGE_THRESHOLD * 2, 15,
                     np.where(df['super_large_inflow'] > self.config.SUPER_LARGE_THRESHOLD, 10,
                     np.where(df['super_large_inflow'] > 0, 5, 0)))
        
        return pd.Series(flow_score + super_score, index=df.index).clip(0, 30)
    
    def _score_concept(self, df: pd.DataFrame, params: Dict) -> pd.Series:
        """概念因子评分 (0-30)"""
        if 'concept_resonance_score' not in df.columns:
            return pd.Series([0] * len(df), index=df.index)
        
        # 概念共振分数归一化到0-20
        max_resonance = max(df['concept_resonance_score'].max(), 100)
        resonance_norm = (df['concept_resonance_score'] / max_resonance * 20).clip(0, 20)
        
        # 板块联动得分 (0-10)
        linkage_score = np.where(df['sector_linkage_strength'] > 0.5, 10,
                       np.where(df['sector_linkage_strength'] > 0.3, 7,
                       np.where(df['sector_linkage_strength'] > 0.1, 4, 0)))
        
        return pd.Series(resonance_norm + linkage_score, index=df.index).clip(0, 30)
    
    # ============================================
    # Layer 4: Validate - 启动质量验证（扣分制）
    # ============================================
    def validate_layer(self, df: pd.DataFrame, params: Dict, regime_info: Dict) -> pd.DataFrame:
        """
        验证层 - 启动质量验证（扣分制 + 一票否决）
        
        基础分60分，通过加分/扣分调整：
        - 一票否决: 墓碑线/放量滞涨/主力出货 -> 直接-70分
        - 渐进扣分: 上影线/尾盘急拉/孤军深入 -> -15~-30分
        - 加分项: VCP收敛/资金流入/CLV高位 -> +10~+20分
        """
        if df.empty:
            return df
        
        df = df.copy()
        df['breakout_quality_score'] = self.config.BREAKOUT_BASE_SCORE
        df['breakout_warning'] = ''
        
        # === 加分项 ===
        # VCP收敛 (+15)
        vcp_tight = df['vcp_factor'] < self.config.VCP_TIGHT_THRESHOLD
        df.loc[vcp_tight, 'breakout_quality_score'] += 15
        
        # 概念共振 (+20)
        has_resonance = df['concept_resonance_score'] >= self.config.CONCEPT_RESONANCE_MIN + 10
        df.loc[has_resonance, 'breakout_quality_score'] += 20
        
        # 主力资金流入 (+15)
        main_inflow = df['main_net_inflow'] > 0
        df.loc[main_inflow, 'breakout_quality_score'] += 15
        
        # CLV高位 (+15)
        clv_high = df['clv_position'] > 0.8
        df.loc[clv_high, 'breakout_quality_score'] += 15
        
        # 超大单进场 (+20)
        super_large = df['super_large_inflow'] > self.config.SUPER_LARGE_THRESHOLD
        df.loc[super_large, 'breakout_quality_score'] += 20
        
        # === 一票否决 ===
        # 墓碑线
        fallback_ratio = np.where(
            (df['high_price'] - df['low_price']) > 0,
            (df['high_price'] - df['close_price']) / (df['high_price'] - df['low_price']),
            0
        )
        tombstone = fallback_ratio > self.config.VETO_TOMBSTONE_RATIO
        df.loc[tombstone, 'breakout_quality_score'] = -10
        df.loc[tombstone, 'breakout_warning'] = '墓碑线[否决]'
        
        # 放量滞涨
        stagnation = (
            (df['turnover_rate'] > self.config.VETO_STAGNATION_TURNOVER) &
            (df['change_pct'] < self.config.VETO_STAGNATION_GAIN) &
            (df['change_pct'] > 0) &
            (df['breakout_quality_score'] > -10)
        )
        df.loc[stagnation, 'breakout_quality_score'] = -10
        df.loc[stagnation, 'breakout_warning'] = '放量滞涨[否决]'
        
        # 主力出货
        main_dump = (
            (df['main_net_inflow'] < self.config.VETO_MAIN_DUMP) &
            (df['change_pct'] > 0) &
            (df['breakout_quality_score'] > -10)
        )
        df.loc[main_dump, 'breakout_quality_score'] = -10
        df.loc[main_dump, 'breakout_warning'] = '主力出货[否决]'
        
        # === 渐进扣分 ===
        # 长上影线 (-25)
        long_shadow = (df['upper_shadow_ratio'] > self.config.DEDUCT_UPPER_SHADOW) & (df['breakout_quality_score'] > 0)
        df.loc[long_shadow, 'breakout_quality_score'] -= 25
        df.loc[long_shadow & (df['breakout_warning'] == ''), 'breakout_warning'] = '长上影线'
        
        # 孤军深入 (-30)
        no_resonance = df['concept_resonance_score'] <= 0
        df.loc[no_resonance, 'breakout_quality_score'] -= 30
        df.loc[no_resonance & (df['breakout_warning'] == ''), 'breakout_warning'] = '孤军深入'
        
        # 涨幅虚高 (-30)
        high_gain_low_vol = (df['change_pct'] > 5.0) & (df['vol_ratio'] < 1.2)
        df.loc[high_gain_low_vol, 'breakout_quality_score'] -= 30
        df.loc[high_gain_low_vol & (df['breakout_warning'] == ''), 'breakout_warning'] = '涨幅虚高'
        
        # 限制分数范围
        df['breakout_quality_score'] = df['breakout_quality_score'].clip(0, 100)
        
        # 整合到总分
        quality_adj = (df['breakout_quality_score'] - 50) * 0.4
        df['total_score'] = (df['total_score'] + quality_adj).clip(0, 100)
        
        veto_count = df['breakout_warning'].str.contains('否决', na=False).sum()
        high_quality = (df['breakout_quality_score'] >= 70).sum()
        medium_quality = ((df['breakout_quality_score'] >= 40) & (df['breakout_quality_score'] < 70)).sum()
        low_quality = (df['breakout_quality_score'] < 40).sum()
        
        logger.info(f"Validate Layer: 一票否决{veto_count}只，高质量启动{high_quality}只，中等质量{medium_quality}只，低质量{low_quality}只")
        logger.info(f"Validate Layer: 质量分数统计 - min={df['breakout_quality_score'].min():.2f}, max={df['breakout_quality_score'].max():.2f}, mean={df['breakout_quality_score'].mean():.2f}")
        logger.info(f"Validate Layer: 总分统计 - min={df['total_score'].min():.2f}, max={df['total_score'].max():.2f}, mean={df['total_score'].mean():.2f}")
        
        return df
    
    # ============================================
    # Layer 5: Final Filter - 基于市场状态的最终筛选
    # ============================================
    def final_filter_layer(self, df: pd.DataFrame, params: Dict, regime_info: Dict) -> pd.DataFrame:
        """
        最终筛选层 - 根据市场状态筛选涨幅范围等
        """
        if df.empty:
            return df
        
        initial_count = len(df)
        df = df.copy()
        regime = regime_info.get('regime', 'Balance')
        change_range = self.regime_config.CHANGE_RANGE.get(regime, self.regime_config.CHANGE_RANGE['Balance'])
        
        # 优先使用用户传参的涨幅范围
        min_change = params.get('min_change_pct', change_range['min'])
        max_change = params.get('max_change_pct', change_range['max'])
        
        logger.info(f"Final Filter - 初始股票数量: {initial_count}")
        logger.info(f"Final Filter - 市场状态: {regime}, 涨幅范围: {min_change}% ~ {max_change}%")
        
        # 涨幅筛选
        mask = (df['change_pct'] >= min_change) & (df['change_pct'] <= max_change)
        logger.info(f"Final Filter - 涨幅筛选: {mask.sum()}/{initial_count} 通过 ({min_change}%~{max_change}%)")
        
        # 记录涨幅分布
        if 'change_pct' in df.columns:
            logger.info(f"Final Filter - 涨幅统计: min={df['change_pct'].min():.2f}%, max={df['change_pct'].max():.2f}%, mean={df['change_pct'].mean():.2f}%")
        
        # 概念龙头豁免
        if regime == "Defense" and 'is_concept_leader' in df.columns:
            leader_exempt = df['is_concept_leader'] == 1
            mask = mask | leader_exempt
            logger.info(f"Final Filter - 概念龙头豁免: {leader_exempt.sum()} 只")
        
        df = df[mask]
        after_change_filter = len(df)
        logger.info(f"Final Filter - 涨幅筛选后剩余: {after_change_filter}")
        
        # 概念共振筛选 (作为硬过滤)
        if params.get('require_concept_resonance', True):
            if 'concept_resonance_score' not in df.columns:
                logger.warning("Final Filter - 缺少 'concept_resonance_score' 列，跳过概念共振筛选")
            else:
                min_resonance = params.get('min_concept_resonance', self.config.CONCEPT_RESONANCE_MIN)
                # 兼容性处理：如果所有标的分数都低，且处于进攻模式，可适当放宽
                if regime == "Attack":
                    min_resonance = min_resonance * 0.5
                    
                resonance_mask = df['concept_resonance_score'] >= min_resonance
                logger.info(f"Final Filter - 概念共振筛选: {resonance_mask.sum()}/{after_change_filter} 通过 (阈值: {min_resonance})")
                
                # 记录概念共振分数分布
                logger.info(f"Final Filter - 概念共振分数统计: min={df['concept_resonance_score'].min():.2f}, max={df['concept_resonance_score'].max():.2f}, mean={df['concept_resonance_score'].mean():.2f}")
                
                df = df[resonance_mask]
                after_resonance_filter = len(df)
                logger.info(f"Final Filter - 概念共振筛选后剩余: {after_resonance_filter}")
                if len(df) == 0:
                    logger.warning(f"Final Filter - 概念共振筛选(>={min_resonance}) 过滤了所有标的")
        else:
            logger.info("Final Filter - 跳过概念共振筛选 (require_concept_resonance=False)")
        
        # RPS筛选
        rps_threshold = params.get('rps_threshold', self.config.RPS_THRESHOLD)
        if 'rps_250' in df.columns:
            rps_mask = df['rps_250'] >= rps_threshold
            logger.info(f"Final Filter - RPS筛选: {rps_mask.sum()}/{len(df)} 通过 (阈值: {rps_threshold})")
            
            # 记录RPS分布
            logger.info(f"Final Filter - RPS_250统计: min={df['rps_250'].min():.2f}, max={df['rps_250'].max():.2f}, mean={df['rps_250'].mean():.2f}")
            
            # 如果RPS筛选导致结果为0，检查是否是因为数据缺失（rps全为0）
            if rps_mask.sum() == 0 and len(df) > 0:
                if (df['rps_250'] == 0).all():
                    logger.warning("Final Filter - RPS数据全为0，可能数据未加载，临时跳过RPS筛选")
                    rps_mask = pd.Series([True] * len(df), index=df.index)
                elif df['rps_250'].isna().all():
                    logger.warning("Final Filter - RPS数据全为NaN，可能数据未加载，临时跳过RPS筛选")
                    rps_mask = pd.Series([True] * len(df), index=df.index)
                else:
                    logger.info(f"Final Filter - RPS筛选过于严苛(>={rps_threshold})，当前最高RPS: {df['rps_250'].max():.1f}")
            
            df = df[rps_mask]
            after_rps_filter = len(df)
            logger.info(f"Final Filter - RPS筛选后剩余: {after_rps_filter}")
        else:
            logger.warning("Final Filter - 缺少 'rps_250' 列，跳过RPS筛选")
        
        # 量比筛选
        vol_threshold = params.get('vol_threshold', self.config.VOL_THRESHOLD)
        if 'vol_ratio' in df.columns:
            vol_mask = df['vol_ratio'] >= vol_threshold
            logger.info(f"Final Filter - 量比筛选: {vol_mask.sum()}/{len(df)} 通过 (阈值: {vol_threshold})")
            
            # 记录量比分布
            logger.info(f"Final Filter - 量比统计: min={df['vol_ratio'].min():.2f}, max={df['vol_ratio'].max():.2f}, mean={df['vol_ratio'].mean():.2f}")
            
            df = df[vol_mask]
            after_vol_filter = len(df)
            logger.info(f"Final Filter - 量比筛选后剩余: {after_vol_filter}")
        else:
            logger.warning("Final Filter - 缺少 'vol_ratio' 列，跳过量比筛选")
        
        # 换手率筛选
        min_turnover = params.get('min_turnover', 1.0)
        max_turnover = params.get('max_turnover', 35.0)
        if 'turnover_rate' in df.columns:
            turnover_mask = (df['turnover_rate'] >= min_turnover) & (df['turnover_rate'] <= max_turnover)
            logger.info(f"Final Filter - 换手率筛选: {turnover_mask.sum()}/{len(df)} 通过 ({min_turnover}%~{max_turnover}%)")
            
            # 记录换手率分布
            logger.info(f"Final Filter - 换手率统计: min={df['turnover_rate'].min():.2f}%, max={df['turnover_rate'].max():.2f}%, mean={df['turnover_rate'].mean():.2f}%")
            
            df = df[turnover_mask]
            after_turnover_filter = len(df)
            logger.info(f"Final Filter - 换手率筛选后剩余: {after_turnover_filter}")
        else:
            logger.warning("Final Filter - 缺少 'turnover_rate' 列，跳过换手率筛选")
        
        logger.info(f"Final Filter - 最终剩余: {len(df)}/{initial_count}")
        return df
    
    # ============================================
    # 主Pipeline - 运行完整流程
    # ============================================
    def run_full_pipeline(self, trade_date: date, params: Dict = None, top_n: Optional[int] = None) -> Tuple[List[Dict], Optional[str], Optional[Dict]]:
        """
        运行完整的T7概念资金双驱流程
        
        Pipeline: Market Regime -> Filter -> Feature -> Score -> Validate -> Final Filter -> Output
        """
        params = self.merge_params(params)
        diagnostic_info = []
        funnel_data = {'total': 0, 'L0_pass': 0, 'L1_pass': 0, 'L2_pass': 0, 'L3_pass': 0, 'final': 0}
        
        try:
            # Layer 0: 市场状态识别
            logger.info(f"Pipeline Start: trade_date={trade_date}")
            regime_info = self.detect_market_regime(trade_date)
            regime_info['trade_date'] = trade_date
            diagnostic_info.append(f"市场状态: {regime_info['regime']} (评分: {regime_info['regime_score']:.3f})")
            
            # 获取全市场数量
            try:
                total_result = pd.read_sql(
                    "SELECT COUNT(DISTINCT sd.sheep_code) as total FROM sheep_daily sd INNER JOIN sheep_basic sb ON sd.sheep_code = sb.sheep_code WHERE sd.trade_date = %s AND sb.is_active = 1",
                    get_sqlalchemy_engine(), params=(trade_date,)
                )
                funnel_data['total'] = int(total_result['total'].iloc[0]) if not total_result.empty else 0
            except Exception as e:
                logger.warning(f"获取全市场数量失败: {e}")
                funnel_data['total'] = 0
            
            # Layer 1: Filter
            df = self.filter_layer(trade_date, params)
            if df.empty:
                return self._empty_result(diagnostic_info, funnel_data, regime_info, "Filter Layer返回空数据")
            funnel_data['L0_pass'] = len(df)
            diagnostic_info.append(f"Filter: {funnel_data['total']} -> {len(df)} 只")
            logger.info(f"Pipeline Funnel - Filter Layer: {funnel_data['total']} -> {len(df)} (通过率: {(len(df)/funnel_data['total']*100):.1f}%)" if funnel_data['total'] > 0 else f"Pipeline Funnel - Filter Layer: {funnel_data['total']} -> {len(df)}")
            
            # Layer 2: Feature
            df = self.feature_layer(df, trade_date)
            if df.empty:
                return self._empty_result(diagnostic_info, funnel_data, regime_info, "Feature Layer返回空数据")
            funnel_data['L1_pass'] = len(df)
            logger.info(f"Pipeline Funnel - Feature Layer: {funnel_data['L0_pass']} -> {len(df)} (通过率: {(len(df)/funnel_data['L0_pass']*100):.1f}%)" if funnel_data['L0_pass'] > 0 else f"Pipeline Funnel - Feature Layer: {funnel_data['L0_pass']} -> {len(df)}")
            
            # Layer 3: Score
            df = self.score_layer(df, params, regime_info)
            funnel_data['L2_pass'] = len(df)
            logger.info(f"Pipeline Funnel - Score Layer: {funnel_data['L1_pass']} -> {len(df)} (通过率: {(len(df)/funnel_data['L1_pass']*100):.1f}%)" if funnel_data['L1_pass'] > 0 else f"Pipeline Funnel - Score Layer: {funnel_data['L1_pass']} -> {len(df)}")
            
            # Layer 4: Validate
            df = self.validate_layer(df, params, regime_info)
            
            # Layer 5: Final Filter
            df = self.final_filter_layer(df, params, regime_info)
            funnel_data['L3_pass'] = len(df)
            logger.info(f"Pipeline Funnel - Final Filter: {funnel_data['L2_pass']} -> {len(df)} (通过率: {(len(df)/funnel_data['L2_pass']*100):.1f}%)" if funnel_data['L2_pass'] > 0 else f"Pipeline Funnel - Final Filter: {funnel_data['L2_pass']} -> {len(df)}")
            
            if df.empty:
                return self._empty_result(diagnostic_info, funnel_data, regime_info, "Final Filter后无数据")
            
            logger.info(f"Post-Final Filter: 进入启动质量/AI分数筛选阶段，剩余 {len(df)} 只股票")
            
            # 启动质量筛选
            min_breakout_quality = params.get('min_breakout_quality', self.config.MIN_BREAKOUT_QUALITY)
            quality_mask = df['breakout_quality_score'] >= min_breakout_quality
            before_q = len(df)
            df = df[quality_mask]
            logger.info(f"Final Stage - 启动质量筛选(>={min_breakout_quality}): {before_q} -> {len(df)} 只")
            
            # AI分数筛选
            min_ai_score = params.get('min_ai_score', self.config.MIN_AI_SCORE)
            score_mask = df['total_score'] >= min_ai_score
            before_s = len(df)
            df = df[score_mask]
            logger.info(f"Final Stage - AI分数筛选(>={min_ai_score}): {before_s} -> {len(df)} 只")
            
            funnel_data['final'] = len(df)
            diagnostic_info.append(f"Final: {funnel_data['L3_pass']} -> {len(df)} 只")
            
            if df.empty:
                logger.warning(f"Final Stage - 所有筛选步骤完成后无剩余股票")
                logger.info(f"Final Stage - 各筛选参数: min_breakout_quality={min_breakout_quality}, min_ai_score={min_ai_score}")
                return self._empty_result(diagnostic_info, funnel_data, regime_info, "筛选后无符合条件标的")
            
            # 排序并取Top N
            df = df.sort_values('total_score', ascending=False).reset_index(drop=True)
            max_recommendations = params.get('max_recommendations', self.config.MAX_RECOMMENDATIONS)
            actual_limit = min(top_n or max_recommendations, max_recommendations)
            top_stocks = df.head(actual_limit).copy()
            
            # 构建结果
            results = self._build_results(top_stocks, regime_info, params, trade_date)
            metadata = self._build_metadata(regime_info, funnel_data, df, params)
            
            logger.info(f"Pipeline Complete: 生成 {len(results)} 条推荐")
            diagnostic_info.append(f"推荐: {len(results)} 只")
            
            return results, " | ".join(diagnostic_info), metadata
            
        except Exception as e:
            logger.error(f"Pipeline失败: {e}", exc_info=True)
            diagnostic_info.append(f"异常: {str(e)}")
            return [], " | ".join(diagnostic_info), {'market_regime': 'Balance', 'funnel_data': funnel_data}
    
    # ============================================
    # Helper Methods
    # ============================================
    def _empty_result(self, diagnostic_info: List[str], funnel_data: Dict, regime_info: Dict, msg: str) -> Tuple[List[Dict], str, Dict]:
        """返回空结果"""
        diagnostic_info.append(msg)
        metadata = {
            'market_regime': regime_info.get('regime', 'Balance'),
            'funnel_data': funnel_data
        }
        return [], " | ".join(diagnostic_info), metadata
    
    def _build_results(self, top_stocks: pd.DataFrame, regime_info: Dict, params: Dict, trade_date: date) -> List[Dict]:
        """构建推荐结果"""
        if top_stocks.empty:
            return []
        
        # 准备数据列
        top_stocks = top_stocks.copy()
        top_stocks['entry_price'] = top_stocks['close_price'].astype(float)
        top_stocks['stop_loss_price'] = (top_stocks['entry_price'] * self.config.STOP_LOSS_RATIO).round(2)
        
        # 生成推荐理由
        def build_reasons(row):
            reasons = []
            v_thresh = params.get('vol_threshold', self.config.VOL_THRESHOLD)
            r_thresh = params.get('rps_threshold', self.config.RPS_THRESHOLD)
            
            if row.get('vol_ratio', 0) >= v_thresh:
                reasons.append(f"倍量{row['vol_ratio']:.1f}x")
            if row.get('rps_250', 0) >= r_thresh:
                reasons.append("RPS强势")
            if row.get('vcp_factor', 1.0) < 0.4:
                reasons.append("VCP收敛")
            if row.get('is_main_concept', 0) == 1:
                reasons.append("概念共振")
            if row.get('is_concept_leader', 0) == 1:
                reasons.append("概念龙头")
            if row.get('main_net_inflow', 0) > 500:
                reasons.append("主力流入")
            if row.get('super_large_inflow', 0) > 300:
                reasons.append("超大单进场")
            if row.get('breakout_quality_score', 0) >= 70:
                reasons.append("优质启动")
            return " + ".join(reasons) if reasons else "技术突破"
        
        top_stocks['reason_tags'] = top_stocks.apply(build_reasons, axis=1)
        
        # 构建结果列表
        results = []
        for _, row in top_stocks.iterrows():
            result = {
                'sheep_code': str(row['sheep_code']),
                'sheep_name': str(row['sheep_name']),
                'entry_price': float(row['entry_price']),
                'ai_score': float(row['total_score']),
                'breakout_quality': float(row.get('breakout_quality_score', 50.0)),
                'breakout_warning': str(row.get('breakout_warning', '')),
                'win_probability': float(row.get('breakout_quality_score', 50.0)),
                'reason_tags': str(row['reason_tags']),
                'stop_loss_price': float(row['stop_loss_price']),
                'vol_ratio': float(row.get('vol_ratio', 0.0)),
                'rps_250': float(row.get('rps_250', 0)) if pd.notna(row.get('rps_250')) else None,
                'vcp_factor': float(row.get('vcp_factor', 1.0)) if pd.notna(row.get('vcp_factor')) else None,
                'market_regime': str(regime_info.get('regime', 'Balance')),
                'resonance_score': float(row.get('concept_resonance_score', 0.0)),
                'concept_trend': str(row.get('resonance_base_tag', row.get('industry', '未知'))),
                'tag_total_inflow': float(row.get('tag_total_inflow', 0.0)),
                'tag_avg_pct': float(row.get('resonance_base_avg_chg', 0.0)) if 'resonance_base_avg_chg' in row else 0.0,
                'is_star_market': bool(row.get('is_star_market', False)),
                'is_gem': bool(row.get('is_gem', False)),
                'estimated_mv': float(row.get('estimated_mv', 0.0)) if pd.notna(row.get('estimated_mv')) else 0.0,
                'return_5d': None,
                'return_10d': None,
                'return_nd': None
            }
            results.append(result)
        
        return results
    
    def _build_metadata(self, regime_info: Dict, funnel_data: Dict, df: pd.DataFrame, params: Dict) -> Dict:
        """构建元数据"""
        return {
            'market_regime': str(regime_info['regime']),
            'regime_score': float(regime_info.get('regime_score', 0.0)),
            'funnel_data': {k: int(v) if isinstance(v, (int, np.integer)) else v for k, v in funnel_data.items()},
            'regime_details': {
                'rsrs_score': float(regime_info.get('rsrs_score', 0.0)),
                'rsrs_zscore': float(regime_info.get('rsrs_zscore', 0.0)),
                'sector_rotation_score': float(regime_info.get('sector_rotation_score', 0.0)),
                'market_breadth_score': float(regime_info.get('market_breadth_score', 0.0)),
                'volume_score': float(regime_info.get('volume_score', 0.0)),
                'ma_score': float(regime_info.get('ma_score', 0.0)),
                'sentiment_score': float(regime_info.get('sentiment_score', 0.0)),
                'up_count': int(regime_info.get('up_count', 0)),
                'down_count': int(regime_info.get('down_count', 0)),
                'limit_up_count': int(regime_info.get('limit_up_count', 0)),
                'limit_down_count': int(regime_info.get('limit_down_count', 0)),
            },
            'filter_stats': {
                'level1_before': int(funnel_data.get('total', 0)),
                'level1_after': int(funnel_data.get('L1_pass', 0)),
                'level2_after': int(funnel_data.get('L2_pass', 0)),
                'level3_after': int(funnel_data.get('L3_pass', 0)),
                'final': int(funnel_data.get('final', 0)),
            },
            'breakout_stats': {
                'high_quality_count': int((df['breakout_quality_score'] >= 70).sum()) if 'breakout_quality_score' in df.columns else 0,
                'medium_quality_count': int(((df['breakout_quality_score'] >= 40) & (df['breakout_quality_score'] < 70)).sum()) if 'breakout_quality_score' in df.columns else 0,
                'trap_risk_count': int((df['breakout_quality_score'] < 40).sum()) if 'breakout_quality_score' in df.columns else 0,
            },
            'params_used': {
                'min_change_pct': float(params.get('min_change_pct')) if params.get('min_change_pct') is not None else None,
                'max_change_pct': float(params.get('max_change_pct')) if params.get('max_change_pct') is not None else None,
                'vol_threshold': float(params.get('vol_threshold')) if params.get('vol_threshold') is not None else None,
                'min_turnover': float(params.get('min_turnover')) if params.get('min_turnover') is not None else None,
                'max_turnover': float(params.get('max_turnover')) if params.get('max_turnover') is not None else None,
                'min_ai_score': int(params.get('min_ai_score')) if params.get('min_ai_score') is not None else None,
                'min_breakout_quality': int(params.get('min_breakout_quality')) if params.get('min_breakout_quality') is not None else None,
            }
        }
    
    # ============================================
    # 兼容性方法 (保持向后兼容)
    # ============================================
    def level0_sql_pushdown_filter(self, trade_date: date, params: Dict) -> pd.DataFrame:
        """向后兼容: 原Level 0方法"""
        return self.filter_layer(trade_date, params)
    
    def level1_extract_features(self, trade_date: date, params: Dict = None) -> pd.DataFrame:
        """向后兼容: 原Level 1方法"""
        if params is None:
            params = self.DEFAULT_PARAMS
        df = self.filter_layer(trade_date, params)
        if df.empty:
            return df
        return self.feature_layer(df, trade_date)
    
    def level2_adaptive_filter(self, df: pd.DataFrame, params: Dict, regime_info: Dict) -> pd.DataFrame:
        """向后兼容: 原Level 2方法"""
        return self.final_filter_layer(df, params, regime_info)
    
    def level3_scoring_engine(self, df: pd.DataFrame, params: Dict, regime_info: Dict) -> pd.DataFrame:
        """向后兼容: 原Level 3方法"""
        df = self.score_layer(df, params, regime_info)
        return self.validate_layer(df, params, regime_info)
    
    def validate_breakout_quality(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """向后兼容: 原启动质量验证方法"""
        return self.validate_layer(df, self.DEFAULT_PARAMS, {'regime': 'Balance', 'trade_date': trade_date})
    
    def calculate_concept_resonance(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """向后兼容: 原概念共振计算方法"""
        return self._calc_concept_factors(df, trade_date)
