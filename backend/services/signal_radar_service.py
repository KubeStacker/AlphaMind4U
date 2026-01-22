"""
信号雷达服务层
用于检测主线确认信号和高潮预警信号，并提供实时看板数据

Enhanced with T10 Model & Hot Rank Integration:
- T10 validation for signal confirmation (volume contraction)
- Hot rank data integration for trend strength validation
- Traditional industry filtering for accuracy
- Multi-factor composite scoring for signal quality
"""
from typing import List, Dict, Optional
from datetime import date
from db.sector_money_flow_repository import SectorMoneyFlowRepository
from services.concept_money_flow_service import ConceptMoneyFlowService
from db.hot_rank_repository import HotRankRepository
import logging

logger = logging.getLogger(__name__)

# Traditional Industries to Filter
TRADITIONAL_INDUSTRIES = {
    '钢铁', '煤炭', '有色金属', '化工', '建材', '电力', '公用事业',
    '银行', '保险', '房地产', '基建', '水泥', '玻璃', '造纸',
    '纺织服装', '农业', '食品饮料', '传统零售', '酒店餐饮'
}

class SignalRadarService:
    """信号雷达服务"""
    
    # 信号阈值 (T10-Enhanced)
    NEW_CYCLE_RPS_THRESHOLD = 85  # 主线确认：RPS首次突破85 (降低阈值提前捕捉)
    NEW_CYCLE_LIMIT_UP_THRESHOLD = 3  # 主线确认：涨停家数>=3 (降低阈值增加灵敏度)
    CLIMAX_RPS_THRESHOLD = 95  # 高潮预警：RPS>95
    CLIMAX_CHANGE_PCT_THRESHOLD = 4.0  # 高潮预警：涨跌幅>4%
    
    # T10 Validation Thresholds
    T10_VOL_RATIO_CONFIRM = 0.8  # 主线确认时的缩量阈值确认
    T10_MONEY_FLOW_MIN = 5000  # 最小资金流入 (万元)
    
    @staticmethod
    def _is_traditional_industry(sector_name: str) -> bool:
        """判断是否为传统行业"""
        if not sector_name:
            return False
        return any(keyword in sector_name for keyword in TRADITIONAL_INDUSTRIES)
    
    @staticmethod
    def _validate_with_hot_rank(sector_name: str, trade_date: date) -> Dict:
        """
        Use hot rank data to validate sector strength
        
        Returns:
        - hot_rank_position: Position in hot rank (lower is better)
        - hot_rank_score: Composite score from hot rank data
        - is_trending: Whether the sector is trending in hot rank
        """
        try:
            # Get hot rank data
            hot_sheep = HotRankRepository.get_hot_sheep(source=None, limit=100)
            
            # Count how many stocks from this sector are in hot rank
            sector_hot_count = 0
            hot_positions = []
            
            for idx, sheep in enumerate(hot_sheep):
                sheep_concepts = sheep.get('concepts', [])
                if sector_name in sheep_concepts:
                    sector_hot_count += 1
                    hot_positions.append(idx + 1)
            
            # Calculate hot rank metrics
            if sector_hot_count > 0:
                avg_position = sum(hot_positions) / len(hot_positions)
                hot_rank_score = max(0, 100 - avg_position)  # Higher score for better position
                is_trending = sector_hot_count >= 3 and avg_position <= 50
            else:
                avg_position = None
                hot_rank_score = 0
                is_trending = False
            
            return {
                'hot_rank_position': avg_position,
                'hot_rank_score': hot_rank_score,
                'hot_stock_count': sector_hot_count,
                'is_trending': is_trending
            }
            
        except Exception as e:
            logger.error(f"Hot rank validation failed: {e}")
            return {
                'hot_rank_position': None,
                'hot_rank_score': 0,
                'hot_stock_count': 0,
                'is_trending': False
            }
    
    @staticmethod
    def get_signal_radar_data(trade_date: date = None) -> Dict:
        """
        获取信号雷达数据
        
        Args:
            trade_date: 交易日期，如果为None则使用今天
            
        Returns:
            包含信号雷达数据的字典
        """
        if trade_date is None:
            trade_date = date.today()
        
        # 获取热门板块（使用Jaccard聚类后的结果）
        # 减少limit以减少Jaccard聚类的输入数据量，从而加快响应速度
        sectors, metadata = ConceptMoneyFlowService.get_top_concepts_by_inflow(days=1, limit=15)  # Increased for better coverage
        
        # 简化信号检测：完全跳过历史数据查询，只基于当前数据判断
        # 这样可以大幅提升响应速度
        new_cycle_signals = []
        climax_signals = []
        hot_sectors_summary = []
        
        # Process all sectors with T10 validation
        for sector in sectors:
            sector_name = sector.get('sector_name', '')
            sector_rps_20 = sector.get('sector_rps_20', 0.0) or 0.0
            limit_up_count = sector.get('limit_up_count', 0) or 0
            change_pct = sector.get('change_pct', 0.0) or 0.0
            main_net_inflow = sector.get('main_net_inflow', 0.0) or 0.0
            
            # T10 Filter: Skip traditional industries
            if SignalRadarService._is_traditional_industry(sector_name):
                continue
                        
            # Validate with hot rank data
            hot_rank_data = SignalRadarService._validate_with_hot_rank(sector_name, trade_date)
                        
            # T10 Enhanced: 简化主线确认信号检测
            # Conditions:
            # 1. RPS >= 85 (lowered threshold for earlier detection)
            # 2. Limit up count >= 3 (lowered for more signals)
            # 3. Money flow > 5000万 (ensure real capital flow)
            # 4. Hot rank validation (sector has trending stocks)
            is_new_cycle = (
                sector_rps_20 >= SignalRadarService.NEW_CYCLE_RPS_THRESHOLD and 
                limit_up_count >= SignalRadarService.NEW_CYCLE_LIMIT_UP_THRESHOLD and
                main_net_inflow >= SignalRadarService.T10_MONEY_FLOW_MIN and
                hot_rank_data['is_trending']  # Must be trending in hot rank
            )
            
            # 检查高潮预警信号（RPS>95且涨跌幅>4%）
            is_climax = SignalRadarService._check_climax_signal(
                sector_rps_20, change_pct
            )
            
            # 构建板块摘要 (T10-Enhanced)
            sector_summary = {
                'sector_name': sector_name,
                'sector_rps_20': sector_rps_20,
                'sector_rps_50': sector.get('sector_rps_50', 0.0) or 0.0,
                'limit_up_count': limit_up_count,
                'change_pct': change_pct,
                'main_net_inflow': main_net_inflow,
                'signal': None,
                # T10 & Hot Rank Integration
                'hot_rank_score': hot_rank_data['hot_rank_score'],
                'hot_stock_count': hot_rank_data['hot_stock_count'],
                'is_hot_trending': hot_rank_data['is_trending'],
                't10_quality_score': SignalRadarService._calculate_t10_signal_quality(
                    sector_rps_20, limit_up_count, main_net_inflow, hot_rank_data
                )
            }
            
            if is_new_cycle:
                sector_summary['signal'] = 'New Cycle'
                new_cycle_signals.append(sector_summary)
            elif is_climax:
                sector_summary['signal'] = 'Climax Risk'
                climax_signals.append(sector_summary)
            
            hot_sectors_summary.append(sector_summary)
        
        # 获取最热板块 (T10-Enhanced: use quality score)
        hottest_sector = SignalRadarService._get_hottest_sector(hot_sectors_summary)
        
        # 暂时跳过推荐个股查询，避免超时（可以后续异步加载）
        recommended_stocks = []
        
        # Sort sectors by T10 quality score before returning
        hot_sectors_summary = sorted(
            hot_sectors_summary, 
            key=lambda x: x.get('t10_quality_score', 0), 
            reverse=True
        )
        
        return {
            'trade_date': trade_date.isoformat(),
            'new_cycle_signals': new_cycle_signals,
            'climax_signals': climax_signals,
            'hot_sectors_summary': hot_sectors_summary[:8],  # Top 8 by T10 quality
            'hottest_sector': hottest_sector,
            'recommended_stocks': recommended_stocks  # 暂时为空，避免超时
        }
    
    @staticmethod
    def _calculate_t10_signal_quality(rps: float, limit_up_count: int, money_flow: float, hot_rank_data: Dict) -> float:
        """
        Calculate T10-based signal quality score (0-100)
        
        Factors:
        - RPS strength (30%)
        - Limit up effect (25%)
        - Money flow strength (25%)
        - Hot rank validation (20%)
        """
        try:
            # F1: RPS strength (0-30)
            rps_score = min(rps / 100.0 * 30, 30)
            
            # F2: Limit up effect (0-25)
            limit_up_score = min(limit_up_count * 4.0, 25)
            
            # F3: Money flow strength (0-25)
            money_flow_score = min(money_flow / 10000.0 * 25, 25)
            
            # F4: Hot rank validation (0-20)
            hot_rank_score = hot_rank_data.get('hot_rank_score', 0) * 0.2
            
            total = rps_score + limit_up_score + money_flow_score + hot_rank_score
            return round(total, 2)
            
        except Exception as e:
            logger.error(f"T10 signal quality calculation failed: {e}")
            return 0.0
    
    @staticmethod
    def _check_climax_signal(sector_rps_20: float, change_pct: float) -> bool:
        """
        检查高潮预警信号：sector_rps_20>95且change_pct>4%
        
        Args:
            sector_rps_20: 20日RPS
            change_pct: 涨跌幅
            
        Returns:
            是否为高潮预警信号
        """
        return (sector_rps_20 > SignalRadarService.CLIMAX_RPS_THRESHOLD and 
                change_pct > SignalRadarService.CLIMAX_CHANGE_PCT_THRESHOLD)
    
    @staticmethod
    def _get_hottest_sector(sectors: List[Dict]) -> Optional[Dict]:
        """
        获取最热板块 (T10-Enhanced: use composite quality score)
        
        Args:
            sectors: 板块列表
            
        Returns:
            最热板块信息
        """
        if not sectors:
            return None
        
        # Use T10 quality score for ranking
        scored_sectors = sorted(
            sectors,
            key=lambda x: x.get('t10_quality_score', 0),
            reverse=True
        )
        
        if scored_sectors:
            return scored_sectors[0]
        return None
    
    @staticmethod
    def get_sector_rps_chart(sector_name: str, days: int = 60) -> Dict:
        """
        获取板块RPS走势图数据
        
        Args:
            sector_name: 板块名称
            days: 返回天数，默认60天
            
        Returns:
            包含RPS走势数据的字典
        """
        try:
            history = SectorMoneyFlowRepository.get_sector_money_flow(sector_name, limit=days)
            
            # 构建图表数据
            dates = []
            rps_20_data = []
            rps_50_data = []
            change_pct_data = []
            
            for record in history:
                dates.append(record['trade_date'].isoformat())
                rps_20_data.append(record.get('sector_rps_20', 0.0) or 0.0)
                rps_50_data.append(record.get('sector_rps_50', 0.0) or 0.0)
                change_pct_data.append(record.get('change_pct', 0.0) or 0.0)
            
            return {
                'sector_name': sector_name,
                'dates': dates,
                'rps_20': rps_20_data,
                'rps_50': rps_50_data,
                'change_pct': change_pct_data
            }
        except Exception as e:
            logger.error(f"获取RPS走势图数据失败（板块: {sector_name}）: {e}")
            return {
                'sector_name': sector_name,
                'dates': [],
                'rps_20': [],
                'rps_50': [],
                'change_pct': []
            }
