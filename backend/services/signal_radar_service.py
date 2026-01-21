"""
信号雷达服务层
用于检测主线确认信号和高潮预警信号，并提供实时看板数据
"""
from typing import List, Dict, Optional
from datetime import date
from db.sector_money_flow_repository import SectorMoneyFlowRepository
from services.concept_money_flow_service import ConceptMoneyFlowService
import logging

logger = logging.getLogger(__name__)

class SignalRadarService:
    """信号雷达服务"""
    
    # 信号阈值
    NEW_CYCLE_RPS_THRESHOLD = 90  # 主线确认：RPS首次突破90
    NEW_CYCLE_LIMIT_UP_THRESHOLD = 5  # 主线确认：涨停家数>=5
    CLIMAX_RPS_THRESHOLD = 95  # 高潮预警：RPS>95
    CLIMAX_CHANGE_PCT_THRESHOLD = 4.0  # 高潮预警：涨跌幅>4%
    
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
        sectors, metadata = ConceptMoneyFlowService.get_top_concepts_by_inflow(days=1, limit=10)
        
        # 简化信号检测：完全跳过历史数据查询，只基于当前数据判断
        # 这样可以大幅提升响应速度
        new_cycle_signals = []
        climax_signals = []
        hot_sectors_summary = []
        
        # 只处理前10个板块，进一步减少处理量
        for sector in sectors[:10]:
            sector_name = sector.get('sector_name', '')
            sector_rps_20 = sector.get('sector_rps_20', 0.0) or 0.0
            limit_up_count = sector.get('limit_up_count', 0) or 0
            change_pct = sector.get('change_pct', 0.0) or 0.0
            main_net_inflow = sector.get('main_net_inflow', 0.0) or 0.0
            
            # 简化主线确认信号检测：只检查当前数据，不查询历史（标记为"可能新周期"）
            # 如果RPS>=90且涨停>=5，就标记为可能的新周期信号
            is_new_cycle = (
                sector_rps_20 >= SignalRadarService.NEW_CYCLE_RPS_THRESHOLD and 
                limit_up_count >= SignalRadarService.NEW_CYCLE_LIMIT_UP_THRESHOLD
            )
            
            # 检查高潮预警信号（RPS>95且涨跌幅>4%）
            is_climax = SignalRadarService._check_climax_signal(
                sector_rps_20, change_pct
            )
            
            # 构建板块摘要
            sector_summary = {
                'sector_name': sector_name,
                'sector_rps_20': sector_rps_20,
                'sector_rps_50': sector.get('sector_rps_50', 0.0) or 0.0,
                'limit_up_count': limit_up_count,
                'change_pct': change_pct,
                'main_net_inflow': main_net_inflow,
                'signal': None
            }
            
            if is_new_cycle:
                sector_summary['signal'] = 'New Cycle'
                new_cycle_signals.append(sector_summary)
            elif is_climax:
                sector_summary['signal'] = 'Climax Risk'
                climax_signals.append(sector_summary)
            
            hot_sectors_summary.append(sector_summary)
        
        # 获取最热板块（RPS最高且资金流入最多）
        hottest_sector = SignalRadarService._get_hottest_sector(hot_sectors_summary)
        
        # 暂时跳过推荐个股查询，避免超时（可以后续异步加载）
        recommended_stocks = []
        
        return {
            'trade_date': trade_date.isoformat(),
            'new_cycle_signals': new_cycle_signals,
            'climax_signals': climax_signals,
            'hot_sectors_summary': hot_sectors_summary[:6],  # 只返回前6个热门板块
            'hottest_sector': hottest_sector,
            'recommended_stocks': recommended_stocks  # 暂时为空，避免超时
        }
    
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
        获取最热板块（综合考虑RPS和资金流入）
        
        Args:
            sectors: 板块列表
            
        Returns:
            最热板块信息
        """
        if not sectors:
            return None
        
        # 按综合热度排序：RPS权重70%，资金流入权重30%
        scored_sectors = []
        for sector in sectors:
            rps_score = sector.get('sector_rps_20', 0.0) or 0.0
            inflow_score = min(sector.get('main_net_inflow', 0.0) or 0.0 / 100000.0, 100.0)  # 归一化到0-100
            composite_score = rps_score * 0.7 + inflow_score * 0.3
            scored_sectors.append((composite_score, sector))
        
        # 按综合得分降序排序
        scored_sectors.sort(key=lambda x: x[0], reverse=True)
        
        if scored_sectors:
            return scored_sectors[0][1]
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
