"""
实时热门板块推荐服务层
基于概念资金流、个股实时表现和综合指标的实时推荐
"""
from typing import List, Dict, Tuple
from datetime import date, datetime, timedelta
from sqlalchemy import text
import logging
from db.database import get_db
from services.concept_service import ConceptService
from services.sheep_service import SheepService
from etl.trade_date_adapter import TradeDateAdapter

logger = logging.getLogger(__name__)

class TrendingSectorService:
    """实时热门板块推荐服务"""
    
    @staticmethod
    def get_real_time_trending_sectors(limit: int = 10) -> List[Dict]:
        """
        获取实时热门板块推荐
        
        基于以下因素综合评估：
        1. 概念资金流入情况（权重最高）
        2. 概念内个股整体表现（涨幅、热度）
        3. 市场关注度（换手率、成交量）
        4. 稳健性指标（避免过度投机）
        
        Args:
            limit: 返回板块数量限制
            
        Returns:
            List[Dict]: 排序后的热门板块列表
        """
        try:
            # 获取最新交易日
            with get_db() as db:
                latest_date_query = text("SELECT MAX(trade_date) FROM sector_money_flow")
                latest_date_result = db.execute(latest_date_query)
                latest_date_row = latest_date_result.fetchone()
                latest_trade_date = latest_date_row[0] if latest_date_row and latest_date_row[0] else None
                
                if not latest_trade_date:
                    logger.warning("数据库中没有交易日期数据")
                    return []
            
            # 获取当日资金净流入前30的板块（用于后续分析）
            from services.concept_money_flow_service import ConceptMoneyFlowService
            top_inflow_sectors, _ = ConceptMoneyFlowService.get_top_concepts_by_inflow(days=1, limit=30)
            
            # 分析每个板块的综合表现
            trending_sectors = []
            for sector in top_inflow_sectors:
                sector_name = sector.get('sector_name', '')
                if not sector_name:
                    continue
                
                # 获取板块下的股票数据
                sector_stocks = ConceptService.get_sector_stocks_by_change_pct(sector_name, limit=20)
                
                # 计算板块综合评分
                sector_score = TrendingSectorService._calculate_sector_score(
                    sector, sector_stocks, latest_trade_date
                )
                
                if sector_score > 0:  # 只保留正向评分的板块
                    trending_sectors.append({
                        'sector_name': sector_name,
                        'inflow_amount': sector.get('main_net_inflow', 0),
                        'super_large_inflow': sector.get('super_large_inflow', 0),
                        'large_inflow': sector.get('large_inflow', 0),
                        'avg_change_pct': sector.get('avg_change_pct', 0),
                        'stock_count': len(sector_stocks),
                        'top_stocks': sector_stocks[:5],  # 前5只个股
                        'score': sector_score,
                        'trend_strength': TrendingSectorService._get_trend_strength(sector_score),
                        'recommendation_reason': TrendingSectorService._get_recommendation_reason(sector, sector_stocks)
                    })
            
            # 按综合评分排序
            trending_sectors.sort(key=lambda x: x['score'], reverse=True)
            
            logger.info(f"实时热门板块推荐完成，找到 {len(trending_sectors)} 个板块")
            return trending_sectors[:limit]
            
        except Exception as e:
            logger.error(f"获取实时热门板块推荐失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def _calculate_sector_score(sector: Dict, sector_stocks: List[Dict], latest_trade_date) -> float:
        """
        计算板块综合评分
        
        评分规则：
        - 资金流入权重：40%
        - 个股表现权重：30%
        - 市场活跃度权重：20%
        - 稳健性权重：10%
        """
        try:
            # 1. 资金流入评分 (40%)
            inflow_amount = sector.get('main_net_inflow', 0) or 0
            # 标准化处理（百万为单位，然后缩放到0-10分）
            inflow_score = min(abs(inflow_amount) / 100.0, 10.0)  # 100万流入=10分，封顶10分
            if inflow_amount < 0:
                inflow_score *= -0.5  # 资金流出给予负分但影响较小
            
            # 2. 个股表现评分 (30%)
            if sector_stocks:
                positive_count = sum(1 for stock in sector_stocks if stock.get('change_pct', 0) and stock['change_pct'] > 0)
                avg_change = sum(stock.get('change_pct', 0) or 0 for stock in sector_stocks) / len(sector_stocks)
                performance_score = min(abs(avg_change) * 2, 10.0)  # 平均涨幅*2，封顶10分
                if avg_change < 0:
                    performance_score *= -0.3  # 负涨幅给予负分但影响较小
                
                # 如果上涨个股比例高，额外加分
                positive_ratio = positive_count / len(sector_stocks)
                if positive_ratio > 0.6:
                    performance_score += positive_ratio * 2
            else:
                performance_score = 0
            
            # 3. 市场活跃度评分 (20%)
            # 使用板块内个股的平均换手率和成交量（如果有数据的话）
            # 暂时简化为基于资金流入规模的活跃度评分
            activity_score = min(abs(inflow_amount) / 200.0, 5.0)  # 基于资金流入，封顶5分
            
            # 4. 稳健性评分 (10%)
            # 避免过度投机，关注资金流入的质量
            if inflow_amount > 0 and sector_stocks:
                # 检查主力资金占比
                super_large_inflow = sector.get('super_large_inflow', 0) or 0
                large_inflow = sector.get('large_inflow', 0) or 0
                total_inflow = inflow_amount
                
                if total_inflow > 0:
                    institutional_ratio = (super_large_inflow + large_inflow) / total_inflow
                    stability_score = institutional_ratio * 5.0  # 机构资金占比*5，封顶5分
                else:
                    stability_score = 0
            else:
                stability_score = 0
            
            # 综合评分
            total_score = (
                inflow_score * 0.4 +
                performance_score * 0.3 +
                activity_score * 0.2 +
                stability_score * 0.1
            )
            
            return round(total_score, 2)
            
        except Exception as e:
            logger.error(f"计算板块评分失败: {e}", exc_info=True)
            return 0
    
    @staticmethod
    def _get_trend_strength(score: float) -> str:
        """根据评分获取趋势强度标签"""
        if score >= 7.0:
            return "强势"
        elif score >= 4.0:
            return "中等"
        elif score >= 1.0:
            return "温和"
        elif score <= -1.0:
            return "弱势"
        else:
            return "平淡"
    
    @staticmethod
    def _get_recommendation_reason(sector: Dict, sector_stocks: List[Dict]) -> str:
        """生成推荐理由"""
        try:
            inflow_amount = sector.get('main_net_inflow', 0) or 0
            super_large_inflow = sector.get('super_large_inflow', 0) or 0
            large_inflow = sector.get('large_inflow', 0) or 0
            
            reasons = []
            
            # 资金流入原因
            if inflow_amount > 0:
                reasons.append(f"资金净流入{inflow_amount:.2f}万元")
                
                # 分析资金结构
                if super_large_inflow + large_inflow > inflow_amount * 0.5:
                    reasons.append("机构资金主导流入")
                else:
                    reasons.append("散户资金参与较多")
            else:
                reasons.append(f"资金净流出{abs(inflow_amount):.2f}万元")
            
            # 个股表现原因
            if sector_stocks:
                positive_count = sum(1 for stock in sector_stocks if stock.get('change_pct', 0) and stock['change_pct'] > 0)
                avg_change = sum(stock.get('change_pct', 0) or 0 for stock in sector_stocks) / len(sector_stocks)
                
                if avg_change > 2.0:
                    reasons.append(f"平均涨幅{avg_change:.2f}%")
                elif avg_change > 0.5:
                    reasons.append(f"平均微涨{avg_change:.2f}%")
                elif avg_change < -2.0:
                    reasons.append(f"平均跌幅{-avg_change:.2f}%")
                
                if positive_count > len(sector_stocks) * 0.7:
                    reasons.append("多数个股上涨")
                elif positive_count < len(sector_stocks) * 0.3:
                    reasons.append("多数个股下跌")
            
            return "，".join(reasons) if reasons else "暂无明显特征"
            
        except Exception as e:
            logger.error(f"生成推荐理由失败: {e}", exc_info=True)
            return "系统分析中"