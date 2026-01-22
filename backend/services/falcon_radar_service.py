"""
猎鹰雷达服务层
实现基于多因子量化模型的推荐系统，包含三种高胜率策略：
1. 主线首阴 (Leader Pullback)
2. 资金背离 (Money Flow Divergence)
3. 平台突破 (Box Breakout)

Enhanced with T10 Model Integration:
- 极致缩量 (Extreme Volume Contraction) validation
- 板块向上 (Sector Bullish) filter
- RPS护盘 (RPS Protection) scoring
- 传统行业过滤 (Traditional Industry Exclusion)
"""
from typing import List, Dict, Optional
from datetime import date, timedelta
from db.sector_money_flow_repository import SectorMoneyFlowRepository
from db.money_flow_repository import MoneyFlowRepository
from db.sheep_repository import SheepRepository
from sqlalchemy import text
from db.database import get_db
from etl.concept_filter import should_filter_concept
import logging
import statistics

logger = logging.getLogger(__name__)

# Traditional Industries to Filter (传统行业过滤)
TRADITIONAL_INDUSTRIES = {
    '钢铁', '煤炭', '有色金属', '化工', '建材', '公用事业',
    '银行', '保险', '房地产', '基建', '水泥', '玻璃', '造纸',
    '纺织服装', '农业', '食品饮料', '传统零售', '酒店餐饮'
}

# T10 Model Thresholds
T10_VOL_RATIO_MAX = 0.6  # 极致缩量阈值
T10_MA_STATUS_BULLISH = 1  # 板块多头状态
T10_RPS_MIN = 70  # 最低RPS要求
T10_TURNOVER_OPTIMAL_MIN = 2.0
T10_TURNOVER_OPTIMAL_MAX = 8.0

class FalconRadarService:
    """猎鹰雷达服务"""
    
    @staticmethod
    def _is_traditional_industry(sector_name: str) -> bool:
        """判断是否为传统行业"""
        if not sector_name:
            return False
        return any(keyword in sector_name for keyword in TRADITIONAL_INDUSTRIES)
    
    @staticmethod
    def _calculate_t10_composite_score(sector_data: Dict) -> float:
        """
        Calculate T10-based composite score for sector
        Based on:
        - 资金流入强度 (Money Flow Strength) 40%
        - 板块RPS (Sector RPS) 30%
        - 涨停效应 (Limit Up Effect) 20%
        - 换手率健康度 (Turnover Health) 10%
        """
        try:
            # F1: 资金流入评分 (0-40)
            inflow = sector_data.get('main_net_inflow', 0) or 0
            f1_score = min(abs(inflow) / 100.0, 40.0) if inflow > 0 else 0
            
            # F2: RPS评分 (0-30)
            rps_20 = sector_data.get('sector_rps_20', 0) or 0
            if rps_20 >= 90:
                f2_score = 30
            elif rps_20 >= 80:
                f2_score = 25
            elif rps_20 >= 70:
                f2_score = 20
            elif rps_20 > 0:  # 有RPS数据但低于70
                f2_score = max(0, (rps_20 - 50) / 2)  # 50-70之间线性评分
            else:
                # TEMPORARY FIX: 如果没有RPS数据，基于资金流入给一个基础分数
                inflow = sector_data.get('main_net_inflow', 0) or 0
                if inflow > 500000:  # 50万以上
                    f2_score = 15
                elif inflow > 200000:  # 20万以上
                    f2_score = 10
                elif inflow > 100000:  # 10万以上
                    f2_score = 5
                else:
                    f2_score = 0
            
            # F3: 涨停效应评分 (0-20)
            limit_up_count = sector_data.get('limit_up_count', 0) or 0
            f3_score = min(limit_up_count * 3.0, 20.0)
            
            # F4: 换手率健康度 (0-10)
            avg_turnover = sector_data.get('avg_turnover', 0) or 0
            if T10_TURNOVER_OPTIMAL_MIN <= avg_turnover <= T10_TURNOVER_OPTIMAL_MAX:
                f4_score = 10
            elif 1.0 <= avg_turnover <= 15.0:
                f4_score = 5
            else:
                f4_score = 0
            
            # 综合评分
            total_score = f1_score + f2_score + f3_score + f4_score
            
            # 板块多头加成 (ma_status=1 bonus)
            if sector_data.get('sector_rps_20', 0) >= 90:
                total_score *= 1.1  # 10% bonus for strong sectors
            
            return round(total_score, 2)
            
        except Exception as e:
            logger.error(f"T10 composite score calculation failed: {e}")
            return 0.0
    
    @staticmethod
    def get_hottest_sectors(limit: int = 10) -> List[Dict]:
        """
        获取当日最热板块（基于客观数据）
        应用Jaccard聚类算法合并相似板块（如半导体、半导体概念、国产芯片等）
        
        Args:
            limit: 返回数量
            
        Returns:
            热门板块列表，包含资金流入、涨停数、封板率等指标
        """
        try:
            from services.dynamic_jaccard_clustering import DynamicJaccardClustering
            
            with get_db() as db:
                # 获取最新交易日
                date_query = text("SELECT MAX(trade_date) FROM sector_money_flow")
                date_result = db.execute(date_query)
                date_row = date_result.fetchone()
                if not date_row or not date_row[0]:
                    return []
                trade_date = date_row[0]
                
                # 获取最新交易日的板块数据（多取一些用于聚类）
                query = text("""
                    SELECT 
                        smf.sector_name,
                        smf.trade_date,
                        smf.main_net_inflow,
                        smf.super_large_inflow,
                        smf.large_inflow,
                        smf.change_pct,
                        smf.limit_up_count,
                        smf.sector_rps_20,
                        smf.sector_rps_50,
                        smf.avg_turnover
                    FROM sector_money_flow smf
                    WHERE smf.trade_date = :trade_date
                    ORDER BY smf.main_net_inflow DESC
                    LIMIT :limit
                """)
                
                result = db.execute(query, {'trade_date': trade_date, 'limit': limit * 3})  # 多取一些用于聚类
                raw_sectors = []
                
                for row in result:
                    sector_name = row[0]
                    
                    # 跳过需要过滤的概念
                    if should_filter_concept(sector_name):
                        continue
                        
                    limit_up_count = int(row[6]) if row[6] else 0
                    
                    # T10 Filter: Skip traditional industries
                    if FalconRadarService._is_traditional_industry(sector_name):
                        continue
                    
                    sector_data = {
                        'sector_name': sector_name,
                        'trade_date': trade_date.isoformat() if hasattr(trade_date, 'isoformat') else str(trade_date),
                        'main_net_inflow': float(row[2]) if row[2] else 0.0,
                        'super_large_inflow': float(row[3]) if row[3] else 0.0,
                        'large_inflow': float(row[4]) if row[4] else 0.0,
                        'change_pct': float(row[5]) if row[5] else 0.0,
                        'limit_up_count': limit_up_count,
                        'sector_rps_20': float(row[7]) if row[7] else 0.0,
                        'sector_rps_50': float(row[8]) if row[8] else 0.0,
                        'avg_turnover': float(row[9]) if row[9] else 0.0,
                    }
                    
                    # Calculate T10 composite score
                    sector_data['t10_score'] = FalconRadarService._calculate_t10_composite_score(sector_data)

                    # TEMPORARY FIX: Allow sectors without RPS data for now
                    # Only keep sectors with RPS >= 70 (T10 threshold) OR sectors with high inflow
                    if (sector_data.get('sector_rps_20') and sector_data['sector_rps_20'] >= T10_RPS_MIN) or \
                       sector_data.get('main_net_inflow', 0) > 100000:  # 10万以上资金流入的板块
                        raw_sectors.append(sector_data)
                
                # 如果没有符合RPS条件的板块，使用资金流入最多的板块作为备选
                if not raw_sectors:
                    logger.warning("没有找到符合RPS条件的板块，使用资金流入最多的板块作为备选")
                    # 获取资金流入最多的前limit个板块
                    top_inflow_query = text("""
                        SELECT
                            smf.sector_name,
                            smf.main_net_inflow,
                            smf.super_large_inflow,
                            smf.large_inflow,
                            smf.change_pct,
                            smf.limit_up_count,
                            smf.sector_rps_20,
                            smf.sector_rps_50,
                            smf.avg_turnover
                        FROM sector_money_flow smf
                        WHERE smf.trade_date = :trade_date
                        ORDER BY smf.main_net_inflow DESC
                        LIMIT :limit
                    """)

                    top_inflow_result = db.execute(top_inflow_query, {'trade_date': trade_date, 'limit': limit})
                    raw_sectors = []

                    for row in top_inflow_result:
                        sector_name = row[0]

                        # 跳过过滤的概念
                        if should_filter_concept(sector_name):
                            continue

                        # T10 Filter: Skip traditional industries
                        if FalconRadarService._is_traditional_industry(sector_name):
                            continue

                        sector_data = {
                            'sector_name': sector_name,
                            'trade_date': trade_date.isoformat() if hasattr(trade_date, 'isoformat') else str(trade_date),
                            'main_net_inflow': float(row[1]) if row[1] else 0.0,
                            'super_large_inflow': float(row[2]) if row[2] else 0.0,
                            'large_inflow': float(row[3]) if row[3] else 0.0,
                            'change_pct': float(row[4]) if row[4] else 0.0,
                            'limit_up_count': int(row[5]) if row[5] else 0,
                            'sector_rps_20': float(row[6]) if row[6] else 0.0,
                            'sector_rps_50': float(row[7]) if row[7] else 0.0,
                            'avg_turnover': float(row[8]) if row[8] else 0.0,
                        }

                        # Calculate T10 composite score (even if RPS is 0)
                        sector_data['t10_score'] = FalconRadarService._calculate_t10_composite_score(sector_data)
                        raw_sectors.append(sector_data)

                # 应用Jaccard聚类算法 (use T10 composite score)
                clustering = DynamicJaccardClustering()
                clustered_sectors = clustering.cluster_sectors(
                    raw_sectors,
                    trade_date if isinstance(trade_date, date) else date.fromisoformat(str(trade_date)),
                    score_key='t10_score'  # Use T10 composite score for clustering
                )

                # Sort by T10 score and return top N
                clustered_sectors = sorted(clustered_sectors, key=lambda x: x.get('t10_score', 0), reverse=True)

                logger.info(f"T10增强: 最终返回 {len(clustered_sectors)} 个板块")
                return clustered_sectors[:limit]
        except Exception as e:
            logger.error(f"获取当日最热板块失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def get_falcon_recommendations() -> Dict:
        """
        获取猎鹰推荐（基于三种策略）
        
        Returns:
            包含三种策略推荐结果的字典
        """
        try:
            # 策略A：主线首阴
            leader_pullback = FalconRadarService._get_leader_pullback_stocks()
            
            # 策略B：资金背离
            money_divergence = FalconRadarService._get_money_divergence_stocks()
            
            # 策略C：平台突破
            box_breakout = FalconRadarService._get_box_breakout_stocks()
            
            return {
                'leader_pullback': leader_pullback,
                'money_divergence': money_divergence,
                'box_breakout': box_breakout,
                'trade_date': date.today().isoformat()
            }
        except Exception as e:
            logger.error(f"获取猎鹰推荐失败: {e}", exc_info=True)
            return {
                'leader_pullback': [],
                'money_divergence': [],
                'box_breakout': [],
                'trade_date': date.today().isoformat()
            }
    
    @staticmethod
    def _get_leader_pullback_stocks(limit: int = 10) -> List[Dict]:
        """
        策略A：主线首阴 (Leader Pullback)
        逻辑：强势主线板块（RPS > 90） + 今日首次回调（跌幅 < -2%） + 缩量（量比 < 0.8）
        """
        try:
            with get_db() as db:
                # 获取RPS > 90的板块
                query = text("""
                    SELECT DISTINCT sector_name
                    FROM sector_money_flow
                    WHERE trade_date = (
                        SELECT MAX(trade_date) FROM sector_money_flow
                    )
                    AND sector_rps_20 >= 90
                """)
                
                result = db.execute(query)
                strong_sectors = [row[0] for row in result]
                
                if not strong_sectors:
                    return []
                
                # 获取这些板块下的个股，筛选首次回调且缩量的
                # 需要查询个股的日K数据和资金流数据
                recommendations = []
                
                # 简化实现：查询最近3天有回调的个股
                for sector_name in strong_sectors[:5]:  # 限制处理数量
                    # 获取板块下的个股（通过概念映射）
                    stocks_query = text("""
                        SELECT DISTINCT scm.sheep_code, sb.sheep_name
                        FROM sheep_concept_mapping scm
                        JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                        LEFT JOIN sheep_basic sb ON scm.sheep_code = sb.sheep_code
                        WHERE ct.concept_name = :sector_name
                        AND sb.is_active = 1
                        LIMIT 20
                    """)
                    
                    stocks_result = db.execute(stocks_query, {'sector_name': sector_name})
                    
                    for stock_row in stocks_result:
                        sheep_code = stock_row[0]
                        sheep_name = stock_row[1] or sheep_code
                        
                        # 获取最近3天的日K数据
                        daily_query = text("""
                            SELECT trade_date, change_pct, volume, ma10
                            FROM sheep_daily
                            WHERE sheep_code = :code
                            ORDER BY trade_date DESC
                            LIMIT 3
                        """)
                        
                        daily_result = db.execute(daily_query, {'code': sheep_code})
                        daily_data = list(daily_result)
                        
                        if len(daily_data) < 2:
                            continue
                        
                        # 检查今日是否首次回调
                        today_data = daily_data[0]
                        yesterday_data = daily_data[1] if len(daily_data) > 1 else None
                        
                        today_change = float(today_data[1]) if today_data[1] else 0.0
                        today_volume = int(today_data[2]) if today_data[2] else 0
                        ma10 = float(today_data[3]) if today_data[3] else None
                        
                        # 计算量比（今日成交量 / 5日均量）
                        volume_ratio = 1.0
                        if len(daily_data) >= 5:
                            avg_volume = sum(int(d[2]) if d[2] else 0 for d in daily_data[:5]) / 5
                            if avg_volume > 0:
                                volume_ratio = today_volume / avg_volume
                        
                        # T10 Enhanced: 极致缩量 + 首次回调
                        # Check: 跌幅 < -2% 且 量比 < 0.6 (T10 extreme contraction)
                        if today_change < -2.0 and volume_ratio < T10_VOL_RATIO_MAX:
                            recommendations.append({
                                'sheep_code': sheep_code,
                                'sheep_name': sheep_name,
                                'sector_name': sector_name,
                                'change_pct': today_change,
                                'volume_ratio': round(volume_ratio, 2),
                                'support_price': ma10,  # 支撑位（MA10）
                                'strategy': 'Leader Pullback',
                                'reason': f'主线良性分歧，T10极致缩量({volume_ratio:.2f})，低吸胜率 > 75%'
                            })
                
                return recommendations[:limit]
        except Exception as e:
            logger.error(f"获取主线首阴推荐失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def _get_money_divergence_stocks(limit: int = 10) -> List[Dict]:
        """
        策略B：资金背离 (Money Flow Divergence)
        逻辑：股价横盘震荡（振幅 < 3%） + 主力资金连续 3 日净流入 > 5000万
        """
        try:
            with get_db() as db:
                # 获取最近3天资金连续流入的个股
                money_query = text("""
                    SELECT
                        smf.sheep_code,
                        COALESCE(sb.sheep_name, smf.sheep_code) AS sheep_name,
                        SUM(smf.main_net_inflow) AS total_inflow
                    FROM sheep_money_flow smf
                    LEFT JOIN sheep_basic sb ON smf.sheep_code = sb.sheep_code
                    INNER JOIN (
                        SELECT DISTINCT trade_date
                        FROM sheep_money_flow
                        ORDER BY trade_date DESC
                        LIMIT 3
                    ) recent_dates ON smf.trade_date = recent_dates.trade_date
                    WHERE smf.main_net_inflow > 0
                    GROUP BY smf.sheep_code, sheep_name
                    HAVING COUNT(*) = 3 AND total_inflow > 5000
                    ORDER BY total_inflow DESC
                    LIMIT 50
                """)
                
                money_result = db.execute(money_query)
                candidates = []
                
                for row in money_result:
                    sheep_code = row[0]
                    sheep_name = row[1] or sheep_code
                    total_inflow = float(row[2])
                    
                    # 获取最近3天的日K数据，计算振幅
                    daily_query = text("""
                        SELECT trade_date, high_price, low_price, close_price, change_pct
                        FROM sheep_daily
                        WHERE sheep_code = :code
                        ORDER BY trade_date DESC
                        LIMIT 3
                    """)
                    
                    daily_result = db.execute(daily_query, {'code': sheep_code})
                    daily_data = list(daily_result)
                    
                    if len(daily_data) < 3:
                        continue
                    
                    # 计算3天内的最大振幅
                    max_amplitude = 0.0
                    for d in daily_data:
                        high = float(d[1]) if d[1] else 0.0
                        low = float(d[2]) if d[2] else 0.0
                        close = float(d[3]) if d[3] else 0.0
                        
                        if close > 0:
                            amplitude = abs((high - low) / close) * 100
                            max_amplitude = max(max_amplitude, amplitude)
                    
                    # 检查振幅是否 < 3%
                    if max_amplitude < 3.0:
                        # 获取最近3天的资金流数据
                        flow_query = text("""
                            SELECT trade_date, main_net_inflow
                            FROM sheep_money_flow
                            WHERE sheep_code = :code
                            ORDER BY trade_date DESC
                            LIMIT 3
                        """)
                        
                        flow_result = db.execute(flow_query, {'code': sheep_code})
                        flow_data = [
                            {
                                'trade_date': row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]),
                                'main_net_inflow': float(row[1]) if row[1] else 0.0
                            }
                            for row in flow_result
                        ]
                        
                        candidates.append({
                            'sheep_code': sheep_code,
                            'sheep_name': sheep_name,
                            'total_inflow': round(total_inflow / 10000, 2),  # 转换为亿元
                            'amplitude': round(max_amplitude, 2),
                            'flow_data': flow_data,
                            'strategy': 'Money Divergence',
                            'reason': '主力隐蔽建仓，量价背离，等待突破'
                        })
                
                return candidates[:limit]
        except Exception as e:
            logger.error(f"获取资金背离推荐失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def _get_box_breakout_stocks(limit: int = 10) -> List[Dict]:
        """
        策略C：平台突破 (Box Breakout)
        逻辑：VCP 形态收敛（波动率 < 5%） + 今日放量（量比 > 1.8）突破平台上轨
        """
        try:
            with get_db() as db:
                # 获取今日放量的个股（量比 > 1.8）
                volume_query = text("""
                    SELECT 
                        sd.sheep_code,
                        COALESCE(sb.sheep_name, sd.sheep_code) AS sheep_name,
                        sd.trade_date,
                        sd.volume,
                        sd.high_price,
                        sd.low_price,
                        sd.close_price,
                        sd.change_pct
                    FROM sheep_daily sd
                    LEFT JOIN sheep_basic sb ON sd.sheep_code = sb.sheep_code
                    WHERE sd.trade_date = (
                        SELECT MAX(trade_date) FROM sheep_daily
                    )
                    AND sb.is_active = 1
                    LIMIT 500
                """)
                
                volume_result = db.execute(volume_query)
                candidates = []
                
                for row in volume_result:
                    sheep_code = row[0]
                    sheep_name = row[1] or sheep_code
                    today_volume = int(row[3]) if row[3] else 0
                    today_high = float(row[4]) if row[4] else 0.0
                    today_low = float(row[5]) if row[5] else 0.0
                    today_close = float(row[6]) if row[6] else 0.0
                    today_change = float(row[7]) if row[7] else 0.0
                    
                    # 计算量比
                    avg_volume_query = text("""
                        SELECT AVG(volume) 
                        FROM sheep_daily
                        WHERE sheep_code = :code
                        AND trade_date < :today
                        ORDER BY trade_date DESC
                        LIMIT 5
                    """)
                    
                    avg_result = db.execute(avg_volume_query, {
                        'code': sheep_code,
                        'today': row[2]
                    })
                    avg_volume_row = avg_result.fetchone()
                    avg_volume = float(avg_volume_row[0]) if avg_volume_row and avg_volume_row[0] else 0.0
                    
                    if avg_volume == 0:
                        continue
                    
                    volume_ratio = today_volume / avg_volume
                    
                    # 检查是否放量（量比 > 1.8）
                    if volume_ratio < 1.8:
                        continue
                    
                    # 获取最近20天的数据，计算VCP波动率
                    vcp_query = text("""
                        SELECT high_price, low_price, close_price
                        FROM sheep_daily
                        WHERE sheep_code = :code
                        ORDER BY trade_date DESC
                        LIMIT 20
                    """)
                    
                    vcp_result = db.execute(vcp_query, {'code': sheep_code})
                    vcp_data = list(vcp_result)
                    
                    if len(vcp_data) < 10:
                        continue
                    
                    # 计算波动率（标准差 / 均值）
                    closes = [float(d[2]) for d in vcp_data if d[2]]
                    if not closes or len(closes) < 10:
                        continue
                    
                    mean_close = statistics.mean(closes)
                    if mean_close == 0:
                        continue
                    
                    std_close = statistics.stdev(closes) if len(closes) > 1 else 0.0
                    volatility = (std_close / mean_close) * 100
                    
                    # 检查波动率是否 < 5%
                    if volatility < 5.0:
                        # 计算平台上轨（最近20天的最高价）
                        highs = [float(d[0]) for d in vcp_data if d[0]]
                        if highs:
                            platform_top = max(highs)
                            
                            # 检查是否突破平台上轨
                            if today_close > platform_top * 0.98:  # 允许2%的误差
                                candidates.append({
                                    'sheep_code': sheep_code,
                                    'sheep_name': sheep_name,
                                    'volume_ratio': round(volume_ratio, 2),
                                    'volatility': round(volatility, 2),
                                    'platform_top': round(platform_top, 2),
                                    'change_pct': today_change,
                                    'strategy': 'Box Breakout',
                                    'reason': 'VCP 极致收敛，放量突破临界点，爆发力强'
                                })
                
                return candidates[:limit]
        except Exception as e:
            logger.error(f"获取平台突破推荐失败: {e}", exc_info=True)
            return []
