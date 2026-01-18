"""
NextDayPredictionService - 下个交易日预测服务

基于资金流向、热度数据的量化预测算法，分析下个交易日可能的热门概念方向和个股推荐。

核心算法：
1. 资金流向动量分析（最近3-5天净流入趋势）
2. 板块热度加速度（热度变化率）
3. 概念共振强度（多股联动）
4. 主力资金布局（大单净买入）
5. 技术形态共振（均线、突破）
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import date, datetime, timedelta
import logging
import json
from db.database import get_db, get_raw_connection
from sqlalchemy import text
from etl.trade_date_adapter import TradeDateAdapter
from etl.concept_filter import should_filter_concept
from db.virtual_board_repository import VirtualBoardRepository

logger = logging.getLogger(__name__)


class NextDayPredictionService:
    """
    下个交易日预测服务
    
    每半小时运行一次，分析最新数据，生成预测结果
    新交易日之前保持预测结果稳定
    """
    
    # 预测模型参数
    PARAMS = {
        # 资金流向权重
        'weight_money_flow': 0.35,
        # 热度权重
        'weight_hot_rank': 0.25,
        # 动量权重
        'weight_momentum': 0.20,
        # 共振强度权重
        'weight_resonance': 0.20,
        
        # 资金流向分析参数
        'money_flow_days': 3,           # 分析最近N天资金流向
        'min_sector_inflow': 5000,      # 板块最小净流入（万元）
        
        # 热度分析参数
        'hot_rank_weight_decay': 0.8,   # 热度时间衰减系数
        'min_hot_count': 3,             # 最小热门股数量
        
        # 动量分析参数
        'momentum_days': 5,             # 动量计算天数
        'min_momentum': 0.02,           # 最小动量阈值(2%)
        
        # 个股筛选参数
        'top_sectors': 5,               # 推荐板块数量
        'top_stocks_per_sector': 5,     # 每个板块推荐肥羊数（增加以确保去重后足够）
        'total_recommended_stocks': 10, # 总推荐肥羊数
        'min_stock_score': 50,          # 最低推荐分数（降低以增加候选）
    }
    
    @classmethod
    def generate_prediction(cls, force: bool = False) -> Dict:
        """
        生成下个交易日预测
        
        Args:
            force: 是否强制重新生成（忽略缓存）
            
        Returns:
            预测结果字典
        """
        try:
            # 获取当前日期信息
            today = date.today()
            current_time = datetime.now()
            
            # 判断是否需要生成新的预测
            # 规则：非交易日或收盘后（15:30后）保持稳定，不重新生成
            is_trading_day = TradeDateAdapter.is_trading_day(today)
            is_after_close = current_time.hour >= 15 and current_time.minute >= 30
            
            # 获取下一个交易日
            next_trading_day = TradeDateAdapter.get_next_trading_day(today)
            
            # 检查缓存
            if not force:
                cached = cls._get_cached_prediction(next_trading_day)
                if cached:
                    # 如果是收盘后或非交易日，直接返回缓存
                    if not is_trading_day or is_after_close:
                        logger.info(f"使用缓存的预测结果（非交易时段）: {next_trading_day}")
                        return cached
                    # 如果缓存时间在30分钟内，也返回缓存
                    cache_time = datetime.fromisoformat(cached.get('generated_at', '2000-01-01T00:00:00'))
                    if (current_time - cache_time).total_seconds() < 1800:
                        logger.info(f"使用缓存的预测结果（30分钟内）: {next_trading_day}")
                        return cached
            
            logger.info(f"开始生成 {next_trading_day} 的预测...")
            
            # 获取最新的有数据的交易日
            latest_trade_date = cls._get_latest_data_date()
            if not latest_trade_date:
                logger.warning("无法获取最新交易日期")
                return {'success': False, 'message': '无数据'}
            
            logger.info(f"使用交易日期 {latest_trade_date} 的数据进行预测")
            
            # 1. 分析板块资金流向
            sector_analysis = cls._analyze_sector_money_flow(latest_trade_date)
            
            # 2. 分析热度数据
            hot_analysis = cls._analyze_hot_rank(latest_trade_date)
            
            # 3. 计算板块综合得分
            sector_scores = cls._calculate_sector_scores(sector_analysis, hot_analysis)
            
            # 4. 生成板块预测
            sector_predictions = cls._generate_sector_predictions(sector_scores, sector_analysis, hot_analysis)
            
            # 5. 筛选推荐个股
            stock_recommendations = cls._recommend_stocks(sector_predictions, latest_trade_date)
            
            # 6. 生成预测描述
            prediction_description = cls._generate_prediction_description(
                sector_predictions, stock_recommendations, latest_trade_date
            )
            
            # 构建预测结果
            result = {
                'success': True,
                'target_date': next_trading_day.isoformat(),
                'data_date': latest_trade_date.isoformat(),
                'generated_at': current_time.isoformat(),
                'description': prediction_description,
                'sector_predictions': sector_predictions[:cls.PARAMS['top_sectors']],
                'stock_recommendations': stock_recommendations[:cls.PARAMS['total_recommended_stocks']],
                'analysis_summary': {
                    'top_sectors_count': len(sector_predictions),
                    'recommended_stocks_count': len(stock_recommendations),
                    'data_freshness': 'real-time' if is_trading_day and not is_after_close else 'post-market',
                }
            }
            
            # 保存到缓存
            cls._save_prediction_cache(next_trading_day, result)
            
            logger.info(f"预测生成完成: {len(sector_predictions)} 个板块, {len(stock_recommendations)} 只个股")
            return result
            
        except Exception as e:
            logger.error(f"生成预测失败: {e}", exc_info=True)
            return {'success': False, 'message': str(e)}
    
    @classmethod
    def _get_latest_data_date(cls) -> Optional[date]:
        """获取数据库中最新的有数据的交易日期"""
        try:
            with get_db() as db:
                result = db.execute(text("""
                    SELECT MAX(trade_date) as max_date
                    FROM sector_money_flow
                """))
                row = result.fetchone()
                if row and row[0]:
                    return row[0] if isinstance(row[0], date) else datetime.strptime(str(row[0]), '%Y-%m-%d').date()
        except Exception as e:
            logger.error(f"获取最新数据日期失败: {e}")
        return None
    
    @classmethod
    def _analyze_sector_money_flow(cls, trade_date: date) -> Dict[str, Dict]:
        """
        分析板块资金流向
        
        返回: {sector_name: {inflow_total, inflow_trend, inflow_acceleration, ...}}
        """
        try:
            days = cls.PARAMS['money_flow_days']
            
            with get_db() as db:
                # 获取最近N天的板块资金流向
                query = text("""
                    SELECT 
                        sector_name,
                        trade_date,
                        main_net_inflow,
                        super_large_inflow,
                        large_inflow
                    FROM sector_money_flow
                    WHERE trade_date <= :trade_date
                    ORDER BY trade_date DESC
                """)
                
                result = db.execute(query, {'trade_date': trade_date})
                rows = result.fetchall()
            
            if not rows:
                return {}
            
            # 按板块分组
            sector_data = {}
            for row in rows:
                sector_name = row[0]
                if sector_name not in sector_data:
                    sector_data[sector_name] = []
                sector_data[sector_name].append({
                    'trade_date': row[1],
                    'main_net_inflow': float(row[2]) if row[2] else 0,
                    'super_large_inflow': float(row[3]) if row[3] else 0,
                    'large_inflow': float(row[4]) if row[4] else 0,
                })
            
            # 计算每个板块的资金流向指标
            analysis = {}
            for sector_name, data_list in sector_data.items():
                # 只取最近N天
                recent_data = sorted(data_list, key=lambda x: x['trade_date'], reverse=True)[:days]
                
                if len(recent_data) < 2:
                    continue
                
                # 计算总流入
                total_inflow = sum(d['main_net_inflow'] for d in recent_data)
                
                # 计算流入趋势（线性回归斜率）
                inflows = [d['main_net_inflow'] for d in reversed(recent_data)]
                x = np.arange(len(inflows))
                if len(inflows) >= 2:
                    slope = np.polyfit(x, inflows, 1)[0]
                else:
                    slope = 0
                
                # 计算流入加速度（二阶导数近似）
                if len(inflows) >= 3:
                    acceleration = inflows[-1] - 2 * inflows[-2] + inflows[-3] if len(inflows) >= 3 else 0
                else:
                    acceleration = 0
                
                # 最近一天的流入
                latest_inflow = recent_data[0]['main_net_inflow']
                
                # 大单占比
                total_large = sum(d['super_large_inflow'] + d['large_inflow'] for d in recent_data)
                large_ratio = total_large / total_inflow if total_inflow > 0 else 0
                
                analysis[sector_name] = {
                    'inflow_total': total_inflow,
                    'inflow_trend': slope,
                    'inflow_acceleration': acceleration,
                    'latest_inflow': latest_inflow,
                    'large_ratio': large_ratio,
                    'data_days': len(recent_data),
                }
            
            return analysis
            
        except Exception as e:
            logger.error(f"分析板块资金流向失败: {e}", exc_info=True)
            return {}
    
    @classmethod
    def _analyze_hot_rank(cls, trade_date: date) -> Dict[str, Dict]:
        """
        分析热度榜数据
        
        返回: {sector_name: {hot_count, avg_rank, hot_score, ...}}
        """
        try:
            with get_db() as db:
                # 获取最新热度榜数据（优先从sheep_basic获取正确的名称）
                query = text("""
                    SELECT 
                        hr.sheep_code,
                        COALESCE(sb.sheep_name, hr.sheep_name) as sheep_name,
                        hr.`rank`,
                        hr.hot_score,
                        scm.concept_id,
                        ct.concept_name
                    FROM market_hot_rank hr
                    LEFT JOIN sheep_basic sb ON hr.sheep_code = sb.sheep_code AND sb.is_active = 1
                    LEFT JOIN sheep_concept_mapping scm ON hr.sheep_code = scm.sheep_code
                    LEFT JOIN concept_theme ct ON scm.concept_id = ct.concept_id AND ct.is_active = 1
                    WHERE hr.trade_date = :trade_date
                    ORDER BY hr.`rank` ASC
                    LIMIT 200
                """)
                
                result = db.execute(query, {'trade_date': trade_date})
                rows = result.fetchall()
            
            if not rows:
                return {}
            
            # 辅助函数：验证名称是否有效（不是代码格式）
            def get_valid_name(name, code):
                if not name or not name.strip():
                    return code
                name_clean = name.strip()
                # 如果是6位纯数字，认为是代码
                if len(name_clean) == 6 and name_clean.isdigit():
                    return code
                # 如果以SH或SZ开头后跟6位数字，也认为是代码
                if (name_clean.startswith('SH') or name_clean.startswith('SZ')) and len(name_clean) == 8 and name_clean[2:].isdigit():
                    return code
                return name_clean
            
            # 按概念/板块分组统计
            sector_hot = {}
            for row in rows:
                concept_name = row[5]
                if not concept_name:
                    continue
                
                if concept_name not in sector_hot:
                    sector_hot[concept_name] = {
                        'stocks': [],
                        'ranks': [],
                        'hot_scores': [],
                    }
                
                sheep_code = row[0]
                sheep_name = get_valid_name(row[1], sheep_code)
                
                sector_hot[concept_name]['stocks'].append({
                    'sheep_code': sheep_code,
                    'sheep_name': sheep_name,
                    'rank': row[2],
                    'hot_score': float(row[3]) if row[3] else 0,
                })
                sector_hot[concept_name]['ranks'].append(row[2])
                if row[3]:
                    sector_hot[concept_name]['hot_scores'].append(float(row[3]))
            
            # 计算每个板块的热度指标
            analysis = {}
            for sector_name, data in sector_hot.items():
                hot_count = len(data['stocks'])
                if hot_count < cls.PARAMS['min_hot_count']:
                    continue
                
                avg_rank = np.mean(data['ranks']) if data['ranks'] else 999
                avg_hot_score = np.mean(data['hot_scores']) if data['hot_scores'] else 0
                
                # 热度得分（考虑数量和排名）
                # 排名越靠前越好，数量越多越好
                hot_score = hot_count * 10 + (200 - avg_rank) * 0.5 + avg_hot_score * 0.1
                
                analysis[sector_name] = {
                    'hot_count': hot_count,
                    'avg_rank': avg_rank,
                    'avg_hot_score': avg_hot_score,
                    'hot_score': hot_score,
                    'top_stocks': sorted(data['stocks'], key=lambda x: x['rank'])[:5],
                }
            
            return analysis
            
        except Exception as e:
            logger.error(f"分析热度榜失败: {e}", exc_info=True)
            return {}
    
    @classmethod
    def _calculate_sector_scores(cls, sector_analysis: Dict, hot_analysis: Dict) -> List[Tuple[str, float, Dict]]:
        """
        计算板块综合得分
        
        Returns: [(sector_name, score, details), ...]
        """
        scores = []
        
        # 获取虚拟板块映射（source_concept -> [virtual_board, ...]）
        concept_to_virtual = VirtualBoardRepository.get_concept_to_virtual_boards()
        
        # 合并所有板块，并按虚拟板块聚合
        all_sectors = set(sector_analysis.keys()) | set(hot_analysis.keys())
        
        # 先聚合到虚拟板块
        virtual_sector_data = {}  # {virtual_name: {'money_list': [], 'hot_list': [], 'source_sectors': []}}
        
        for sector_name in all_sectors:
            # 过滤黑名单板块
            if should_filter_concept(sector_name):
                continue
            
            # 获取虚拟板块名（如果有映射则用映射，否则用原名）
            virtual_boards = concept_to_virtual.get(sector_name, [sector_name])
            # 取第一个虚拟板块（主映射）
            virtual_name = virtual_boards[0] if virtual_boards else sector_name
            
            # 虚拟板块也需要过滤黑名单
            if should_filter_concept(virtual_name):
                continue
            
            if virtual_name not in virtual_sector_data:
                virtual_sector_data[virtual_name] = {
                    'money_list': [],
                    'hot_list': [],
                    'source_sectors': [],
                }
            
            money_data = sector_analysis.get(sector_name, {})
            hot_data = hot_analysis.get(sector_name, {})
            
            if money_data:
                virtual_sector_data[virtual_name]['money_list'].append(money_data)
            if hot_data:
                virtual_sector_data[virtual_name]['hot_list'].append(hot_data)
            virtual_sector_data[virtual_name]['source_sectors'].append(sector_name)
        
        # 计算每个虚拟板块的综合得分
        for virtual_name, data in virtual_sector_data.items():
            # 聚合资金数据（取最大值或求和）
            money_data = {}
            if data['money_list']:
                money_data = {
                    'inflow_total': sum(m.get('inflow_total', 0) for m in data['money_list']),
                    'inflow_trend': max((m.get('inflow_trend', 0) for m in data['money_list']), default=0),
                    'inflow_acceleration': max((m.get('inflow_acceleration', 0) for m in data['money_list']), default=0),
                }
            
            # 聚合热度数据
            hot_data = {}
            if data['hot_list']:
                # 合并所有top_stocks
                all_top_stocks = []
                for h in data['hot_list']:
                    all_top_stocks.extend(h.get('top_stocks', []))
                # 按rank排序去重
                seen_codes = set()
                unique_stocks = []
                for stock in sorted(all_top_stocks, key=lambda x: x.get('rank', 999)):
                    if stock['sheep_code'] not in seen_codes:
                        seen_codes.add(stock['sheep_code'])
                        unique_stocks.append(stock)
                
                hot_data = {
                    'hot_count': sum(h.get('hot_count', 0) for h in data['hot_list']),
                    'avg_rank': min((h.get('avg_rank', 999) for h in data['hot_list']), default=999),
                    'hot_score': sum(h.get('hot_score', 0) for h in data['hot_list']),
                    'top_stocks': unique_stocks[:10],  # 保留更多肥羊
                }
            
            # 资金流向得分（0-100）
            money_score = 0
            if money_data:
                inflow = money_data.get('inflow_total', 0)
                trend = money_data.get('inflow_trend', 0)
                
                # 净流入得分（-50到+50）
                if inflow > 0:
                    money_score = min(50, inflow / 10000 * 10)  # 每亿元10分，最高50分
                else:
                    money_score = max(-50, inflow / 10000 * 5)  # 流出减分
                
                # 趋势加分（-20到+20）
                if trend > 0:
                    money_score += min(20, trend / 1000 * 5)
                else:
                    money_score += max(-20, trend / 1000 * 2)
                
                # 加速度加分（-10到+10）
                accel = money_data.get('inflow_acceleration', 0)
                if accel > 0:
                    money_score += min(10, accel / 1000 * 2)
                
                # 标准化到0-100
                money_score = max(0, min(100, money_score + 50))
            
            # 热度得分（0-100）
            hot_score = 0
            if hot_data:
                hot_score = min(100, hot_data.get('hot_score', 0))
            
            # 动量得分（简化：使用热度变化作为代理）
            momentum_score = hot_score * 0.8  # 简化处理
            
            # 共振强度（热门股数量）
            resonance_score = 0
            if hot_data:
                hot_count = hot_data.get('hot_count', 0)
                resonance_score = min(100, hot_count * 15)  # 每只热门股15分
            
            # 加权综合得分
            weights = cls.PARAMS
            total_score = (
                money_score * weights['weight_money_flow'] +
                hot_score * weights['weight_hot_rank'] +
                momentum_score * weights['weight_momentum'] +
                resonance_score * weights['weight_resonance']
            )
            
            details = {
                'money_score': round(money_score, 2),
                'hot_score': round(hot_score, 2),
                'momentum_score': round(momentum_score, 2),
                'resonance_score': round(resonance_score, 2),
                'money_data': money_data,
                'hot_data': hot_data,
                'source_sectors': data['source_sectors'],  # 记录原始板块来源
            }
            
            scores.append((virtual_name, total_score, details))
        
        # 按得分降序排序
        scores.sort(key=lambda x: x[1], reverse=True)
        
        return scores
    
    @classmethod
    def _generate_sector_predictions(cls, sector_scores: List, sector_analysis: Dict, hot_analysis: Dict) -> List[Dict]:
        """
        生成板块预测
        """
        predictions = []
        
        for sector_name, score, details in sector_scores[:10]:  # 取前10个
            money_data = details.get('money_data', {})
            hot_data = details.get('hot_data', {})
            
            # 生成预测理由
            reasons = []
            
            # 资金流向理由
            if money_data:
                inflow = money_data.get('inflow_total', 0)
                if inflow > 5000:
                    reasons.append(f"主力资金净流入{inflow/10000:.2f}亿")
                trend = money_data.get('inflow_trend', 0)
                if trend > 0:
                    reasons.append("资金流入呈上升趋势")
                accel = money_data.get('inflow_acceleration', 0)
                if accel > 0:
                    reasons.append("资金流入加速")
            
            # 热度理由
            if hot_data:
                hot_count = hot_data.get('hot_count', 0)
                if hot_count >= 5:
                    reasons.append(f"{hot_count}只个股进入热度榜")
                avg_rank = hot_data.get('avg_rank', 999)
                if avg_rank < 50:
                    reasons.append(f"平均热度排名第{int(avg_rank)}")
            
            if not reasons:
                reasons.append("综合技术面向好")
            
            prediction = {
                'sector_name': sector_name,
                'score': round(score, 2),
                'prediction_level': 'high' if score >= 70 else ('medium' if score >= 50 else 'low'),
                'reasons': reasons[:3],  # 最多3个理由
                'details': {
                    'money_score': details['money_score'],
                    'hot_score': details['hot_score'],
                    'hot_count': hot_data.get('hot_count', 0) if hot_data else 0,
                    'inflow_total': money_data.get('inflow_total', 0) if money_data else 0,
                },
                'top_stocks': hot_data.get('top_stocks', [])[:3] if hot_data else [],
            }
            
            predictions.append(prediction)
        
        return predictions
    
    @classmethod
    def _recommend_stocks(cls, sector_predictions: List[Dict], trade_date: date) -> List[Dict]:
        """
        从预测板块中筛选推荐个股
        
        算法：
        1. 从top板块中获取候选股
        2. 结合技术指标、资金流向筛选
        3. 综合评分排序
        """
        try:
            candidates = []
            
            # 从预测板块中收集候选股
            for pred in sector_predictions[:cls.PARAMS['top_sectors']]:
                sector_name = pred['sector_name']
                sector_score = pred['score']
                
                top_stocks = pred.get('top_stocks', [])
                for stock in top_stocks[:cls.PARAMS['top_stocks_per_sector']]:
                    candidates.append({
                        'sheep_code': stock['sheep_code'],
                        'sheep_name': stock['sheep_name'],
                        'sector_name': sector_name,
                        'sector_score': sector_score,
                        'hot_rank': stock.get('rank', 999),
                        'hot_score': stock.get('hot_score', 0),
                    })
            
            if not candidates:
                return []
            
            # 获取候选股的详细数据
            sheep_codes = [c['sheep_code'] for c in candidates]
            stock_details = cls._get_stock_details(sheep_codes, trade_date)
            
            # 计算每只肥羊的推荐得分
            recommendations = []
            for cand in candidates:
                sheep_code = cand['sheep_code']
                details = stock_details.get(sheep_code, {})
                
                # 使用详细数据中的名称更新候选股名称（确保使用正确的肥羊名称）
                sheep_name = cand['sheep_name']
                if details.get('sheep_name') and details['sheep_name'] != sheep_code:
                    sheep_name = details['sheep_name']
                
                # 基础分（板块得分）
                base_score = cand['sector_score'] * 0.4
                
                # 热度排名分（排名越靠前越高）
                rank_score = max(0, (200 - cand['hot_rank'])) * 0.2
                
                # 技术面分
                tech_score = 0
                if details:
                    # 涨幅适中（2%-8%最佳）
                    change_pct = details.get('change_pct', 0)
                    if 2 <= change_pct <= 8:
                        tech_score += 20
                    elif 0 < change_pct < 2:
                        tech_score += 10
                    
                    # 量比（1.5-3最佳）
                    volume_ratio = details.get('volume_ratio', 1)
                    if 1.5 <= volume_ratio <= 3:
                        tech_score += 15
                    elif volume_ratio > 1:
                        tech_score += 5
                    
                    # 资金流入
                    main_inflow = details.get('main_net_inflow', 0)
                    if main_inflow > 0:
                        tech_score += min(15, main_inflow / 1000 * 5)
                
                total_score = base_score + rank_score + tech_score
                
                if total_score >= cls.PARAMS['min_stock_score']:
                    # 生成推荐理由
                    reasons = []
                    reasons.append(f"所属板块【{cand['sector_name']}】资金活跃")
                    if cand['hot_rank'] <= 50:
                        reasons.append(f"热度排名第{cand['hot_rank']}")
                    if details.get('main_net_inflow', 0) > 0:
                        reasons.append(f"主力净流入{details.get('main_net_inflow', 0)/10000:.2f}亿")
                    
                    recommendations.append({
                        'sheep_code': sheep_code,
                        'sheep_name': sheep_name,  # 使用更新后的名称
                        'sector_name': cand['sector_name'],
                        'score': round(total_score, 2),
                        'hot_rank': cand['hot_rank'],
                        'reasons': reasons[:3],
                        'details': {
                            'change_pct': details.get('change_pct'),
                            'current_price': details.get('close_price'),
                            'main_net_inflow': details.get('main_net_inflow'),
                            'volume_ratio': details.get('volume_ratio'),
                        }
                    })
            
            # 按得分排序
            recommendations.sort(key=lambda x: x['score'], reverse=True)
            
            # 按sheep_code去重，保留得分最高的记录
            seen_codes = set()
            unique_recommendations = []
            for rec in recommendations:
                if rec['sheep_code'] not in seen_codes:
                    seen_codes.add(rec['sheep_code'])
                    unique_recommendations.append(rec)
            
            return unique_recommendations[:cls.PARAMS['total_recommended_stocks']]
            
        except Exception as e:
            logger.error(f"筛选推荐个股失败: {e}", exc_info=True)
            return []
    
    @classmethod
    def _get_stock_details(cls, sheep_codes: List[str], trade_date: date) -> Dict[str, Dict]:
        """获取肥羊详细数据（包含肥羊名称）"""
        if not sheep_codes:
            return {}
        
        try:
            with get_db() as db:
                # 构建参数化查询
                placeholders = ','.join([f':code_{i}' for i in range(len(sheep_codes))])
                params = {f'code_{i}': code for i, code in enumerate(sheep_codes)}
                params['trade_date'] = trade_date
                
                # 获取日K数据和肥羊名称
                query = text(f"""
                    SELECT 
                        sd.sheep_code,
                        sb.sheep_name,
                        sd.close_price,
                        sd.change_pct,
                        sd.volume,
                        sd.turnover_rate,
                        (SELECT AVG(volume) FROM sheep_daily sd2 
                         WHERE sd2.sheep_code = sd.sheep_code 
                         AND sd2.trade_date < :trade_date
                         ORDER BY sd2.trade_date DESC LIMIT 5) as avg_volume_5
                    FROM sheep_daily sd
                    LEFT JOIN sheep_basic sb ON sd.sheep_code = sb.sheep_code AND sb.is_active = 1
                    WHERE sd.sheep_code IN ({placeholders})
                    AND sd.trade_date = :trade_date
                """)
                
                result = db.execute(query, params)
                rows = result.fetchall()
                
                details = {}
                for row in rows:
                    sheep_code = row[0]
                    avg_volume = float(row[6]) if row[6] else 1
                    current_volume = float(row[4]) if row[4] else 0
                    volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
                    
                    details[sheep_code] = {
                        'sheep_name': row[1] if row[1] else sheep_code,
                        'close_price': float(row[2]) if row[2] else 0,
                        'change_pct': float(row[3]) if row[3] else 0,
                        'volume': current_volume,
                        'turnover_rate': float(row[5]) if row[5] else 0,
                        'volume_ratio': round(volume_ratio, 2),
                    }
                
                # 获取资金流向数据
                flow_query = text(f"""
                    SELECT sheep_code, main_net_inflow
                    FROM sheep_money_flow
                    WHERE sheep_code IN ({placeholders})
                    AND trade_date = :trade_date
                """)
                
                flow_result = db.execute(flow_query, params)
                for row in flow_result:
                    sheep_code = row[0]
                    if sheep_code in details:
                        details[sheep_code]['main_net_inflow'] = float(row[1]) if row[1] else 0
                
                return details
                
        except Exception as e:
            logger.error(f"获取肥羊详细数据失败: {e}", exc_info=True)
            return {}
    
    @classmethod
    def _generate_prediction_description(cls, sector_predictions: List, stock_recommendations: List, trade_date: date) -> str:
        """生成预测描述文本"""
        if not sector_predictions:
            return "暂无足够数据生成预测"
        
        desc_parts = []
        
        # 开头
        desc_parts.append(f"【明日热点预判】基于{trade_date}市场数据分析:")
        desc_parts.append("")
        
        # 板块预测
        top_sectors = sector_predictions[:3]
        if top_sectors:
            sector_names = [s['sector_name'] for s in top_sectors]
            desc_parts.append(f"📈 重点关注板块：{' / '.join(sector_names)}")
            desc_parts.append("")
            
            for i, pred in enumerate(top_sectors, 1):
                score = pred['score']
                level_emoji = "🔥" if score >= 70 else ("⭐" if score >= 50 else "💡")
                reasons = '，'.join(pred['reasons'][:2])
                desc_parts.append(f"{i}. {level_emoji} {pred['sector_name']}（评分{score:.0f}分）")
                desc_parts.append(f"   理由：{reasons}")
        
        # 个股推荐
        if stock_recommendations:
            desc_parts.append("")
            desc_parts.append(f"📊 精选个股（共{len(stock_recommendations)}只）：")
            for i, rec in enumerate(stock_recommendations[:5], 1):
                desc_parts.append(f"  {i}. {rec['sheep_name']}（{rec['sheep_code']}）- {rec['sector_name']}")
        
        # 风险提示
        desc_parts.append("")
        desc_parts.append("⚠️ 以上分析仅供参考，不构成投资建议，请注意风险控制。")
        
        return '\n'.join(desc_parts)
    
    @classmethod
    def _get_cached_prediction(cls, target_date: date) -> Optional[Dict]:
        """获取缓存的预测结果"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT prediction_data
                    FROM next_day_prediction_cache
                    WHERE target_date = :target_date
                    ORDER BY created_at DESC
                    LIMIT 1
                """)
                result = db.execute(query, {'target_date': target_date})
                row = result.fetchone()
                if row and row[0]:
                    return json.loads(row[0])
        except Exception as e:
            logger.warning(f"获取预测缓存失败: {e}")
        return None
    
    @classmethod
    def _save_prediction_cache(cls, target_date: date, prediction: Dict):
        """保存预测结果到缓存"""
        try:
            with get_db() as db:
                # 先删除旧的缓存
                db.execute(text("""
                    DELETE FROM next_day_prediction_cache
                    WHERE target_date = :target_date
                """), {'target_date': target_date})
                
                # 插入新的缓存
                db.execute(text("""
                    INSERT INTO next_day_prediction_cache (target_date, prediction_data, created_at)
                    VALUES (:target_date, :prediction_data, :created_at)
                """), {
                    'target_date': target_date,
                    'prediction_data': json.dumps(prediction, ensure_ascii=False, default=str),
                    'created_at': datetime.now(),
                })
                db.commit()
                logger.info(f"预测结果已缓存: {target_date}")
        except Exception as e:
            logger.error(f"保存预测缓存失败: {e}", exc_info=True)
    
    @classmethod
    def get_latest_prediction(cls) -> Optional[Dict]:
        """
        获取最新的预测结果（供API调用）
        
        如果缓存过期或不存在，会自动生成新的预测
        """
        return cls.generate_prediction(force=False)
