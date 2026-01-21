"""
板块资金流向数据仓储层
"""
from typing import List, Dict, Optional, Tuple
from datetime import date, timedelta
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class SectorMoneyFlowRepository:
    """板块资金流向数据仓储"""
    
    @staticmethod
    def batch_upsert_sector_money_flow(data_list: List[Dict]):
        """批量插入或更新板块资金流向数据"""
        if not data_list:
            return
        
        # 为每条数据补充缺失的字段（如果不存在则设为None）
        normalized_data = []
        for item in data_list:
            normalized_item = {
                'sector_name': item.get('sector_name'),
                'trade_date': item.get('trade_date'),
                'main_net_inflow': item.get('main_net_inflow', 0.0),
                'super_large_inflow': item.get('super_large_inflow', 0.0),
                'large_inflow': item.get('large_inflow', 0.0),
                'medium_inflow': item.get('medium_inflow', 0.0),
                'small_inflow': item.get('small_inflow', 0.0),
                # 这些字段可能在初次采集时不存在，设为None（允许NULL）
                'change_pct': item.get('change_pct'),
                'avg_turnover': item.get('avg_turnover'),
                'limit_up_count': item.get('limit_up_count'),
                'top_weight_stocks': item.get('top_weight_stocks'),
            }
            normalized_data.append(normalized_item)
        
        with get_db() as db:
            query = text("""
                INSERT INTO sector_money_flow
                (sector_name, trade_date, main_net_inflow, super_large_inflow, 
                 large_inflow, medium_inflow, small_inflow, change_pct, avg_turnover,
                 limit_up_count, top_weight_stocks)
                VALUES 
                (:sector_name, :trade_date, :main_net_inflow, :super_large_inflow, 
                 :large_inflow, :medium_inflow, :small_inflow, :change_pct, :avg_turnover,
                 :limit_up_count, :top_weight_stocks)
                ON DUPLICATE KEY UPDATE
                    main_net_inflow = VALUES(main_net_inflow),
                    super_large_inflow = VALUES(super_large_inflow),
                    large_inflow = VALUES(large_inflow),
                    medium_inflow = VALUES(medium_inflow),
                    small_inflow = VALUES(small_inflow),
                    change_pct = COALESCE(VALUES(change_pct), change_pct),
                    avg_turnover = COALESCE(VALUES(avg_turnover), avg_turnover),
                    limit_up_count = COALESCE(VALUES(limit_up_count), limit_up_count),
                    top_weight_stocks = COALESCE(VALUES(top_weight_stocks), top_weight_stocks),
                    updated_at = CURRENT_TIMESTAMP
            """)
            
            batch_size = 500
            for i in range(0, len(normalized_data), batch_size):
                batch = normalized_data[i:i+batch_size]
                db.execute(query, batch)
                db.commit()
    
    @staticmethod
    def get_sector_money_flow(sector_name: str, limit: int = 90) -> List[Dict]:
        """
        获取板块资金流向数据（默认返回最新的数据）
        
        Args:
            sector_name: 板块名称
            limit: 返回的记录数，默认90条（3个月）
        """
        with get_db() as db:
            query = text("""
                SELECT trade_date, main_net_inflow, super_large_inflow, large_inflow,
                       medium_inflow, small_inflow, change_pct, avg_turnover,
                       limit_up_count, top_weight_stocks, sector_rps_20, sector_rps_50, ma_status
                FROM sector_money_flow
                WHERE sector_name = :sector_name
                ORDER BY trade_date DESC
                LIMIT :limit
            """)
            
            result = db.execute(query, {'sector_name': sector_name, 'limit': limit})
            data_list = []
            
            for row in result:
                trade_date = row[0]
                
                data_list.append({
                    'trade_date': trade_date,
                    'main_net_inflow': float(row[1]) if row[1] is not None else 0.0,
                    'super_large_inflow': float(row[2]) if row[2] is not None else 0.0,
                    'large_inflow': float(row[3]) if row[3] is not None else 0.0,
                    'medium_inflow': float(row[4]) if row[4] is not None else 0.0,
                    'small_inflow': float(row[5]) if row[5] is not None else 0.0,
                    'change_pct': float(row[6]) if row[6] is not None else 0.0,
                    'avg_turnover': float(row[7]) if row[7] is not None else 0.0,
                    'limit_up_count': int(row[8]) if row[8] is not None else 0,
                    'top_weight_stocks': row[9] if row[9] is not None else [],
                    'sector_rps_20': float(row[10]) if row[10] is not None else 0.0,
                    'sector_rps_50': float(row[11]) if row[11] is not None else 0.0,
                    'ma_status': int(row[12]) if row[12] is not None else 0,
                })
            
            # 按日期升序排序（用于图表显示）
            data_list.sort(key=lambda x: x['trade_date'])
            return data_list
    
    @staticmethod
    def get_top_sectors_by_inflow(days: int = 1, limit: int = 30) -> Tuple[List[Dict], Dict]:
        """
        获取资金净流入最多的板块（按净流入降序排列）
        
        Args:
            days: 统计天数（1=当日，3=最近3天，5=最近5天）
            limit: 返回数量，默认30
        """
        try:
            with get_db() as db:
                if days == 1:
                    # 当日 - 使用最新的交易日期
                    from datetime import date
                    today = date.today()
                    query = text("""
                        SELECT 
                            sector_name,
                            trade_date,
                            main_net_inflow,
                            super_large_inflow,
                            large_inflow,
                            change_pct,
                            limit_up_count,
                            sector_rps_20,
                            sector_rps_50,
                            ma_status
                        FROM sector_money_flow
                        WHERE trade_date = (
                            SELECT MAX(trade_date) FROM sector_money_flow
                        )
                        ORDER BY main_net_inflow DESC
                        LIMIT :limit
                    """)
                    result = db.execute(query, {'limit': limit})
                    
                    sectors = []
                    for row in result:
                        sectors.append({
                            'sector_name': row[0],
                            'trade_date': row[1],
                            'main_net_inflow': float(row[2]) if row[2] is not None else 0.0,
                            'super_large_inflow': float(row[3]) if row[3] is not None else 0.0,
                            'large_inflow': float(row[4]) if row[4] is not None else 0.0,
                            'change_pct': float(row[5]) if row[5] is not None else 0.0,
                            'limit_up_count': int(row[6]) if row[6] is not None else 0,
                            'sector_rps_20': float(row[7]) if row[7] is not None else 0.0,
                            'sector_rps_50': float(row[8]) if row[8] is not None else 0.0,
                            'ma_status': int(row[9]) if row[9] is not None else 0,
                        })
                    # 确保按净流入降序排列（最多的在前）
                    sectors.sort(key=lambda x: x['main_net_inflow'], reverse=True)
                    
                    # 返回诊断信息
                    metadata = {
                        'total_days_in_db': 1,  # 当日查询不需要检查总天数
                        'actual_days_used': 1,
                        'requested_days': days,
                        'has_sufficient_data': True
                    }
                    return sectors, metadata
                else:
                    # 最近N天累计，使用交易日而不是日历天数
                    # 先获取最近N个交易日的日期列表
                    from etl.trade_date_adapter import TradeDateAdapter
                    from datetime import date, timedelta
                    
                    # 获取最新的交易日期
                    max_date_query = text("SELECT MAX(trade_date) FROM sector_money_flow")
                    max_date_result = db.execute(max_date_query)
                    max_date_row = max_date_result.fetchone()
                    if not max_date_row or not max_date_row[0]:
                        logger.warning("无法获取最新的交易日期")
                        metadata = {
                            'total_days_in_db': 0,
                            'actual_days_used': 0,
                            'requested_days': days,
                            'has_sufficient_data': False,
                            'warning': '数据库中没有任何板块资金流数据'
                        }
                        return [], metadata
                    
                    latest_trade_date = max_date_row[0]
                    if isinstance(latest_trade_date, str):
                        from datetime import datetime
                        latest_trade_date = datetime.strptime(latest_trade_date, '%Y-%m-%d').date()
                    
                    logger.info(f"查询最近{days}天的板块资金流数据，最新交易日期: {latest_trade_date}")
                    
                    # 先检查数据库中有多少天的数据
                    count_query = text("SELECT COUNT(DISTINCT trade_date) FROM sector_money_flow")
                    count_result = db.execute(count_query)
                    total_days = count_result.scalar()
                    logger.info(f"数据库中总共有 {total_days} 个不同的交易日期")
                    
                    # 获取最近N个交易日
                    # 从数据库中找到最近N个不同的交易日期
                    trade_dates_query = text("""
                        SELECT DISTINCT trade_date 
                        FROM sector_money_flow 
                        WHERE trade_date <= :max_date
                        ORDER BY trade_date DESC 
                        LIMIT :days
                    """)
                    trade_dates_result = db.execute(trade_dates_query, {'max_date': latest_trade_date, 'days': days})
                    trade_dates = [row[0] for row in trade_dates_result]
                    
                    if not trade_dates:
                        logger.warning(f"无法获取最近{days}个交易日的数据")
                        metadata = {
                            'total_days_in_db': total_days,
                            'actual_days_used': 0,
                            'requested_days': days,
                            'has_sufficient_data': False,
                            'warning': f'无法获取最近{days}个交易日的数据'
                        }
                        return [], metadata
                    
                    # 如果交易日数量不足，使用所有可用的交易日
                    actual_days = len(trade_dates)
                    logger.debug(f"实际获取到 {actual_days} 个交易日: {trade_dates}")
                    
                    if actual_days < days:
                        logger.warning(f"⚠️ 数据库中只有 {actual_days} 天的数据，少于请求的 {days} 天。累计数据可能不准确！")
                    
                    # 如果只有1天的数据，直接返回当日数据（避免累计计算错误）
                    if actual_days == 1:
                        logger.warning(f"⚠️ 数据库中只有1天的数据，返回当日数据而不是累计数据")
                        # 返回当日数据（与days=1的逻辑相同）
                        query = text("""
                            SELECT 
                                sector_name,
                                trade_date,
                                main_net_inflow,
                                super_large_inflow,
                                large_inflow,
                                change_pct,
                                limit_up_count,
                                sector_rps_20,
                                sector_rps_50,
                                ma_status
                            FROM sector_money_flow
                            WHERE trade_date = :trade_date
                            ORDER BY main_net_inflow DESC
                            LIMIT :limit
                        """)
                        result = db.execute(query, {'trade_date': trade_dates[0], 'limit': limit})
                        
                        sectors = []
                        for row in result:
                            sectors.append({
                                'sector_name': row[0],
                                'trade_date': row[1],
                                'main_net_inflow': float(row[2]) if row[2] is not None else 0.0,
                                'super_large_inflow': float(row[3]) if row[3] is not None else 0.0,
                                'large_inflow': float(row[4]) if row[4] is not None else 0.0,
                                'change_pct': float(row[5]) if row[5] is not None else 0.0,
                                'limit_up_count': int(row[6]) if row[6] is not None else 0,
                                'sector_rps_20': float(row[7]) if row[7] is not None else 0.0,
                                'sector_rps_50': float(row[8]) if row[8] is not None else 0.0,
                                'ma_status': int(row[9]) if row[9] is not None else 0,
                                'total_inflow': float(row[2]) if row[2] is not None else 0.0,  # 当日数据作为累计数据
                                'total_super_large': float(row[3]) if row[3] is not None else 0.0,
                                'total_large': float(row[4]) if row[4] is not None else 0.0,
                                'daily_data': [{
                                    'trade_date': row[1].strftime('%Y-%m-%d') if hasattr(row[1], 'strftime') else str(row[1]),
                                    'main_net_inflow': float(row[2]) if row[2] is not None else 0.0,
                                    'super_large_inflow': float(row[3]) if row[3] is not None else 0.0,
                                    'large_inflow': float(row[4]) if row[4] is not None else 0.0,
                                    'change_pct': float(row[5]) if row[5] is not None else 0.0,
                                    'limit_up_count': int(row[6]) if row[6] is not None else 0,
                                    'sector_rps_20': float(row[7]) if row[7] is not None else 0.0,
                                    'sector_rps_50': float(row[8]) if row[8] is not None else 0.0,
                                    'ma_status': int(row[9]) if row[9] is not None else 0,
                                }]
                            })
                        sectors.sort(key=lambda x: x['main_net_inflow'], reverse=True)
                        
                        # 返回诊断信息
                        metadata = {
                            'total_days_in_db': total_days,
                            'actual_days_used': 1,
                            'requested_days': days,
                            'has_sufficient_data': False,
                            'warning': f'数据库中只有1天的数据，返回当日数据而不是{days}天累计数据'
                        }
                        return sectors, metadata
                    
                    # 获取这些交易日范围内的累计排名TOP的板块
                    min_date = min(trade_dates)
                    logger.info(f"查询日期范围: {min_date} 到 {latest_trade_date}，共 {actual_days} 天")
                    query = text("""
                    SELECT 
                        sector_name,
                        SUM(main_net_inflow) AS total_inflow,
                        SUM(super_large_inflow) AS total_super_large,
                        SUM(large_inflow) AS total_large,
                        MAX(trade_date) AS latest_date
                    FROM sector_money_flow
                    WHERE trade_date >= :min_date
                      AND trade_date <= :max_date
                    GROUP BY sector_name
                    ORDER BY total_inflow DESC
                    LIMIT :limit
                    """)
                    result = db.execute(query, {'min_date': min_date, 'max_date': latest_trade_date, 'limit': limit})
                    
                    sectors = []
                    sector_names = []
                    for row in result:
                        sector_name = row[0]
                        sector_names.append(sector_name)
                        total_inflow = float(row[1]) if row[1] is not None else 0.0
                        sectors.append({
                            'sector_name': sector_name,
                            'total_inflow': total_inflow,
                            'total_super_large': float(row[2]) if row[2] is not None else 0.0,
                            'total_large': float(row[3]) if row[3] is not None else 0.0,
                            'latest_date': row[4],
                            'daily_data': []  # 每日详细数据
                        })
                        logger.debug(f"板块 {sector_name}: 累计净流入 {total_inflow:.2f} 万元")
                    
                    logger.info(f"找到 {len(sectors)} 个板块，累计净流入范围: {min(s['total_inflow'] for s in sectors):.2f} ~ {max(s['total_inflow'] for s in sectors):.2f} 万元")
                    
                    # 确保按总净流入降序排列（最多的在前）
                    sectors.sort(key=lambda x: x['total_inflow'], reverse=True)
                    
                    # 获取这些板块的每日详细数据
                    if sector_names:
                        # 使用正确的IN子句语法
                        from sqlalchemy import bindparam
                        # 构建参数化查询
                        placeholders = ','.join([f':name{i}' for i in range(len(sector_names))])
                        daily_query_str = f"""
                        SELECT 
                            sector_name,
                            trade_date,
                            main_net_inflow,
                            super_large_inflow,
                            large_inflow
                        FROM sector_money_flow
                        WHERE sector_name IN ({placeholders})
                        AND trade_date >= :min_date
                        AND trade_date <= :max_date
                        ORDER BY sector_name, trade_date ASC
                        """
                        daily_query = text(daily_query_str)
                        # 构建参数字典
                        params = {f'name{i}': name for i, name in enumerate(sector_names)}
                        params['min_date'] = min_date
                        params['max_date'] = latest_trade_date
                        try:
                            daily_result = db.execute(daily_query, params)
                        except Exception as e:
                            logger.warning(f"参数化查询失败，使用降级方案: {e}")
                            # 降级方案：使用转义的字符串（已转义单引号防止SQL注入）
                            escaped_names = [name.replace("'", "''") for name in sector_names]
                            names_list = "', '".join(escaped_names)
                            daily_query_fallback = text(f"""
                                SELECT 
                                    sector_name,
                                    trade_date,
                                    main_net_inflow,
                                    super_large_inflow,
                                    large_inflow
                                FROM sector_money_flow
                                WHERE sector_name IN ('{names_list}')
                                AND trade_date >= :min_date
                                AND trade_date <= :max_date
                                ORDER BY sector_name, trade_date ASC
                            """)
                            daily_result = db.execute(daily_query_fallback, {'min_date': min_date, 'max_date': latest_trade_date})
                        
                        # 按板块分组每日数据
                        daily_data_map: Dict[str, List[Dict]] = {}
                        for row in daily_result:
                            sector_name = row[0]
                            if sector_name not in daily_data_map:
                                daily_data_map[sector_name] = []
                            daily_data_map[sector_name].append({
                                'trade_date': row[1].strftime('%Y-%m-%d') if hasattr(row[1], 'strftime') else str(row[1]),
                                'main_net_inflow': float(row[2]) if row[2] is not None else 0.0,
                                'super_large_inflow': float(row[3]) if row[3] is not None else 0.0,
                                'large_inflow': float(row[4]) if row[4] is not None else 0.0,
                            })
                        
                        # 将每日数据添加到对应的板块
                        for sector in sectors:
                            daily_data = daily_data_map.get(sector['sector_name'], [])
                            sector['daily_data'] = daily_data
                            logger.debug(f"板块 {sector['sector_name']}: 每日数据 {len(daily_data)} 条")
                            if len(daily_data) < actual_days:
                                logger.warning(f"⚠️ 板块 {sector['sector_name']} 只有 {len(daily_data)} 天的数据，少于 {actual_days} 天")
                    
                    logger.info(f"返回 {len(sectors)} 个板块的累计数据（{actual_days} 天）")
                    
                    # 返回诊断信息
                    metadata = {
                        'total_days_in_db': total_days,
                        'actual_days_used': actual_days,
                        'requested_days': days,
                        'has_sufficient_data': actual_days >= days,
                        'warning': None if actual_days >= days else f'数据库中只有{actual_days}天的数据，少于请求的{days}天'
                    }
                    return sectors, metadata
        except Exception as e:
            logger.error(f"获取板块资金流入推荐失败: {e}", exc_info=True)
            # 返回空列表而不是抛出异常，避免前端网络错误
            return []

    @staticmethod
    def get_sector_performance_history(sector_name: str, days: int = 60) -> List[Dict]:
        """
        获取板块历史表现数据（用于RPS计算）
        """
        with get_db() as db:
            query = text("""
                SELECT 
                    trade_date, 
                    change_pct
                FROM sector_money_flow
                WHERE sector_name = :sector_name
                  AND change_pct IS NOT NULL
                  AND trade_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                ORDER BY trade_date ASC
            """)
            
            result = db.execute(query, {
                'sector_name': sector_name,
                'days': days
            })
            
            history = []
            for row in result:
                history.append({
                    'trade_date': row[0],
                    'change_pct': float(row[1]) if row[1] is not None else 0.0
                })
            
            return history

    @staticmethod
    def update_sector_rps_scores(sector_name: str, trade_date: date, rps_20: float, rps_50: float) -> bool:
        """
        更新板块RPS分数
        """
        with get_db() as db:
            query = text("""
                UPDATE sector_money_flow 
                SET sector_rps_20 = :rps_20, 
                    sector_rps_50 = :rps_50,
                    updated_at = CURRENT_TIMESTAMP
                WHERE sector_name = :sector_name 
                  AND trade_date = :trade_date
            """)
            
            result = db.execute(query, {
                'sector_name': sector_name,
                'trade_date': trade_date,
                'rps_20': rps_20,
                'rps_50': rps_50
            })
            
            db.commit()
            return result.rowcount > 0

    @staticmethod
    def update_sector_ma_status(sector_name: str, trade_date: date, ma_status: int) -> bool:
        """
        更新板块均线状态
        """
        with get_db() as db:
            query = text("""
                UPDATE sector_money_flow 
                SET ma_status = :ma_status,
                    updated_at = CURRENT_TIMESTAMP
                WHERE sector_name = :sector_name 
                  AND trade_date = :trade_date
            """)
            
            result = db.execute(query, {
                'sector_name': sector_name,
                'trade_date': trade_date,
                'ma_status': ma_status
            })
            
            db.commit()
            return result.rowcount > 0

    @staticmethod
    def get_records_with_missing_fields(trade_date: date) -> List[Dict]:
        """
        获取指定日期存在缺失字段的记录
        
        新增字段包括：
        - change_pct: 板块指数加权涨跌幅
        - avg_turnover: 平均换手率
        - limit_up_count: 涨停家数
        - top_weight_stocks: 前5大权重股代码（JSON）
        - sector_rps_20: 20日相对强度
        - sector_rps_50: 50日相对强度
        - ma_status: 均线状态
        
        Returns:
            List[Dict]: 包含缺失字段信息的记录列表
        """
        with get_db() as db:
            query = text("""
                SELECT 
                    sector_name,
                    trade_date,
                    change_pct,
                    avg_turnover,
                    limit_up_count,
                    top_weight_stocks,
                    sector_rps_20,
                    sector_rps_50,
                    ma_status
                FROM sector_money_flow
                WHERE trade_date = :trade_date
                  AND (
                      change_pct IS NULL
                      OR avg_turnover IS NULL
                      OR limit_up_count IS NULL
                      OR top_weight_stocks IS NULL
                      OR sector_rps_20 IS NULL
                      OR sector_rps_50 IS NULL
                      OR ma_status IS NULL
                  )
            """)
            
            result = db.execute(query, {'trade_date': trade_date})
            
            records = []
            for row in result:
                missing_fields = []
                
                # 检查每个新增字段是否缺失
                if row[2] is None:  # change_pct
                    missing_fields.append('change_pct')
                if row[3] is None:  # avg_turnover
                    missing_fields.append('avg_turnover')
                if row[4] is None:  # limit_up_count
                    missing_fields.append('limit_up_count')
                if row[5] is None:  # top_weight_stocks
                    missing_fields.append('top_weight_stocks')
                if row[6] is None:  # sector_rps_20
                    missing_fields.append('sector_rps_20')
                if row[7] is None:  # sector_rps_50
                    missing_fields.append('sector_rps_50')
                if row[8] is None:  # ma_status
                    missing_fields.append('ma_status')
                
                if missing_fields:
                    records.append({
                        'sector_name': row[0],
                        'trade_date': row[1],
                        'missing_fields': missing_fields
                    })
            
            return records
    
    @staticmethod
    def get_all_sectors_for_rps_calculation(trade_date: date) -> List[Dict]:
        """
        获取指定日期的所有板块数据用于RPS计算
        """
        with get_db() as db:
            query = text("""
                SELECT 
                    sector_name,
                    change_pct,
                    avg_turnover,
                    limit_up_count,
                    top_weight_stocks,
                    sector_rps_20,
                    sector_rps_50,
                    ma_status
                FROM sector_money_flow
                WHERE trade_date = :trade_date
                ORDER BY change_pct DESC
            """)
            
            result = db.execute(query, {'trade_date': trade_date})
            
            sectors = []
            for row in result:
                sectors.append({
                    'sector_name': row[0],
                    'change_pct': float(row[1]) if row[1] is not None else 0.0,
                    'avg_turnover': float(row[2]) if row[2] is not None else 0.0,
                    'limit_up_count': int(row[3]) if row[3] is not None else 0,
                    'top_weight_stocks': row[4] if row[4] is not None else [],
                    'sector_rps_20': float(row[5]) if row[5] is not None else 0.0,
                    'sector_rps_50': float(row[6]) if row[6] is not None else 0.0,
                    'ma_status': int(row[7]) if row[7] is not None else 0
                })
            
            return sectors
    
    @staticmethod
    def update_sector_additional_fields(sector_name: str, trade_date: date, change_pct: Optional[float] = None, 
                                         avg_turnover: Optional[float] = None, limit_up_count: Optional[int] = None, 
                                         top_weight_stocks: Optional[List[str]] = None) -> bool:
        """
        更新板块的额外字段
        
        Args:
            sector_name: 板块名称
            trade_date: 交易日期
            change_pct: 涨跌幅
            avg_turnover: 平均换手率
            limit_up_count: 涨停家数
            top_weight_stocks: 前几大权重股
        
        Returns:
            bool: 是否更新成功
        """
        with get_db() as db:
            # 构建动态更新语句
            update_fields = []
            params = {
                'sector_name': sector_name,
                'trade_date': trade_date
            }
            
            if change_pct is not None:
                update_fields.append('change_pct = :change_pct')
                params['change_pct'] = change_pct
            
            if avg_turnover is not None:
                update_fields.append('avg_turnover = :avg_turnover')
                params['avg_turnover'] = avg_turnover
            
            if limit_up_count is not None:
                update_fields.append('limit_up_count = :limit_up_count')
                params['limit_up_count'] = limit_up_count
            
            if top_weight_stocks is not None:
                update_fields.append('top_weight_stocks = :top_weight_stocks')
                # 将列表转换为JSON字符串
                import json
                if isinstance(top_weight_stocks, list):
                    params['top_weight_stocks'] = json.dumps(top_weight_stocks, ensure_ascii=False)
                elif top_weight_stocks == []:
                    params['top_weight_stocks'] = json.dumps([], ensure_ascii=False)
                else:
                    params['top_weight_stocks'] = top_weight_stocks
            
            if not update_fields:
                return False  # 没有要更新的字段
            
            query = text(f"""
                UPDATE sector_money_flow 
                SET {', '.join(update_fields)},
                    updated_at = CURRENT_TIMESTAMP
                WHERE sector_name = :sector_name 
                  AND trade_date = :trade_date
            """)
            
            result = db.execute(query, params)
            db.commit()
            return result.rowcount > 0

    @staticmethod
    def get_concept_money_flow_count_for_date(trade_date: date) -> int:
        """
        获取指定日期的概念资金流数据数量
        
        Args:
            trade_date: 交易日期
            
        Returns:
            该日期的数据记录数（概念数量）
        """
        try:
            with get_db() as db:
                query = text("""
                    SELECT COUNT(*) 
                    FROM sector_money_flow
                    WHERE trade_date = :trade_date
                """)
                result = db.execute(query, {'trade_date': trade_date})
                row = result.fetchone()
                return row[0] if row and row[0] is not None else 0
        except Exception as e:
            logger.error(f"获取日期 {trade_date} 的概念资金流数据数量失败: {e}")
            return 0
    
    @staticmethod
    def cleanup_old_data(retention_days: int = 90):
        """
        清理旧数据（保留最近N天）
        
        Args:
            retention_days: 保留天数，默认90天（3个月）
        """
        cutoff_date = date.today() - timedelta(days=retention_days)
        
        with get_db() as db:
            # 先查询要删除的数量
            count_query = text("""
                SELECT COUNT(*) 
                FROM sector_money_flow 
                WHERE trade_date < :cutoff_date
            """)
            result = db.execute(count_query, {'cutoff_date': cutoff_date})
            count = result.scalar()
            
            if count > 0:
                # 删除旧数据
                delete_query = text("""
                    DELETE FROM sector_money_flow 
                    WHERE trade_date < :cutoff_date
                """)
                db.execute(delete_query, {'cutoff_date': cutoff_date})
                db.commit()
                logger.info(f"清理板块资金流旧数据完成，删除了 {count} 条记录（保留最近{retention_days}天）")
                return count
            else:
                logger.info(f"无需清理板块资金流数据（保留最近{retention_days}天）")
                return 0
