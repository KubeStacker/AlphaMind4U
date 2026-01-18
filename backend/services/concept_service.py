"""
概念板块服务层
"""
from typing import List, Dict
import logging
from sqlalchemy import text
from db.database import get_db

logger = logging.getLogger(__name__)

class ConceptService:
    """概念板块服务"""
    
    @staticmethod
    def get_sector_daily(sector_name: str) -> List[Dict]:
        """
        获取板块K线数据
        注意：新表结构中暂时没有板块日K数据表，这里返回空列表
        后续可以根据需要实现板块聚合计算
        """
        # 板块K线数据聚合计算待实现
        return []
    
    @staticmethod
    def get_sector_stocks(sector_name: str) -> List[Dict]:
        """获取板块下的肥羊"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT 
                        sb.sheep_code,
                        sb.sheep_name,
                        MIN(hr.rank) as min_rank,
                        COUNT(DISTINCT hr.trade_date) as consecutive_days
                    FROM sheep_basic sb USE INDEX (idx_is_active)
                    INNER JOIN sheep_concept_mapping scm USE INDEX (idx_sheep_code, idx_concept_id) 
                        ON sb.sheep_code = scm.sheep_code
                    INNER JOIN concept_theme ct USE INDEX (idx_concept_name, idx_is_active) 
                        ON scm.concept_id = ct.concept_id
                    LEFT JOIN market_hot_rank hr USE INDEX (idx_sheep_code, idx_trade_date) 
                        ON sb.sheep_code = hr.sheep_code
                        AND hr.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    WHERE ct.concept_name = :sector_name
                      AND ct.is_active = 1
                      AND sb.is_active = 1
                    GROUP BY sb.sheep_code, sb.sheep_name
                    ORDER BY min_rank ASC, consecutive_days DESC
                    LIMIT 100
                """)
                
                result = db.execute(query, {'sector_name': sector_name})
                
                stocks = []
                for row in result:
                    sheep_code = row[0]
                    sheep_name_raw = row[1]
                    
                    # 判断名称是否有效（不是代码格式）
                    def is_valid_name(name):
                        if not name or not name.strip():
                            return False
                        name_clean = name.strip()
                        # 如果是6位纯数字，认为是代码
                        if len(name_clean) == 6 and name_clean.isdigit():
                            return False
                        # 如果以SH或SZ开头后跟6位数字，也认为是代码
                        if (name_clean.startswith('SH') or name_clean.startswith('SZ')) and len(name_clean) == 8 and name_clean[2:].isdigit():
                            return False
                        return True
                    
                    # 如果名称无效，使用代码
                    if is_valid_name(sheep_name_raw):
                        sheep_name = sheep_name_raw.strip()
                    else:
                        sheep_name = sheep_code
                    
                    stocks.append({
                        'sheep_code': sheep_code,
                        'sheep_name': sheep_name,
                        'rank': row[2] if row[2] != 999 else None,
                        'consecutive_days': row[3]
                    })
                
                return stocks
                
        except Exception as e:
            logger.error(f"获取板块肥羊失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def get_sector_sheep(sector_name: str) -> List[Dict]:
        """获取板块下的肥羊（别名方法，兼容API调用）"""
        return ConceptService.get_sector_stocks(sector_name)
    
    @staticmethod
    def get_sector_stocks_by_change_pct(sector_name: str, limit: int = 10) -> List[Dict]:
        """获取板块下涨幅前N的概念股"""
        try:
            with get_db() as db:
                # 首先检查概念是否存在
                concept_check = text("""
                    SELECT concept_id, concept_name 
                    FROM concept_theme 
                    WHERE concept_name = :sector_name 
                      AND is_active = 1
                    LIMIT 1
                """)
                concept_result = db.execute(concept_check, {'sector_name': sector_name})
                concept_row = concept_result.fetchone()
                
                if not concept_row:
                    # 尝试查询所有相似的概念名称（用于调试）
                    debug_query = text("""
                        SELECT concept_name, is_active, source
                        FROM concept_theme 
                        WHERE concept_name LIKE :pattern
                        LIMIT 10
                    """)
                    debug_result = db.execute(debug_query, {'pattern': f'%{sector_name}%'})
                    similar_concepts = [row[0] for row in debug_result]
                    logger.warning(f"概念板块不存在或未激活: {sector_name}，相似的概念名称: {similar_concepts}")
                    return []
                
                # 获取最新交易日（从sheep_daily表中）
                latest_date_query = text("SELECT MAX(trade_date) FROM sheep_daily")
                latest_date_result = db.execute(latest_date_query)
                latest_date_row = latest_date_result.fetchone()
                latest_trade_date = latest_date_row[0] if latest_date_row and latest_date_row[0] else None
                
                if not latest_trade_date:
                    logger.warning(f"数据库中没有交易日期数据，返回概念股列表（无涨幅数据）")
                    # 即使没有交易日期，也返回概念股列表（只是没有涨幅数据）
                    query = text("""
                        SELECT 
                            sb.sheep_code,
                            sb.sheep_name,
                            MIN(hr.rank) as min_rank
                        FROM sheep_basic sb USE INDEX (idx_is_active)
                        INNER JOIN sheep_concept_mapping scm USE INDEX (idx_sheep_code, idx_concept_id) 
                            ON sb.sheep_code = scm.sheep_code
                        INNER JOIN concept_theme ct USE INDEX (idx_concept_name, idx_is_active) 
                            ON scm.concept_id = ct.concept_id
                        LEFT JOIN market_hot_rank hr USE INDEX (idx_sheep_code, idx_trade_date) 
                            ON sb.sheep_code = hr.sheep_code
                            AND hr.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                        WHERE ct.concept_name = :sector_name
                          AND ct.is_active = 1
                          AND sb.is_active = 1
                        GROUP BY sb.sheep_code, sb.sheep_name
                        ORDER BY min_rank ASC
                        LIMIT :limit
                    """)
                    
                    result = db.execute(query, {'sector_name': sector_name, 'limit': limit})
                    stocks = []
                    for row in result:
                        sheep_code = row[0]
                        sheep_name_raw = row[1]
                        min_rank = row[2] if row[2] != 999 else None
                        
                        # 判断名称是否有效
                        def is_valid_name(name):
                            if not name or not name.strip():
                                return False
                            name_clean = name.strip()
                            if len(name_clean) == 6 and name_clean.isdigit():
                                return False
                            if (name_clean.startswith('SH') or name_clean.startswith('SZ')) and len(name_clean) == 8 and name_clean[2:].isdigit():
                                return False
                            return True
                        
                        sheep_name = sheep_name_raw.strip() if is_valid_name(sheep_name_raw) else sheep_code
                        
                        stocks.append({
                            'sheep_code': sheep_code,
                            'sheep_name': sheep_name,
                            'change_pct': None,
                            'current_price': None,
                            'rank': min_rank
                        })
                    
                    logger.info(f"概念板块 {sector_name} 找到 {len(stocks)} 只概念股（无涨幅数据）")
                    return stocks
                
                # 有交易日期，查询带涨幅的数据（移除sd.change_pct IS NOT NULL限制，允许返回无涨幅数据的肥羊）
                # 先检查概念下是否有肥羊
                stock_count_query = text("""
                    SELECT COUNT(DISTINCT sb.sheep_code) as stock_count
                    FROM sheep_basic sb
                    INNER JOIN sheep_concept_mapping scm ON sb.sheep_code = scm.sheep_code
                    INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                    WHERE ct.concept_name = :sector_name
                      AND ct.is_active = 1
                      AND sb.is_active = 1
                """)
                count_result = db.execute(stock_count_query, {'sector_name': sector_name})
                count_row = count_result.fetchone()
                stock_count = count_row[0] if count_row else 0
                
                if stock_count == 0:
                    logger.warning(f"概念板块 {sector_name} 下没有肥羊")
                    return []
                
                logger.info(f"概念板块 {sector_name} 下有 {stock_count} 只肥羊，开始查询涨幅数据（最新交易日: {latest_trade_date}）")
                
                query = text("""
                    SELECT 
                        sb.sheep_code,
                        sb.sheep_name,
                        sd.change_pct,
                        sd.close_price as current_price,
                        MIN(hr.rank) as min_rank
                    FROM sheep_basic sb
                    INNER JOIN sheep_concept_mapping scm 
                        ON sb.sheep_code = scm.sheep_code
                    INNER JOIN concept_theme ct 
                        ON scm.concept_id = ct.concept_id
                    LEFT JOIN sheep_daily sd
                        ON sb.sheep_code = sd.sheep_code
                        AND sd.trade_date = :latest_date
                    LEFT JOIN market_hot_rank hr 
                        ON sb.sheep_code = hr.sheep_code
                        AND hr.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    WHERE ct.concept_name = :sector_name
                      AND ct.is_active = 1
                      AND sb.is_active = 1
                    GROUP BY sb.sheep_code, sb.sheep_name, sd.change_pct, sd.close_price
                    ORDER BY 
                        CASE WHEN sd.change_pct IS NOT NULL THEN 0 ELSE 1 END,
                        COALESCE(sd.change_pct, -999) DESC,
                        COALESCE(MIN(hr.rank), 999) ASC
                    LIMIT :limit
                """)
                
                result = db.execute(query, {
                    'sector_name': sector_name, 
                    'limit': limit,
                    'latest_date': latest_trade_date
                })
                
                stocks = []
                for row in result:
                    sheep_code = row[0]
                    sheep_name_raw = row[1]
                    change_pct = float(row[2]) if row[2] is not None else None
                    current_price = float(row[3]) if row[3] is not None else None
                    min_rank = row[4] if row[4] != 999 else None
                    
                    # 判断名称是否有效（不是代码格式）
                    def is_valid_name(name):
                        if not name or not name.strip():
                            return False
                        name_clean = name.strip()
                        # 如果是6位纯数字，认为是代码
                        if len(name_clean) == 6 and name_clean.isdigit():
                            return False
                        # 如果以SH或SZ开头后跟6位数字，也认为是代码
                        if (name_clean.startswith('SH') or name_clean.startswith('SZ')) and len(name_clean) == 8 and name_clean[2:].isdigit():
                            return False
                        return True
                    
                    # 如果名称无效，使用代码
                    if is_valid_name(sheep_name_raw):
                        sheep_name = sheep_name_raw.strip()
                    else:
                        sheep_name = sheep_code
                    
                    stocks.append({
                        'sheep_code': sheep_code,
                        'sheep_name': sheep_name,
                        'change_pct': change_pct,
                        'current_price': current_price,
                        'rank': min_rank
                    })
                
                stocks_with_change = [s for s in stocks if s['change_pct'] is not None]
                logger.info(f"概念板块 {sector_name} 找到 {len(stocks)} 只概念股（其中 {len(stocks_with_change)} 只有涨幅数据，最新交易日: {latest_trade_date}）")
                return stocks
                
        except Exception as e:
            logger.error(f"获取板块涨幅前N概念股失败: {e}", exc_info=True)
            return []
