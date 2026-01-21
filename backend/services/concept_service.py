"""
概念板块服务层
"""
from typing import List, Dict, Set
import logging
from sqlalchemy import text
from db.database import get_db
from services.clustered_sector_mapping_service import ClusteredSectorMappingService
from services.dynamic_jaccard_clustering import DynamicJaccardClustering
from datetime import date

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
    def _get_original_concept_names(clustered_sector_name: str) -> Set[str]:
        """
        获取聚类后板块名对应的所有原始概念名
        
        Args:
            clustered_sector_name: 聚类后的板块名
            
        Returns:
            原始概念名集合
        """
        reverse_mapping = ClusteredSectorMappingService.get_reverse_mapping()
        return set(reverse_mapping.get(clustered_sector_name, [clustered_sector_name]))
    
    @staticmethod
    def get_sector_stocks(sector_name: str) -> List[Dict]:
        """获取板块下的肥羊（支持聚类后的板块名）"""
        try:
            # 获取所有关联的原始概念名
            original_concept_names = ConceptService._get_original_concept_names(sector_name)
            
            with get_db() as db:
                # 构建参数化查询
                placeholders = ','.join([f':name{i}' for i in range(len(original_concept_names))])
                params = {f'name{i}': name for i, name in enumerate(original_concept_names)}
                
                query = text(f"""
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
                    WHERE ct.concept_name IN ({placeholders})
                      AND ct.is_active = 1
                      AND sb.is_active = 1
                    GROUP BY sb.sheep_code, sb.sheep_name
                    ORDER BY min_rank ASC, consecutive_days DESC
                    LIMIT 100
                """)
                
                result = db.execute(query, params)
                
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
    def get_all_concepts() -> List[Dict]:
        """
        获取所有活跃概念
        
        Returns:
            概念列表，包含概念名称和相关信息
        """
        try:
            with get_db() as db:
                query = text("""
                    SELECT 
                        concept_id,
                        concept_name,
                        description,
                        created_at,
                        updated_at
                    FROM concept_theme 
                    WHERE is_active = 1
                    ORDER BY concept_name
                """)
                
                result = db.execute(query)
                concepts = []
                for row in result:
                    concepts.append({
                        'concept_id': row[0],
                        'concept_name': row[1],
                        'description': row[2],
                        'created_at': row[3],
                        'updated_at': row[4]
                    })
                
                return concepts
                
        except Exception as e:
            logger.error(f"获取所有概念失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def cluster_concepts_by_jaccard(threshold: float = 0.35, limit: int = 100) -> List[Dict]:
        """
        使用Jaccard系数对概念进行聚类
        
        Args:
            threshold: Jaccard相似度阈值
            limit: 限制参与聚类的概念数量
        
        Returns:
            聚类结果，包括主概念和聚合的概念
        """
        try:
            # 获取活跃概念列表
            all_concepts = ConceptService.get_all_concepts()
            
            if not all_concepts:
                return []
            
            # 限制数量
            concepts_to_process = all_concepts[:limit]
            
            # 准备聚类数据
            sectors_for_clustering = []
            for concept in concepts_to_process:
                concept_name = concept['concept_name']
                
                # 获取该概念下的股票数量作为参考指标
                with get_db() as db:
                    stock_count_query = text("""
                        SELECT COUNT(DISTINCT scm.sheep_code) as stock_count
                        FROM sheep_concept_mapping scm
                        INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                        WHERE ct.concept_name = :concept_name
                        AND ct.is_active = 1
                    """)
                    
                    result = db.execute(stock_count_query, {'concept_name': concept_name})
                    stock_count_row = result.fetchone()
                    stock_count = stock_count_row[0] if stock_count_row else 0
                
                sectors_for_clustering.append({
                    'sector_name': concept_name,
                    'main_net_inflow': float(stock_count),  # 使用股票数量作为权重
                    'stock_count': stock_count
                })
            
            # 使用Jaccard聚类算法对概念进行聚类
            jaccard_clustering = DynamicJaccardClustering()
            
            # 设置阈值
            jaccard_clustering.jaccard_threshold = threshold
            
            clustered_sectors = jaccard_clustering.cluster_sectors(
                sectors_for_clustering,
                date.today(),
                score_key='main_net_inflow'
            )
            
            return clustered_sectors
            
        except Exception as e:
            logger.error(f"概念Jaccard聚类失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def get_sector_stocks_by_change_pct(sector_name: str, limit: int = 10) -> List[Dict]:
        """获取板块下涨幅前N的概念股（支持聚类后的板块名）"""
        try:
            # 获取所有关联的原始概念名
            original_concept_names = ConceptService._get_original_concept_names(sector_name)
            
            with get_db() as db:
                # 检查概念是否存在（检查所有关联的原始概念）
                placeholders = ','.join([f':name{i}' for i in range(len(original_concept_names))])
                params_check = {f'name{i}': name for i, name in enumerate(original_concept_names)}
                
                concept_check = text(f"""
                    SELECT concept_id, concept_name 
                    FROM concept_theme 
                    WHERE concept_name IN ({placeholders})
                      AND is_active = 1
                    LIMIT 1
                """)
                concept_result = db.execute(concept_check, params_check)
                concept_row = concept_result.fetchone()
                
                if not concept_row:
                    logger.warning(f"聚类板块 {sector_name} 对应的原始概念不存在或未激活: {original_concept_names}")
                    return []
                
                # 获取最新交易日（从sheep_daily表中）
                latest_date_query = text("SELECT MAX(trade_date) FROM sheep_daily")
                latest_date_result = db.execute(latest_date_query)
                latest_date_row = latest_date_result.fetchone()
                latest_trade_date = latest_date_row[0] if latest_date_row and latest_date_row[0] else None
                
                if not latest_trade_date:
                    logger.warning(f"数据库中没有交易日期数据，返回概念股列表（无涨幅数据）")
                    # 即使没有交易日期，也返回概念股列表（只是没有涨幅数据）
                    placeholders = ','.join([f':name{i}' for i in range(len(original_concept_names))])
                    params_no_date = {f'name{i}': name for i, name in enumerate(original_concept_names)}
                    params_no_date['limit'] = limit
                    
                    query = text(f"""
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
                        WHERE ct.concept_name IN ({placeholders})
                          AND ct.is_active = 1
                          AND sb.is_active = 1
                        GROUP BY sb.sheep_code, sb.sheep_name
                        ORDER BY min_rank ASC
                        LIMIT :limit
                    """)
                    
                    result = db.execute(query, params_no_date)
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
                placeholders = ','.join([f':name{i}' for i in range(len(original_concept_names))])
                params_count = {f'name{i}': name for i, name in enumerate(original_concept_names)}
                
                stock_count_query = text(f"""
                    SELECT COUNT(DISTINCT sb.sheep_code) as stock_count
                    FROM sheep_basic sb
                    INNER JOIN sheep_concept_mapping scm ON sb.sheep_code = scm.sheep_code
                    INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                    WHERE ct.concept_name IN ({placeholders})
                      AND ct.is_active = 1
                      AND sb.is_active = 1
                """)
                count_result = db.execute(stock_count_query, params_count)
                count_row = count_result.fetchone()
                stock_count = count_row[0] if count_row else 0
                
                if stock_count == 0:
                    logger.warning(f"聚类板块 {sector_name} 下没有肥羊（原始概念: {original_concept_names}）")
                    return []
                
                logger.info(f"聚类板块 {sector_name} 下有 {stock_count} 只肥羊，开始查询涨幅数据（最新交易日: {latest_trade_date}，原始概念: {original_concept_names}）")
                
                params_query = params_count.copy()
                params_query['limit'] = limit
                params_query['latest_date'] = latest_trade_date
                
                query = text(f"""
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
                    WHERE ct.concept_name IN ({placeholders})
                      AND ct.is_active = 1
                      AND sb.is_active = 1
                    GROUP BY sb.sheep_code, sb.sheep_name, sd.change_pct, sd.close_price
                    ORDER BY 
                        CASE WHEN sd.change_pct IS NOT NULL THEN 0 ELSE 1 END,
                        COALESCE(sd.change_pct, -999) DESC,
                        COALESCE(MIN(hr.rank), 999) ASC
                    LIMIT :limit
                """)
                
                result = db.execute(query, params_query)
                
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
