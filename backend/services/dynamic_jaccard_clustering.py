"""
动态Jaccard聚类算法 (Dynamic Jaccard Clustering)
解决"概念同质化"问题，替代人工mapping

核心逻辑：
1. 遍历候选池，两两比对板块A和B的top_weight_stocks(前5大权重股)
2. 快速判定：若前5大权重股中，有≥2只股票相同，直接判定为"同质化"
3. 深度判定：若快速判定未命中，则获取全量成分股计算Jaccard系数 J = |A∩B|/|A∪B|，阈值设为0.35
4. 聚合策略：若判定相似，将分数较低的板块标记为"关联板块"，折叠进分数较高的板块中
"""
import json
import logging
from typing import List, Dict, Optional, Tuple, Set
from datetime import date
from collections import defaultdict
from db.database import get_raw_connection, get_sqlalchemy_engine
from sqlalchemy import text
import pandas as pd

logger = logging.getLogger(__name__)


class DynamicJaccardClustering:
    """动态Jaccard聚类算法"""
    
    # 快速判定阈值：前5大权重股中相同股票数量
    FAST_MATCH_THRESHOLD = 2
    
    # 深度判定阈值：Jaccard系数
    JACCARD_THRESHOLD = 0.35
    
    def __init__(self):
        """初始化聚类算法"""
        self.fast_match_threshold = self.FAST_MATCH_THRESHOLD
        self.jaccard_threshold = self.JACCARD_THRESHOLD
    
    def cluster_sectors(
        self, 
        sectors: List[Dict], 
        trade_date: date,
        score_key: str = 'main_net_inflow'
    ) -> List[Dict]:
        """
        对板块列表进行聚类
        
        Args:
            sectors: 板块列表，每个板块包含 sector_name, score_key(默认main_net_inflow) 等字段
            trade_date: 交易日期
            score_key: 用于排序的分数字段，默认使用 main_net_inflow
            
        Returns:
            聚类后的板块列表，包含关联板块信息
        """
        if not sectors:
            return []
        
        # 按分数降序排序
        sorted_sectors = sorted(
            sectors, 
            key=lambda x: float(x.get(score_key, 0)), 
            reverse=True
        )
        
        # 初始化聚类结果
        clustered = []
        processed = set()
        
        # 构建板块信息映射
        sector_info_map = {
            s['sector_name']: s for s in sorted_sectors
        }
        
        # 两两比对进行聚类
        # 优化：限制最大比对数量，避免O(n²)复杂度导致超时
        MAX_COMPARISONS = 50  # 最多比对50次（约10个板块的两两比对）
        comparison_count = 0
        
        for i, sector_a in enumerate(sorted_sectors):
            sector_name_a = sector_a['sector_name']
            
            # 如果已经处理过（作为关联板块），跳过
            if sector_name_a in processed:
                continue
            
            # 查找与当前板块相似的板块
            similar_sectors = []
            
            # 限制比对的板块数量，只比对后续的10个板块（而不是所有）
            remaining_sectors = sorted_sectors[i+1:i+11]  # 只比对后续10个
            
            for sector_b in remaining_sectors:
                # 如果已经达到最大比对次数，停止
                if comparison_count >= MAX_COMPARISONS:
                    logger.debug(f"达到最大比对次数限制({MAX_COMPARISONS})，停止聚类")
                    break
                
                sector_name_b = sector_b['sector_name']
                
                # 如果已经处理过，跳过
                if sector_name_b in processed:
                    continue
                
                comparison_count += 1
                
                # 判断是否相似
                is_similar, similarity_type = self._check_similarity(
                    sector_name_a, 
                    sector_name_b, 
                    trade_date
                )
                
                if is_similar:
                    similar_sectors.append({
                        'sector_name': sector_name_b,
                        'score': sector_b.get(score_key, 0),
                        'similarity_type': similarity_type
                    })
                    processed.add(sector_name_b)
            
            # 构建聚类结果
            clustered_sector = sector_a.copy()
            
            if similar_sectors:
                # 有关联板块，进行折叠
                similar_sectors.sort(
                    key=lambda x: x['score'], 
                    reverse=True
                )
                
                clustered_sector['aggregated_sectors'] = [
                    s['sector_name'] for s in similar_sectors
                ]
                clustered_sector['aggregated_count'] = len(similar_sectors)
                
                # 更新显示名称（可选）
                if 'display_name' not in clustered_sector:
                    aggregated_names = [s['sector_name'] for s in similar_sectors]
                    clustered_sector['display_name'] = (
                        f"{sector_name_a} (聚合: {', '.join(aggregated_names)})"
                    )
            else:
                clustered_sector['aggregated_sectors'] = []
                clustered_sector['aggregated_count'] = 0
                clustered_sector['display_name'] = sector_name_a
            
            clustered.append(clustered_sector)
            processed.add(sector_name_a)
            
            # 如果达到最大比对次数，跳出外层循环
            if comparison_count >= MAX_COMPARISONS:
                # 将剩余的未处理板块直接添加到结果中
                for remaining_sector in sorted_sectors[i+1:]:
                    if remaining_sector['sector_name'] not in processed:
                        remaining_clustered = remaining_sector.copy()
                        remaining_clustered['aggregated_sectors'] = []
                        remaining_clustered['aggregated_count'] = 0
                        remaining_clustered['display_name'] = remaining_sector['sector_name']
                        clustered.append(remaining_clustered)
                        processed.add(remaining_sector['sector_name'])
                break
        
        logger.debug(
            f"Jaccard聚类完成: {len(sectors)} 个板块 -> {len(clustered)} 个聚类 "
            f"(折叠了 {len(sectors) - len(clustered)} 个同质板块)"
        )
        
        return clustered
    
    def _check_similarity(
        self, 
        sector_name_a: str, 
        sector_name_b: str, 
        trade_date: date
    ) -> Tuple[bool, str]:
        """
        检查两个板块是否相似
        
        Returns:
            (is_similar, similarity_type): 
            - is_similar: 是否相似
            - similarity_type: 相似类型 ('fast' 或 'jaccard')
        """
        # 快速判定：比对前5大权重股
        top_stocks_a = self._get_top_weight_stocks(sector_name_a, trade_date)
        top_stocks_b = self._get_top_weight_stocks(sector_name_b, trade_date)
        
        if top_stocks_a and top_stocks_b:
            common_count = len(set(top_stocks_a) & set(top_stocks_b))
            if common_count >= self.fast_match_threshold:
                logger.debug(
                    f"快速判定相似: {sector_name_a} <-> {sector_name_b} "
                    f"(前5大权重股重叠 {common_count} 只)"
                )
                return True, 'fast'
        
        # 深度判定：计算全量成分股的Jaccard系数
        jaccard_score = self._calculate_jaccard(
            sector_name_a, 
            sector_name_b, 
            trade_date
        )
        
        if jaccard_score is not None and jaccard_score >= self.jaccard_threshold:
            logger.debug(
                f"深度判定相似: {sector_name_a} <-> {sector_name_b} "
                f"(Jaccard系数: {jaccard_score:.3f})"
            )
            return True, 'jaccard'
        
        return False, 'none'
    
    def _get_top_weight_stocks(
        self, 
        sector_name: str, 
        trade_date: date
    ) -> Optional[List[str]]:
        """
        获取板块的前5大权重股
        
        Returns:
            股票代码列表，最多5只
        """
        try:
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                
                # 从 sector_money_flow 表获取 top_weight_stocks
                cursor.execute("""
                    SELECT top_weight_stocks
                    FROM sector_money_flow
                    WHERE sector_name = %s AND trade_date = %s
                    LIMIT 1
                """, (sector_name, trade_date))
                
                row = cursor.fetchone()
                cursor.close()
                
                if row and row[0]:
                    # top_weight_stocks 是 JSON 格式
                    if isinstance(row[0], str):
                        stocks = json.loads(row[0])
                    else:
                        stocks = row[0]
                    
                    # 确保返回的是列表
                    if isinstance(stocks, list):
                        return [str(s) for s in stocks[:5]]
                    else:
                        return []
                
                # 如果表中没有数据，尝试从概念映射表计算
                return self._calculate_top_weight_stocks(sector_name, trade_date)
                
        except Exception as e:
            logger.warning(
                f"获取板块 {sector_name} 前5大权重股失败: {e}"
            )
            return self._calculate_top_weight_stocks(sector_name, trade_date)
    
    def _calculate_top_weight_stocks(
        self, 
        sector_name: str, 
        trade_date: date
    ) -> List[str]:
        """
        从概念映射表计算板块的前5大权重股
        
        按成交额排序，取前5只
        """
        try:
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                
                # 获取概念ID
                cursor.execute("""
                    SELECT concept_id 
                    FROM concept_theme 
                    WHERE concept_name = %s AND is_active = 1
                    LIMIT 1
                """, (sector_name,))
                
                row = cursor.fetchone()
                if not row:
                    return []
                
                concept_id = row[0]
                
                # 获取该概念下的股票代码（按成交额排序）
                cursor.execute("""
                    SELECT scm.sheep_code
                    FROM sheep_concept_mapping scm
                    LEFT JOIN sheep_daily sd 
                        ON scm.sheep_code = sd.sheep_code 
                        AND sd.trade_date = %s
                    WHERE scm.concept_id = %s
                    ORDER BY COALESCE(sd.amount, 0) DESC
                    LIMIT 5
                """, (trade_date, concept_id))
                
                stocks = [row[0] for row in cursor.fetchall()]
                cursor.close()
                
                return stocks
                
        except Exception as e:
            logger.warning(
                f"计算板块 {sector_name} 前5大权重股失败: {e}"
            )
            return []
    
    def _calculate_jaccard(
        self, 
        sector_name_a: str, 
        sector_name_b: str, 
        trade_date: date
    ) -> Optional[float]:
        """
        计算两个板块全量成分股的Jaccard系数
        
        Jaccard系数 = |A∩B| / |A∪B|
        
        Returns:
            Jaccard系数 (0-1之间)，如果计算失败返回None
        """
        try:
            # 获取两个板块的全量成分股
            stocks_a = self._get_all_constituent_stocks(sector_name_a)
            stocks_b = self._get_all_constituent_stocks(sector_name_b)
            
            if not stocks_a or not stocks_b:
                return None
            
            # 转换为集合
            set_a = set(stocks_a)
            set_b = set(stocks_b)
            
            # 计算交集和并集
            intersection = len(set_a & set_b)
            union = len(set_a | set_b)
            
            if union == 0:
                return None
            
            jaccard = intersection / union
            
            return jaccard
            
        except Exception as e:
            logger.warning(
                f"计算板块 {sector_name_a} 和 {sector_name_b} 的Jaccard系数失败: {e}"
            )
            return None
    
    def _get_all_constituent_stocks(self, sector_name: str) -> List[str]:
        """
        获取板块的全量成分股
        
        Returns:
            股票代码列表
        """
        try:
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                
                # 获取概念ID
                cursor.execute("""
                    SELECT concept_id 
                    FROM concept_theme 
                    WHERE concept_name = %s AND is_active = 1
                    LIMIT 1
                """, (sector_name,))
                
                row = cursor.fetchone()
                if not row:
                    return []
                
                concept_id = row[0]
                
                # 获取该概念下的所有股票代码
                cursor.execute("""
                    SELECT sheep_code
                    FROM sheep_concept_mapping
                    WHERE concept_id = %s
                """, (concept_id,))
                
                stocks = [row[0] for row in cursor.fetchall()]
                cursor.close()
                
                return stocks
                
        except Exception as e:
            logger.warning(
                f"获取板块 {sector_name} 全量成分股失败: {e}"
            )
            return []