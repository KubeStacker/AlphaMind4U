"""
板块匹配服务层
实现基于Jaccard聚类的热门股板块匹配算法
"""
from typing import List, Dict, Optional, Tuple, Set
import logging
from collections import defaultdict

from etl.concept_filter import should_filter_concept
from sqlalchemy import text
from db.database import get_db
from services.dynamic_jaccard_clustering import DynamicJaccardClustering
from datetime import date

logger = logging.getLogger(__name__)

class SectorMatchingService:
    """板块匹配服务 - 基于Jaccard聚类的算法"""
    
    def __init__(self):
        self.jaccard_clustering = DynamicJaccardClustering()
    

    
    def match_hot_sheep_to_sectors(
        self, 
        hot_sheep: List[Dict], 
        max_sectors: int = 12
    ) -> List[Dict]:
        """
        匹配热门股到主板块（使用Jaccard聚类算法）
        
        算法逻辑：
        1. 获取热门股的概念信息
        2. 构建板块数据用于聚类
        3. 使用Jaccard聚类算法对概念进行聚类
        4. 根据聚类结果分配热门股到相应板块
        5. 按板块热度排序返回结果
        
        Args:
            hot_sheep: 热门股列表，每个元素包含 sheep_code, sheep_name, rank, hot_score 等
            max_sectors: 返回的最大板块数量
            
        Returns:
            板块列表，每个板块包含 sector_name, hot_score, hot_sheep, color
        """
        if not hot_sheep:
            return []
        
        try:
            # 1. 批量获取所有热门股的概念（优化：单次查询，包含权重）
            sheep_codes = [s['sheep_code'] for s in hot_sheep]
            sheep_concepts_map = self._get_sheep_concepts_batch(sheep_codes)
            
            # 2. 构建肥羊热度映射（用于权重计算）
            sheep_hot_scores = {}
            for stock in hot_sheep:
                sheep_code = stock['sheep_code']
                rank = stock.get('rank', 999)
                hot_score = stock.get('hot_score')
                # 热度权重：如果有hot_score则使用，否则使用 1/(rank+1)
                if hot_score is not None and hot_score > 0:
                    sheep_hot_scores[sheep_code] = float(hot_score)
                else:
                    sheep_hot_scores[sheep_code] = 1.0 / (rank + 1)
            
            # 3. 收集所有涉及的概念并构建板块数据用于聚类
            all_concepts = set()
            for concepts in sheep_concepts_map.values():
                for concept_name, _ in concepts:
                    all_concepts.add(concept_name)
            
            # 4. 准备板块数据用于聚类（模拟板块资金流数据）
            sectors_for_clustering = []
            for concept_name in all_concepts:
                # 为每个概念创建一个“板块”记录用于聚类
                # 使用涉及该概念的股票总数和热度分数作为权重
                concept_stock_count = 0
                concept_total_hot_score = 0.0
                for sheep_code, concepts in sheep_concepts_map.items():
                    for c_name, c_weight in concepts:
                        if c_name == concept_name:
                            concept_stock_count += 1
                            concept_total_hot_score += sheep_hot_scores.get(sheep_code, 0.0) * c_weight
                
                sectors_for_clustering.append({
                    'sector_name': concept_name,
                    'main_net_inflow': concept_total_hot_score,  # 使用热度分数作为聚类的权重
                    'stock_count': concept_stock_count
                })
            
            # 5. 使用Jaccard聚类算法对概念进行聚类
            today = date.today()
            clustered_sectors = self.jaccard_clustering.cluster_sectors(
                sectors_for_clustering, 
                today,
                score_key='main_net_inflow'
            )
            
            # 6. 构建概念到聚类后板块的映射
            concept_to_clustered_sector = {}
            for sector in clustered_sectors:
                main_sector = sector['sector_name']
                concept_to_clustered_sector[main_sector] = main_sector
                
                # 映射聚合的板块
                aggregated_sectors = sector.get('aggregated_sectors', [])
                for agg_sector in aggregated_sectors:
                    concept_to_clustered_sector[agg_sector] = main_sector
            
            # 7. 为每个肥羊计算每个可能板块的匹配得分
            # sheep_sector_scores: {sheep_code: {sector_name: score, ...}}
            sheep_sector_scores = defaultdict(lambda: defaultdict(float))
            
            for sheep_code, concepts in sheep_concepts_map.items():
                hot_weight = sheep_hot_scores.get(sheep_code, 1.0)
                
                for concept_name, concept_weight in concepts:
                    # 过滤无意义的概念
                    if should_filter_concept(concept_name):
                        continue
                    
                    # 通过聚类映射找到对应的聚类后板块
                    clustered_sector = concept_to_clustered_sector.get(concept_name, concept_name)
                    
                    # 过滤无意义的板块
                    if should_filter_concept(clustered_sector):
                        continue
                    
                    # 计算匹配得分：概念权重 × 热度权重
                    score = concept_weight * hot_weight
                    sheep_sector_scores[sheep_code][clustered_sector] += score
            
            # 8. 分配肥羊到板块
            sheep_to_sector = {}
            sector_stocks = defaultdict(lambda: {
                'stocks': [],
                'best_rank': float('inf'),
                'weighted_score': 0.0  # 加权热度分数
            })
            
            for sheep_code in sheep_codes:
                if sheep_code in sheep_sector_scores:
                    sector_scores = sheep_sector_scores[sheep_code]
                    if sector_scores:
                        # 选择得分最高的板块
                        best_sector = max(sector_scores.items(), key=lambda x: x[1])[0]
                        if not should_filter_concept(best_sector):
                            sheep_to_sector[sheep_code] = best_sector
                            
                            # 找到肥羊信息
                            sheep_info = next((s for s in hot_sheep if s['sheep_code'] == sheep_code), None)
                            if sheep_info:
                                sector_stocks[best_sector]['stocks'].append({
                                    'sheep_code': sheep_code,
                                    'sheep_name': sheep_info.get('sheep_name', ''),
                                    'rank': sheep_info.get('rank', 999)
                                })
                                sector_stocks[best_sector]['best_rank'] = min(
                                    sector_stocks[best_sector]['best_rank'],
                                    sheep_info.get('rank', 999)
                                )
                                sector_stocks[best_sector]['weighted_score'] += sheep_hot_scores.get(sheep_code, 1.0)
            
            # 9. 排序并选择Top板块（按加权热度分数和肥羊数量排序）
            sorted_sectors = sorted(
                sector_stocks.items(),
                key=lambda x: (x[1]['weighted_score'], len(x[1]['stocks']), -x[1]['best_rank']),
                reverse=True
            )
            
            # 10. 构建返回结果
            result = []
            for idx, (sector_name, sector_data) in enumerate(sorted_sectors[:max_sectors]):
                # 过滤无意义的概念
                if should_filter_concept(sector_name):
                    continue
                
                # 对肥羊按排名排序
                stocks = sorted(sector_data['stocks'], key=lambda x: x['rank'])
                
                # 确定颜色：最热的2个板块分别红色、橙色，其他为蓝色
                if idx == 0:
                    color = 'red'
                elif idx == 1:
                    color = 'orange'
                else:
                    color = 'blue'
                
                result.append({
                    'sector_name': sector_name,
                    'hot_count': len(stocks),  # 热门肥羊数量
                    'hot_score': len(stocks),  # 保持兼容性：hot_score为肥羊数量
                    'hot_sheep': stocks[:5],  # 只返回前5只肥羊
                    'color': color
                })
            
            return result
            
        except Exception as e:
            logger.error(f"板块匹配失败: {e}", exc_info=True)
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def _get_sheep_concepts_batch(self, sheep_codes: List[str]) -> Dict[str, List[Tuple[str, float]]]:
        """
        批量获取肥羊的概念列表（优化：单次查询）
        
        Returns:
            {sheep_code: [(concept_name, weight), ...]}
        """
        if not sheep_codes:
            return {}
        
        try:
            with get_db() as db:
                # 构建参数化查询
                params = {}
                placeholders = []
                for i, code in enumerate(sheep_codes):
                    param_name = f'code_{i}'
                    placeholders.append(f':{param_name}')
                    params[param_name] = code
                
                # 使用text()和参数化查询
                query_str = f"""
                    SELECT 
                        scm.sheep_code,
                        ct.concept_name,
                        scm.weight
                    FROM sheep_concept_mapping scm
                    INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                    WHERE scm.sheep_code IN ({','.join(placeholders)})
                      AND ct.is_active = 1
                    ORDER BY scm.sheep_code, scm.weight DESC
                """
                
                query = text(query_str)
                result = db.execute(query, params)
                
                # 构建映射，保留原始概念名
                stock_concepts = defaultdict(list)
                for row in result:
                    sheep_code = row[0]
                    concept_name = row[1]
                    weight = float(row[2]) if row[2] else 1.0
                    
                    # 保留原始概念名，聚类将在后续步骤中处理
                    stock_concepts[sheep_code].append((concept_name, weight))
                
                return dict(stock_concepts)
                
        except Exception as e:
            logger.error(f"批量获取肥羊概念失败: {e}", exc_info=True)
            import traceback
            logger.error(traceback.format_exc())
            return {}
