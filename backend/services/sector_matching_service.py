"""
板块匹配服务层
实现基于虚拟板块聚合的热门股板块匹配算法
"""
from typing import List, Dict, Optional, Tuple, Set
import logging
from collections import defaultdict
from db.virtual_board_repository import VirtualBoardRepository
from etl.concept_filter import should_filter_concept
from sqlalchemy import text
from db.database import get_db

logger = logging.getLogger(__name__)

class SectorMatchingService:
    """板块匹配服务 - 基于虚拟板块聚合的算法（优化版：引入权重机制）"""
    
    def __init__(self):
        self._concept_to_virtual_boards_cache: Optional[Dict[str, List[str]]] = None
        self._concept_to_virtual_boards_weight_cache: Optional[Dict[str, List[Tuple[str, float]]]] = None
    
    def _load_concept_to_virtual_boards(self):
        """加载概念到虚拟板块的映射缓存（兼容旧接口）"""
        if self._concept_to_virtual_boards_cache is None:
            self._concept_to_virtual_boards_cache = VirtualBoardRepository.get_concept_to_virtual_boards()
        return self._concept_to_virtual_boards_cache
    
    def _load_concept_to_virtual_boards_with_weight(self):
        """加载概念到虚拟板块的映射缓存（带权重）"""
        if self._concept_to_virtual_boards_weight_cache is None:
            self._concept_to_virtual_boards_weight_cache = VirtualBoardRepository.get_concept_to_virtual_boards_with_weight()
        return self._concept_to_virtual_boards_weight_cache
    
    def match_hot_sheep_to_sectors(
        self, 
        hot_sheep: List[Dict], 
        max_sectors: int = 12
    ) -> List[Dict]:
        """
        匹配热门股到主板块（优化版：引入权重机制和热度分数）
        
        算法逻辑（优化）：
        1. 获取top100热门股的所有概念（细分概念）及其权重
        2. 通过virtual_board_aggregation，将细分概念映射到主板块（考虑权重）
        3. 对于每个肥羊，计算每个可能主板块的匹配得分：
           - 得分 = Σ(概念权重 × 虚拟板块权重 × 热度分数权重)
           - 热度分数权重 = 1 / (rank + 1) 或 hot_score（如果可用）
        4. 对于每个肥羊：
           - 如果只匹配到一个主板块，归到这个板块
           - 如果匹配到多个主板块，选择匹配得分最高的主板块
        5. 按板块加权热度分数排序
        
        Args:
            hot_sheep: 热门股列表，每个元素包含 sheep_code, sheep_name, rank, hot_score 等
            max_sectors: 返回的最大板块数量
            
        Returns:
            板块列表，每个板块包含 sector_name, hot_score, hot_sheep, color
        """
        if not hot_sheep:
            return []
        
        try:
            # 1. 加载概念到虚拟板块的映射（带权重）
            concept_to_virtual_boards_weight = self._load_concept_to_virtual_boards_with_weight()
            
            # 2. 批量获取所有热门股的概念（优化：单次查询，包含权重）
            sheep_codes = [s['sheep_code'] for s in hot_sheep]
            sheep_concepts_map = self._get_sheep_concepts_batch(sheep_codes)
            
            # 3. 构建肥羊热度映射（用于权重计算）
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
            
            # 4. 为每个肥羊计算每个可能主板块的匹配得分
            # sheep_sector_scores: {sheep_code: {sector_name: score, ...}}
            sheep_sector_scores = defaultdict(lambda: defaultdict(float))
            
            for sheep_code, concepts in sheep_concepts_map.items():
                hot_weight = sheep_hot_scores.get(sheep_code, 1.0)
                
                for concept_name, concept_weight in concepts:
                    # 过滤无意义的概念
                    if should_filter_concept(concept_name):
                        continue
                    
                    # 通过映射找到主板块（带权重）
                    if concept_name in concept_to_virtual_boards_weight:
                        for virtual_board, board_weight in concept_to_virtual_boards_weight[concept_name]:
                            # 过滤无意义的主板块
                            if should_filter_concept(virtual_board):
                                continue
                            
                            # 计算匹配得分：概念权重 × 虚拟板块权重 × 热度权重
                            score = concept_weight * board_weight * hot_weight
                            sheep_sector_scores[sheep_code][virtual_board] += score
            
            # 5. 迭代式板块分配：先汇总热度，选择最热板块，然后从剩余肥羊中剔除
            sheep_to_sector = {}
            unassigned_sheep = set(sheep_codes)  # 未分配的肥羊
            sector_stocks = defaultdict(lambda: {
                'stocks': [],
                'best_rank': float('inf'),
                'weighted_score': 0.0  # 加权热度分数
            })
            
            # 迭代分配，直到所有肥羊都被分配或达到最大板块数
            while unassigned_sheep and len(sector_stocks) < max_sectors:
                # 5.1 计算当前未分配肥羊的板块热度汇总
                # sector_sheep_map: {sector_name: set(sheep_codes)} - 每个板块匹配的肥羊集合
                sector_sheep_map = defaultdict(set)
                sheep_info_map = {}  # 缓存肥羊信息
                
                for sheep_code in list(unassigned_sheep):
                    if sheep_code not in sheep_sector_scores:
                        continue
                    
                    sector_scores = sheep_sector_scores[sheep_code]
                    if not sector_scores:
                        continue
                    
                    # 缓存肥羊信息
                    if sheep_code not in sheep_info_map:
                        sheep_info_map[sheep_code] = next(
                            (s for s in hot_sheep if s['sheep_code'] == sheep_code), 
                            None
                        )
                    
                    # 将该肥羊添加到所有匹配的板块中
                    for sector_name in sector_scores.keys():
                        # 过滤无意义的概念
                        if not should_filter_concept(sector_name):
                            sector_sheep_map[sector_name].add(sheep_code)
                
                if not sector_sheep_map:
                    break
                
                # 5.2 选择最热的板块（按肥羊数量排序，数量相同时按总得分排序）
                # 注意：sheep_set是集合类型，存储该板块匹配的肥羊代码
                sector_aggregated = []
                for sector_name, sheep_set in sector_sheep_map.items():
                    total_score = 0.0
                    for sheep_code in sheep_set:
                        if sheep_code in sheep_sector_scores:
                            sector_scores = sheep_sector_scores[sheep_code]
                            if sector_name in sector_scores:
                                total_score += sector_scores[sector_name]
                    
                    sector_aggregated.append({
                        'sector_name': sector_name,
                        'sheep_count': len(sheep_set),
                        'total_score': total_score,
                        'sheep': sheep_set
                    })
                
                # 按肥羊数量和总得分排序
                sorted_sectors = sorted(
                    sector_aggregated,
                    key=lambda x: (x['sheep_count'], x['total_score']),
                    reverse=True
                )
                
                if not sorted_sectors:
                    break
                
                # 5.3 选择最热的板块
                best_sector = sorted_sectors[0]
                best_sector_name = best_sector['sector_name']
                best_sheep = best_sector['sheep']  # 修复：应该是'sheep'而不是'stocks'
                
                # 5.4 将该板块匹配的所有肥羊分配到该板块
                assigned_sheep = set()
                for sheep_code in best_sheep:
                    if sheep_code in unassigned_sheep:
                        sheep_to_sector[sheep_code] = best_sector_name
                        assigned_sheep.add(sheep_code)
                        
                        sheep_info = sheep_info_map.get(sheep_code)
                        if sheep_info:
                            # 添加到板块肥羊列表
                            sector_stocks[best_sector_name]['stocks'].append({
                                'sheep_code': sheep_code,
                                'sheep_name': sheep_info.get('sheep_name', ''),
                                'rank': sheep_info.get('rank', 999)
                            })
                            sector_stocks[best_sector_name]['best_rank'] = min(
                                sector_stocks[best_sector_name]['best_rank'],
                                sheep_info.get('rank', 999)
                            )
                            # 累加加权热度分数
                            sector_stocks[best_sector_name]['weighted_score'] += sheep_hot_scores.get(sheep_code, 1.0)
                
                # 5.5 从未分配列表中移除已分配的肥羊
                unassigned_sheep -= assigned_sheep
            
            # 6. 处理剩余未分配的肥羊（如果有）
            for sheep_code in unassigned_sheep:
                if sheep_code in sheep_sector_scores:
                    sector_scores = sheep_sector_scores[sheep_code]
                    if sector_scores:
                        # 选择得分最高的板块
                        best_sector = max(sector_scores.items(), key=lambda x: x[1])[0]
                        if not should_filter_concept(best_sector):
                            sheep_to_sector[sheep_code] = best_sector
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
            
            # 7. 排序并选择Top板块（按加权热度分数和肥羊数量排序）
            sorted_sectors = sorted(
                sector_stocks.items(),
                key=lambda x: (x[1]['weighted_score'], len(x[1]['stocks']), -x[1]['best_rank']),
                reverse=True
            )
            
            # 8. 构建返回结果
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
                
                # 构建映射
                stock_concepts = defaultdict(list)
                for row in result:
                    sheep_code = row[0]
                    concept_name = row[1]
                    weight = float(row[2]) if row[2] else 1.0
                    stock_concepts[sheep_code].append((concept_name, weight))
                
                return dict(stock_concepts)
                
        except Exception as e:
            logger.error(f"批量获取肥羊概念失败: {e}", exc_info=True)
            import traceback
            logger.error(traceback.format_exc())
            return {}
