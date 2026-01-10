"""
热度榜数据采集适配器
"""
import akshare as ak
import pandas as pd
from datetime import date
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class HotRankAdapter:
    """热度榜数据采集适配器"""
    
    @staticmethod
    def get_hot_rank(source: str = 'xueqiu') -> Optional[pd.DataFrame]:
        """
        获取热度榜数据
        
        Args:
            source: 数据源（xueqiu=雪球，dongcai=东财）
            
        Returns:
            热度榜DataFrame
        """
        try:
            if source == 'xueqiu':
                # 雪球热度榜
                hot_data = ak.stock_hot_rank_em()
                if hot_data is not None and not hot_data.empty:
                    logger.info(f"获取雪球热度榜 {len(hot_data)} 条")
                    return hot_data
            
            if source == 'dongcai':
                # 东财热度榜（如果akshare有对应接口）
                try:
                    hot_data = ak.stock_hot_rank_latest_em()
                    if hot_data is not None and not hot_data.empty:
                        logger.info(f"获取东财热度榜 {len(hot_data)} 条")
                        return hot_data
                except:
                    # 如果东财接口不可用，使用雪球数据
                    logger.warning("东财接口不可用，使用雪球数据")
                    return HotRankAdapter.get_hot_rank('xueqiu')
            
            return None
            
        except Exception as e:
            logger.error(f"获取热度榜失败: {e}", exc_info=True)
            return None
    
    @staticmethod
    def normalize_hot_rank_data(df: pd.DataFrame, source: str) -> Optional[pd.DataFrame]:
        """
        标准化热度榜数据格式
        
        Args:
            df: 原始DataFrame
            source: 数据源
            
        Returns:
            标准化后的DataFrame
        """
        if df is None or df.empty:
            return None
        
        try:
            # 调试：打印DataFrame的前几行和列名
            logger.debug(f"原始DataFrame形状: {df.shape}, 列名: {list(df.columns)}")
            if len(df) > 0:
                logger.debug(f"第一行数据: {df.iloc[0].to_dict()}")
            
            result_rows = []
            
            # 遍历每一行数据
            for idx, row in df.iterrows():
                stock_code = None
                stock_name = None
                
                # 尝试多种方式提取股票代码
                code_candidates = ['代码', '股票代码', 'code', '证券代码', 'symbol', '股票代码']
                for col in code_candidates:
                    if col in df.columns:
                        code_val = str(row[col]).strip()
                        # 只保留6位数字的股票代码
                        code_clean = ''.join(filter(str.isdigit, code_val))
                        if len(code_clean) == 6:
                            stock_code = code_clean
                            break
                
                # 如果没找到，尝试从第一列提取
                if not stock_code:
                    first_col_val = str(row.iloc[0]).strip()
                    code_clean = ''.join(filter(str.isdigit, first_col_val))
                    if len(code_clean) == 6:
                        stock_code = code_clean
                
                # 如果还是没找到有效的股票代码，跳过这一行
                if not stock_code or len(stock_code) != 6:
                    continue
                
                # 提取股票名称
                name_candidates = ['股票名称', '名称', 'name', '证券名称']
                for col in name_candidates:
                    if col in df.columns:
                        name_val = str(row[col]).strip()
                        # 过滤掉纯数字或明显不是股票名称的值
                        if name_val and not name_val.isdigit() and len(name_val) > 0:
                            stock_name = name_val
                            break
                
                # 如果没找到，尝试从第二列提取
                if not stock_name and len(df.columns) > 1:
                    second_col_val = str(row.iloc[1]).strip()
                    if second_col_val and not second_col_val.isdigit() and len(second_col_val) > 0:
                        stock_name = second_col_val
                
                # 如果还是没找到，使用默认值
                if not stock_name:
                    stock_name = ''
                
                # 提取排名（优先从数据中获取，否则使用索引+1）
                rank = idx + 1
                rank_candidates = ['排名', 'rank', '序号', '排名变化']
                for col in rank_candidates:
                    if col in df.columns:
                        try:
                            rank_val = int(row[col])
                            if rank_val > 0:
                                rank = rank_val
                                break
                        except:
                            pass
                
                # 提取成交量
                volume = 0
                volume_candidates = ['成交量', 'volume', '成交额']
                for col in volume_candidates:
                    if col in df.columns:
                        try:
                            vol_val = float(row[col])
                            if vol_val > 0:
                                volume = int(vol_val)
                                break
                        except:
                            pass
                
                result_rows.append({
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'rank': rank,
                    'source': source,
                    'volume': volume
                })
            
            if not result_rows:
                logger.warning(f"未能从数据中提取到有效的股票信息")
                return None
            
            result = pd.DataFrame(result_rows)
            logger.info(f"标准化后得到 {len(result)} 条有效数据")
            
            return result
            
        except Exception as e:
            logger.error(f"标准化热度榜数据失败: {e}", exc_info=True)
            import traceback
            logger.error(traceback.format_exc())
            return None
