"""
概念板块数据采集适配器
"""
import akshare as ak
import pandas as pd
from typing import List, Dict, Optional
import logging
import time

logger = logging.getLogger(__name__)

class ConceptAdapter:
    """概念板块数据采集适配器"""
    
    @staticmethod
    def get_concept_list(source: str = 'ths') -> Optional[pd.DataFrame]:
        """
        获取概念板块列表
        
        Args:
            source: 数据源（ths=同花顺，em=东财）
            
        Returns:
            概念列表DataFrame
        """
        try:
            concept_list = None
            
            if source == 'ths':
                # 优先使用同花顺
                if hasattr(ak, 'stock_board_concept_name_ths'):
                    try:
                        concept_list = ak.stock_board_concept_name_ths()
                        if concept_list is not None and not concept_list.empty:
                            logger.info(f"使用同花顺接口获取到 {len(concept_list)} 个概念")
                            return concept_list
                    except Exception as e:
                        logger.warning(f"同花顺接口获取失败: {e}")
            
            # 降级到东财
            if source == 'em' or concept_list is None or concept_list.empty:
                if hasattr(ak, 'stock_board_concept_name_em'):
                    try:
                        concept_list = ak.stock_board_concept_name_em()
                        if concept_list is not None and not concept_list.empty:
                            logger.info(f"使用东财接口获取到 {len(concept_list)} 个概念")
                            return concept_list
                    except Exception as e:
                        logger.warning(f"东财接口获取失败: {e}")
            
            logger.warning("无法获取概念列表")
            return None
            
        except Exception as e:
            logger.error(f"获取概念列表失败: {e}", exc_info=True)
            return None
    
    @staticmethod
    def get_concept_stocks(concept_name: str, concept_code: Optional[str] = None, source: str = 'ths') -> Optional[List[str]]:
        """
        获取概念板块下的股票列表
        
        Args:
            concept_name: 概念名称
            concept_code: 概念代码（可选）
            source: 数据源
            
        Returns:
            股票代码列表
        """
        try:
            stocks_df = None
            
            # 方法1: 使用同花顺接口
            if source == 'ths' and hasattr(ak, 'stock_board_concept_cons_ths'):
                try:
                    if concept_code:
                        stocks_df = ak.stock_board_concept_cons_ths(symbol=concept_code)
                    else:
                        stocks_df = ak.stock_board_concept_cons_ths(symbol=concept_name)
                except:
                    pass
            
            # 方法2: 降级到东财
            if stocks_df is None or stocks_df.empty:
                if hasattr(ak, 'stock_board_concept_cons_em'):
                    try:
                        stocks_df = ak.stock_board_concept_cons_em(symbol=concept_name)
                    except:
                        pass
            
            if stocks_df is None or stocks_df.empty:
                return None
            
            # 提取股票代码
            stock_codes = []
            possible_code_columns = ['代码', '股票代码', 'code', '证券代码', 'symbol']
            
            for col_name in possible_code_columns:
                if col_name in stocks_df.columns:
                    stock_codes = stocks_df[col_name].astype(str).tolist()
                    break
            
            if not stock_codes and len(stocks_df.columns) > 0:
                stock_codes = stocks_df.iloc[:, 0].astype(str).tolist()
            
            # 标准化股票代码（只保留6位数字）
            clean_codes = []
            for code in stock_codes:
                clean_code = ''.join(filter(str.isdigit, str(code)))
                if len(clean_code) >= 6:
                    clean_codes.append(clean_code[:6].zfill(6))
            
            return clean_codes if clean_codes else None
            
        except Exception as e:
            logger.debug(f"获取概念 {concept_name} 的股票列表失败: {e}")
            return None
