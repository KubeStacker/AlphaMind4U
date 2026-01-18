"""
概念板块数据采集适配器
支持同花顺和东财数据源，提供统一的概念数据获取接口
"""
import akshare as ak
import pandas as pd
from typing import List, Dict, Optional
import logging
import time

logger = logging.getLogger(__name__)

class ConceptAdapter:
    """概念板块数据采集适配器"""
    
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # 秒
    
    @classmethod
    def get_concept_list(cls, source: str = 'ths', retry: bool = False) -> Optional[pd.DataFrame]:
        """
        获取概念板块列表
        
        Args:
            source: 数据源（ths=同花顺，em=东财）
            retry: 是否启用重试机制（默认False，用于增量同步时启用）
            
        Returns:
            概念列表DataFrame
        """
        max_attempts = cls.MAX_RETRIES if retry else 1
        
        for attempt in range(max_attempts):
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
                
                if retry and attempt < max_attempts - 1:
                    logger.warning(f"第{attempt + 1}次获取概念列表失败，{cls.RETRY_DELAY * (attempt + 1)}秒后重试...")
                    time.sleep(cls.RETRY_DELAY * (attempt + 1))  # 指数退避
                    continue
                
                logger.warning("无法获取概念列表")
                return None
                
            except Exception as e:
                if retry and attempt < max_attempts - 1:
                    logger.warning(f"第{attempt + 1}次获取概念列表失败: {e}，{cls.RETRY_DELAY * (attempt + 1)}秒后重试...")
                    time.sleep(cls.RETRY_DELAY * (attempt + 1))
                    continue
                logger.error(f"获取概念列表失败: {e}", exc_info=True)
                return None
        
        return None
    
    @classmethod
    def get_concept_stocks(cls, concept_name: str, concept_code: Optional[str] = None, 
                          source: str = 'ths', retry: bool = False) -> Optional[List[str]]:
        """
        获取概念板块下的肥羊列表
        
        Args:
            concept_name: 概念名称
            concept_code: 概念代码（可选）
            source: 数据源
            retry: 是否启用重试机制（默认False，用于增量同步时启用）
            
        Returns:
            肥羊代码列表
        """
        max_attempts = cls.MAX_RETRIES if retry else 1
        
        for attempt in range(max_attempts):
            try:
                stocks_df = None
                
                # 方法1: 使用同花顺接口
                if source == 'ths' and hasattr(ak, 'stock_board_concept_cons_ths'):
                    try:
                        if concept_code:
                            stocks_df = ak.stock_board_concept_cons_ths(symbol=concept_code)
                        else:
                            stocks_df = ak.stock_board_concept_cons_ths(symbol=concept_name)
                    except Exception as e:
                        if not retry:
                            logger.debug(f"同花顺接口获取失败: {e}")
                
                # 方法2: 降级到东财
                if stocks_df is None or stocks_df.empty:
                    if hasattr(ak, 'stock_board_concept_cons_em'):
                        try:
                            stocks_df = ak.stock_board_concept_cons_em(symbol=concept_name)
                        except Exception as e:
                            if not retry:
                                logger.debug(f"东财接口获取失败: {e}")
                
                if stocks_df is None or stocks_df.empty:
                    if retry and attempt < max_attempts - 1:
                        logger.warning(f"第{attempt + 1}次获取概念 {concept_name} 的成分股失败，{cls.RETRY_DELAY * (attempt + 1)}秒后重试...")
                        time.sleep(cls.RETRY_DELAY * (attempt + 1))
                        continue
                    return None
                
                # 提取肥羊代码
                sheep_codes = []
                possible_code_columns = ['代码', '肥羊代码', 'code', '证券代码', 'symbol', '肥羊代码']
                
                for col_name in possible_code_columns:
                    if col_name in stocks_df.columns:
                        sheep_codes = stocks_df[col_name].astype(str).tolist()
                        break
                
                if not sheep_codes and len(stocks_df.columns) > 0:
                    sheep_codes = stocks_df.iloc[:, 0].astype(str).tolist()
                
                # 标准化肥羊代码（只保留6位数字）
                clean_codes = []
                for code in sheep_codes:
                    clean_code = ''.join(filter(str.isdigit, str(code)))
                    if len(clean_code) >= 6:
                        clean_codes.append(clean_code[:6].zfill(6))
                
                if clean_codes:
                    if retry:
                        logger.debug(f"概念 {concept_name} 包含 {len(clean_codes)} 只肥羊")
                    return clean_codes
                else:
                    if retry and attempt < max_attempts - 1:
                        logger.warning(f"第{attempt + 1}次无法提取概念 {concept_name} 的有效肥羊代码，{cls.RETRY_DELAY * (attempt + 1)}秒后重试...")
                        time.sleep(cls.RETRY_DELAY * (attempt + 1))
                        continue
                    return None
                    
            except Exception as e:
                if retry and attempt < max_attempts - 1:
                    logger.warning(f"第{attempt + 1}次获取概念 {concept_name} 的成分股失败: {e}，{cls.RETRY_DELAY * (attempt + 1)}秒后重试...")
                    time.sleep(cls.RETRY_DELAY * (attempt + 1))
                    continue
                logger.debug(f"获取概念 {concept_name} 的肥羊列表失败: {e}")
                return None
        
        return None
    
    # ========== 便捷方法（向后兼容，用于增量同步） ==========
    
    @classmethod
    def fetch_eastmoney_concepts(cls) -> Optional[pd.DataFrame]:
        """
        从EastMoney获取最新的概念列表（带重试机制）
        
        Returns:
            包含概念名称和代码的DataFrame
        """
        return cls.get_concept_list(source='em', retry=True)
    
    @classmethod
    def fetch_concept_constituents(cls, concept_name: str) -> Optional[List[str]]:
        """
        获取指定概念下的肥羊代码列表（带重试机制）
        
        Args:
            concept_name: 概念名称
            
        Returns:
            肥羊代码列表（6位数字字符串）
        """
        return cls.get_concept_stocks(concept_name=concept_name, source='em', retry=True)
