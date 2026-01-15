"""
板块资金流向数据采集适配器
"""
import akshare as ak
import pandas as pd
from typing import List, Dict, Optional
from datetime import date
import logging
import time

logger = logging.getLogger(__name__)

class SectorMoneyFlowAdapter:
    """板块资金流向数据采集适配器"""
    
    @staticmethod
    def get_sector_money_flow_today(sector_type: str = '概念资金流') -> Optional[pd.DataFrame]:
        """
        获取今日板块资金流向数据
        
        Args:
            sector_type: 板块类型（'行业资金流'、'概念资金流'、'地域资金流'）
            
        Returns:
            板块资金流向DataFrame
        """
        try:
            # 使用akshare获取今日板块资金流排名
            df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type=sector_type)
            
            if df is None or df.empty:
                logger.warning(f"获取{sector_type}资金流数据为空")
                return None
            
            logger.info(f"获取到 {len(df)} 条{sector_type}资金流数据")
            return df
            
        except Exception as e:
            logger.error(f"获取{sector_type}资金流数据失败: {e}", exc_info=True)
            return None
    
    @staticmethod
    def normalize_sector_money_flow(df: pd.DataFrame, sector_type: str = '概念资金流') -> List[Dict]:
        """
        标准化板块资金流向数据
        
        Args:
            df: 原始DataFrame
            sector_type: 板块类型
            
        Returns:
            标准化后的数据列表
        """
        if df is None or df.empty:
            return []
        
        data_list = []
        
        # 列名映射（akshare返回的列名可能是中文，且带有"今日"前缀）
        column_mapping = {
            '名称': 'sector_name',
            '板块名称': 'sector_name',
            # 带"今日"前缀的列名（akshare新版本）
            '今日主力净流入-净额': 'main_net_inflow',
            '今日超大单净流入-净额': 'super_large_inflow',
            '今日大单净流入-净额': 'large_inflow',
            '今日中单净流入-净额': 'medium_inflow',
            '今日小单净流入-净额': 'small_inflow',
            # 不带前缀的列名（兼容旧版本）
            '主力净流入-净额': 'main_net_inflow',
            '主力净流入': 'main_net_inflow',
            '超大单净流入-净额': 'super_large_inflow',
            '超大单净流入': 'super_large_inflow',
            '大单净流入-净额': 'large_inflow',
            '大单净流入': 'large_inflow',
            '中单净流入-净额': 'medium_inflow',
            '中单净流入': 'medium_inflow',
            '小单净流入-净额': 'small_inflow',
            '小单净流入': 'small_inflow',
        }
        
        # 标准化列名
        normalized_df = df.copy()
        for old_col, new_col in column_mapping.items():
            if old_col in normalized_df.columns:
                normalized_df[new_col] = normalized_df[old_col]
        
        # 提取数据
        for _, row in normalized_df.iterrows():
            try:
                # 获取板块名称
                sector_name = None
                for col in ['sector_name', '名称', '板块名称']:
                    if col in row.index and pd.notna(row[col]):
                        sector_name = str(row[col]).strip()
                        break
                
                if not sector_name:
                    continue
                
                # 获取资金流数据（转换为万元）
                main_net_inflow = 0.0
                super_large_inflow = 0.0
                large_inflow = 0.0
                medium_inflow = 0.0
                small_inflow = 0.0
                
                # 主力净流入（可能是元，需要转换为万元）
                if 'main_net_inflow' in row.index and pd.notna(row['main_net_inflow']):
                    try:
                        main_net_inflow = float(row['main_net_inflow']) / 10000  # 转换为万元
                    except (ValueError, TypeError):
                        pass
                
                # 超大单
                if 'super_large_inflow' in row.index and pd.notna(row['super_large_inflow']):
                    try:
                        super_large_inflow = float(row['super_large_inflow']) / 10000
                    except (ValueError, TypeError):
                        pass
                
                # 大单
                if 'large_inflow' in row.index and pd.notna(row['large_inflow']):
                    try:
                        large_inflow = float(row['large_inflow']) / 10000
                    except (ValueError, TypeError):
                        pass
                
                # 中单
                if 'medium_inflow' in row.index and pd.notna(row['medium_inflow']):
                    try:
                        medium_inflow = float(row['medium_inflow']) / 10000
                    except (ValueError, TypeError):
                        pass
                
                # 小单
                if 'small_inflow' in row.index and pd.notna(row['small_inflow']):
                    try:
                        small_inflow = float(row['small_inflow']) / 10000
                    except (ValueError, TypeError):
                        pass
                
                data_list.append({
                    'sector_name': sector_name,
                    'trade_date': date.today(),
                    'main_net_inflow': main_net_inflow,
                    'super_large_inflow': super_large_inflow,
                    'large_inflow': large_inflow,
                    'medium_inflow': medium_inflow,
                    'small_inflow': small_inflow,
                })
                
            except Exception as e:
                logger.debug(f"处理板块资金流数据行失败: {e}")
                continue
        
        return data_list
    
    @staticmethod
    def get_all_sector_money_flow_today() -> List[Dict]:
        """
        获取所有类型的板块资金流向数据（今日）
        
        Returns:
            所有板块资金流向数据列表
        """
        all_data = []
        
        # 获取概念资金流
        concept_df = SectorMoneyFlowAdapter.get_sector_money_flow_today('概念资金流')
        if concept_df is not None:
            concept_data = SectorMoneyFlowAdapter.normalize_sector_money_flow(concept_df, '概念资金流')
            all_data.extend(concept_data)
            logger.info(f"获取到 {len(concept_data)} 条概念资金流数据")
        
        # 延迟，避免请求过快
        time.sleep(0.5)
        
        # 获取行业资金流
        industry_df = SectorMoneyFlowAdapter.get_sector_money_flow_today('行业资金流')
        if industry_df is not None:
            industry_data = SectorMoneyFlowAdapter.normalize_sector_money_flow(industry_df, '行业资金流')
            all_data.extend(industry_data)
            logger.info(f"获取到 {len(industry_data)} 条行业资金流数据")
        
        return all_data
    
    @staticmethod
    def get_industry_money_flow_hist(industry_name: str) -> List[Dict]:
        """
        获取单个行业的历史资金流数据（约120天）
        
        Args:
            industry_name: 行业名称（如：半导体、消费电子等）
            
        Returns:
            历史资金流数据列表
        """
        try:
            df = ak.stock_sector_fund_flow_hist(symbol=industry_name)
            
            if df is None or df.empty:
                logger.warning(f"获取行业 {industry_name} 历史数据为空")
                return []
            
            data_list = []
            for _, row in df.iterrows():
                try:
                    trade_date = pd.to_datetime(row['日期']).date()
                    
                    # 获取资金流数据（转换为万元）
                    main_net_inflow = float(row.get('主力净流入-净额', 0)) / 10000 if pd.notna(row.get('主力净流入-净额')) else 0.0
                    super_large_inflow = float(row.get('超大单净流入-净额', 0)) / 10000 if pd.notna(row.get('超大单净流入-净额')) else 0.0
                    large_inflow = float(row.get('大单净流入-净额', 0)) / 10000 if pd.notna(row.get('大单净流入-净额')) else 0.0
                    medium_inflow = float(row.get('中单净流入-净额', 0)) / 10000 if pd.notna(row.get('中单净流入-净额')) else 0.0
                    small_inflow = float(row.get('小单净流入-净额', 0)) / 10000 if pd.notna(row.get('小单净流入-净额')) else 0.0
                    
                    data_list.append({
                        'sector_name': industry_name,
                        'trade_date': trade_date,
                        'main_net_inflow': main_net_inflow,
                        'super_large_inflow': super_large_inflow,
                        'large_inflow': large_inflow,
                        'medium_inflow': medium_inflow,
                        'small_inflow': small_inflow,
                    })
                except Exception as e:
                    logger.debug(f"处理行业 {industry_name} 历史数据行失败: {e}")
                    continue
            
            return data_list
            
        except Exception as e:
            logger.error(f"获取行业 {industry_name} 历史数据失败: {e}")
            return []
    
    @staticmethod
    def get_all_industry_money_flow_hist(days: int = 90) -> List[Dict]:
        """
        获取所有行业的历史资金流数据
        
        Args:
            days: 获取最近N天的数据，默认90天
            
        Returns:
            所有行业的历史资金流数据列表
        """
        from datetime import date, timedelta
        
        all_data = []
        cutoff_date = date.today() - timedelta(days=days)
        
        # 先获取行业列表
        industry_df = SectorMoneyFlowAdapter.get_sector_money_flow_today('行业资金流')
        if industry_df is None or industry_df.empty:
            logger.warning("无法获取行业列表")
            return []
        
        # 获取行业名称列表
        industry_names = industry_df['名称'].tolist() if '名称' in industry_df.columns else []
        
        logger.info(f"开始采集 {len(industry_names)} 个行业的历史资金流数据（最近{days}天）...")
        
        for idx, industry_name in enumerate(industry_names):
            try:
                hist_data = SectorMoneyFlowAdapter.get_industry_money_flow_hist(industry_name)
                
                # 只保留最近N天的数据
                filtered_data = [d for d in hist_data if d['trade_date'] >= cutoff_date]
                all_data.extend(filtered_data)
                
                if (idx + 1) % 10 == 0:
                    logger.info(f"进度: {idx + 1}/{len(industry_names)}，累计 {len(all_data)} 条数据")
                
                # 延迟，避免请求过快
                time.sleep(0.3)
                
            except Exception as e:
                logger.warning(f"获取行业 {industry_name} 历史数据失败: {e}")
                continue
        
        logger.info(f"行业历史资金流数据采集完成，共 {len(all_data)} 条")
        return all_data
