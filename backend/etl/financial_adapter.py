"""
财务数据采集适配器
"""
import akshare as ak
import pandas as pd
from typing import List, Dict, Optional
from datetime import date, datetime
import logging
import time

logger = logging.getLogger(__name__)

class FinancialAdapter:
    """财务数据采集适配器"""
    
    @staticmethod
    def get_financial_data(sheep_code: str, period: str = "report") -> Optional[pd.DataFrame]:
        """
        获取肥羊财务数据
        
        Args:
            sheep_code: 肥羊代码（6位数字）
            period: 报告期类型（report=年报季报, quarterly=季报）
            
        Returns:
            财务数据DataFrame
        """
        try:
            # 使用akshare获取财务摘要数据（同花顺）
            # 注意：需要将6位代码转换为akshare格式（如000001 -> 000001.SZ）
            market = 'SZ' if sheep_code.startswith(('0', '3')) else 'SH'
            ts_code = f"{sheep_code}.{market}"
            
            # 获取财务摘要（包含研发费用、净利润、营业收入等）
            df = ak.stock_financial_abstract_ths(symbol=sheep_code)
            
            if df is None or df.empty:
                logger.warning(f"肥羊 {sheep_code} 财务数据为空")
                return None
            
            logger.debug(f"获取到肥羊 {sheep_code} {len(df)} 条财务数据")
            return df
            
        except Exception as e:
            logger.error(f"获取肥羊 {sheep_code} 财务数据失败: {e}", exc_info=True)
            return None
    
    @staticmethod
    def normalize_financial_data(df: pd.DataFrame, sheep_code: str) -> List[Dict]:
        """
        标准化财务数据格式
        
        Args:
            df: 原始财务数据DataFrame
            sheep_code: 肥羊代码
            
        Returns:
            标准化后的财务数据列表
        """
        if df is None or df.empty:
            return []
        
        data_list = []
        
        try:
            # akshare返回的列名可能不同，需要适配
            # 常见的列名：报告期、研发费用、净利润、净利润同比增长、营业收入等
            
            for _, row in df.iterrows():
                try:
                    # 尝试提取报告期
                    report_date_str = None
                    if '报告期' in df.columns:
                        report_date_str = str(row['报告期']).strip()
                    elif '日期' in df.columns:
                        report_date_str = str(row['日期']).strip()
                    elif 'period' in df.columns:
                        report_date_str = str(row['period']).strip()
                    elif len(df.columns) > 0:
                        # 如果第一列是日期
                        first_col = df.columns[0]
                        if 'date' in first_col.lower() or '期' in first_col:
                            report_date_str = str(row[first_col]).strip()
                    
                    if not report_date_str or report_date_str == 'nan':
                        continue
                    
                    # 解析报告期日期（格式可能是：2024-03-31 或 20240331 或 2024年第一季度）
                    report_date = FinancialAdapter._parse_report_date(report_date_str)
                    if not report_date:
                        continue
                    
                    # 提取财务指标
                    rd_exp = FinancialAdapter._extract_value(row, ['研发费用', '研发支出', 'rd_exp', '研发'])
                    net_profit = FinancialAdapter._extract_value(row, ['净利润', 'net_profit', '归母净利润', '净利润(元)'])
                    net_profit_growth = FinancialAdapter._extract_value(row, ['净利润同比增长', '净利润增长率', 'net_profit_growth', '净利润同比增长(%)'])
                    total_revenue = FinancialAdapter._extract_value(row, ['营业收入', '营业总收入', 'total_revenue', '营业收入(元)'])
                    
                    data = {
                        'sheep_code': sheep_code,
                        'report_date': report_date,
                        'rd_exp': rd_exp,
                        'net_profit': net_profit,
                        'net_profit_growth': net_profit_growth,
                        'total_revenue': total_revenue
                    }
                    
                    data_list.append(data)
                    
                except Exception as e:
                    logger.debug(f"处理财务数据行失败: {e}")
                    continue
            
            logger.info(f"标准化财务数据完成，共 {len(data_list)} 条")
            return data_list
            
        except Exception as e:
            logger.error(f"标准化财务数据失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def _parse_report_date(date_str: str) -> Optional[date]:
        """
        解析报告期日期
        
        Args:
            date_str: 日期字符串（可能是多种格式）
            
        Returns:
            date对象，解析失败返回None
        """
        if not date_str or pd.isna(date_str):
            return None
        
        date_str = str(date_str).strip()
        
        # 尝试多种日期格式
        formats = [
            '%Y-%m-%d',
            '%Y%m%d',
            '%Y-%m',
            '%Y年%m月',
            '%Y/%m/%d',
        ]
        
        for fmt in formats:
            try:
                if fmt == '%Y年%m月':
                    # 处理季度格式，如：2024年第一季度 -> 2024-03-31
                    if '第一' in date_str or 'Q1' in date_str.upper():
                        year = date_str.split('年')[0]
                        return datetime.strptime(f"{year}-03-31", '%Y-%m-%d').date()
                    elif '第二' in date_str or 'Q2' in date_str.upper():
                        year = date_str.split('年')[0]
                        return datetime.strptime(f"{year}-06-30", '%Y-%m-%d').date()
                    elif '第三' in date_str or 'Q3' in date_str.upper():
                        year = date_str.split('年')[0]
                        return datetime.strptime(f"{year}-09-30", '%Y-%m-%d').date()
                    elif '第四' in date_str or 'Q4' in date_str.upper() or '年报' in date_str:
                        year = date_str.split('年')[0]
                        return datetime.strptime(f"{year}-12-31", '%Y-%m-%d').date()
                else:
                    parsed = datetime.strptime(date_str[:len(fmt.replace('%', ''))], fmt)
                    return parsed.date()
            except (ValueError, IndexError):
                continue
        
        logger.warning(f"无法解析日期格式: {date_str}")
        return None
    
    @staticmethod
    def _extract_value(row: pd.Series, possible_columns: List[str]) -> Optional[float]:
        """
        从行中提取数值，尝试多个可能的列名
        
        Args:
            row: DataFrame行
            possible_columns: 可能的列名列表
            
        Returns:
            提取的数值，失败返回None
        """
        for col in possible_columns:
            if col in row.index:
                value = row[col]
                if pd.notna(value):
                    try:
                        # 处理可能的字符串格式（如：带逗号的数字）
                        if isinstance(value, str):
                            value = value.replace(',', '').replace('，', '')
                        return float(value)
                    except (ValueError, TypeError):
                        continue
        return None
