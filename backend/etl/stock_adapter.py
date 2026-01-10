"""
股票数据采集适配器
"""
import akshare as ak
import pandas as pd
from datetime import date, datetime
from typing import List, Dict, Optional
import logging
import time

logger = logging.getLogger(__name__)

class StockAdapter:
    """股票数据采集适配器，封装akshare股票相关接口"""
    
    @staticmethod
    def get_all_stock_codes() -> List[Dict[str, str]]:
        """
        获取A股市场全量股票代码
        
        Returns:
            股票列表，每个元素包含code和name
        """
        all_stocks = []
        
        try:
            # 优先使用stock_info_a_code_name
            stock_list = ak.stock_info_a_code_name()
            if stock_list is not None and not stock_list.empty:
                if 'code' in stock_list.columns and 'name' in stock_list.columns:
                    for _, row in stock_list.iterrows():
                        code = str(row['code']).strip()
                        name = str(row['name']).strip()
                        if code and len(code) >= 6:
                            clean_code = code[:6].zfill(6)
                            all_stocks.append({'code': clean_code, 'name': name})
                
                logger.info(f"获取到 {len(all_stocks)} 只股票")
                return all_stocks
        except Exception as e:
            logger.warning(f"获取股票列表失败: {e}")
        
        return all_stocks
    
    @staticmethod
    def get_stock_daily_data(stock_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取股票日K数据
        
        Args:
            stock_code: 股票代码（6位数字）
            start_date: 开始日期（YYYYMMDD）
            end_date: 结束日期（YYYYMMDD）
            
        Returns:
            包含日K数据的DataFrame，如果失败返回None
        """
        try:
            # 标准化股票代码
            clean_code = str(stock_code).strip().zfill(6)
            
            # 调用akshare接口
            df = ak.stock_zh_a_hist(
                symbol=clean_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=""
            )
            
            if df is None or df.empty:
                return None
            
            # 标准化列名
            column_mapping = {
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '涨跌幅': 'change_pct',
                '换手率': 'turnover_rate'
            }
            
            df = df.rename(columns=column_mapping)
            
            # 确保date列存在
            if 'date' not in df.columns:
                if len(df.columns) > 0:
                    df = df.rename(columns={df.columns[0]: 'date'})
                else:
                    return None
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['stock_code'] = clean_code
            
            # 计算均线
            df = df.sort_values('date')
            df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()
            df['ma10'] = df['close'].rolling(window=10, min_periods=1).mean()
            df['ma20'] = df['close'].rolling(window=20, min_periods=1).mean()
            df['ma30'] = df['close'].rolling(window=30, min_periods=1).mean()
            df['ma60'] = df['close'].rolling(window=60, min_periods=1).mean()
            
            return df
            
        except Exception as e:
            logger.warning(f"获取股票 {stock_code} 日K数据失败: {e}")
            return None
    
    @staticmethod
    def get_stock_money_flow(stock_code: str) -> Optional[Dict]:
        """
        获取股票资金流向数据（今日）
        
        Args:
            stock_code: 股票代码
            
        Returns:
            资金流向字典，如果失败返回None
        """
        try:
            # 尝试获取个股资金流
            market_code = stock_code
            if not stock_code.startswith(('0', '3', '6', '8')):
                if stock_code.startswith('6'):
                    market_code = f"sh{stock_code}"
                else:
                    market_code = f"sz{stock_code}"
            
            flow_df = ak.stock_individual_fund_flow(stock=market_code, indicator="资金流向")
            
            if flow_df is None or flow_df.empty:
                return None
            
            # 解析资金流数据（根据实际列名调整）
            result = {
                'main_net_inflow': 0,
                'super_large_inflow': 0,
                'large_inflow': 0,
                'medium_inflow': 0,
                'small_inflow': 0
            }
            
            # 尝试匹配列名（不同接口可能不同）
            for col in flow_df.columns:
                col_str = str(col)
                if '主力' in col_str and '净流入' in col_str:
                    val = flow_df[col].iloc[0] if len(flow_df) > 0 else 0
                    result['main_net_inflow'] = float(val) / 10000 if pd.notna(val) else 0
                elif '超大单' in col_str and '净流入' in col_str:
                    val = flow_df[col].iloc[0] if len(flow_df) > 0 else 0
                    result['super_large_inflow'] = float(val) / 10000 if pd.notna(val) else 0
                elif '大单' in col_str and '超大' not in col_str and '净流入' in col_str:
                    val = flow_df[col].iloc[0] if len(flow_df) > 0 else 0
                    result['large_inflow'] = float(val) / 10000 if pd.notna(val) else 0
            
            return result
            
        except Exception as e:
            logger.debug(f"获取股票 {stock_code} 资金流失败: {e}")
            return None
    
    @staticmethod
    def get_all_stocks_money_flow() -> Optional[pd.DataFrame]:
        """
        批量获取所有股票的资金流向数据（今日）
        
        Returns:
            包含所有股票资金流的DataFrame
        """
        try:
            flow_data = ak.stock_individual_fund_flow_rank(indicator="今日")
            
            if flow_data is None or flow_data.empty:
                return None
            
            # 标准化列名和数据类型
            result_df = pd.DataFrame()
            
            if '代码' in flow_data.columns:
                result_df['stock_code'] = flow_data['代码'].astype(str).str.zfill(6)
            elif '股票代码' in flow_data.columns:
                result_df['stock_code'] = flow_data['股票代码'].astype(str).str.zfill(6)
            
            # 解析资金流列
            money_flow_cols = {
                'main_net_inflow': ['今日主力净流入-净额', '主力净流入'],
                'super_large_inflow': ['今日超大单净流入-净额', '超大单净流入'],
                'large_inflow': ['今日大单净流入-净额', '大单净流入'],
                'medium_inflow': ['今日中单净流入-净额', '中单净流入'],
                'small_inflow': ['今日小单净流入-净额', '小单净流入']
            }
            
            for target_col, source_cols in money_flow_cols.items():
                for src_col in source_cols:
                    if src_col in flow_data.columns:
                        result_df[target_col] = flow_data[src_col].fillna(0).astype(float) / 10000
                        break
                else:
                    result_df[target_col] = 0.0
            
            return result_df
            
        except Exception as e:
            logger.error(f"批量获取资金流数据失败: {e}", exc_info=True)
            return None
