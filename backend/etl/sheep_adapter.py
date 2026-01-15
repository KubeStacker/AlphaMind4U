"""
肥羊数据采集适配器
"""
import akshare as ak
import pandas as pd
from datetime import date, datetime
from typing import List, Dict, Optional
import logging
import time

logger = logging.getLogger(__name__)

class SheepAdapter:
    """肥羊数据采集适配器，封装akshare肥羊相关接口"""
    
    @staticmethod
    def get_all_sheep_codes() -> List[Dict[str, str]]:
        """
        获取A股市场全量肥羊代码
        
        Returns:
            肥羊列表，每个元素包含code和name
        """
        all_stocks = []
        
        try:
            # 优先使用akshare的股票代码名称接口
            stock_list = ak.stock_info_a_code_name()
            if stock_list is not None and not stock_list.empty:
                if 'code' in stock_list.columns and 'name' in stock_list.columns:
                    for _, row in stock_list.iterrows():
                        code = str(row['code']).strip()
                        name = str(row['name']).strip()
                        if code and len(code) >= 6:
                            clean_code = code[:6].zfill(6)
                            all_stocks.append({'code': clean_code, 'name': name})
                
                logger.info(f"获取到 {len(all_stocks)} 只肥羊")
                return all_stocks
        except Exception as e:
            logger.warning(f"获取肥羊列表失败: {e}")
        
        return all_stocks
    
    @staticmethod
    def get_sheep_daily_data(sheep_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取肥羊日K数据
        
        Args:
            sheep_code: 肥羊代码（6位数字）
            start_date: 开始日期（YYYYMMDD）
            end_date: 结束日期（YYYYMMDD）
            
        Returns:
            包含日K数据的DataFrame，如果失败返回None
        """
        try:
            # 标准化肥羊代码
            clean_code = str(sheep_code).strip().zfill(6)
            
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
            df['sheep_code'] = clean_code
            
            # 计算均线
            df = df.sort_values('date')
            df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()
            df['ma10'] = df['close'].rolling(window=10, min_periods=1).mean()
            df['ma20'] = df['close'].rolling(window=20, min_periods=1).mean()
            df['ma30'] = df['close'].rolling(window=30, min_periods=1).mean()
            df['ma60'] = df['close'].rolling(window=60, min_periods=1).mean()
            
            return df
            
        except Exception as e:
            logger.warning(f"获取肥羊 {sheep_code} 日K数据失败: {e}")
            return None
    
    @staticmethod
    def get_sheep_money_flow(sheep_code: str) -> Optional[Dict]:
        """
        获取肥羊资金流向数据（今日）
        
        Args:
            sheep_code: 肥羊代码
            
        Returns:
            资金流向字典，如果失败返回None
        """
        try:
            # 使用历史数据接口获取，然后提取今日数据
            flow_df = SheepAdapter.get_sheep_money_flow_history(sheep_code)
            
            if flow_df is None or flow_df.empty:
                return None
            
            # 获取最新的数据（今日）
            latest_row = flow_df.iloc[-1]  # 最后一行是最新的数据
            
            result = {
                'main_net_inflow': float(latest_row.get('main_net_inflow', 0)),
                'super_large_inflow': float(latest_row.get('super_large_inflow', 0)),
                'large_inflow': float(latest_row.get('large_inflow', 0)),
                'medium_inflow': float(latest_row.get('medium_inflow', 0)),
                'small_inflow': float(latest_row.get('small_inflow', 0))
            }
            
            return result
            
        except Exception as e:
            logger.debug(f"获取肥羊 {sheep_code} 资金流失败: {e}")
            return None
    
    @staticmethod
    def get_all_sheep_money_flow() -> Optional[pd.DataFrame]:
        """
        批量获取所有肥羊的资金流向数据（今日）
        
        Returns:
            包含所有肥羊资金流的DataFrame
        """
        try:
            flow_data = ak.stock_individual_fund_flow_rank(indicator="今日")
            
            if flow_data is None or flow_data.empty:
                return None
            
            # 标准化列名和数据类型
            result_df = pd.DataFrame()
            
            if '代码' in flow_data.columns:
                result_df['sheep_code'] = flow_data['代码'].astype(str).str.zfill(6)
            elif '肥羊代码' in flow_data.columns:
                result_df['sheep_code'] = flow_data['肥羊代码'].astype(str).str.zfill(6)
            
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
    
    @staticmethod
    def get_sheep_money_flow_history(sheep_code: str) -> Optional[pd.DataFrame]:
        """
        获取股票的历史资金流数据（近100个交易日）
        
        Args:
            sheep_code: 股票代码（6位数字）
            
        Returns:
            包含历史资金流数据的DataFrame，如果失败返回None
        """
        try:
            # 构建市场代码和市场标识
            if sheep_code.startswith('6'):
                market = 'sh'
            else:
                market = 'sz'
            
            # 调用akshare接口获取近100个交易日的数据
            # 注意：stock_individual_fund_flow 需要 stock（6位代码）和 market（sh/sz）参数
            flow_df = ak.stock_individual_fund_flow(stock=sheep_code, market=market)
            
            if flow_df is None or flow_df.empty:
                return None
            
            # 标准化列名
            result_df = pd.DataFrame()
            
            # 提取日期列（akshare返回的列名是'日期'）
            if '日期' in flow_df.columns:
                result_df['trade_date'] = pd.to_datetime(flow_df['日期']).dt.date
            else:
                logger.warning(f"股票 {sheep_code} 无法找到日期列，列名: {list(flow_df.columns)}")
                return None
            
            result_df['sheep_code'] = sheep_code
            
            # 解析资金流列（akshare返回的列名格式：'主力净流入-净额'等）
            # 注意：akshare返回的是元，需要转换为万元
            money_flow_mapping = {
                'main_net_inflow': '主力净流入-净额',
                'super_large_inflow': '超大单净流入-净额',
                'large_inflow': '大单净流入-净额',
                'medium_inflow': '中单净流入-净额',
                'small_inflow': '小单净流入-净额'
            }
            
            for target_col, source_col in money_flow_mapping.items():
                if source_col in flow_df.columns:
                    # 转换为万元（akshare返回的是元）
                    result_df[target_col] = flow_df[source_col].fillna(0).astype(float) / 10000
                else:
                    result_df[target_col] = 0.0
            
            return result_df
            
        except Exception as e:
            logger.debug(f"获取股票 {sheep_code} 资金流历史数据失败: {e}")
            return None
