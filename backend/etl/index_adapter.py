"""
大盘指数数据采集适配器
封装akshare指数相关接口，用于RSRS牛熊市判断
"""
import akshare as ak
import pandas as pd
from datetime import date, datetime
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


class IndexAdapter:
    """大盘指数数据采集适配器"""
    
    # 指数代码映射（akshare代码 -> 数据库代码）
    INDEX_CODE_MAP = {
        'CSI1000': '000852',  # 中证1000（akshare使用数字代码）
        'CSI300': '000300',   # 沪深300
        'CSI500': '000905',   # 中证500
        'SSE50': '000016',    # 上证50
    }
    
    # 指数名称映射
    INDEX_NAME_MAP = {
        'CSI1000': '中证1000',
        'CSI300': '沪深300',
        'CSI500': '中证500',
        'SSE50': '上证50',
    }
    
    @classmethod
    def get_index_daily_data(cls, index_code: str, start_date: Optional[date] = None, 
                             end_date: Optional[date] = None) -> pd.DataFrame:
        """
        获取指数日K数据
        
        Args:
            index_code: 指数代码（CSI1000, CSI300等）
            start_date: 开始日期，默认为3年前
            end_date: 结束日期，默认为今天
            
        Returns:
            DataFrame，包含以下列：
            - trade_date: 交易日期
            - open_price: 开盘价
            - close_price: 收盘价
            - high_price: 最高价
            - low_price: 最低价
            - volume: 成交量
            - amount: 成交额
        """
        try:
            # 获取akshare的指数代码
            akshare_code = cls.INDEX_CODE_MAP.get(index_code)
            if not akshare_code:
                logger.error(f"不支持的指数代码: {index_code}")
                return pd.DataFrame()
            
            # 默认日期范围：最近3年
            if end_date is None:
                end_date = date.today()
            if start_date is None:
                from datetime import timedelta
                start_date = end_date - timedelta(days=1095)  # 3年
            
            # 转换日期格式为字符串（akshare需要YYYYMMDD格式）
            start_str = start_date.strftime('%Y%m%d')
            end_str = end_date.strftime('%Y%m%d')
            
            logger.info(f"获取指数数据: {index_code} ({akshare_code}), {start_str} 至 {end_str}")
            
            # 调用akshare接口获取指数数据
            # akshare的指数接口：index_zh_a_hist
            try:
                # 使用指数日K数据接口
                # 参数说明：
                # - symbol: 指数代码（如'000852'表示中证1000）
                # - period: 周期（'daily'表示日K线，'weekly'表示周K线，'monthly'表示月K线）
                # - start_date: 开始日期（YYYYMMDD格式）
                # - end_date: 结束日期（YYYYMMDD格式）
                # 注意：该接口不支持adjust参数
                df = ak.index_zh_a_hist(
                    symbol=akshare_code, 
                    period="daily", 
                    start_date=start_str, 
                    end_date=end_str
                )
            except Exception as e:
                logger.error(f"获取指数数据失败: {index_code} ({akshare_code}), 错误: {e}", exc_info=True)
                # 如果akshare接口失败，返回空DataFrame，后续会降级为实时计算
                return pd.DataFrame()
            
            if df is None or df.empty:
                logger.warning(f"获取指数数据为空: {index_code}")
                return pd.DataFrame()
            
            # 标准化列名（akshare返回的列名是中文）
            # 根据akshare文档，index_zh_a_hist返回的列名包括：日期、开盘、收盘、最高、最低、成交量、成交额等
            column_mapping = {
                '日期': 'trade_date',
                'date': 'trade_date',
                '交易日期': 'trade_date',
                '开盘': 'open_price',
                'open': 'open_price',
                '开盘价': 'open_price',
                '收盘': 'close_price',
                'close': 'close_price',
                '收盘价': 'close_price',
                '最高': 'high_price',
                'high': 'high_price',
                '最高价': 'high_price',
                '最低': 'low_price',
                'low': 'low_price',
                '最低价': 'low_price',
                '成交量': 'volume',
                'volume': 'volume',
                '成交额': 'amount',
                'amount': 'amount',
            }
            
            # 重命名列（只重命名存在的列）
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            # 确保必要的列存在
            required_columns = ['trade_date', 'open_price', 'close_price', 'high_price', 'low_price']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"缺少必要的列: {missing_columns}")
                logger.info(f"可用列: {list(df.columns)}")
                return pd.DataFrame()
            
            # 转换日期格式
            if df['trade_date'].dtype == 'object':
                try:
                    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y-%m-%d').dt.date
                except:
                    try:
                        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
                    except:
                        logger.error(f"无法解析日期格式: {df['trade_date'].head()}")
                        return pd.DataFrame()
            
            # 确保数值列为float类型
            for col in ['open_price', 'close_price', 'high_price', 'low_price']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 添加指数代码和名称
            df['index_code'] = index_code
            df['index_name'] = cls.INDEX_NAME_MAP.get(index_code, index_code)
            
            # 排序（按日期升序）
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取指数数据: {index_code}, 共 {len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取指数数据失败: {index_code}, 错误: {e}", exc_info=True)
            return pd.DataFrame()
    
    @classmethod
    def get_index_list(cls) -> List[str]:
        """获取支持的指数代码列表"""
        return list(cls.INDEX_CODE_MAP.keys())
