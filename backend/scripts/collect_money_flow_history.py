"""
采集历史资金流数据脚本（一次性脚本）

注意：这是一个一次性数据补全脚本，用于初始化或补全历史资金流数据。
akshare 的 stock_individual_fund_flow 接口只能获取近100个交易日的数据，
无法一次性获取3年历史数据。本脚本会获取所有股票近100个交易日的数据。

使用方法：
  docker-compose exec backend python scripts/collect_money_flow_history.py

数据采集完成后，日常数据更新由定时任务自动处理，无需再次运行此脚本。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, datetime, timedelta
from typing import List, Dict, Optional
import logging
import time
import akshare as ak
import pandas as pd
from etl.trade_date_adapter import TradeDateAdapter
from db.money_flow_repository import MoneyFlowRepository
from db.sheep_repository import SheepRepository
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MoneyFlowHistoryCollector:
    """历史资金流数据采集器"""
    
    def __init__(self):
        self.trade_date_adapter = TradeDateAdapter
        self.batch_size = 50  # 每批处理的股票数量
        self.delay_between_batches = 2  # 批次之间的延迟（秒）
        self.delay_between_stocks = 0.5  # 股票之间的延迟（秒）
    
    def get_all_sheep_codes(self) -> List[str]:
        """获取所有股票代码"""
        try:
            stock_list = ak.stock_info_a_code_name()
            if stock_list is None or stock_list.empty:
                logger.error("无法获取股票列表")
                return []
            
            codes = []
            if 'code' in stock_list.columns:
                for _, row in stock_list.iterrows():
                    code = str(row['code']).strip()
                    if code and len(code) >= 6:
                        clean_code = code[:6].zfill(6)
                        codes.append(clean_code)
            
            logger.info(f"获取到 {len(codes)} 只股票")
            return codes
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}", exc_info=True)
            return []
    
    def get_sheep_money_flow_history(self, sheep_code: str) -> Optional[pd.DataFrame]:
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
            logger.warning(f"获取股票 {sheep_code} 资金流历史数据失败: {e}")
            return None
    
    def format_flow_data(self, df: pd.DataFrame) -> List[Dict]:
        """格式化资金流数据为数据库格式"""
        if df is None or df.empty:
            return []
        
        data_list = []
        for _, row in df.iterrows():
            data_list.append({
                'code': str(row['sheep_code']).strip(),
                'date': row['trade_date'],
                'main': float(row.get('main_net_inflow', 0)),
                'super_large': float(row.get('super_large_inflow', 0)),
                'large': float(row.get('large_inflow', 0)),
                'medium': float(row.get('medium_inflow', 0)),
                'small': float(row.get('small_inflow', 0)),
            })
        
        return data_list
    
    def collect_history_data(self, years: int = 3):
        """
        采集历史资金流数据
        
        Args:
            years: 采集最近N年的数据（实际只能获取近100个交易日的数据）
        """
        logger.info(f"开始采集近{years}年历史资金流数据...")
        
        # 获取所有股票代码
        sheep_codes = self.get_all_sheep_codes()
        if not sheep_codes:
            logger.error("未获取到股票代码，退出")
            return
        
        total = len(sheep_codes)
        success_count = 0
        error_count = 0
        total_records = 0
        
        logger.info(f"共需处理 {total} 只股票")
        
        # 分批处理
        for batch_start in range(0, total, self.batch_size):
            batch_end = min(batch_start + self.batch_size, total)
            batch_codes = sheep_codes[batch_start:batch_end]
            
            logger.info(f"处理批次 {batch_start + 1}-{batch_end}/{total}，包含 {len(batch_codes)} 只股票")
            
            batch_success = 0
            batch_error = 0
            batch_records = 0
            
            for idx, sheep_code in enumerate(batch_codes):
                try:
                    # 获取历史资金流数据
                    flow_df = self.get_sheep_money_flow_history(sheep_code)
                    
                    if flow_df is None or flow_df.empty:
                        continue
                    
                    # 格式化数据
                    data_list = self.format_flow_data(flow_df)
                    
                    if not data_list:
                        continue
                    
                    # 批量保存
                    MoneyFlowRepository.batch_upsert_money_flow(data_list)
                    
                    success_count += 1
                    batch_success += 1
                    total_records += len(data_list)
                    batch_records += len(data_list)
                    
                    if (idx + 1) % 10 == 0:
                        logger.info(f"  批次内进度: {idx + 1}/{len(batch_codes)}, 本批次成功: {batch_success}, 本批次失败: {batch_error}, 本批次记录: {batch_records}")
                        logger.info(f"  累计: 成功: {success_count}, 失败: {error_count}, 总记录: {total_records}")
                    
                    # 延迟，避免请求过快
                    if idx < len(batch_codes) - 1:
                        time.sleep(self.delay_between_stocks)
                    
                except Exception as e:
                    logger.warning(f"处理股票 {sheep_code} 失败: {e}")
                    error_count += 1
                    batch_error += 1
                    continue
            
            logger.info(f"批次 {batch_start + 1}-{batch_end} 完成: 成功 {batch_success}, 失败 {batch_error}, 记录 {batch_records}")
            
            # 批次之间的延迟
            if batch_end < total:
                logger.info(f"批次完成，等待 {self.delay_between_batches} 秒后继续...")
                time.sleep(self.delay_between_batches)
        
        logger.info(f"历史资金流数据采集完成！")
        logger.info(f"成功: {success_count}, 失败: {error_count}, 总记录数: {total_records}")

def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("开始采集历史资金流数据")
    logger.info("注意：akshare接口限制，只能获取近100个交易日的数据")
    logger.info("=" * 60)
    
    collector = MoneyFlowHistoryCollector()
    
    # 采集历史数据（实际只能获取近100个交易日，约4-5个月）
    # 虽然参数是years=3，但实际只能获取近100个交易日的数据
    collector.collect_history_data(years=3)
    
    logger.info("=" * 60)
    logger.info("数据采集完成！")
    logger.info("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n用户中断，退出程序")
    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)
        sys.exit(1)
