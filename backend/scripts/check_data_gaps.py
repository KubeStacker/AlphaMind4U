"""
数据完整性检查和补全脚本

功能：
1. 检查各个数据表的完整性
2. 找出缺失的数据（日期、股票等）
3. 提供数据刷新功能

使用方法：
  docker-compose exec backend python scripts/check_data_gaps.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging
from db.database import get_db
from sqlalchemy import text
from db.sheep_repository import SheepRepository
from db.money_flow_repository import MoneyFlowRepository
from db.hot_rank_repository import HotRankRepository
from db.index_repository import IndexRepository
from etl.trade_date_adapter import TradeDateAdapter
from etl.sheep_adapter import SheepAdapter
from services.data_collection_service import DataCollectionService
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataGapChecker:
    """数据完整性检查器"""
    
    def __init__(self):
        self.sheep_adapter = SheepAdapter
        self.data_service = DataCollectionService()
    
    def check_sheep_daily_gaps(self, days: int = 30) -> Dict:
        """
        检查肥羊日K数据缺失
        
        Args:
            days: 检查最近N天的数据
            
        Returns:
            缺失数据统计
        """
        logger.info(f"开始检查肥羊日K数据缺失（最近{days}天）...")
        
        today = date.today()
        start_date = today - timedelta(days=days)
        
        # 获取所有活跃股票
        sheep_list = self.sheep_adapter.get_all_sheep_codes()
        if not sheep_list:
            logger.error("未能获取到股票列表")
            return {}
        
        sheep_codes = [item['code'] for item in sheep_list]
        logger.info(f"共需检查 {len(sheep_codes)} 只股票")
        
        # 获取所有交易日
        trading_days = []
        current_date = start_date
        while current_date <= today:
            if TradeDateAdapter.is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        logger.info(f"共 {len(trading_days)} 个交易日需要检查")
        
        # 检查每只股票的数据
        missing_data = {}
        total_missing = 0
        
        for sheep_code in sheep_codes:
            # 查询该股票在日期范围内的数据
            query = text("""
                SELECT DISTINCT trade_date 
                FROM sheep_daily 
                WHERE sheep_code = :code 
                AND trade_date >= :start_date 
                AND trade_date <= :end_date
                ORDER BY trade_date
            """)
            
            with get_db() as db:
                result = db.execute(
                    query,
                    {'code': sheep_code, 'start_date': start_date, 'end_date': today}
                )
                existing_dates = {row[0] for row in result}
            
            # 找出缺失的日期
            missing_dates = [d for d in trading_days if d not in existing_dates]
            
            if missing_dates:
                missing_data[sheep_code] = {
                    'count': len(missing_dates),
                    'dates': sorted(missing_dates)
                }
                total_missing += len(missing_dates)
        
        logger.info(f"检查完成！共发现 {len(missing_data)} 只股票有数据缺失，总计 {total_missing} 条缺失记录")
        
        # 输出前10只缺失最多的股票
        if missing_data:
            sorted_missing = sorted(missing_data.items(), key=lambda x: x[1]['count'], reverse=True)
            logger.info("缺失数据最多的前10只股票：")
            for i, (code, info) in enumerate(sorted_missing[:10], 1):
                logger.info(f"  {i}. {code}: 缺失 {info['count']} 个交易日")
        
        return {
            'total_stocks': len(sheep_codes),
            'total_trading_days': len(trading_days),
            'stocks_with_gaps': len(missing_data),
            'total_missing_records': total_missing,
            'missing_data': missing_data
        }
    
    def check_money_flow_gaps(self, days: int = 30) -> Dict:
        """
        检查资金流数据缺失
        
        Args:
            days: 检查最近N天的数据
            
        Returns:
            缺失数据统计
        """
        logger.info(f"开始检查资金流数据缺失（最近{days}天）...")
        
        today = date.today()
        start_date = today - timedelta(days=days)
        
        # 获取所有活跃股票
        sheep_list = self.sheep_adapter.get_all_sheep_codes()
        if not sheep_list:
            logger.error("未能获取到股票列表")
            return {}
        
        sheep_codes = [item['code'] for item in sheep_list]
        
        # 获取所有交易日
        trading_days = []
        current_date = start_date
        while current_date <= today:
            if TradeDateAdapter.is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        # 检查每只股票的数据
        missing_data = {}
        total_missing = 0
        
        for sheep_code in sheep_codes:
            # 查询该股票在日期范围内的数据
            query = text("""
                SELECT DISTINCT trade_date 
                FROM sheep_money_flow 
                WHERE sheep_code = :code 
                AND trade_date >= :start_date 
                AND trade_date <= :end_date
                ORDER BY trade_date
            """)
            
            with get_db() as db:
                result = db.execute(
                    query,
                    {'code': sheep_code, 'start_date': start_date, 'end_date': today}
                )
                existing_dates = {row[0] for row in result}
            
            # 找出缺失的日期
            missing_dates = [d for d in trading_days if d not in existing_dates]
            
            if missing_dates:
                missing_data[sheep_code] = {
                    'count': len(missing_dates),
                    'dates': sorted(missing_dates)
                }
                total_missing += len(missing_dates)
        
        logger.info(f"检查完成！共发现 {len(missing_data)} 只股票有资金流数据缺失，总计 {total_missing} 条缺失记录")
        
        return {
            'total_stocks': len(sheep_codes),
            'total_trading_days': len(trading_days),
            'stocks_with_gaps': len(missing_data),
            'total_missing_records': total_missing,
            'missing_data': missing_data
        }
    
    def check_hot_rank_gaps(self, days: int = 7) -> Dict:
        """
        检查热度榜数据缺失
        
        Args:
            days: 检查最近N天的数据
            
        Returns:
            缺失数据统计
        """
        logger.info(f"开始检查热度榜数据缺失（最近{days}天）...")
        
        today = date.today()
        start_date = today - timedelta(days=days)
        
        # 获取所有日期（热度榜是自然日数据）
        all_dates = []
        current_date = start_date
        while current_date <= today:
            all_dates.append(current_date)
            current_date += timedelta(days=1)
        
        # 检查每个日期的数据
        missing_dates = []
        
        for check_date in all_dates:
            query = text("""
                SELECT COUNT(*) 
                FROM market_hot_rank 
                WHERE trade_date = :date
            """)
            
            with get_db() as db:
                result = db.execute(query, {'date': check_date})
                count = result.scalar()
            
            if count == 0:
                missing_dates.append(check_date)
        
        logger.info(f"检查完成！共发现 {len(missing_dates)} 天缺少热度榜数据")
        
        return {
            'total_days': len(all_dates),
            'missing_days': len(missing_dates),
            'missing_dates': missing_dates
        }
    
    def check_index_data_gaps(self, index_code: str = 'CSI1000', days: int = 30) -> Dict:
        """
        检查指数数据缺失
        
        Args:
            index_code: 指数代码
            days: 检查最近N天的数据
            
        Returns:
            缺失数据统计
        """
        logger.info(f"开始检查指数数据缺失（{index_code}，最近{days}天）...")
        
        try:
            # 使用IndexRepository检查数据
            latest_date = IndexRepository.get_latest_trade_date(index_code)
            
            today = date.today()
            start_date = today - timedelta(days=days)
            
            # 获取所有交易日
            trading_days = []
            current_date = start_date
            while current_date <= today:
                if TradeDateAdapter.is_trading_day(current_date):
                    trading_days.append(current_date)
                current_date += timedelta(days=1)
            
            if latest_date is None:
                # 没有数据，全部缺失
                missing_dates = trading_days
            else:
                # 查询已有的数据
                query = text("""
                    SELECT DISTINCT trade_date 
                    FROM market_index_daily 
                    WHERE index_code = :code 
                    AND trade_date >= :start_date 
                    AND trade_date <= :end_date
                    ORDER BY trade_date
                """)
                
                with get_db() as db:
                    result = db.execute(
                        query,
                        {'code': index_code, 'start_date': start_date, 'end_date': today}
                    )
                    existing_dates = {row[0] for row in result}
                
                # 找出缺失的日期
                missing_dates = [d for d in trading_days if d not in existing_dates]
            
            logger.info(f"检查完成！指数 {index_code} 缺失 {len(missing_dates)} 个交易日的数据")
            
            return {
                'index_code': index_code,
                'total_trading_days': len(trading_days),
                'missing_days': len(missing_dates),
                'missing_dates': missing_dates
            }
        except Exception as e:
            logger.warning(f"检查指数数据缺失失败（可能表不存在）: {e}")
            return {
                'index_code': index_code,
                'total_trading_days': 0,
                'missing_days': 0,
                'missing_dates': [],
                'error': str(e)
            }
    
    def refresh_missing_data(self, data_type: str = 'all', days: int = 30):
        """
        刷新缺失的数据
        
        Args:
            data_type: 数据类型 ('sheep_daily', 'money_flow', 'hot_rank', 'index', 'all')
            days: 刷新最近N天的数据
        """
        logger.info(f"开始刷新缺失数据（类型: {data_type}, 天数: {days}）...")
        
        if data_type in ('sheep_daily', 'all'):
            logger.info("刷新肥羊日K数据...")
            self.data_service.collect_sheep_daily_data(days=days)
        
        if data_type in ('money_flow', 'all'):
            logger.info("刷新资金流数据...")
            # 资金流数据需要逐个交易日刷新
            today = date.today()
            start_date = today - timedelta(days=days)
            current_date = start_date
            while current_date <= today:
                if TradeDateAdapter.is_trading_day(current_date):
                    logger.info(f"刷新 {current_date} 的资金流数据...")
                    # 这里需要手动触发，因为collect_money_flow_data只采集当天
                    # 可以调用历史数据采集脚本
                current_date += timedelta(days=1)
        
        if data_type in ('hot_rank', 'all'):
            logger.info("刷新热度榜数据...")
            # 热度榜数据只刷新最近30天，避免耗时过长
            actual_days = min(days, 30)
            logger.info(f"热度榜数据刷新限制为最近{actual_days}天（避免耗时过长）")
            today = date.today()
            start_date = today - timedelta(days=actual_days)
            current_date = start_date
            count = 0
            while current_date <= today:
                logger.info(f"刷新 {current_date} 的热度榜数据...")
                self.data_service.collect_hot_rank_data(target_date=current_date)
                current_date += timedelta(days=1)
                count += 1
                if count % 10 == 0:
                    logger.info(f"已刷新 {count} 天的热度榜数据...")
        
        if data_type in ('index', 'all'):
            logger.info("刷新指数数据...")
            self.data_service.collect_index_data(index_code='CSI1000', days=days)
        
        logger.info("数据刷新完成！")
    
    def generate_report(self, days: int = 30) -> str:
        """
        生成数据完整性报告
        
        Args:
            days: 检查最近N天的数据
            
        Returns:
            报告文本
        """
        logger.info("生成数据完整性报告...")
        
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("数据完整性检查报告")
        report_lines.append(f"检查日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"检查范围: 最近 {days} 天")
        report_lines.append("=" * 60)
        report_lines.append("")
        
        # 检查肥羊日K数据
        sheep_gaps = self.check_sheep_daily_gaps(days=days)
        report_lines.append("1. 肥羊日K数据")
        report_lines.append(f"   总股票数: {sheep_gaps.get('total_stocks', 0)}")
        report_lines.append(f"   交易日数: {sheep_gaps.get('total_trading_days', 0)}")
        report_lines.append(f"   有缺失的股票数: {sheep_gaps.get('stocks_with_gaps', 0)}")
        report_lines.append(f"   缺失记录总数: {sheep_gaps.get('total_missing_records', 0)}")
        report_lines.append("")
        
        # 检查资金流数据
        money_flow_gaps = self.check_money_flow_gaps(days=days)
        report_lines.append("2. 资金流数据")
        report_lines.append(f"   总股票数: {money_flow_gaps.get('total_stocks', 0)}")
        report_lines.append(f"   交易日数: {money_flow_gaps.get('total_trading_days', 0)}")
        report_lines.append(f"   有缺失的股票数: {money_flow_gaps.get('stocks_with_gaps', 0)}")
        report_lines.append(f"   缺失记录总数: {money_flow_gaps.get('total_missing_records', 0)}")
        report_lines.append("")
        
        # 检查热度榜数据
        hot_rank_gaps = self.check_hot_rank_gaps(days=7)
        report_lines.append("3. 热度榜数据")
        report_lines.append(f"   检查天数: {hot_rank_gaps.get('total_days', 0)}")
        report_lines.append(f"   缺失天数: {hot_rank_gaps.get('missing_days', 0)}")
        report_lines.append("")
        
        # 检查指数数据
        index_gaps = self.check_index_data_gaps(days=days)
        report_lines.append("4. 指数数据")
        report_lines.append(f"   指数代码: {index_gaps.get('index_code', 'N/A')}")
        report_lines.append(f"   交易日数: {index_gaps.get('total_trading_days', 0)}")
        report_lines.append(f"   缺失天数: {index_gaps.get('missing_days', 0)}")
        report_lines.append("")
        
        report_lines.append("=" * 60)
        
        report_text = "\n".join(report_lines)
        logger.info("\n" + report_text)
        
        return report_text


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据完整性检查和刷新工具')
    parser.add_argument('--check', action='store_true', help='检查数据缺失')
    parser.add_argument('--refresh', action='store_true', help='刷新缺失数据')
    parser.add_argument('--type', type=str, default='all', 
                       choices=['sheep_daily', 'money_flow', 'hot_rank', 'index', 'all'],
                       help='数据类型')
    parser.add_argument('--days', type=int, default=30, help='检查/刷新最近N天的数据')
    parser.add_argument('--report', action='store_true', help='生成完整报告')
    
    args = parser.parse_args()
    
    checker = DataGapChecker()
    
    if args.report:
        checker.generate_report(days=args.days)
    elif args.check:
        if args.type in ('sheep_daily', 'all'):
            checker.check_sheep_daily_gaps(days=args.days)
        if args.type in ('money_flow', 'all'):
            checker.check_money_flow_gaps(days=args.days)
        if args.type in ('hot_rank', 'all'):
            checker.check_hot_rank_gaps(days=min(args.days, 7))
        if args.type in ('index', 'all'):
            checker.check_index_data_gaps(days=args.days)
    elif args.refresh:
        checker.refresh_missing_data(data_type=args.type, days=args.days)
    else:
        # 默认执行检查和报告
        checker.generate_report(days=args.days)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n用户中断，退出程序")
    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)
        sys.exit(1)
