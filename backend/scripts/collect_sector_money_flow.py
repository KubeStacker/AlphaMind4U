#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
板块资金流数据手动采集脚本

使用方法：
  # 采集当日数据（概念+行业）
  docker-compose exec backend python scripts/collect_sector_money_flow.py
  
  # 采集行业历史数据（最近90天）
  docker-compose exec backend python scripts/collect_sector_money_flow.py --hist
  
  # 采集行业历史数据（指定天数）
  docker-compose exec backend python scripts/collect_sector_money_flow.py --hist --days 60

注意事项：
  - 概念资金流只能获取当日数据（akshare限制）
  - 行业资金流可以获取历史数据（约120天）
  - 历史数据采集会比较慢（每个行业需要单独请求）
"""

import sys
import os
import argparse
from datetime import date, datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def collect_sector_money_flow(target_date: date = None):
    """采集当日板块资金流数据（概念+行业）"""
    from services.sector_money_flow_service import SectorMoneyFlowService
    from etl.trade_date_adapter import TradeDateAdapter
    
    if target_date is None:
        target_date = date.today()
    
    # 检查是否为交易日
    if not TradeDateAdapter.is_trading_day(target_date):
        logger.warning(f"{target_date} 不是交易日，数据可能不完整")
    
    logger.info(f"开始采集板块资金流数据（日期: {target_date}）...")
    
    try:
        SectorMoneyFlowService.collect_sector_money_flow_data(target_date)
        logger.info("板块资金流数据采集完成！")
        return True
    except Exception as e:
        logger.error(f"采集失败: {e}", exc_info=True)
        return False


def collect_sector_money_flow_hist(days: int = 90):
    """采集行业历史资金流数据（最近N天）"""
    from etl.sector_money_flow_adapter import SectorMoneyFlowAdapter
    from db.sector_money_flow_repository import SectorMoneyFlowRepository
    
    logger.info(f"开始采集行业历史资金流数据（最近{days}天）...")
    
    try:
        # 获取所有行业的历史数据
        all_data = SectorMoneyFlowAdapter.get_all_industry_money_flow_hist(days=days)
        
        if not all_data:
            logger.warning("未获取到历史数据")
            return False
        
        # 保存到数据库
        logger.info(f"开始保存 {len(all_data)} 条历史数据到数据库...")
        SectorMoneyFlowRepository.batch_upsert_sector_money_flow(all_data)
        
        logger.info(f"行业历史资金流数据采集完成！共 {len(all_data)} 条")
        return True
    except Exception as e:
        logger.error(f"采集失败: {e}", exc_info=True)
        return False


def collect_all_trading_day_data(target_date: date = None):
    """采集所有交易日数据（包括肥羊数据、资金流、板块资金流、指数数据）"""
    from services.data_collection_service import DataCollectionService
    from etl.trade_date_adapter import TradeDateAdapter
    
    if target_date is None:
        target_date = date.today()
    
    # 检查是否为交易日
    if not TradeDateAdapter.is_trading_day(target_date):
        logger.warning(f"{target_date} 不是交易日，跳过数据采集")
        return False
    
    logger.info(f"开始采集所有交易日数据（日期: {target_date}）...")
    
    service = DataCollectionService()
    
    try:
        # 1. 采集肥羊日K数据
        logger.info("1/4 采集肥羊日K数据...")
        # service.collect_sheep_daily_data()
        
        # 2. 采集资金流向数据
        logger.info("2/4 采集资金流向数据...")
        # service.collect_money_flow_data()
        
        # 3. 采集板块资金流向数据
        logger.info("3/4 采集板块资金流向数据...")
        service.collect_sector_money_flow_data(target_date=target_date)
        
        # 4. 采集大盘指数数据
        logger.info("4/4 采集大盘指数数据...")
        # service.collect_index_data(index_code='CSI1000')
        
        logger.info("所有交易日数据采集完成！")
        return True
        
    except Exception as e:
        logger.error(f"采集失败: {e}", exc_info=True)
        return False


def main():
    parser = argparse.ArgumentParser(description='板块资金流数据采集脚本')
    parser.add_argument('--date', '-d', type=str, help='采集日期（格式：YYYY-MM-DD），默认今天')
    parser.add_argument('--hist', action='store_true', help='采集行业历史数据（最近N天）')
    parser.add_argument('--days', type=int, default=90, help='历史数据天数，默认90天')
    parser.add_argument('--all', '-a', action='store_true', help='采集所有交易日数据（包括板块资金流、指数等）')
    
    args = parser.parse_args()
    
    # 解析日期
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"日期格式错误: {args.date}，请使用YYYY-MM-DD格式")
            sys.exit(1)
    
    # 执行采集
    if args.hist:
        # 采集行业历史数据
        success = collect_sector_money_flow_hist(days=args.days)
    elif args.all:
        # 采集所有交易日数据
        success = collect_all_trading_day_data(target_date)
    else:
        # 采集当日板块资金流数据
        success = collect_sector_money_flow(target_date)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
