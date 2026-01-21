"""
Falcon Data Engine CLI 命令行工具
支持字段级修补和历史回溯
"""
import argparse
from datetime import date, timedelta
from utils.logger import get_logger
from config import Config
from services.data_collection_service import DataCollectionService
from etl.trade_date_adapter import TradeDateAdapter
from tqdm import tqdm

logger = get_logger(__name__)

def mode_cold_start(args):
    """
    模式A: 行级冷启动
    场景: 数据库为空，根据配置回溯N天
    """
    days = args.days or Config.get_cold_start_days()
    logger.info(f"开始冷启动数据采集，回溯 {days} 天")
    
    service = DataCollectionService()
    
    # 计算日期范围
    today = date.today()
    start_date = today - timedelta(days=days)
    
    # 获取交易日列表
    trading_days = TradeDateAdapter.get_trading_days_in_range(start_date, today)
    
    if not trading_days:
        logger.warning("未找到交易日")
        return
    
    logger.info(f"共需采集 {len(trading_days)} 个交易日的数据")
    
    # 使用tqdm显示进度
    for trading_day in tqdm(trading_days, desc="冷启动采集"):
        try:
            service._collect_sheep_daily_for_date(trading_day)
            service._collect_money_flow_data_individual(trading_day)
        except Exception as e:
            logger.error(f"采集交易日 {trading_day} 失败: {e}")
            continue
    
    logger.info("冷启动数据采集完成")

def mode_patch(args):
    """
    模式B: 字段级修补
    场景: 表中已有数据，但新增了字段（如sector_rps）
    命令: python cli.py --mode=patch --target=sector_rps --days=365
    """
    target_field = args.target
    days = args.days or 365
    
    if not target_field:
        logger.error("字段级修补需要指定 --target 参数")
        return
    
    logger.info(f"开始字段级修补: {target_field}，回溯 {days} 天")
    
    # 根据字段类型选择修补方法
    if target_field == 'sector_rps':
        _patch_sector_rps(days)
    elif target_field == 'rps_250':
        _patch_rps_250(days)
    elif target_field == 'vcp_factor' or target_field == 'vol_ma_5':
        _patch_vcp_vol_ma5(days)
    else:
        logger.error(f"不支持的字段: {target_field}")
        logger.info("支持的字段: sector_rps, rps_250, vcp_factor, vol_ma_5")

def _patch_sector_rps(days: int):
    """修补sector_rps字段"""
    from db.sector_money_flow_repository import SectorMoneyFlowRepository
    from etl.sector_money_flow_adapter import SectorMetricsCalculator
    
    logger.info("开始修补sector_rps字段...")
    
    # 计算日期范围
    today = date.today()
    start_date = today - timedelta(days=days)
    
    # 获取交易日列表
    trading_days = TradeDateAdapter.get_trading_days_in_range(start_date, today)
    
    if not trading_days:
        logger.warning("未找到交易日")
        return
    
    calculator = SectorMetricsCalculator()
    
    # 使用tqdm显示进度
    for trading_day in tqdm(trading_days, desc="修补sector_rps"):
        try:
            calculator.calculate_comprehensive_sector_metrics(trading_day)
        except Exception as e:
            logger.error(f"修补交易日 {trading_day} 的sector_rps失败: {e}")
            continue
    
    logger.info("sector_rps字段修补完成")

def _patch_rps_250(days: int):
    """修补rps_250字段"""
    service = DataCollectionService()
    
    logger.info("开始修补rps_250字段...")
    
    # 计算日期范围
    today = date.today()
    start_date = today - timedelta(days=days)
    
    # 获取交易日列表
    trading_days = TradeDateAdapter.get_trading_days_in_range(start_date, today)
    
    if not trading_days:
        logger.warning("未找到交易日")
        return
    
    # 使用tqdm显示进度
    for trading_day in tqdm(trading_days, desc="修补rps_250"):
        try:
            service.calculate_and_update_rps(target_date=trading_day)
        except Exception as e:
            logger.error(f"修补交易日 {trading_day} 的rps_250失败: {e}")
            continue
    
    logger.info("rps_250字段修补完成")

def _patch_vcp_vol_ma5(days: int):
    """修补vcp_factor和vol_ma_5字段"""
    service = DataCollectionService()
    
    logger.info("开始修补vcp_factor和vol_ma_5字段...")
    
    # 计算日期范围
    today = date.today()
    start_date = today - timedelta(days=days)
    
    # 获取交易日列表
    trading_days = TradeDateAdapter.get_trading_days_in_range(start_date, today)
    
    if not trading_days:
        logger.warning("未找到交易日")
        return
    
    # 使用tqdm显示进度
    for trading_day in tqdm(trading_days, desc="修补vcp/vol_ma_5"):
        try:
            service.calculate_and_update_vcp_vol_ma5(target_date=trading_day)
        except Exception as e:
            logger.error(f"修补交易日 {trading_day} 的vcp/vol_ma_5失败: {e}")
            continue
    
    logger.info("vcp_factor和vol_ma_5字段修补完成")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Falcon Data Engine CLI')
    parser.add_argument('--mode', choices=['cold_start', 'patch'], required=True,
                       help='运行模式: cold_start=冷启动, patch=字段级修补')
    parser.add_argument('--target', type=str,
                       help='修补目标字段（仅patch模式需要）')
    parser.add_argument('--days', type=int,
                       help='回溯天数（默认使用配置值）')
    
    args = parser.parse_args()
    
    if args.mode == 'cold_start':
        mode_cold_start(args)
    elif args.mode == 'patch':
        mode_patch(args)

if __name__ == '__main__':
    main()
