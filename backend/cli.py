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
    elif target_field == 'sector_change_pct':
        _patch_sector_change_pct(days)
    elif target_field == 'rps_250':
        _patch_rps_250(days)
    elif target_field == 'vcp_factor' or target_field == 'vol_ma_5':
        _patch_vcp_vol_ma5(days)
    elif target_field == 'ma':
        _patch_ma_fields(days)
    else:
        logger.error(f"不支持的字段: {target_field}")
        logger.info("支持的字段: sector_rps, sector_change_pct, rps_250, vcp_factor, vol_ma_5, ma")

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

def _patch_sector_change_pct(days: int):
    """
    修补sector_money_flow表的change_pct字段
    这是RPS计算的前置条件，需要至尠20天的change_pct数据
    
    用法: python cli.py --mode=patch --target=sector_change_pct --days=60
    """
    from sqlalchemy import text
    from db.database import get_db
    from etl.sector_money_flow_adapter import SectorMetricsCalculator
    
    logger.info(f"开始修补sector_change_pct字段，回溯 {days} 天...")
    
    # 计算日期范围
    today = date.today()
    start_date = today - timedelta(days=days)
    
    # 获取交易日列表
    trading_days = TradeDateAdapter.get_trading_days_in_range(start_date, today)
    
    if not trading_days:
        logger.warning("未找到交易日")
        return
    
    calculator = SectorMetricsCalculator()
    total_updated = 0
    
    # 使用tqdm显示进度
    for trading_day in tqdm(trading_days, desc="修补sector_change_pct"):
        try:
            # 获取该日期的所有板块
            with get_db() as db:
                sectors_query = text("""
                    SELECT sector_name, change_pct 
                    FROM sector_money_flow 
                    WHERE trade_date = :trade_date
                """)
                result = db.execute(sectors_query, {'trade_date': trading_day})
                sectors = list(result)
            
            updated_count = 0
            for sector_name, current_change_pct in sectors:
                # 跳过已有有效change_pct的记录
                if current_change_pct is not None and current_change_pct != 0:
                    continue
                
                # 从肥羊数据计算change_pct
                new_change_pct = calculator._calculate_change_pct_from_stocks(sector_name, trading_day)
                
                if new_change_pct is not None:
                    # 更新数据库
                    with get_db() as db:
                        update_query = text("""
                            UPDATE sector_money_flow 
                            SET change_pct = :change_pct 
                            WHERE sector_name = :sector_name AND trade_date = :trade_date
                        """)
                        db.execute(update_query, {
                            'change_pct': new_change_pct,
                            'sector_name': sector_name,
                            'trade_date': trading_day
                        })
                        db.commit()
                        updated_count += 1
            
            total_updated += updated_count
            
        except Exception as e:
            logger.error(f"修补交易日 {trading_day} 的sector_change_pct失败: {e}")
            continue
    
    logger.info(f"sector_change_pct字段修补完成，共更新 {total_updated} 条记录")
    
    # 补充完成后，重新计算RPS
    logger.info("开始重新计算sector_rps...")
    _patch_sector_rps(min(days, 30))  # RPS只需要最近30天

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

def _patch_ma_fields(days: int):
    """修补MA字段（ma5, ma10, ma20, ma30, ma60）"""
    from sqlalchemy import text
    import pandas as pd

    logger.info("开始修补MA字段...")

    # 计算日期范围
    today = date.today()
    start_date = today - timedelta(days=days)

    # 获取交易日列表
    trading_days = TradeDateAdapter.get_trading_days_in_range(start_date, today)

    if not trading_days:
        logger.warning("未找到交易日")
        return

    # 使用tqdm显示进度
    for trading_day in tqdm(trading_days, desc="修补MA字段"):
        try:
            # 为每个交易日重新计算MA
            _recalculate_ma_for_date(trading_day)
        except Exception as e:
            logger.error(f"修补交易日 {trading_day} 的MA字段失败: {e}")
            continue

    logger.info("MA字段修补完成")

def _recalculate_ma_for_date(target_date: date):
    """重新计算指定日期的MA数据"""
    from sqlalchemy import text
    import pandas as pd

    try:
        # 导入数据库连接
        from db.database import get_db

        with get_db() as db:
            # 获取最近60天的所有股票数据（用于计算MA）
            start_date = target_date - timedelta(days=60)

            query = text("""
                SELECT sd.sheep_code, sd.trade_date, sd.close_price
                FROM sheep_daily sd
                WHERE sd.trade_date BETWEEN :start_date AND :target_date
                ORDER BY sd.sheep_code, sd.trade_date
            """)

            result = db.execute(query, {
                'start_date': start_date,
                'target_date': target_date
            })

            rows = result.fetchall()
            if not rows:
                logger.debug(f"没有找到 {target_date} 的数据")
                return

            # 转换为DataFrame
            df = pd.DataFrame(rows, columns=['sheep_code', 'trade_date', 'close_price'])

            # 为每只股票计算MA
            updated_records = []

            for sheep_code in df['sheep_code'].unique():
                stock_data = df[df['sheep_code'] == sheep_code].copy()
                stock_data = stock_data.sort_values('trade_date')

                # 计算均线
                stock_data['ma5'] = stock_data['close_price'].rolling(window=5, min_periods=1).mean()
                stock_data['ma10'] = stock_data['close_price'].rolling(window=10, min_periods=1).mean()
                stock_data['ma20'] = stock_data['close_price'].rolling(window=20, min_periods=1).mean()
                stock_data['ma30'] = stock_data['close_price'].rolling(window=30, min_periods=1).mean()
                stock_data['ma60'] = stock_data['close_price'].rolling(window=60, min_periods=1).mean()

                # 只更新目标日期的数据
                target_row = stock_data[stock_data['trade_date'] == target_date]
                if not target_row.empty:
                    row = target_row.iloc[0]
                    updated_records.append({
                        'sheep_code': sheep_code,
                        'trade_date': target_date,
                        'ma5': round(row['ma5'], 2) if pd.notna(row['ma5']) else None,
                        'ma10': round(row['ma10'], 2) if pd.notna(row['ma10']) else None,
                        'ma20': round(row['ma20'], 2) if pd.notna(row['ma20']) else None,
                        'ma30': round(row['ma30'], 2) if pd.notna(row['ma30']) else None,
                        'ma60': round(row['ma60'], 2) if pd.notna(row['ma60']) else None,
                    })

            if updated_records:
                # 批量更新数据库
                update_query = text("""
                    UPDATE sheep_daily
                    SET ma5 = :ma5, ma10 = :ma10, ma20 = :ma20, ma30 = :ma30, ma60 = :ma60
                    WHERE sheep_code = :sheep_code AND trade_date = :trade_date
                """)

                for record in updated_records:
                    db.execute(update_query, record)

                db.commit()
                logger.info(f"成功更新 {len(updated_records)} 只股票的MA数据 ({target_date})")
            else:
                logger.debug(f"没有找到需要更新的记录 ({target_date})")

    except Exception as e:
        logger.error(f"重新计算MA数据失败 ({target_date}): {e}")
        raise

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
