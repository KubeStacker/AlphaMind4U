"""
Falcon Data Engine 任务函数
所有调度任务的实现
"""
from datetime import date, datetime
from utils.logger import get_logger
from etl.trade_date_adapter import TradeDateAdapter
from services.data_collection_service import DataCollectionService
from utils.validation import DataValidator
from config import Config

logger = get_logger(__name__)

def task_realtime_kline():
    """
    实时K线采集任务（1分钟）
    目标表: sheep_min_kline (code, time, open, high, low, close, vol)
    逻辑: 仅采集当前时刻的数据，使用 INSERT ON DUPLICATE KEY UPDATE
    """
    try:
        # 交易日历检查（静默退出）
        today = date.today()
        if not TradeDateAdapter.is_trading_day(today):
            return
        
        if not TradeDateAdapter.is_trading_hours():
            return
        
        # 采集实时K线数据（极度静默，除非发生错误）
        service = DataCollectionService()
        result = service.collect_realtime_trading_data()
        
        if not result.get('success'):
            logger.error(f"实时K线采集失败: {result.get('message')}")
        
    except Exception as e:
        logger.error(f"实时K线采集异常: {e}", exc_info=True)

def task_market_heat():
    """
    市场热度雷达任务（30分钟）
    目标表: market_hot_rank_snapshot
    逻辑: 抓取热榜并计算连板高度
    """
    try:
        # 交易日历检查（静默退出）
        today = date.today()
        if not TradeDateAdapter.is_trading_day(today):
            return
        
        if not TradeDateAdapter.is_trading_hours():
            return
        
        service = DataCollectionService()
        service.collect_hot_rank_data()
        
        logger.info("市场热度雷达数据采集完成")
    except Exception as e:
        logger.error(f"市场热度雷达采集失败: {e}", exc_info=True)

def task_daily_settlement():
    """
    每日盘后结算任务（16:00）
    逻辑：
    1. 增量采集：调用AKShare的"当日榜单"接口，一次请求获取全市场数据
    2. 智能断点续传：查询数据库max(trade_date)，补齐缺失日期的数据
    """
    try:
        today = date.today()
        
        # 交易日历检查（静默退出）
        if not TradeDateAdapter.is_trading_day(today):
            return
        
        logger.info("=" * 60)
        logger.info("每日盘后结算任务开始")
        logger.info("=" * 60)
        
        service = DataCollectionService()
        
        # 1. 查询数据库最大交易日期
        from db.sheep_repository import SheepRepository
        max_date = SheepRepository.get_max_trade_date()
        
        # 2. 智能断点续传：如果max_date < today - 1，自动补齐
        if max_date and max_date < today:
            from datetime import timedelta
            gap_days = (today - max_date).days
            
            if gap_days > 1:
                logger.info(f"发现数据缺失：最新日期 {max_date}，目标日期 {today}，缺失 {gap_days} 天")
                logger.info("开始自动补齐缺失数据...")
                
                # 获取缺失的交易日列表
                missing_trading_days = TradeDateAdapter.get_trading_days_in_range(
                    max_date + timedelta(days=1), 
                    today - timedelta(days=1)
                )
                
                if missing_trading_days:
                    total_days = len(missing_trading_days)
                    logger.info(f"需要补齐 {total_days} 个交易日的数据")
                    
                    # 逐天补齐（每100个交易日输出一次进度）
                    for idx, trading_day in enumerate(missing_trading_days, 1):
                        try:
                            # 采集该交易日的数据
                            service._collect_sheep_daily_for_date(trading_day)
                            service._collect_money_flow_data_individual(trading_day)
                            
                            if idx % 100 == 0 or idx == total_days:
                                logger.info(f"补齐进度: {idx}/{total_days}")
                        except Exception as e:
                            logger.error(f"补齐交易日 {trading_day} 数据失败: {e}")
                            continue
                    
                    logger.info(f"数据补齐完成，共处理 {total_days} 个交易日")
        
        # 3. 采集当日数据（使用批量接口）
        logger.info("开始采集当日数据...")
        
        # 采集肥羊日K数据
        service.collect_sheep_daily_data()
        
        # 采集资金流向数据（优先使用批量接口）
        service.collect_money_flow_data()
        
        # 采集概念资金流向数据
        service.collect_concept_money_flow_data(target_date=today)
        
        # 采集大盘指数数据
        service.collect_index_data(index_code='CSI1000')
        
        logger.info("=" * 60)
        logger.info("每日盘后结算任务完成")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"每日盘后结算任务失败: {e}", exc_info=True)

def task_hot_rank():
    """热度榜数据采集任务（每10分钟）"""
    try:
        service = DataCollectionService()
        service.collect_hot_rank_data()
        logger.info("热度榜数据采集任务完成")
    except Exception as e:
        logger.error(f"热度榜数据采集失败: {e}", exc_info=True)

def task_concept_money_flow_realtime():
    """概念资金流实时采集任务（交易时间每10分钟）"""
    try:
        today = date.today()
        if not TradeDateAdapter.is_trading_day(today):
            return
        
        if not TradeDateAdapter.is_trading_hours():
            return
        
        service = DataCollectionService()
        service.collect_concept_money_flow_data(target_date=today)
        logger.info("概念资金流实时采集任务完成")
    except Exception as e:
        logger.error(f"概念资金流实时采集失败: {e}", exc_info=True)

def task_rps_calculation():
    """RPS指标计算任务（交易日15:30）"""
    try:
        today = date.today()
        if not TradeDateAdapter.is_trading_day(today):
            return
        
        service = DataCollectionService()
        result = service.calculate_and_update_rps(target_date=today)
        
        if result.get('success'):
            logger.info(f"RPS指标计算完成: 更新 {result.get('updated_count', 0)} 只肥羊")
        else:
            logger.warning(f"RPS指标计算失败: {result.get('message')}")
    except Exception as e:
        logger.error(f"RPS指标计算失败: {e}", exc_info=True)

def task_vcp_calculation():
    """VCP和vol_ma_5指标计算任务（交易日15:35）"""
    try:
        today = date.today()
        if not TradeDateAdapter.is_trading_day(today):
            return
        
        service = DataCollectionService()
        result = service.calculate_and_update_vcp_vol_ma5(target_date=today)
        
        if result.get('success'):
            logger.info(f"VCP和vol_ma_5指标计算完成: 更新 {result.get('updated_count', 0)} 只肥羊")
        else:
            logger.warning(f"VCP和vol_ma_5指标计算失败: {result.get('message')}")
    except Exception as e:
        logger.error(f"VCP和vol_ma_5指标计算失败: {e}", exc_info=True)

def task_sector_rps_calculation():
    """Sector RPS指标计算任务（交易日15:45）"""
    try:
        today = date.today()
        if not TradeDateAdapter.is_trading_day(today):
            return
        
        service = DataCollectionService()
        result = service.calculate_and_update_sector_rps(target_date=today)
        
        if result.get('success'):
            logger.info(f"Sector RPS指标计算完成: 更新 {result.get('updated_count', 0)} 个板块")
        else:
            logger.warning(f"Sector RPS指标计算失败: {result.get('message')}")
    except Exception as e:
        logger.error(f"Sector RPS指标计算失败: {e}", exc_info=True)

def task_financial_data():
    """财务数据采集任务（每天凌晨0点）"""
    try:
        service = DataCollectionService()
        service.collect_financial_data()
        logger.info("财务数据采集任务完成")
    except Exception as e:
        logger.error(f"财务数据采集失败: {e}", exc_info=True)

def task_concept_data():
    """概念板块数据采集任务（每天凌晨3点）"""
    try:
        service = DataCollectionService()
        service.collect_concept_data()
        logger.info("概念板块数据采集任务完成")
    except Exception as e:
        logger.error(f"概念板块数据采集失败: {e}", exc_info=True)

def task_concept_metadata_sync():
    """概念元数据同步任务（每天08:00）"""
    try:
        service = DataCollectionService()
        result = service.sync_concept_metadata()
        if result['success']:
            logger.info(f"概念元数据同步完成: 新增 {result['new_concepts']} 个概念，共 {result['total_stocks']} 只肥羊")
        else:
            logger.error(f"概念元数据同步失败: {result.get('errors', [])}")
    except Exception as e:
        logger.error(f"概念元数据同步失败: {e}", exc_info=True)

def task_cleanup_old_data():
    """数据清理任务（每天凌晨4点）"""
    try:
        service = DataCollectionService()
        service.cleanup_old_money_flow_data()
        service.cleanup_old_concept_money_flow_data()
        service.cleanup_old_hot_rank_data()
        logger.info("数据清理任务完成")
    except Exception as e:
        logger.error(f"数据清理失败: {e}", exc_info=True)
