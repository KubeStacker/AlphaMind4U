"""
定时任务调度器 - 重构版
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from services.data_collection_service import DataCollectionService
from services.next_day_prediction_service import NextDayPredictionService
from etl.trade_date_adapter import TradeDateAdapter
from config import Config
import logging
from datetime import datetime, date, timedelta
import time

logger = logging.getLogger(__name__)
# Reduce scheduler job logs by setting level to ERROR to suppress INFO and WARNING logs
logger.setLevel(logging.ERROR)

# Track the last execution time for rate limiting
last_execution_times = {}
TRADING_HOURS_NORMAL_INTERVAL = 60  # 60 seconds in normal market
TRADING_HOURS_VOLATILE_INTERVAL = 30  # 30 seconds in volatile market
NON_TRADING_EXECUTION_INTERVAL = 1800  # 30 minutes in seconds

# Adaptive scheduling based on market conditions
MARKET_VOLATILE_THRESHOLD = 2.5  # Percentage change threshold for volatile market

def collect_hot_rank_data():
    """热度榜数据采集任务（每10分钟执行）"""
    try:
        service = DataCollectionService()
        
        # 采集热度榜数据（自然日数据）
        service.collect_hot_rank_data()
        
        logger.info("热度榜数据采集任务完成")
    except Exception as e:
        logger.error(f"热度榜数据采集失败: {e}", exc_info=True)

def collect_trading_day_data():
    """交易日数据采集任务（仅在交易日15:00执行）"""
    today = date.today()
    
    try:
        # 判断是否为交易日
        if not TradeDateAdapter.is_trading_day(today):
            logger.info(f"{today} 不是交易日，跳过交易日数据采集")
            return
        
        service = DataCollectionService()
        
        # 采集肥羊日K数据（仅在交易日）
        service.collect_sheep_daily_data()
        
        # 采集资金流向数据（仅在交易日）
        service.collect_money_flow_data()
        
        # 采集概念资金流向数据（仅在交易日，15:00最终刷新）
        service.collect_concept_money_flow_data(target_date=today)
        
        # 采集大盘指数数据（仅在交易日，用于RSRS牛熊市判断）
        service.collect_index_data(index_code='CSI1000')
        
        logger.info("交易日数据采集任务完成")
    except Exception as e:
        error_msg = f"交易日数据采集失败: {e}"
        logger.error(error_msg, exc_info=True)

def collect_sheep_daily_data_fallback():
    """非交易日肥羊日K数据采集任务（用于补全历史数据或处理遗漏数据）"""
    today = date.today()
    
    try:
        # 仅在非交易日执行
        if not TradeDateAdapter.is_trading_day(today):
            service = DataCollectionService()
            
            # 采集肥羊日K数据（用于补全历史数据）
            service.collect_sheep_daily_data()
            
            logger.info("非交易日肥羊日K数据采集任务完成（数据补全）")
    except Exception as e:
        error_msg = f"非交易日肥羊日K数据采集失败: {e}"
        logger.error(error_msg, exc_info=True)

def calculate_rps_indicator():
    """
    RPS指标计算任务（交易日15:30执行，等待日K数据采集完成后）
    
    RPS（Relative Price Strength）= 250日价格强度的百分位排名
    """
    today = date.today()
    
    try:
        # 判断是否为交易日
        if not TradeDateAdapter.is_trading_day(today):
            logger.info(f"{today} 不是交易日，跳过RPS计算")
            return
        
        service = DataCollectionService()
        result = service.calculate_and_update_rps(target_date=today)
        
        if result.get('success'):
            logger.info(f"RPS指标计算完成: 更新 {result.get('updated_count', 0)} 只肥羊")
        else:
            logger.warning(f"RPS指标计算失败: {result.get('message')}")
            
    except Exception as e:
        logger.error(f"RPS指标计算失败: {e}", exc_info=True)

def collect_concept_money_flow_realtime():
    """概念资金流实时采集任务（交易时间每1分钟执行）"""
    from datetime import datetime
    
    try:
        # 判断是否为交易时间
        if not TradeDateAdapter.is_trading_hours():
            return
        
        today = date.today()
        # 判断是否为交易日
        if not TradeDateAdapter.is_trading_day(today):
            return
        
        service = DataCollectionService()
        # 采集当日概念资金流数据（实时刷新）
        service.collect_concept_money_flow_data(target_date=today)
        logger.info("概念资金流实时采集任务完成")
    except Exception as e:
        logger.error(f"概念资金流实时采集失败: {e}", exc_info=True)

def generate_next_day_prediction():
    """
    生成下个交易日预测任务（每半小时执行）
    
    规则：
    - 新交易日之前（收盘后）的预测结果保持稳定
    - 盘中每半小时更新一次
    - 非交易日不更新
    """
    try:
        today = date.today()
        current_time = datetime.now()
        
        # 判断是否为交易日
        is_trading_day = TradeDateAdapter.is_trading_day(today)
        
        # 判断是否在交易时段（9:30-15:00）
        is_trading_hours = TradeDateAdapter.is_trading_hours()
        
        # 判断是否收盘后（15:00之后）
        is_after_close = current_time.hour >= 15
        
        # 收盘后或非交易日：只在第一次调用时生成预测，之后保持稳定
        if not is_trading_day or (is_trading_day and is_after_close):
            # 检查是否已有今天的预测缓存
            next_trading_day = TradeDateAdapter.get_next_trading_day(today)
            cached = NextDayPredictionService._get_cached_prediction(next_trading_day)
            if cached:
                logger.debug(f"收盘后/非交易日，使用已有预测缓存: {next_trading_day}")
                return
            else:
                # 第一次生成预测
                logger.info(f"收盘后/非交易日，首次生成预测: {next_trading_day}")
        
        # 交易时段：每次都更新
        if is_trading_hours:
            logger.info("交易时段，更新下个交易日预测...")
        
        # 生成预测
        result = NextDayPredictionService.generate_prediction(force=False)
        
        if result.get('success'):
            logger.info(f"下个交易日预测生成成功: {result.get('target_date')}")
        else:
            logger.warning(f"下个交易日预测生成失败: {result.get('message')}")
            
    except Exception as e:
        logger.error(f"生成下个交易日预测失败: {e}", exc_info=True)

def collect_realtime_data_trading_hours():
    """实时交易数据采集任务（仅在交易时间执行，根据市场波动性调整频率）"""
    try:
        # 确认是在交易时间内执行
        if TradeDateAdapter.is_trading_hours():
            # Use adaptive frequency based on market conditions
            current_time = time.time()
            key = 'realtime_data_trading'
            
            # Determine interval based on market volatility
            if adaptive_scheduler.is_market_volatile():
                interval = TRADING_HOURS_VOLATILE_INTERVAL
                market_status = "波动市场"
            else:
                interval = TRADING_HOURS_NORMAL_INTERVAL
                market_status = "平稳市场"
            
            if key not in last_execution_times or \
               (current_time - last_execution_times[key]) >= interval:
                
                service = DataCollectionService()
                # 采集实时交易数据（行情、资金流）
                result = service.collect_realtime_trading_data()
                
                last_execution_times[key] = current_time
                logger.debug(f"实时交易数据采集任务完成（交易时间，{market_status}，间隔{interval}秒）")
            else:
                logger.debug(f"实时交易数据采集跳过（交易时间，{market_status}，尚未到达执行间隔{interval}秒）")
    except Exception as e:
        logger.error(f"实时交易数据采集失败: {e}", exc_info=True)

class AdaptiveScheduler:
    """自适应调度器，根据市场条件调整采集频率"""
    
    def __init__(self):
        self.last_market_status_check = 0
        self.current_market_state = 'normal'  # 'volatile' or 'normal'
        self.market_state_check_interval = 300  # 5 minutes
        
    def is_market_volatile(self):
        """检查市场是否处于波动状态"""
        import time
        current_time = time.time()
        
        # Check market status every 5 minutes at most
        if current_time - self.last_market_status_check < self.market_state_check_interval:
            return self.current_market_state == 'volatile'
        
        try:
            from db.sheep_repository import SheepRepository
            from datetime import datetime, timedelta
            
            # 获取最近一段时间的市场平均波动率
            # 检查最近的数据是否有大的波动
            yesterday = datetime.now().date() - timedelta(days=1)
            recent_data = SheepRepository.get_sheep_daily_for_date(yesterday)
            
            if recent_data:
                # 计算平均涨跌幅
                avg_change = abs(sum([
                    item.get('change_pct', 0) or 0 
                    for item in recent_data 
                    if item.get('change_pct') is not None
                ]) / len(recent_data)) if len(recent_data) > 0 else 0
                
                self.current_market_state = 'volatile' if avg_change > MARKET_VOLATILE_THRESHOLD else 'normal'
            else:
                self.current_market_state = 'normal'
            
            self.last_market_status_check = current_time
            return self.current_market_state == 'volatile'
            
        except Exception as e:
            logger.warning(f"检查市场波动性失败: {e}")
            # 如果无法确定，假设为正常市场
            self.current_market_state = 'normal'
            return False

# 创建全局自适应调度器实例
adaptive_scheduler = AdaptiveScheduler()

def collect_realtime_data_non_trading_hours():
    """实时交易数据采集任务（仅在非交易时间每30分钟执行）"""
    try:
        # 确认是在非交易时间内执行
        if not TradeDateAdapter.is_trading_hours():
            # Rate limiting for non-trading hours
            current_time = time.time()
            key = 'realtime_data_non_trading'
            
            if key not in last_execution_times or \
               (current_time - last_execution_times[key]) >= NON_TRADING_EXECUTION_INTERVAL:
                
                service = DataCollectionService()
                # 采集实时交易数据（行情、资金流）
                service.collect_realtime_trading_data()
                
                last_execution_times[key] = current_time
                logger.debug("实时交易数据采集任务完成（非交易时间）")
            else:
                logger.debug("实时交易数据采集跳过（非交易时间，尚未到达执行间隔）")
    except Exception as e:
        logger.error(f"实时交易数据采集失败: {e}", exc_info=True)

def collect_concept_data():
    """概念板块数据采集任务（每天凌晨3点执行）"""
    try:
        service = DataCollectionService()
        service.collect_concept_data()
        logger.info("概念板块数据采集任务完成")
    except Exception as e:
        logger.error(f"概念板块数据采集失败: {e}", exc_info=True)

def sync_concept_metadata():
    """概念元数据同步任务（每天08:00执行，开盘前同步最新概念）"""
    try:
        service = DataCollectionService()
        result = service.sync_concept_metadata()
        if result['success']:
            logger.info(f"概念元数据同步完成: 新增 {result['new_concepts']} 个概念，共 {result['total_stocks']} 只肥羊")
        else:
            logger.error(f"概念元数据同步失败: {result.get('errors', [])}")
    except Exception as e:
        logger.error(f"概念元数据同步失败: {e}", exc_info=True)

def collect_financial_data():
    """财务数据采集任务（每天凌晨0点执行）"""
    try:
        service = DataCollectionService()
        # 采集所有肥羊的财务数据（季度/年度数据，持久保存）
        service.collect_financial_data()
        logger.info("财务数据采集任务完成")
    except Exception as e:
        logger.error(f"财务数据采集失败: {e}", exc_info=True)

def cleanup_old_data():
    """数据清理任务（每天凌晨4点执行）"""
    try:
        service = DataCollectionService()
        
        # 清理资金流旧数据（保留最近3年）
        service.cleanup_old_money_flow_data()
        
        # 清理概念资金流旧数据（保留最近3个月）
        service.cleanup_old_concept_money_flow_data()
        
        # 清理热度榜旧数据（保留最近30天）
        service.cleanup_old_hot_rank_data()
        
        logger.info("数据清理任务完成")
    except Exception as e:
        logger.error(f"数据清理失败: {e}", exc_info=True)

def calculate_vcp_vol_ma5_indicator():
    """
    VCP和vol_ma_5指标计算任务（交易日15:35执行，等待日K数据采集完成后）
    
    VCP（Volume Contraction Pattern）= 成交量收缩模式因子
    vol_ma_5 = 5日平均成交量
    """
    today = date.today()
    
    try:
        # 判断是否为交易日
        if not TradeDateAdapter.is_trading_day(today):
            logger.info(f"{today} 不是交易日，跳过VCP/vol_ma_5计算")
            return
        
        service = DataCollectionService()
        result = service.calculate_and_update_vcp_vol_ma5(target_date=today)
        
        if result.get('success'):
            logger.info(f"VCP和vol_ma_5指标计算完成: 更新 {result.get('updated_count', 0)} 只肥羊")
        else:
            logger.warning(f"VCP和vol_ma_5指标计算失败: {result.get('message')}")
            
    except Exception as e:
        logger.error(f"VCP和vol_ma_5指标计算失败: {e}", exc_info=True)

def calculate_historical_indicators_once():
    """
    一次性计算所有历史指标数据（rps_250, vol_ma_5, vcp_factor）
    这个任务只需要运行一次，用于补全历史数据
    """
    try:
        service = DataCollectionService()
        result = service.calculate_all_indicators_historical()
        logger.info(f"历史指标一次性计算完成: {result.get('message', '未知结果')}")
    except Exception as e:
        logger.error(f"历史指标一次性计算失败: {e}", exc_info=True)

def check_and_trigger_missed_tasks():
    """检查并触发错过的任务（启动时和定期检查）"""
    from datetime import datetime
    
    today = date.today()
    current_time = datetime.now()
    
    # 检查交易日数据采集任务
    # 如果当前时间已经过了15:00，且是交易日，立即执行
    if TradeDateAdapter.is_trading_day(today) and current_time.hour >= 15:
        logger.info(f"检测到当前时间已过15:00，且是交易日，触发交易日数据采集任务")
        try:
            collect_trading_day_data()
        except Exception as e:
            logger.error(f"补偿执行交易日数据采集失败: {e}", exc_info=True)

def start_scheduler():
    """启动定时任务调度器"""
    # 配置APScheduler日志级别以完全抑制其内部日志
    import logging
    logging.getLogger('apscheduler').setLevel(logging.CRITICAL)
    
    scheduler = BackgroundScheduler()
    
    # 热度榜数据：每10分钟执行一次
    scheduler.add_job(
        collect_hot_rank_data,
        trigger=IntervalTrigger(minutes=10),
        id='hot_rank_data_collection',
        name='热度榜数据采集（每10分钟）',
        replace_existing=True
    )
    
    # 交易日数据：每奩15点执行（仅在交易日）
    scheduler.add_job(
        collect_trading_day_data,
        trigger=CronTrigger(hour=15, minute=0),
        id='trading_day_data_collection',
        name='交易日数据采集（肥羊数据、资金流）',
        replace_existing=True
    )
        
    # RPS指标计算：每奩15:30执行（仅在交易日，等待日K数据采集完成后）
    scheduler.add_job(
        calculate_rps_indicator,
        trigger=CronTrigger(hour=15, minute=30),
        id='rps_calculation',
        name='RPS指标计算（25015:30）',
        replace_existing=True
    )
    
    # VCP和vol_ma_5指标计算：每奩15:35执行（仅在交易日，等待日K数据采集完成后）
    scheduler.add_job(
        calculate_vcp_vol_ma5_indicator,
        trigger=CronTrigger(hour=15, minute=35),
        id='vcp_calculation',
        name='VCP和vol_ma_5指标计算（15:35）',
        replace_existing=True
    )
    
    # 财务数据：每天凌晨0点执行
    scheduler.add_job(
        collect_financial_data,
        trigger=CronTrigger(hour=0, minute=0),
        id='financial_data_collection',
        name='财务数据采集（每天0点）',
        replace_existing=True
    )
    
    # 概念板块数据：每天凌晨3点执行
    scheduler.add_job(
        collect_concept_data,
        trigger=CronTrigger(hour=3, minute=0),
        id='concept_data_collection',
        name='概念板块数据采集',
        replace_existing=True
    )
    
    # 概念元数据同步：每天08:00执行（开盘前同步最新概念）
    scheduler.add_job(
        sync_concept_metadata,
        trigger=CronTrigger(hour=8, minute=0),
        id='concept_metadata_sync',
        name='概念元数据同步（EastMoney）',
        replace_existing=True
    )
    
    # 数据清理任务：每天凌晨4点执行
    scheduler.add_job(
        cleanup_old_data,
        trigger=CronTrigger(hour=4, minute=0),
        id='cleanup_old_data',
        name='数据清理（资金流等）',
        replace_existing=True
    )
    
    # 实时数据采集：交易时间每1分钟执行（9:30-11:30, 13:00-15:00）
    scheduler.add_job(
        collect_realtime_data_trading_hours,
        trigger=IntervalTrigger(minutes=1),
        id='realtime_data_collection_trading',
        name='实时交易数据采集（交易时间每1分钟）',
        replace_existing=True
    )
    
    # 实时数据采集：非交易时间每30分钟执行一次
    scheduler.add_job(
        collect_realtime_data_non_trading_hours,
        trigger=CronTrigger(minute='*/30'),  # 每30分钟执行一次
        id='realtime_data_collection_non_trading',
        name='实时交易数据采集（非交易时间每30分钟）',
        replace_existing=True
    )
    
    # 概念资金流实时采集：交易时间每1分钟执行（9:30-11:30, 13:00-15:00）
    scheduler.add_job(
        collect_concept_money_flow_realtime,
        trigger=IntervalTrigger(minutes=1),  # 每1分钟执行一次
        id='concept_money_flow_realtime',
        name='概念资金流实时采集（交易时间每1分钟）',
        replace_existing=True
    )
    
    # 下个交易日预测：每半小时执行一次（9:00-23:59）
    # 交易时段实时更新，收盘后保持稳定
    scheduler.add_job(
        generate_next_day_prediction,
        trigger=CronTrigger(hour='9-23', minute='0,30'),  # 每半小时执行
        id='next_day_prediction',
        name='下个交易日预测（每半小时）',
        replace_existing=True
    )
    
    # 非交易日肥羊数据采集：每天上午10点执行（用于补全历史数据）
    scheduler.add_job(
        collect_sheep_daily_data_fallback,
        trigger=CronTrigger(hour=10, minute=0),  # 每天上午10点
        id='sheep_daily_fallback_collection',
        name='非交易日肥羊日K数据采集（数据补全）',
        replace_existing=True
    )
    
    # 补偿检查任务：每小时检查一次，如果错过就执行（15:00-23:59之间）
    scheduler.add_job(
        check_and_trigger_missed_tasks,
        trigger=CronTrigger(hour='15-23', minute=0),  # 15:00-23:59每小时检查
        id='check_missed_tasks',
        name='检查错过的任务',
        replace_existing=True
    )
    

    
    scheduler.start()
    logger.info("定时任务调度器已启动")
    logger.info("- 每天00:00执行财务数据采集（所有肥羊的历史财务数据，持久保存）")
    logger.info("- 每10分钟执行热度榜数据采集")
    logger.info("- 交易日15:00执行交易日数据采集（肥羊数据、资金流、概念资金流，仅在交易日）")
    logger.info("- 交易日15:30执行RPS指标计算（250日价格强度百分位排名）")
    logger.info("- 交易日15:35执行VCP和vol_ma_5指标计算（成交量收缩模式因子和5日均量）")
    logger.info("- 交易时间每1分钟执行概念资金流实时采集（9:30-11:30, 13:00-15:00）")
    logger.info("- 交易时间每1分钟执行实时交易数据采集（行情、资金流）")
    logger.info("- 非交易时间每30分钟执行实时交易数据采集（行情、资金流）")
    logger.info("- 每半小时执行下个交易日预测（9:00-23:59，收盘后保持稳定）")
    logger.info("- 每天03:00执行概念板块数据采集")
    logger.info("- 每天08:00执行概念元数据同步（EastMoney，开盘前同步最新概念）")
    logger.info("- 每天04:00执行数据清理（资金流保留3年；概念资金流保留3个月；热度榜保留30天）")
    logger.info("- 每天10:00执行非交易日数据补全（肥羊日K数据补全）")
    logger.info("- 每小时检查错过的任务（15:00-23:59）")
    
    # 启动时延迟检查并触发错过的任务（避免阻塞HTTP服务器启动）
    # 使用线程异步执行，确保HTTP服务器先启动
    import threading
    def delayed_check():
        import time
        time.sleep(5)  # 等待5秒，确保HTTP服务器先启动
        check_and_trigger_missed_tasks()
    # 启动时运行历史指标计算（异步执行，不阻塞启动）
    def delayed_historical_calc():
        import time
        time.sleep(10)  # 等待10秒，确保数据库连接等资源准备就绪
        try:
            calculate_historical_indicators_once()
        except Exception as e:
            logger.error(f"启动时历史指标计算失败: {e}", exc_info=True)
    
    threading.Thread(target=delayed_historical_calc, daemon=True).start()
    
    threading.Thread(target=delayed_check, daemon=True).start()
    
    return scheduler
