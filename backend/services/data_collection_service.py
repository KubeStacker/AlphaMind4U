"""
数据采集服务层
"""
from datetime import date, datetime, timedelta
from typing import Optional, List, Dict, Set
import logging
from etl.trade_date_adapter import TradeDateAdapter
from etl.sheep_adapter import SheepAdapter
from etl.concept_adapter import ConceptAdapter
from etl.hot_rank_adapter import HotRankAdapter
from etl.index_adapter import IndexAdapter
from etl.financial_adapter import FinancialAdapter
from etl.concept_filter import should_filter_concept
from db.sheep_repository import SheepRepository
from db.money_flow_repository import MoneyFlowRepository
from db.concept_repository import ConceptRepository
from db.hot_rank_repository import HotRankRepository
from db.index_repository import IndexRepository
from db.financial_repository import FinancialRepository
from config import Config
import pandas as pd

logger = logging.getLogger(__name__)

class DataCollectionService:
    """数据采集服务"""
    
    def __init__(self):
        self.trade_date_adapter = TradeDateAdapter
        self.sheep_adapter = SheepAdapter
        self.concept_adapter = ConceptAdapter
        self.hot_rank_adapter = HotRankAdapter
        self.index_adapter = IndexAdapter
        self.financial_adapter = FinancialAdapter
    
    @staticmethod
    def _validate_change_pct(value) -> Optional[float]:
        """
        验证并限制涨跌幅值在合理范围内
        
        Args:
            value: 涨跌幅值（可能是NaN、None或数值）
            
        Returns:
            验证后的涨跌幅值（1位小数精度），如果无效则返回None
        """
        if pd.isna(value) or value is None:
            return None
        
        try:
            change_pct = float(value)
            # 限制在合理范围内：-1000% 到 1000%（覆盖所有极端情况）
            # 如果超出范围，记录警告并限制值
            if change_pct < -1000:
                logger.warning(f"涨跌幅值 {change_pct}% 超出下限，限制为 -1000%")
                return -1000.0
            elif change_pct > 1000:
                logger.warning(f"涨跌幅值 {change_pct}% 超出上限，限制为 1000%")
                return 1000.0
            return round(change_pct, 1)  # 1位小数精度
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _format_price(value) -> Optional[float]:
        """
        格式化价格为1位小数精度
        
        Args:
            value: 价格值（可能是NaN、None或数值）
            
        Returns:
            格式化后的价格值（1位小数精度），如果无效则返回None
        """
        if pd.isna(value) or value is None:
            return None
        try:
            return round(float(value), 1)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _format_percentage(value) -> Optional[float]:
        """
        格式化百分比为1位小数精度
        
        Args:
            value: 百分比值（可能是NaN、None或数值）
            
        Returns:
            格式化后的百分比值（1位小数精度），如果无效则返回None
        """
        if pd.isna(value) or value is None:
            return None
        try:
            return round(float(value), 1)
        except (ValueError, TypeError):
            return None
    
    def collect_sheep_daily_data(self, days: int = None):
        """
        采集肥羊日K数据（仅在交易日执行）
        
        Args:
            days: 采集最近N天的数据，默认使用配置的保留天数。将采集这N天中包含的M个交易日的数据。
        """
        if days is None:
            days = Config.SHEEP_DATA_RETENTION_DAYS
        
        today = date.today()
        
        # 判断是否为交易日
        if not self.trade_date_adapter.is_trading_day(today):
            last_trading_day = self.trade_date_adapter.get_last_trading_day(today)
            logger.debug(f"{today} 不是交易日，将采集上一个交易日 {last_trading_day} 的数据")
            target_date = last_trading_day
        else:
            target_date = today
        
        # 计算日期范围：从目标日期往前推N天（自然日）
        start_date_natural = target_date - timedelta(days=days)
        
        # 获取这N天内的所有交易日（M个）
        trading_days = self.trade_date_adapter.get_trading_days_in_range(start_date_natural, target_date)
        
        if not trading_days:
            logger.warning(f"在最近{days}天内未找到交易日，将采集最近交易日的数据")
            trading_days = [target_date]
        
        # 使用交易日范围
        start_date = min(trading_days)
        end_date = max(trading_days)
        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')
        
        logger.debug(f"开始采集肥羊日K数据，设置{days}天，包含{len(trading_days)}个交易日，日期范围: {start_date_str} 至 {end_date_str}")
        
        # 获取所有肥羊代码
        sheep_list = self.sheep_adapter.get_all_sheep_codes()
        if not sheep_list:
            logger.error("未能获取到肥羊列表")
            return
        
        logger.debug(f"共需处理 {len(sheep_list)} 只肥羊")
        
        success_count = 0
        error_count = 0
        
        for idx, sheep_item in enumerate(sheep_list):
            try:
                sheep_code = sheep_item['code']
                sheep_name = sheep_item['name']
                
                # 检查数据库中已有的最新日期
                latest_date = SheepRepository.get_latest_trade_date(sheep_code)
                
                # 确定实际需要采集的日期范围
                if latest_date:
                    # 如果已有数据，从最新日期+1天开始采集到目标日期
                    # 但也要确保至少采集最近N天的数据（用于补全可能缺失的数据）
                    actual_start_date = latest_date + timedelta(days=1)
                    # 如果最新日期已经很接近目标日期，说明数据较新，只需补全最近的数据
                    days_since_latest = (target_date - latest_date).days
                    if days_since_latest < 30:
                        # 数据较新，只补全最近30天
                        actual_start_date = max(actual_start_date, target_date - timedelta(days=30))
                    else:
                        # 数据较旧，从最新日期开始补全到目标日期
                        actual_start_date = latest_date + timedelta(days=1)
                    
                    # 如果实际开始日期已经超过目标日期，说明数据已是最新，跳过
                    if actual_start_date > target_date:
                        continue
                    
                    actual_start_str = actual_start_date.strftime('%Y%m%d')
                    logger.debug(f"肥羊 {sheep_code} 已有数据至 {latest_date}，将从 {actual_start_date} 开始补全")
                else:
                    # 如果没有历史数据，从配置的起始日期开始采集
                    actual_start_str = start_date_str
                    logger.debug(f"肥羊 {sheep_code} 无历史数据，将从 {actual_start_str} 开始采集")
                
                # 获取日K数据
                df = self.sheep_adapter.get_sheep_daily_data(sheep_code, actual_start_str, end_date_str)
                
                if df is None or df.empty:
                    continue
                
                # 转换为数据库格式（向量化操作，避免iterrows）
                def format_row(row):
                    return {
                        'code': sheep_code,
                        'date': row['date'],
                        'open': DataCollectionService._format_price(row.get('open')),
                        'close': DataCollectionService._format_price(row.get('close')),
                        'high': DataCollectionService._format_price(row.get('high')),
                        'low': DataCollectionService._format_price(row.get('low')),
                        'volume': int(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0,
                        'amount': float(row.get('amount', 0)) if pd.notna(row.get('amount')) else 0,
                        'turnover_rate': DataCollectionService._format_percentage(row.get('turnover_rate')),
                        'change_pct': DataCollectionService._validate_change_pct(row.get('change_pct')),
                        'ma5': DataCollectionService._format_price(row.get('ma5')),
                        'ma10': DataCollectionService._format_price(row.get('ma10')),
                        'ma20': DataCollectionService._format_price(row.get('ma20')),
                        'ma30': DataCollectionService._format_price(row.get('ma30')),
                        'ma60': DataCollectionService._format_price(row.get('ma60')),
                    }
                
                data_list = df.apply(format_row, axis=1).tolist()
                
                # 批量保存
                SheepRepository.batch_upsert_sheep_daily(data_list)
                
                # 更新肥羊基本信息
                market = 'SH' if sheep_code.startswith('6') else 'SZ'
                SheepRepository.upsert_sheep_basic(sheep_code, sheep_name, market=market)
                
                success_count += 1
                
                if (idx + 1) % 100 == 0:
                    logger.debug(f"进度: {idx + 1}/{len(sheep_list)}, 成功: {success_count}, 失败: {error_count}")
                
            except Exception as e:
                logger.warning(f"处理肥羊 {sheep_item.get('code', 'unknown')} 失败: {e}")
                error_count += 1
                continue
        
        logger.debug(f"肥羊日K数据采集完成！成功: {success_count}, 失败: {error_count}")
    
    def refresh_single_sheep_data(self, sheep_code: str) -> bool:
        """
        刷新单个肥羊的最新数据（仅在交易时段执行）
        
        Args:
            sheep_code: 肥羊代码
            
        Returns:
            True表示刷新成功，False表示失败或非交易时段
        """
        from datetime import datetime
        
        # 判断是否为交易时段
        if not self.trade_date_adapter.is_trading_hours():
            logger.debug(f"当前不是交易时段，跳过刷新肥羊 {sheep_code} 的数据")
            return False
        
        today = date.today()
        
        # 判断是否为交易日
        if not self.trade_date_adapter.is_trading_day(today):
            logger.debug(f"{today} 不是交易日，跳过刷新肥羊 {sheep_code} 的数据")
            return False
        
        try:
            logger.debug(f"开始刷新肥羊 {sheep_code} 的最新数据...")
            
            # 获取今日数据
            today_str = today.strftime('%Y%m%d')
            
            # 获取日K数据
            df = self.sheep_adapter.get_sheep_daily_data(sheep_code, today_str, today_str)
            
            if df is not None and not df.empty:
                # 转换为数据库格式（向量化操作）
                def format_row(row):
                    return {
                        'code': sheep_code,
                        'date': row['date'],
                        'open': DataCollectionService._format_price(row.get('open')),
                        'close': DataCollectionService._format_price(row.get('close')),
                        'high': DataCollectionService._format_price(row.get('high')),
                        'low': DataCollectionService._format_price(row.get('low')),
                        'volume': int(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0,
                        'amount': float(row.get('amount', 0)) if pd.notna(row.get('amount')) else 0,
                        'turnover_rate': DataCollectionService._format_percentage(row.get('turnover_rate')),
                        'change_pct': DataCollectionService._validate_change_pct(row.get('change_pct')),
                        'ma5': DataCollectionService._format_price(row.get('ma5')),
                        'ma10': DataCollectionService._format_price(row.get('ma10')),
                        'ma20': DataCollectionService._format_price(row.get('ma20')),
                        'ma30': DataCollectionService._format_price(row.get('ma30')),
                        'ma60': DataCollectionService._format_price(row.get('ma60')),
                    }
                
                data_list = df.apply(format_row, axis=1).tolist()
                
                # 保存数据
                SheepRepository.batch_upsert_sheep_daily(data_list)
                logger.debug(f"肥羊 {sheep_code} 日K数据刷新成功")
            
            # 获取资金流向数据
            try:
                flow_data_dict = self.sheep_adapter.get_sheep_money_flow(sheep_code)
                if flow_data_dict:
                    flow_data = [{
                        'code': sheep_code,
                        'date': today,
                        'main': float(flow_data_dict.get('main_net_inflow', 0)),
                        'super_large': float(flow_data_dict.get('super_large_inflow', 0)),
                        'large': float(flow_data_dict.get('large_inflow', 0)),
                        'medium': float(flow_data_dict.get('medium_inflow', 0)),
                        'small': float(flow_data_dict.get('small_inflow', 0)),
                    }]
                    
                    MoneyFlowRepository.batch_upsert_money_flow(flow_data)
                    logger.debug(f"肥羊 {sheep_code} 资金流向数据刷新成功")
            except Exception as e:
                logger.warning(f"刷新肥羊 {sheep_code} 资金流向数据失败: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"刷新肥羊 {sheep_code} 数据失败: {e}", exc_info=True)
            return False
    
    def collect_realtime_trading_data(self) -> Dict[str, any]:
        """
        采集实时交易数据（1分钟刷新）：包括所有肥羊的实时行情和今日资金流向
        仅在交易时段执行
        """
        from datetime import date
        import time
        
        start_time = time.time()
        results = {'spot': False, 'money_flow': False}
        
        # 1. 判断是否为交易时段
        if not self.trade_date_adapter.is_trading_hours():
            logger.debug("当前不是交易时段，跳过实时数据采集")
            return {'success': False, 'message': '非交易时段'}
        
        today = date.today()
        
        try:
            # 2. 采集所有肥羊的实时行情
            logger.debug("开始采集实时行情数据...")
            spot_df = self.sheep_adapter.get_all_sheep_spot_data()
            
            if spot_df is not None and not spot_df.empty:
                # 转换为数据库格式
                data_list = []
                for _, row in spot_df.iterrows():
                    data_list.append({
                        'code': row['sheep_code'],
                        'date': today,
                        'open': self._format_price(row['open']),
                        'close': self._format_price(row['close']),
                        'high': self._format_price(row['high']),
                        'low': self._format_price(row['low']),
                        'volume': int(row['volume']),
                        'amount': float(row['amount']),
                        'change_pct': self._validate_change_pct(row['change_pct']),
                        'turnover_rate': self._format_percentage(row['turnover_rate']),
                        # 实时更新不重新计算均线，保留原有值或设为None
                        'ma5': None, 'ma10': None, 'ma20': None, 'ma30': None, 'ma60': None
                    })
                
                if data_list:
                    # 使用批量更新（ON DUPLICATE KEY UPDATE 会保留未提供的字段吗？不，VALUES()会覆盖）
                    # Wait, if I provide None for MAs, and the query is:
                    # ON DUPLICATE KEY UPDATE ma5 = VALUES(ma5), ...
                    # It WILL overwrite with NULL. That's bad.
                    
                    # I should use a specific query for real-time update that doesn't touch MAs.
                    self._batch_update_realtime_spot(data_list)
                    results['spot'] = True
            
            # 3. 采集所有肥羊的资金流向
            logger.debug("开始采集实时资金流向数据...")
            flow_df = self.sheep_adapter.get_all_sheep_money_flow()
            if flow_df is not None and not flow_df.empty:
                flow_data = []
                for _, row in flow_df.iterrows():
                    flow_data.append({
                        'code': row['sheep_code'],
                        'date': today,
                        'main': float(row.get('main_net_inflow', 0)),
                        'super_large': float(row.get('super_large_inflow', 0)),
                        'large': float(row.get('large_inflow', 0)),
                        'medium': float(row.get('medium_inflow', 0)),
                        'small': float(row.get('small_inflow', 0)),
                    })
                
                if flow_data:
                    MoneyFlowRepository.batch_upsert_money_flow(flow_data)
                    results['money_flow'] = True
            
            # 4. 同时采集概念资金流向（每分钟刷新一次也无妨）
            self.collect_concept_money_flow_data(target_date=today)
            results['concept_money_flow'] = True
            
            total_time = time.time() - start_time
            logger.debug(f"实时数据采集完成，耗时: {total_time:.2f}s")
            return {'success': True, 'results': results, 'total_time': total_time}
            
        except Exception as e:
            logger.error(f"实时数据采集失败: {e}", exc_info=True)
            return {'success': False, 'message': str(e)}

    def _batch_update_realtime_spot(self, data_list: List[Dict]):
        """批量更新实时行情数据（不触动均线字段）"""
        from db.database import get_db
        from sqlalchemy import text
        
        if not data_list:
            return
            
        with get_db() as db:
            query = text("""
                INSERT INTO sheep_daily 
                (sheep_code, trade_date, open_price, close_price, high_price, low_price,
                 volume, amount, turnover_rate, change_pct)
                VALUES 
                (:code, :date, :open, :close, :high, :low, :volume, :amount, 
                 :turnover_rate, :change_pct)
                ON DUPLICATE KEY UPDATE
                    open_price = VALUES(open_price),
                    close_price = VALUES(close_price),
                    high_price = VALUES(high_price),
                    low_price = VALUES(low_price),
                    volume = VALUES(volume),
                    amount = VALUES(amount),
                    turnover_rate = VALUES(turnover_rate),
                    change_pct = VALUES(change_pct)
            """)
            
            batch_size = 2000
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                db.execute(query, batch)
                db.commit()

    def collect_money_flow_data(self):
        """
        采集资金流向数据（如果不是交易日，则采集最近交易日的数据）
        优先使用批量接口，如果失败则使用逐个获取的方式
        """
        today = date.today()
        
        # 如果不是交易日，使用最近交易日
        if not self.trade_date_adapter.is_trading_day(today):
            target_date = self.trade_date_adapter.get_last_trading_day(today)
            logger.debug(f"{today} 不是交易日，将采集最近交易日 {target_date} 的资金流数据")
        else:
            target_date = today
        
        logger.debug(f"开始采集资金流向数据（日期: {target_date}）...")
        
        # 方法1：尝试批量获取所有肥羊的资金流（更高效）
        flow_df = self.sheep_adapter.get_all_sheep_money_flow()
        
        if flow_df is not None and not flow_df.empty:
            # 转换为数据库格式（向量化操作）
            def format_flow_row(row):
                return {
                    'code': row['sheep_code'],
                    'date': target_date,
                    'main': float(row.get('main_net_inflow', 0)),
                    'super_large': float(row.get('super_large_inflow', 0)),
                    'large': float(row.get('large_inflow', 0)),
                    'medium': float(row.get('medium_inflow', 0)),
                    'small': float(row.get('small_inflow', 0)),
                }
            
            data_list = flow_df.apply(format_flow_row, axis=1).tolist()
            
            # 批量保存
            MoneyFlowRepository.batch_upsert_money_flow(data_list)
            
            logger.debug(f"资金流向数据采集完成（批量方式）！共 {len(data_list)} 条")
            return
        
        # 方法2：批量接口失败，使用逐个获取的方式（更可靠）
        logger.warning("批量获取资金流数据失败，改用逐个获取方式...")
        self._collect_money_flow_data_individual(today)
    
    def _collect_money_flow_data_individual(self, target_date: date):
        """
        逐个获取每只肥羊的资金流数据（备用方法）
        
        Args:
            target_date: 目标日期
        """
        # 获取所有肥羊代码（使用适配器的方法）
        sheep_list = self.sheep_adapter.get_all_sheep_codes()
        if not sheep_list:
            logger.error("未能获取到肥羊列表")
            return
        
        # 提取肥羊代码列表
        sheep_codes = [item['code'] for item in sheep_list]
        
        logger.debug(f"开始逐个获取 {len(sheep_codes)} 只肥羊在 {target_date} 的资金流数据...")
        
        success_count = 0
        error_count = 0
        total_records = 0
        skipped_count = 0  # 跳过的数量（数据不在历史范围内）
        
        # 检查目标日期是否在可获取的历史范围内（近100个交易日）
        today = date.today()
        last_trading_day = self.trade_date_adapter.get_last_trading_day(today) if not self.trade_date_adapter.is_trading_day(today) else today
        
        # 获取近100个交易日
        start_date_for_check = last_trading_day - timedelta(days=200)  # 乘以2是为了覆盖非交易日
        recent_trading_days = self.trade_date_adapter.get_trading_days_in_range(start_date_for_check, last_trading_day)
        recent_trading_days_set = set(recent_trading_days[-100:]) if len(recent_trading_days) > 100 else set(recent_trading_days)
        
        if target_date not in recent_trading_days_set:
            logger.warning(f"目标日期 {target_date} 不在可获取的历史范围内（近100个交易日），可能无法获取数据")
        
        for idx, sheep_code in enumerate(sheep_codes):
            try:
                # 获取单只肥羊的资金流数据（近100个交易日）
                flow_df = self.sheep_adapter.get_sheep_money_flow_history(sheep_code)
                
                if flow_df is None or flow_df.empty:
                    continue
                
                # 筛选出目标日期的数据
                target_data = flow_df[flow_df['trade_date'] == target_date]
                
                if target_data.empty:
                    # 如果目标日期不在历史数据范围内，跳过
                    skipped_count += 1
                    continue
                
                # 格式化数据
                row = target_data.iloc[0]
                data_list = [{
                    'code': sheep_code,
                    'date': target_date,
                    'main': float(row.get('main_net_inflow', 0)),
                    'super_large': float(row.get('super_large_inflow', 0)),
                    'large': float(row.get('large_inflow', 0)),
                    'medium': float(row.get('medium_inflow', 0)),
                    'small': float(row.get('small_inflow', 0)),
                }]
                
                # 保存数据
                MoneyFlowRepository.batch_upsert_money_flow(data_list)
                
                success_count += 1
                total_records += 1
                
                if (idx + 1) % 100 == 0:
                    logger.debug(f"进度: {idx + 1}/{len(sheep_codes)}, 成功: {success_count}, 失败: {error_count}, 跳过: {skipped_count}, 记录: {total_records}")
                
                # 延迟，避免请求过快
                import time
                time.sleep(0.1)  # 100ms延迟
                
            except Exception as e:
                logger.debug(f"处理肥羊 {sheep_code} 失败: {e}")
                error_count += 1
                continue
        
        logger.debug(f"资金流向数据采集完成（逐个方式，日期: {target_date}）！成功: {success_count}, 失败: {error_count}, 跳过: {skipped_count}, 总记录: {total_records}")
    
    def cleanup_old_money_flow_data(self):
        """
        清理资金流旧数据（保留最近3年）
        """
        logger.debug("开始清理资金流旧数据...")
        try:
            deleted_count = MoneyFlowRepository.cleanup_old_data(Config.MONEY_FLOW_RETENTION_DAYS)
            logger.debug(f"资金流数据清理完成，删除了 {deleted_count} 条旧数据")
        except Exception as e:
            logger.error(f"清理资金流旧数据失败: {e}", exc_info=True)
    
    def collect_hot_rank_data(self, target_date: Optional[date] = None):
        """
        采集热度榜数据（自然日数据，每天18点执行）
        
        Args:
            target_date: 目标日期，如果为None则使用今天或上一个交易日
        """
        today = date.today()
        
        if target_date is None:
            # 如果不是交易日，使用上一个交易日
            if not self.trade_date_adapter.is_trading_day(today):
                target_date = self.trade_date_adapter.get_last_trading_day(today)
                logger.debug(f"{today} 不是交易日，将采集上一个交易日 {target_date} 的热度榜数据")
            else:
                target_date = today
        
        logger.debug(f"开始采集热度榜数据（日期: {target_date}）...")
        
        # 采集多个数据源
        sources = ['xueqiu', 'dongcai']
        
        all_data = []
        for source in sources:
            try:
                hot_df = self.hot_rank_adapter.get_hot_rank(source)
                if hot_df is not None and not hot_df.empty:
                    normalized_df = self.hot_rank_adapter.normalize_hot_rank_data(hot_df, source)
                    if normalized_df is not None and not normalized_df.empty:
                        # 转换为数据库格式，添加数据验证（向量化操作）
                        def format_hot_rank_row(row):
                            sheep_code = str(row['sheep_code']).strip()
                            sheep_name = str(row['sheep_name']).strip()[:50]
                            
                            # 验证肥羊代码：必须是6位数字
                            if not sheep_code or len(sheep_code) != 6 or not sheep_code.isdigit():
                                return None
                            
                            # 验证排名：必须是正整数
                            try:
                                rank = int(row['rank'])
                                if rank <= 0:
                                    return None
                            except (ValueError, TypeError):
                                return None
                            
                            # 验证成交量
                            try:
                                volume = int(row.get('volume', 0))
                                if volume < 0:
                                    volume = 0
                            except (ValueError, TypeError):
                                volume = 0
                            
                            return {
                                'code': sheep_code,
                                'name': sheep_name,
                                'rank': rank,
                                'source': source,
                                'date': target_date,
                                'score': None,
                                'volume': volume
                            }
                        
                        # 使用apply并过滤None值
                        source_data = normalized_df.apply(format_hot_rank_row, axis=1).tolist()
                        all_data.extend([d for d in source_data if d is not None])
            except Exception as e:
                logger.warning(f"采集 {source} 热度榜失败: {e}")
                continue
        
        if all_data:
            HotRankRepository.batch_upsert_hot_rank(all_data, target_date)
            logger.debug(f"热度榜数据采集完成！共 {len(all_data)} 条")
        else:
            logger.warning("未获取到任何热度榜数据")
    
    def collect_concept_data(self):
        """
        采集概念板块数据（自然日数据，每天执行）
        """
        logger.debug("开始采集概念板块数据...")
        
        # 获取概念列表（优先同花顺）
        concept_list = self.concept_adapter.get_concept_list('ths')
        
        if concept_list is None or concept_list.empty:
            logger.warning("无法获取概念列表")
            return
        
        success_count = 0
        error_count = 0
        
        # 向量化提取概念名称和代码
        def extract_concept_info(row):
            concept_name = None
            concept_code = None
            
            if 'name' in concept_list.columns:
                concept_name = str(row['name']).strip()
                concept_code = str(row.get('code', '')).strip() if 'code' in concept_list.columns else None
            elif '板块名称' in concept_list.columns:
                concept_name = str(row['板块名称']).strip()
                concept_code = str(row.get('板块代码', '')).strip() if '板块代码' in concept_list.columns else None
            elif len(concept_list.columns) >= 2:
                concept_name = str(row.iloc[0]).strip()
                concept_code = str(row.iloc[1]).strip() if len(row) > 1 else None
            
            return concept_name, concept_code
        
        # 批量处理（使用apply，但保留循环用于错误处理）
        for idx in concept_list.index:
            try:
                row = concept_list.loc[idx]
                concept_name, concept_code = extract_concept_info(row)
                
                if not concept_name or concept_name == 'nan':
                    continue
                
                # 过滤无意义的概念
                if should_filter_concept(concept_name):
                    continue
                
                # 保存概念
                concept_id = ConceptRepository.upsert_concept(
                    concept_name, concept_code, source='ths'
                )
                
                if concept_id == 0:
                    continue
                
                # 获取概念下的肥羊
                sheep_codes = self.concept_adapter.get_concept_stocks(
                    concept_name, concept_code, source='ths'
                )
                
                if sheep_codes:
                    # 批量保存肥羊-概念关联
                    ConceptRepository.batch_upsert_sheep_concept_mapping(
                        concept_id, sheep_codes
                    )
                    success_count += 1
                
                if (idx + 1) % 10 == 0:
                    logger.debug(f"进度: {idx + 1}/{len(concept_list)}, 成功: {success_count}, 失败: {error_count}")
                
            except Exception as e:
                logger.warning(f"处理概念失败: {e}")
                error_count += 1
                continue
        
        logger.debug(f"概念板块数据采集完成！成功: {success_count}, 失败: {error_count}")
    
    def sync_concept_metadata(self) -> Dict[str, any]:
        """
        同步概念元数据：从EastMoney获取最新概念列表，增量更新数据库
        
        Returns:
            {
                'success': bool,
                'total_concepts': int,
                'new_concepts': int,
                'updated_concepts': int,
                'total_stocks': int,
                'errors': List[str]
            }
        """
        result = {
            'success': False,
            'total_concepts': 0,
            'new_concepts': 0,
            'updated_concepts': 0,
            'total_stocks': 0,
            'errors': []
        }
        
        try:
            logger.debug("开始同步概念元数据...")
            
            # 步骤1: 从EastMoney获取最新概念列表（带重试机制）
            concept_df = self.concept_adapter.fetch_eastmoney_concepts()
            
            if concept_df is None or concept_df.empty:
                error_msg = "无法从EastMoney获取概念列表"
                logger.error(error_msg)
                result['errors'].append(error_msg)
                return result
            
            # 提取概念名称列
            # akshare返回的列名可能是：'板块名称', '概念名称', 'name', '板块'等
            concept_name_col = None
            possible_name_columns = ['板块名称', '概念名称', 'name', '板块', '概念']
            
            for col_name in possible_name_columns:
                if col_name in concept_df.columns:
                    concept_name_col = col_name
                    break
            
            if concept_name_col is None and len(concept_df.columns) > 0:
                # 如果没找到标准列名，使用第一列
                concept_name_col = concept_df.columns[0]
            
            if concept_name_col is None:
                error_msg = "无法识别概念名称列"
                logger.error(error_msg)
                result['errors'].append(error_msg)
                return result
            
            # 获取概念名称列表
            eastmoney_concepts = set(concept_df[concept_name_col].dropna().astype(str).unique())
            result['total_concepts'] = len(eastmoney_concepts)
            
            logger.debug(f"从EastMoney获取到 {len(eastmoney_concepts)} 个概念")
            
            # 步骤2: 从数据库获取现有概念列表
            existing_concepts = self._get_existing_concepts()
            logger.debug(f"数据库中现有 {len(existing_concepts)} 个概念")
            
            # 步骤3: 计算差异 - 找出新增/缺失的概念
            new_concepts = eastmoney_concepts - existing_concepts
            missing_concepts = new_concepts  # 新增的概念就是缺失的概念
            
            logger.debug(f"发现 {len(missing_concepts)} 个新概念需要同步")
            
            # 步骤4: 增量更新 - 只处理缺失的概念
            synced_count = 0
            total_stocks_synced = 0
            
            for concept_name in missing_concepts:
                try:
                    logger.debug(f"正在同步概念: {concept_name}")
                    
                    # 获取概念下的肥羊列表（带重试机制）
                    stock_codes = self.concept_adapter.fetch_concept_constituents(concept_name)
                    
                    if stock_codes is None or len(stock_codes) == 0:
                        logger.warning(f"概念 {concept_name} 没有肥羊数据，跳过")
                        continue
                    
                    # 插入或更新概念
                    concept_id = ConceptRepository.upsert_concept(
                        concept_name=concept_name,
                        source='em',  # EastMoney
                        description=f'从EastMoney同步，包含{len(stock_codes)}只肥羊'
                    )
                    
                    if concept_id > 0:
                        # 批量插入肥羊-概念关联
                        ConceptRepository.batch_upsert_sheep_concept_mapping(
                            concept_id=concept_id,
                            sheep_codes=stock_codes
                        )
                        
                        synced_count += 1
                        total_stocks_synced += len(stock_codes)
                        
                        logger.debug(f"成功同步概念: {concept_name} ({len(stock_codes)} 只肥羊)")
                    else:
                        error_msg = f"概念 {concept_name} 插入失败"
                        logger.error(error_msg)
                        result['errors'].append(error_msg)
                        
                except Exception as e:
                    error_msg = f"同步概念 {concept_name} 失败: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    result['errors'].append(error_msg)
                    continue
            
            result['success'] = True
            result['new_concepts'] = synced_count
            result['total_stocks'] = total_stocks_synced
            
            logger.debug(f"概念元数据同步完成: 新增 {synced_count} 个概念，共 {total_stocks_synced} 只肥羊")
            
        except Exception as e:
            error_msg = f"概念元数据同步过程失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            result['errors'].append(error_msg)
            result['success'] = False
        
        return result
    
    @staticmethod
    def _get_existing_concepts() -> Set[str]:
        """
        从数据库获取现有概念名称集合
        
        Returns:
            概念名称集合
        """
        from db.database import get_raw_connection
        
        existing_concepts = set()
        
        try:
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT concept_name FROM concept_theme WHERE is_active = 1")
                rows = cursor.fetchall()
                existing_concepts = {row[0] for row in rows if row[0]}
                cursor.close()
        except Exception as e:
            logger.error(f"获取现有概念列表失败: {e}", exc_info=True)
        
        return existing_concepts
    
    def collect_index_data(self, index_code: str = 'CSI1000', days: int = None):
        """
        采集大盘指数数据（用于RSRS牛熊市判断）
        
        Args:
            index_code: 指数代码，默认CSI1000（中证1000）
            days: 采集最近N天的数据，默认采集最近3年数据
        """
        logger.debug(f"开始采集指数数据: {index_code}")
        
        try:
            # 获取数据库中已有的最新日期
            latest_date = IndexRepository.get_latest_trade_date(index_code)
            
            # 确定采集日期范围
            end_date = date.today()
            if latest_date:
                # 如果已有数据，从最新日期+1天开始采集
                from datetime import timedelta
                start_date = latest_date + timedelta(days=1)
                logger.debug(f"指数 {index_code} 已有数据至 {latest_date}，将从 {start_date} 开始采集")
            else:
                # 如果没有数据，采集最近3年
                from datetime import timedelta
                start_date = end_date - timedelta(days=1095)  # 3年
                logger.debug(f"指数 {index_code} 无历史数据，将采集最近3年数据")
            
            # 如果start_date >= end_date，说明数据已是最新
            if start_date >= end_date:
                logger.debug(f"指数 {index_code} 数据已是最新，无需更新")
                return
            
            # 获取指数数据
            df = self.index_adapter.get_index_daily_data(
                index_code=index_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                logger.warning(f"未获取到指数数据: {index_code}")
                return
            
            # 保存到数据库
            IndexRepository.save_index_daily_data(df, index_code)
            
            logger.debug(f"指数数据采集完成: {index_code}, 共 {len(df)} 条记录")
            
        except Exception as e:
            logger.error(f"采集指数数据失败: {index_code}, 错误: {e}", exc_info=True)
    
    def collect_concept_money_flow_data(self, target_date=None):
        """
        采集概念资金流向数据（如果不是交易日，则采集最近交易日的数据）
        
        Args:
            target_date: 目标日期，如果为None则使用今天
        """
        from services.concept_money_flow_service import ConceptMoneyFlowService
        from datetime import date
        
        if target_date is None:
            target_date = date.today()
        
        # 如果不是交易日，使用最近交易日
        if not self.trade_date_adapter.is_trading_day(target_date):
            target_date = self.trade_date_adapter.get_last_trading_day(target_date)
            logger.debug(f"原日期不是交易日，将采集最近交易日 {target_date} 的概念资金流数据")
        
        logger.debug(f"开始采集概念资金流向数据（日期: {target_date}）...")
        
        try:
            ConceptMoneyFlowService.collect_concept_money_flow_data(target_date)
            logger.debug("概念资金流向数据采集完成")
        except Exception as e:
            logger.error(f"采集概念资金流向数据失败: {e}", exc_info=True)
    
    def cleanup_old_concept_money_flow_data(self):
        """
        清理概念资金流旧数据（保留最近3个月）
        """
        from services.concept_money_flow_service import ConceptMoneyFlowService
        logger.debug("开始清理概念资金流旧数据...")
        try:
            deleted_count = ConceptMoneyFlowService.cleanup_old_data(Config.SECTOR_MONEY_FLOW_RETENTION_DAYS)
            logger.debug(f"概念资金流数据清理完成，删除了 {deleted_count} 条旧数据")
        except Exception as e:
            logger.error(f"清理概念资金流旧数据失败: {e}", exc_info=True)
    
    def cleanup_old_hot_rank_data(self):
        """
        清理热度榜旧数据（保留最近30天）
        """
        from db.hot_rank_repository import HotRankRepository
        logger.debug("开始清理热度榜旧数据...")
        try:
            deleted_count = HotRankRepository.cleanup_old_data(Config.HOT_RANK_RETENTION_DAYS)
            logger.debug(f"热度榜数据清理完成，删除了 {deleted_count} 条旧数据")
        except Exception as e:
            logger.error(f"清理热度榜旧数据失败: {e}", exc_info=True)
    
    def collect_all_data(self, force_trading_day: bool = False) -> Dict[str, any]:
        """
        一次采集所有数据
        
        Args:
            force_trading_day: 是否强制在非交易日也执行（默认False，非交易日会跳过交易日数据）
            
        Returns:
            {
                'success': bool,
                'results': {
                    'sheep_daily': {'success': bool, 'message': str},
                    'money_flow': {'success': bool, 'message': str},
                    'concept_money_flow': {'success': bool, 'message': str},
                    'hot_rank': {'success': bool, 'message': str},
                    'concept_data': {'success': bool, 'message': str},
                    'index_data': {'success': bool, 'message': str},
                    'concept_metadata_sync': {'success': bool, 'message': str},
                },
                'total_time': float
            }
        """
        import time
        start_time = time.time()
        results = {}
        
        logger.debug("=" * 60)
        logger.debug("开始批量采集所有数据")
        logger.debug("=" * 60)
        
        today = date.today()
        is_trading_day = self.trade_date_adapter.is_trading_day(today)
        
        # 1. 采集肥羊日K数据
        try:
            if is_trading_day or force_trading_day:
                logger.debug("[1/7] 开始采集肥羊日K数据...")
                self.collect_sheep_daily_data()
                results['sheep_daily'] = {'success': True, 'message': '采集成功'}
            else:
                results['sheep_daily'] = {'success': False, 'message': '非交易日，跳过'}
        except Exception as e:
            error_msg = f"采集失败: {str(e)}"
            logger.error(f"肥羊日K数据{error_msg}", exc_info=True)
            results['sheep_daily'] = {'success': False, 'message': error_msg}
        
        # 2. 采集资金流向数据
        try:
            if is_trading_day or force_trading_day:
                logger.debug("[2/7] 开始采集资金流向数据...")
                self.collect_money_flow_data()
                results['money_flow'] = {'success': True, 'message': '采集成功'}
            else:
                results['money_flow'] = {'success': False, 'message': '非交易日，跳过'}
        except Exception as e:
            error_msg = f"采集失败: {str(e)}"
            logger.error(f"资金流向数据{error_msg}", exc_info=True)
            results['money_flow'] = {'success': False, 'message': error_msg}
        
        # 3. 采集概念资金流向数据
        try:
            if is_trading_day or force_trading_day:
                logger.debug("[3/7] 开始采集概念资金流向数据...")
                self.collect_concept_money_flow_data(target_date=today)
                results['concept_money_flow'] = {'success': True, 'message': '采集成功'}
            else:
                results['concept_money_flow'] = {'success': False, 'message': '非交易日，跳过'}
        except Exception as e:
            error_msg = f"采集失败: {str(e)}"
            logger.error(f"概念资金流向数据{error_msg}", exc_info=True)
            results['concept_money_flow'] = {'success': False, 'message': error_msg}
        
        # 4. 采集热度榜数据（自然日数据，总是执行）
        try:
            logger.debug("[4/7] 开始采集热度榜数据...")
            self.collect_hot_rank_data()
            results['hot_rank'] = {'success': True, 'message': '采集成功'}
        except Exception as e:
            error_msg = f"采集失败: {str(e)}"
            logger.error(f"热度榜数据{error_msg}", exc_info=True)
            results['hot_rank'] = {'success': False, 'message': error_msg}
        
        # 5. 采集概念板块数据（自然日数据，总是执行）
        try:
            logger.debug("[5/7] 开始采集概念板块数据...")
            self.collect_concept_data()
            results['concept_data'] = {'success': True, 'message': '采集成功'}
        except Exception as e:
            error_msg = f"采集失败: {str(e)}"
            logger.error(f"概念板块数据{error_msg}", exc_info=True)
            results['concept_data'] = {'success': False, 'message': error_msg}
        
        # 6. 采集大盘指数数据
        try:
            if is_trading_day or force_trading_day:
                logger.debug("[6/7] 开始采集大盘指数数据...")
                self.collect_index_data(index_code='CSI1000')
                results['index_data'] = {'success': True, 'message': '采集成功'}
            else:
                results['index_data'] = {'success': False, 'message': '非交易日，跳过'}
        except Exception as e:
            error_msg = f"采集失败: {str(e)}"
            logger.error(f"大盘指数数据{error_msg}", exc_info=True)
            results['index_data'] = {'success': False, 'message': error_msg}
        
        # 7. 同步概念元数据（总是执行）
        try:
            logger.debug("[7/8] 开始同步概念元数据...")
            sync_result = self.sync_concept_metadata()
            if sync_result['success']:
                results['concept_metadata_sync'] = {
                    'success': True, 
                    'message': f"同步成功：新增 {sync_result['new_concepts']} 个概念，共 {sync_result['total_stocks']} 只肥羊"
                }
            else:
                results['concept_metadata_sync'] = {
                    'success': False,
                    'message': f"同步失败：{', '.join(sync_result.get('errors', []))}"
                }
        except Exception as e:
            error_msg = f"同步失败: {str(e)}"
            logger.error(f"概念元数据{error_msg}", exc_info=True)
            results['concept_metadata_sync'] = {'success': False, 'message': error_msg}
        
        total_time = time.time() - start_time
        success_count = sum(1 for r in results.values() if r.get('success', False))
        total_count = len(results)
        
        logger.debug("=" * 60)
        logger.debug(f"批量采集完成！成功: {success_count}/{total_count}，总耗时: {total_time:.2f}秒")
        logger.debug("=" * 60)
        
        return {
            'success': success_count == total_count,
            'results': results,
            'total_time': round(total_time, 2),
            'success_count': success_count,
            'total_count': total_count
        }
    
    def collect_specific_data(self, data_type: str, **kwargs) -> Dict[str, any]:
        """
        采集特定数据表
        
        Args:
            data_type: 数据类型，可选值：
                - 'sheep_daily': 肥羊日K数据
                - 'money_flow': 资金流向数据
                - 'concept_money_flow': 概念资金流向数据
                - 'hot_rank': 热度榜数据
                - 'concept_data': 概念板块数据
                - 'index_data': 大盘指数数据
                - 'concept_metadata_sync': 概念元数据同步
                - 'financial_data': 财务数据
                - 'realtime_data': 实时交易数据
            **kwargs: 额外参数
                - days: 对于sheep_daily和index_data，指定采集天数
                - target_date: 对于需要日期的数据，指定目标日期
                - force: 是否强制在非交易日执行
                
        Returns:
            {
                'success': bool,
                'message': str,
                'data_type': str
            }
        """
        import time
        start_time = time.time()
        
        data_type_map = {
            'sheep_daily': {
                'name': '肥羊日K数据',
                'method': self._collect_sheep_daily_wrapper,
                'requires_trading_day': True
            },
            'money_flow': {
                'name': '资金流向数据',
                'method': self._collect_money_flow_wrapper,
                'requires_trading_day': True
            },
            'concept_money_flow': {
                'name': '概念资金流向数据',
                'method': self._collect_concept_money_flow_wrapper,
                'requires_trading_day': True
            },
            'hot_rank': {
                'name': '热度榜数据',
                'method': self._collect_hot_rank_wrapper,
                'requires_trading_day': False
            },
            'concept_data': {
                'name': '概念板块数据',
                'method': self._collect_concept_data_wrapper,
                'requires_trading_day': False
            },
            'index_data': {
                'name': '大盘指数数据',
                'method': self._collect_index_data_wrapper,
                'requires_trading_day': True
            },
            'concept_metadata_sync': {
                'name': '概念元数据同步',
                'method': self._sync_concept_metadata_wrapper,
                'requires_trading_day': False
            },
            'financial_data': {
                'name': '财务数据',
                'method': self._collect_financial_data_wrapper,
                'requires_trading_day': False
            },
            'realtime_data': {
                'name': '实时交易数据',
                'method': self._collect_realtime_data_wrapper,
                'requires_trading_day': True
            },

        }
        
        if data_type not in data_type_map:
            return {
                'success': False,
                'message': f'不支持的数据类型: {data_type}',
                'data_type': data_type
            }
        
        config = data_type_map[data_type]
        
        # 如果需要交易日数据，检查目标日期或今天是否为交易日
        # 如果不是交易日，在具体的采集方法中会自动使用最近交易日
        
        try:
            logger.info(f"开始采集 {config['name']}...")
            result = config['method'](**kwargs)
            elapsed_time = time.time() - start_time
            logger.info(f"{config['name']}采集完成，耗时: {elapsed_time:.2f}秒")
            
            return {
                'success': True,
                'message': result.get('message', '采集成功'),
                'data_type': data_type,
                'elapsed_time': round(elapsed_time, 2)
            }
        except Exception as e:
            error_msg = str(e)
            logger.error(f"{config['name']}采集失败: {error_msg}", exc_info=True)
            return {
                'success': False,
                'message': f'采集失败: {error_msg}',
                'data_type': data_type
            }
    
    def _collect_sheep_daily_wrapper(self, **kwargs):
        """肥羊日K数据采集包装器"""
        # 根据配置的保留天数，获取N天前M个交易日的数据，逐天采集
        today = date.today()
        last_trading_day = self.trade_date_adapter.get_last_trading_day(today) if not self.trade_date_adapter.is_trading_day(today) else today
        
        # 根据配置的保留天数计算起始日期（自然日）
        retention_days = kwargs.get('days', Config.SHEEP_DATA_RETENTION_DAYS)
        # 从最近交易日往前推N天（自然日），获取这N天内的所有交易日（M个）
        start_date = last_trading_day - timedelta(days=retention_days)
        
        # 获取这N天内的所有交易日（M个）
        trading_days = self.trade_date_adapter.get_trading_days_in_range(start_date, last_trading_day)
        
        if not trading_days:
            # 如果没有交易日，只采集最近交易日
            logger.warning("未找到交易日，仅采集最近交易日数据")
            self.collect_sheep_daily_data(days=retention_days)
            return {'message': '采集成功'}
        
        # 按日期排序，从最早到最晚
        sorted_trading_days = sorted(trading_days)
        total_trading_days = len(sorted_trading_days)
        
        logger.info(f"开始采集肥羊日K数据：设置{retention_days}天，包含{total_trading_days}个交易日，从 {sorted_trading_days[0]} 到 {sorted_trading_days[-1]}")
        
        # 逐天检查并采集缺失的数据
        success_count = 0
        error_count = 0
        skipped_count = 0
        
        for idx, trading_day in enumerate(sorted_trading_days, 1):
            try:
                # 检查数据库中是否已有该日期的数据（至少要有100只肥羊的数据）
                existing_count = SheepRepository.get_sheep_daily_count_for_date(trading_day)
                
                # 如果已有数据且数据量足够（至少有100只肥羊的数据），跳过
                if existing_count and existing_count >= 100:
                    skipped_count += 1
                    if idx % 50 == 0:  # 每50个交易日输出一次进度
                        logger.info(f"进度: {idx}/{total_trading_days}，已跳过 {skipped_count} 个已有数据的交易日")
                    continue
                
                logger.info(f"[{idx}/{total_trading_days}] 开始采集交易日 {trading_day} 的肥羊日K数据...")
                
                # 采集该交易日的数据（通过调用collect_sheep_daily_data，但只采集该交易日）
                # 由于collect_sheep_daily_data是按肥羊逐个处理的，我们需要一个更精细的方法
                # 这里我们使用原有的方法，但会检查每个肥羊在该交易日是否有数据
                self._collect_sheep_daily_for_date(trading_day)
                success_count += 1
                
                # 每采集10个交易日输出一次进度
                if success_count % 10 == 0:
                    logger.info(f"进度: {idx}/{total_trading_days}，成功: {success_count}，失败: {error_count}，跳过: {skipped_count}")
                
            except Exception as e:
                logger.warning(f"采集交易日 {trading_day} 的肥羊日K数据失败: {e}")
                error_count += 1
                continue
        
        logger.info(f"肥羊日K数据采集完成：成功 {success_count} 个交易日，失败 {error_count} 个交易日，跳过 {skipped_count} 个已有数据的交易日")
        
        if success_count > 0:
            return {'message': f'采集完成：成功 {success_count} 个交易日，失败 {error_count} 个交易日，跳过 {skipped_count} 个已有数据的交易日'}
        elif skipped_count > 0:
            return {'message': f'所有交易日数据已存在，共跳过 {skipped_count} 个交易日'}
        else:
            # 如果没有需要补全的，采集最近交易日的数据
            self.collect_sheep_daily_data(days=retention_days)
            return {'message': '采集成功'}
    
    def _collect_sheep_daily_for_date(self, target_date: date):
        """
        采集指定交易日的肥羊日K数据
        
        Args:
            target_date: 目标交易日
        """
        target_date_str = target_date.strftime('%Y%m%d')
        
        # 获取所有肥羊代码
        sheep_list = self.sheep_adapter.get_all_sheep_codes()
        if not sheep_list:
            logger.error("未能获取到肥羊列表")
            return
        
        success_count = 0
        error_count = 0
        
        for idx, sheep_item in enumerate(sheep_list):
            try:
                sheep_code = sheep_item['code']
                sheep_name = sheep_item['name']
                
                # 检查该肥羊在该交易日是否已有数据
                from db.sheep_repository import SheepRepository
                latest_date = SheepRepository.get_latest_trade_date(sheep_code)
                
                # 如果最新日期大于等于目标日期，说明该交易日的数据可能已存在
                # 但为了确保准确性，我们仍然尝试采集（upsert会处理重复数据）
                if latest_date and latest_date >= target_date:
                    # 检查是否有该日期的数据
                    existing_data = SheepRepository.get_sheep_daily(sheep_code, limit=10000)
                    has_data = any(d.get('date') == target_date for d in existing_data)
                    if has_data:
                        continue  # 已有数据，跳过
                
                # 获取该交易日的日K数据
                df = self.sheep_adapter.get_sheep_daily_data(sheep_code, target_date_str, target_date_str)
                
                if df is None or df.empty:
                    continue
                
                # 转换为数据库格式
                def format_row(row):
                    return {
                        'code': sheep_code,
                        'date': row['date'],
                        'open': DataCollectionService._format_price(row.get('open')),
                        'close': DataCollectionService._format_price(row.get('close')),
                        'high': DataCollectionService._format_price(row.get('high')),
                        'low': DataCollectionService._format_price(row.get('low')),
                        'volume': int(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0,
                        'amount': float(row.get('amount', 0)) if pd.notna(row.get('amount')) else 0,
                        'turnover_rate': DataCollectionService._format_percentage(row.get('turnover_rate')),
                        'change_pct': DataCollectionService._validate_change_pct(row.get('change_pct')),
                        'ma5': DataCollectionService._format_price(row.get('ma5')),
                        'ma10': DataCollectionService._format_price(row.get('ma10')),
                        'ma20': DataCollectionService._format_price(row.get('ma20')),
                        'ma30': DataCollectionService._format_price(row.get('ma30')),
                        'ma60': DataCollectionService._format_price(row.get('ma60')),
                    }
                
                data_list = df.apply(format_row, axis=1).tolist()
                
                # 批量保存
                SheepRepository.batch_upsert_sheep_daily(data_list)
                
                # 更新肥羊基本信息
                market = 'SH' if sheep_code.startswith('6') else 'SZ'
                SheepRepository.upsert_sheep_basic(sheep_code, sheep_name, market=market)
                
                success_count += 1
                
                if (idx + 1) % 100 == 0:
                    logger.debug(f"进度: {idx + 1}/{len(sheep_list)}, 成功: {success_count}, 失败: {error_count}")
                
            except Exception as e:
                logger.debug(f"处理肥羊 {sheep_item.get('code', 'unknown')} 失败: {e}")
                error_count += 1
                continue
        
        logger.debug(f"交易日 {target_date} 的肥羊日K数据采集完成：成功 {success_count}，失败 {error_count}")
    
    def _collect_money_flow_wrapper(self, **kwargs):
        """资金流向数据采集包装器"""
        # 根据配置的保留天数，获取N天前M个交易日的数据，逐天采集
        today = date.today()
        last_trading_day = self.trade_date_adapter.get_last_trading_day(today) if not self.trade_date_adapter.is_trading_day(today) else today
        
        # 根据配置的保留天数计算起始日期（自然日）
        retention_days = Config.MONEY_FLOW_RETENTION_DAYS
        # 从最近交易日往前推N天（自然日），获取这N天内的所有交易日（M个）
        start_date = last_trading_day - timedelta(days=retention_days)
        
        # 获取这N天内的所有交易日（M个）
        trading_days = self.trade_date_adapter.get_trading_days_in_range(start_date, last_trading_day)
        
        if not trading_days:
            # 如果没有交易日，只采集最近交易日
            logger.warning("未找到交易日，仅采集最近交易日数据")
            self.collect_money_flow_data()
            return {'message': '采集成功'}
        
        # 按日期排序，从最早到最晚
        sorted_trading_days = sorted(trading_days)
        total_trading_days = len(sorted_trading_days)
        
        logger.info(f"开始采集资金流向数据：设置{retention_days}天，包含{total_trading_days}个交易日，从 {sorted_trading_days[0]} 到 {sorted_trading_days[-1]}")
        
        # 逐天采集所有交易日的数据
        success_count = 0
        error_count = 0
        skipped_count = 0
        
        for idx, trading_day in enumerate(sorted_trading_days, 1):
            try:
                # 检查数据库中是否已有该日期的数据
                existing_count = MoneyFlowRepository.get_sheep_money_flow_count_for_date(trading_day)
                
                # 如果已有数据且数据量足够（至少有100只肥羊的数据），跳过
                if existing_count and existing_count >= 100:
                    skipped_count += 1
                    if idx % 50 == 0:  # 每50个交易日输出一次进度
                        logger.info(f"进度: {idx}/{total_trading_days}，已跳过 {skipped_count} 个已有数据的交易日")
                    continue
                
                logger.info(f"[{idx}/{total_trading_days}] 开始采集交易日 {trading_day} 的资金流数据...")
                
                # 使用逐个获取的方式补全历史数据
                self._collect_money_flow_data_individual(trading_day)
                success_count += 1
                
                # 每采集10个交易日输出一次进度
                if success_count % 10 == 0:
                    logger.info(f"进度: {idx}/{total_trading_days}，成功: {success_count}，失败: {error_count}，跳过: {skipped_count}")
                
            except Exception as e:
                logger.warning(f"采集交易日 {trading_day} 的资金流数据失败: {e}")
                error_count += 1
                continue
        
        logger.info(f"资金流向数据采集完成：成功 {success_count} 个交易日，失败 {error_count} 个交易日，跳过 {skipped_count} 个已有数据的交易日")
        
        if success_count > 0:
            return {'message': f'采集完成：成功 {success_count} 个交易日，失败 {error_count} 个交易日，跳过 {skipped_count} 个已有数据的交易日'}
        elif skipped_count > 0:
            return {'message': f'所有交易日数据已存在，共跳过 {skipped_count} 个交易日'}
        else:
            # 如果没有需要补全的，采集最近交易日的数据
            self.collect_money_flow_data()
            return {'message': '采集成功'}
    
    def _collect_concept_money_flow_wrapper(self, **kwargs):
        """概念资金流向数据采集包装器"""
        # 根据配置的保留天数，获取N天前M个交易日的数据，逐天采集
        today = date.today()
        last_trading_day = self.trade_date_adapter.get_last_trading_day(today) if not self.trade_date_adapter.is_trading_day(today) else today
        
        # 根据配置的保留天数计算起始日期（自然日）
        retention_days = Config.SECTOR_MONEY_FLOW_RETENTION_DAYS
        # 从最近交易日往前推N天（自然日），获取这N天内的所有交易日（M个）
        start_date = last_trading_day - timedelta(days=retention_days)
        
        # 获取这N天内的所有交易日（M个）
        trading_days = self.trade_date_adapter.get_trading_days_in_range(start_date, last_trading_day)
        
        if not trading_days:
            # 如果没有交易日，只采集最近交易日
            logger.warning("未找到交易日，仅采集最近交易日数据")
            self.collect_concept_money_flow_data(target_date=last_trading_day)
            return {'message': '采集成功'}
        
        # 按日期排序，从最早到最晚
        sorted_trading_days = sorted(trading_days)
        total_trading_days = len(sorted_trading_days)
        
        logger.info(f"开始采集概念资金流向数据：设置{retention_days}天，包含{total_trading_days}个交易日，从 {sorted_trading_days[0]} 到 {sorted_trading_days[-1]}")
        
        # 逐天采集所有交易日的数据
        success_count = 0
        error_count = 0
        skipped_count = 0
        
        from db.sector_money_flow_repository import SectorMoneyFlowRepository
        
        for idx, trading_day in enumerate(sorted_trading_days, 1):
            try:
                # 检查数据库中是否已有该日期的数据
                existing_count = SectorMoneyFlowRepository.get_concept_money_flow_count_for_date(trading_day)
                
                # 如果已有数据且数据量足够（至少有10个概念的数据），跳过
                if existing_count and existing_count >= 10:
                    skipped_count += 1
                    if idx % 50 == 0:  # 每50个交易日输出一次进度
                        logger.info(f"进度: {idx}/{total_trading_days}，已跳过 {skipped_count} 个已有数据的交易日")
                    continue
                
                logger.info(f"[{idx}/{total_trading_days}] 开始采集交易日 {trading_day} 的概念资金流数据...")
                self.collect_concept_money_flow_data(target_date=trading_day)
                success_count += 1
                
                # 每采集10个交易日输出一次进度
                if success_count % 10 == 0:
                    logger.info(f"进度: {idx}/{total_trading_days}，成功: {success_count}，失败: {error_count}，跳过: {skipped_count}")
                
            except Exception as e:
                logger.warning(f"采集交易日 {trading_day} 的概念资金流数据失败: {e}")
                error_count += 1
                continue
        
        logger.info(f"概念资金流向数据采集完成：成功 {success_count} 个交易日，失败 {error_count} 个交易日，跳过 {skipped_count} 个已有数据的交易日")
        
        if success_count > 0:
            return {'message': f'采集完成：成功 {success_count} 个交易日，失败 {error_count} 个交易日，跳过 {skipped_count} 个已有数据的交易日'}
        elif skipped_count > 0:
            return {'message': f'所有交易日数据已存在，共跳过 {skipped_count} 个交易日'}
        else:
            # 如果没有需要补全的，采集最近交易日的数据
            self.collect_concept_money_flow_data(target_date=last_trading_day)
            return {'message': '采集成功'}
    
    def _collect_hot_rank_wrapper(self, **kwargs):
        """热度榜数据采集包装器"""
        target_date = kwargs.get('target_date', None)
        self.collect_hot_rank_data(target_date=target_date)
        return {'message': '采集成功'}
    
    def _collect_concept_data_wrapper(self, **kwargs):
        """概念板块数据采集包装器"""
        self.collect_concept_data()
        return {'message': '采集成功'}
    
    def _collect_index_data_wrapper(self, **kwargs):
        """大盘指数数据采集包装器"""
        # 根据配置的保留天数，获取N天前M个交易日的数据，逐天采集
        index_code = kwargs.get('index_code', 'CSI1000')
        today = date.today()
        last_trading_day = self.trade_date_adapter.get_last_trading_day(today) if not self.trade_date_adapter.is_trading_day(today) else today
        
        # 根据配置的保留天数计算起始日期（自然日）
        retention_days = kwargs.get('days', 1095)  # 默认3年
        # 从最近交易日往前推N天（自然日），获取这N天内的所有交易日（M个）
        start_date = last_trading_day - timedelta(days=retention_days)
        
        # 获取这N天内的所有交易日（M个）
        trading_days = self.trade_date_adapter.get_trading_days_in_range(start_date, last_trading_day)
        
        if not trading_days:
            # 如果没有交易日，只采集最近交易日
            logger.warning("未找到交易日，仅采集最近交易日数据")
            self.collect_index_data(index_code=index_code, days=retention_days)
            return {'message': '采集成功'}
        
        # 按日期排序，从最早到最晚
        sorted_trading_days = sorted(trading_days)
        total_trading_days = len(sorted_trading_days)
        
        logger.info(f"开始采集指数数据（{index_code}）：设置{retention_days}天，包含{total_trading_days}个交易日，从 {sorted_trading_days[0]} 到 {sorted_trading_days[-1]}")
        
        # 逐天采集所有交易日的数据
        success_count = 0
        error_count = 0
        skipped_count = 0
        
        from db.index_repository import IndexRepository
        
        for idx, trading_day in enumerate(sorted_trading_days, 1):
            try:
                # 检查数据库中是否已有该日期的数据
                existing_count = IndexRepository.get_index_data_count_for_date(trading_day, index_code)
                
                # 如果已有数据（指数数据每个交易日只有1条），跳过
                if existing_count > 0:
                    skipped_count += 1
                    if idx % 50 == 0:  # 每50个交易日输出一次进度
                        logger.info(f"进度: {idx}/{total_trading_days}，已跳过 {skipped_count} 个已有数据的交易日")
                    continue
                
                logger.info(f"[{idx}/{total_trading_days}] 开始采集交易日 {trading_day} 的指数数据...")
                
                # 采集该交易日的数据
                self._collect_index_data_for_date(trading_day, index_code)
                success_count += 1
                
                # 每采集10个交易日输出一次进度
                if success_count % 10 == 0:
                    logger.info(f"进度: {idx}/{total_trading_days}，成功: {success_count}，失败: {error_count}，跳过: {skipped_count}")
                
            except Exception as e:
                logger.warning(f"采集交易日 {trading_day} 的指数数据失败: {e}")
                error_count += 1
                continue
        
        logger.info(f"指数数据采集完成（{index_code}）：成功 {success_count} 个交易日，失败 {error_count} 个交易日，跳过 {skipped_count} 个已有数据的交易日")
        
        if success_count > 0:
            return {'message': f'采集完成：成功 {success_count} 个交易日，失败 {error_count} 个交易日，跳过 {skipped_count} 个已有数据的交易日'}
        elif skipped_count > 0:
            return {'message': f'所有交易日数据已存在，共跳过 {skipped_count} 个交易日'}
        else:
            # 如果没有需要补全的，采集最近交易日的数据
            self.collect_index_data(index_code=index_code, days=retention_days)
            return {'message': '采集成功'}
    
    def _collect_index_data_for_date(self, target_date: date, index_code: str = 'CSI1000'):
        """
        采集指定交易日的指数数据
        
        Args:
            target_date: 目标交易日
            index_code: 指数代码
        """
        try:
            # 获取指数数据（单日）
            df = self.index_adapter.get_index_daily_data(
                index_code=index_code,
                start_date=target_date,
                end_date=target_date
            )
            
            if df.empty:
                logger.debug(f"未获取到交易日 {target_date} 的指数数据: {index_code}")
                return
            
            # 保存到数据库
            IndexRepository.save_index_daily_data(df, index_code)
            
            logger.debug(f"交易日 {target_date} 的指数数据采集完成: {index_code}")
            
        except Exception as e:
            logger.error(f"采集交易日 {target_date} 的指数数据失败: {index_code}, 错误: {e}", exc_info=True)
    
    def _sync_concept_metadata_wrapper(self, **kwargs):
        """概念元数据同步包装器"""
        result = self.sync_concept_metadata()
        if result['success']:
            return {
                'message': f"同步成功：新增 {result['new_concepts']} 个概念，共 {result['total_stocks']} 只肥羊"
            }
        else:
            return {
                'message': f"同步失败：{', '.join(result.get('errors', []))}"
            }
    
    def _collect_financial_data_wrapper(self, **kwargs):
        """财务数据采集包装器"""
        sheep_codes = kwargs.get('sheep_codes', None)
        self.collect_financial_data(sheep_codes=sheep_codes)
        return {'message': '采集成功'}
    
    def _collect_realtime_data_wrapper(self, **kwargs):
        """实时数据采集包装器"""
        result = self.collect_realtime_trading_data()
        if result.get('success'):
            return {'message': '实时数据采集成功'}
        else:
            return {'message': f"实时数据采集失败: {result.get('message')}"}
    
    def collect_financial_data(self, sheep_codes: Optional[List[str]] = None):
        """
        采集财务数据（季度/年度数据，非每日采集）
        
        Args:
            sheep_codes: 需要采集的肥羊代码列表，如果为None则采集所有肥羊
        """
        logger.info("开始采集财务数据...")
        
        # 获取需要采集的肥羊列表
        if sheep_codes is None:
            sheep_list = self.sheep_adapter.get_all_sheep_codes()
            sheep_codes = [item['code'] for item in sheep_list]
        
        if not sheep_codes:
            logger.warning("没有需要采集的肥羊")
            return
        
        logger.info(f"共需处理 {len(sheep_codes)} 只肥羊的财务数据")
        
        success_count = 0
        error_count = 0
        total_records = 0
        
        for idx, sheep_code in enumerate(sheep_codes):
            try:
                # 获取财务数据
                df = self.financial_adapter.get_financial_data(sheep_code)
                
                if df is None or df.empty:
                    continue
                
                # 标准化财务数据
                financial_data = self.financial_adapter.normalize_financial_data(df, sheep_code)
                
                if financial_data:
                    # 批量保存
                    FinancialRepository.batch_upsert_financial_data(financial_data)
                    total_records += len(financial_data)
                    success_count += 1
                
                if (idx + 1) % 100 == 0:
                    logger.info(f"进度: {idx + 1}/{len(sheep_codes)}, 成功: {success_count}, 失败: {error_count}, 记录: {total_records}")
                
                # 延迟，避免请求过快
                import time
                time.sleep(0.1)  # 100ms延迟
                
            except Exception as e:
                logger.warning(f"处理肥羊 {sheep_code} 财务数据失败: {e}")
                error_count += 1
                continue
        
        logger.info(f"财务数据采集完成！成功: {success_count}, 失败: {error_count}, 总记录: {total_records}")
    
    def calculate_and_update_rps(self, target_date: date = None) -> Dict[str, any]:
        """
        计算并更新所有肥羊的RPS（Relative Price Strength）指标
        
        RPS计算逻辑：
        1. 获取所有股票当前收盘价和250天前收盘价
        2. 计算250日涨幅 = (当前价 - 250日前价) / 250日前价 * 100
        3. 按涨幅从高到低排序
        4. RPS = 当前排名百分位 * 100 (0-100)
        
        Args:
            target_date: 目标日期，默认使用最新交易日
            
        Returns:
            计算结果统计
        """
        from db.database import get_raw_connection
        import numpy as np
        
        logger.debug("开始计算RPS指标...")
        
        try:
            if target_date is None:
                today = date.today()
                if not self.trade_date_adapter.is_trading_day(today):
                    target_date = self.trade_date_adapter.get_last_trading_day(today)
                else:
                    target_date = today
            
            logger.debug(f"RPS计算目标日期: {target_date}")
            
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                
                # 优化版本：使用更简单的查询，避免临时表空间不足
                query = """
                    SELECT 
                        sd1.sheep_code,
                        sd1.close_price as current_close,
                        sd2.close_price as past_close,
                        CASE 
                            WHEN sd2.close_price > 0 
                            THEN (sd1.close_price - sd2.close_price) / sd2.close_price * 100
                            ELSE NULL
                        END AS return_250
                    FROM sheep_daily sd1
                    LEFT JOIN sheep_daily sd2 ON sd1.sheep_code = sd2.sheep_code
                        AND sd2.trade_date = (
                            SELECT trade_date 
                            FROM sheep_daily 
                            WHERE sheep_code = sd1.sheep_code 
                                AND trade_date < sd1.trade_date
                            ORDER BY trade_date DESC
                            LIMIT 1 OFFSET 249  -- 第250个交易日（倒数第250个交易日）
                        )
                    WHERE sd1.trade_date = %s
                        AND sd1.close_price > 0
                """
                
                cursor.execute(query, (target_date,))
                rows = cursor.fetchall()
                cursor.close()
            
            if not rows:
                logger.warning("没有找到足够的数据来计算RPS")
                return {'success': False, 'message': '没有足够的数据'}
            
            # 转换为DataFrame并计算RPS
            df = pd.DataFrame(rows, columns=['sheep_code', 'current_close', 'past_close', 'return_250'])
            
            # 过滤掉没有250日前数据的股票
            df_with_return = df.dropna(subset=['return_250'])
            df_no_return = df[df['return_250'].isna()]
            
            logger.debug(f"有250日数据的股票: {len(df_with_return)} 只, 数据不足的股票: {len(df_no_return)} 只")
            
            if len(df_with_return) == 0:
                logger.warning("没有股票有完整的250日数据")
                return {'success': False, 'message': '没有股票有完整的250日数据'}
            
            # 计算RPS百分位排名 (0-100)
            df_with_return = df_with_return.copy()
            df_with_return['rank'] = df_with_return['return_250'].rank(method='min')
            total = len(df_with_return)
            df_with_return['rps_250'] = ((df_with_return['rank'] - 1) / (total - 1) * 99.9 if total > 1 else 0).round(1)
            
            # 确保RPS在0-99.9范围内
            df_with_return['rps_250'] = df_with_return['rps_250'].clip(0, 99.9)
            
            logger.debug(f"RPS计算完成，最高RPS: {df_with_return['rps_250'].max():.1f}, 最低RPS: {df_with_return['rps_250'].min():.1f}")
            logger.debug(f"RPS分布: 90+: {(df_with_return['rps_250'] >= 90).sum()} 只, 80-90: {((df_with_return['rps_250'] >= 80) & (df_with_return['rps_250'] < 90)).sum()} 只")
            
            # 批量更新数据库
            update_data = df_with_return[['sheep_code', 'rps_250']].to_dict('records')
            
            updated_count = self._batch_update_rps(update_data, target_date)
            
            logger.debug(f"RPS更新完成: 成功更新 {updated_count} 条记录")
            
            return {
                'success': True,
                'total_stocks': len(df),
                'stocks_with_rps': len(df_with_return),
                'stocks_without_rps': len(df_no_return),
                'updated_count': updated_count,
                'rps_max': float(df_with_return['rps_250'].max()) if len(df_with_return) > 0 else 0,
                'rps_min': float(df_with_return['rps_250'].min()) if len(df_with_return) > 0 else 0,
                'target_date': str(target_date)
            }
            
        except Exception as e:
            logger.error(f"计算RPS失败: {e}", exc_info=True)
            return {'success': False, 'message': str(e)}
    
    def _batch_update_rps(self, update_data: List[Dict], target_date: date) -> int:
        """
        批量更新RPS数据到数据库
        
        Args:
            update_data: 包含sheep_code和rps_250的字典列表
            target_date: 目标日期
            
        Returns:
            更新的记录数
        """
        from db.database import get_raw_connection
        
        if not update_data:
            return 0
        
        updated_count = 0
        batch_size = 1000
        
        try:
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                
                # 使用批量UPDATE语句
                for i in range(0, len(update_data), batch_size):
                    batch = update_data[i:i + batch_size]
                    
                    # 构建批量UPDATE语句
                    # 使用CASE WHEN语法一次更新多条记录
                    sheep_codes = [item['sheep_code'] for item in batch]
                    placeholders = ','.join(['%s'] * len(sheep_codes))
                    
                    case_when_clauses = []
                    params = []
                    for item in batch:
                        case_when_clauses.append(f"WHEN sheep_code = %s THEN %s")
                        params.append(item['sheep_code'])
                        params.append(item['rps_250'])
                    
                    params.extend(sheep_codes)
                    
                    update_query = f"""
                        UPDATE sheep_daily 
                        SET rps_250 = CASE {' '.join(case_when_clauses)} END
                        WHERE sheep_code IN ({placeholders})
                          AND trade_date = %s
                    """
                    params.append(target_date)
                    
                    cursor.execute(update_query, params)
                    updated_count += cursor.rowcount
                
                conn.commit()
                cursor.close()
                
        except Exception as e:
            logger.error(f"批量更新RPS失败: {e}", exc_info=True)
            raise
        
        return updated_count
    

    def backfill_rps_history(self, start_date: date = None, end_date: date = None, days: int = 365) -> Dict[str, any]:
        """
        批量补全历史RPS数据
        
        Args:
            start_date: 起始日期，默认为 end_date - days
            end_date: 结束日期，默认为最新交易日
            days: 补全天数（默认365天，即1年）
            
        Returns:
            补全结果统计
        """
        from datetime import timedelta
        
        logger.info(f"开始批量补全RPS历史数据...")
        
        try:
            # 确定日期范围
            if end_date is None:
                today = date.today()
                if not self.trade_date_adapter.is_trading_day(today):
                    end_date = self.trade_date_adapter.get_last_trading_day(today)
                else:
                    end_date = today
            
            if start_date is None:
                start_date = end_date - timedelta(days=days)
            
            # 获取日期范围内的所有交易日
            trading_days = self.trade_date_adapter.get_trading_days_in_range(start_date, end_date)
            
            if not trading_days:
                logger.warning(f"在 {start_date} 至 {end_date} 范围内没有找到交易日")
                return {'success': False, 'message': '没有找到交易日'}
            
            logger.info(f"RPS补全日期范围: {start_date} 至 {end_date}, 共 {len(trading_days)} 个交易日")
            
            # 逐日计算RPS（按时间倒序处理，从最近的日期到最远的日期）
            results = {
                'success': True,
                'start_date': str(start_date),
                'end_date': str(end_date),
                'total_days': len(trading_days),
                'success_days': 0,
                'failed_days': 0,
                'total_updated': 0,
                'details': []
            }
            
            # 按时间倒序处理（从最近的日期到最远的日期）
            trading_days_reversed = sorted(trading_days, reverse=True)
            
            for i, trade_date in enumerate(trading_days_reversed):
                try:
                    result = self.calculate_and_update_rps(target_date=trade_date)
                    
                    if result.get('success'):
                        results['success_days'] += 1
                        results['total_updated'] += result.get('updated_count', 0)
                        results['details'].append({
                            'date': str(trade_date),
                            'success': True,
                            'updated': result.get('updated_count', 0)
                        })
                    else:
                        results['failed_days'] += 1
                        results['details'].append({
                            'date': str(trade_date),
                            'success': False,
                            'message': result.get('message')
                        })
                    
                    # 进度日志
                    if (i + 1) % 5 == 0 or i == len(trading_days_reversed) - 1:
                        logger.info(f"RPS补全进度: {i + 1}/{len(trading_days_reversed)}, 成功: {results['success_days']}, 总更新: {results['total_updated']}")
                        
                except Exception as e:
                    logger.error(f"计算 {trade_date} 的RPS失败: {e}")
                    results['failed_days'] += 1
                    results['details'].append({
                        'date': str(trade_date),
                        'success': False,
                        'message': str(e)
                    })
            
            logger.info(f"RPS历史补全完成: 成功 {results['success_days']}/{results['total_days']} 天, 总更新 {results['total_updated']} 条")
            
            return results
            
        except Exception as e:
            logger.error(f"RPS历史补全失败: {e}", exc_info=True)
            return {'success': False, 'message': str(e)}
    
    def _calculate_rps_batch(self, trading_days: List[date]) -> Dict[str, any]:
        """
        批量计算多个交易日的RPS值
        
        Args:
            trading_days: 交易日列表
            
        Returns:
            计算结果统计
        """
        import numpy as np
        from db.database import get_raw_connection
        
        logger.info(f"开始批量计算 {len(trading_days)} 个交易日的RPS")
        
        try:
            with get_raw_connection() as conn:
                # 获取所有交易日的RPS数据
                dates_str = ','.join([f"'{d}'" for d in trading_days])
                
                # 一次性获取所有需要的股价数据
                query = f"""
                    WITH price_data AS (
                        SELECT 
                            sd1.sheep_code,
                            sd1.trade_date,
                            sd1.close_price,
                            -- 计算250个交易日前的日期
                            sd2.close_price as close_250d_ago,
                            ROW_NUMBER() OVER (PARTITION BY sd1.sheep_code, sd1.trade_date ORDER BY sd2.trade_date DESC) as rn
                        FROM sheep_daily sd1
                        LEFT JOIN sheep_daily sd2 ON sd1.sheep_code = sd2.sheep_code
                            AND sd2.trade_date <= sd1.trade_date
                            AND DATEDIFF(sd1.trade_date, sd2.trade_date) >= 250
                        WHERE sd1.trade_date IN ({dates_str})
                            AND sd1.close_price > 0
                    ),
                    filtered_price_data AS (
                        SELECT *
                        FROM price_data
                        WHERE rn = 1 OR rn IS NULL  -- 取最接近250天的那个，如果没有则rn为NULL
                    )
                    SELECT 
                        fpd.sheep_code,
                        fpd.trade_date,
                        fpd.close_price as current_close,
                        fpd.close_250d_ago,
                        CASE 
                            WHEN fpd.close_250d_ago > 0 
                            THEN (fpd.close_price - fpd.close_250d_ago) / fpd.close_250d_ago * 100
                            ELSE NULL
                        END AS return_250
                    FROM filtered_price_data fpd
                """
                
                cursor = conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                cursor.close()
            
            if not rows:
                logger.warning("没有找到足够的数据来计算RPS")
                return {'success': False, 'message': '没有足够的数据'}
            
            # 转换为DataFrame
            df = pd.DataFrame(rows, columns=['sheep_code', 'trade_date', 'current_close', 'close_250d_ago', 'return_250'])
            
            # 计算每个交易日的RPS
            rps_updates = []
            
            # 按时间倒序处理（从最近的日期到最远的日期）
            trading_days_reversed = sorted(trading_days, reverse=True)
            
            for trade_date in trading_days_reversed:
                daily_df = df[df['trade_date'] == trade_date]
                
                if daily_df.empty:
                    logger.warning(f"交易日 {trade_date} 没有数据")
                    continue
                
                # 计算当日RPS
                daily_with_return = daily_df.dropna(subset=['return_250'])
                
                if len(daily_with_return) == 0:
                    logger.warning(f"交易日 {trade_date} 没有250日数据")
                    continue
                
                # 计算RPS百分位排名
                daily_with_return = daily_with_return.copy()
                daily_with_return['rank'] = daily_with_return['return_250'].rank(method='min')
                total = len(daily_with_return)
                daily_with_return['rps_250'] = ((daily_with_return['rank'] - 1) / (total - 1) * 99.9 if total > 1 else 0).round(1)
                daily_with_return['rps_250'] = daily_with_return['rps_250'].clip(0, 99.9)
                
                # 添加到更新列表
                for _, row in daily_with_return.iterrows():
                    rps_updates.append({
                        'sheep_code': row['sheep_code'],
                        'trade_date': trade_date,
                        'rps_250': row['rps_250']
                    })
            
            # 批量更新数据库
            updated_count = self._batch_update_rps_multiple_dates(rps_updates)
            
            logger.info(f"RPS批量计算完成，更新了 {updated_count} 条记录")
            
            return {
                'success': True,
                'start_date': str(min(trading_days)),
                'end_date': str(max(trading_days)),
                'total_days': len(trading_days),
                'success_days': len(set([r['trade_date'] for r in rps_updates])),
                'failed_days': len(trading_days) - len(set([r['trade_date'] for r in rps_updates])),
                'total_updated': updated_count,
                'details': [],
            }
        
        except Exception as e:
            logger.error(f"批量计算RPS失败: {e}", exc_info=True)
            raise
    
    def _batch_update_rps_multiple_dates(self, update_data: List[Dict]) -> int:
        """
        批量更新多个日期的RPS数据到数据库
        
        Args:
            update_data: 包含sheep_code, trade_date, rps_250的字典列表
            
        Returns:
            更新的记录数
        """
        from db.database import get_raw_connection
        
        if not update_data:
            return 0
        
        updated_count = 0
        batch_size = 1000
        
        try:
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                
                # 使用批量INSERT ... ON DUPLICATE KEY UPDATE
                for i in range(0, len(update_data), batch_size):
                    batch = update_data[i:i + batch_size]
                    
                    values_placeholders = ','.join(['(%s, %s, %s)'] * len(batch))
                    query = f"""
                        INSERT INTO sheep_daily (sheep_code, trade_date, rps_250)
                        VALUES {values_placeholders}
                        ON DUPLICATE KEY UPDATE rps_250 = VALUES(rps_250)
                    """
                    
                    params = []
                    for item in batch:
                        params.extend([item['sheep_code'], item['trade_date'], item['rps_250']])
                    
                    cursor.execute(query, params)
                    updated_count += cursor.rowcount
                
                conn.commit()
                cursor.close()
                
        except Exception as e:
            logger.error(f"批量更新RPS失败: {e}", exc_info=True)
            raise
        
        return updated_count
    
    def calculate_and_update_vcp_vol_ma5(self, target_date: date = None) -> Dict[str, any]:
        """
        计算并更新所有肥羊的VCP（Volatility Contraction Pattern）因子和5日均量指标
        
        VCP计算逻辑：
        1. 计算一段时间内（如20天）的价格波动率
        2. 计算同一段时间内成交量的波动率  
        3. 寻找价格波动收敛但成交量放大后的缩量阶段
        4. VCP因子越小越好，表示波动收敛
        
        vol_ma_5计算逻辑：
        1. 计算5日平均成交量
        2. 用于后续的量比计算（vol_ratio = 当日成交量 / vol_ma_5）
        
        Args:
            target_date: 目标日期，默认使用最新交易日
            
        Returns:
            计算结果统计
        """
        from db.database import get_raw_connection
        import numpy as np
        
        logger.debug("开始计算VCP和vol_ma_5指标...")
        
        try:
            if target_date is None:
                today = date.today()
                if not self.trade_date_adapter.is_trading_day(today):
                    target_date = self.trade_date_adapter.get_last_trading_day(today)
                else:
                    target_date = today
            
            logger.debug(f"VCP/vol_ma_5计算目标日期: {target_date}")
            
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                
                # 查询最近25个交易日的数据，用于计算VCP因子和vol_ma_5
                query = """
                    SELECT 
                        sd.sheep_code,
                        sd.trade_date,
                        sd.close_price,
                        sd.volume
                    FROM sheep_daily sd
                    WHERE sd.trade_date <= %s
                      AND sd.trade_date >= DATE_SUB(%s, INTERVAL 25 DAY)
                      AND sd.close_price IS NOT NULL
                      AND sd.volume IS NOT NULL
                      AND sd.close_price > 0
                      AND sd.volume > 0
                    ORDER BY sd.sheep_code, sd.trade_date
                """
                
                cursor.execute(query, (target_date, target_date))
                rows = cursor.fetchall()
                cursor.close()
            
            if not rows:
                logger.warning("没有找到足够的数据来计算VCP和vol_ma_5")
                return {'success': False, 'message': '没有足够的数据'}
            
            # 转换为DataFrame
            df = pd.DataFrame(rows, columns=['sheep_code', 'trade_date', 'close_price', 'volume'])
            
            # 计算VCP因子和vol_ma_5
            results = []
            
            # 按股票分组计算
            for sheep_code in df['sheep_code'].unique():
                stock_data = df[df['sheep_code'] == sheep_code].sort_values('trade_date')
                
                if len(stock_data) < 5:
                    continue
                
                # 获取最新的交易日数据
                latest_data = stock_data.iloc[-1]
                
                # 计算vol_ma_5 (5日均量)
                if len(stock_data) >= 5:
                    vol_ma_5 = stock_data['volume'].tail(5).mean()
                else:
                    vol_ma_5 = stock_data['volume'].mean()
                
                # 计算VCP因子 - 一种简化的VCP计算方式
                # VCP衡量成交量的稳定性：标准差/均值，值越小表示越稳定收敛
                if len(stock_data) >= 10:
                    # 取最近10天的成交量来计算VCP
                    recent_volumes = stock_data['volume'].tail(10)
                    vol_std = recent_volumes.std()
                    vol_mean = recent_volumes.mean()
                    if vol_mean > 0:
                        vcp_factor = vol_std / vol_mean  # 标准差/均值，越小越稳定
                    else:
                        vcp_factor = 1.0
                else:
                    # 数据不足时，设定默认值
                    vcp_factor = 1.0
                
                # 确保VCP因子不会出现极值，并设置合理范围
                vcp_factor = max(0.01, min(vcp_factor, 10.0))
                
                results.append({
                    'sheep_code': sheep_code,
                    'trade_date': latest_data['trade_date'],
                    'vcp_factor': round(vcp_factor, 3),
                    'vol_ma_5': round(vol_ma_5, 1)
                })
            
            if not results:
                logger.warning("没有计算出有效的VCP和vol_ma_5数据")
                return {'success': False, 'message': '没有计算出有效数据'}
            
            # 批量更新数据库
            updated_count = self._batch_update_vcp_vol_ma5(results, target_date)
            
            logger.debug(f"VCP和vol_ma_5更新完成: 成功更新 {updated_count} 条记录")
            
            # 统计信息
            vcp_values = [r['vcp_factor'] for r in results if r['vcp_factor'] is not None]
            vol_ma5_values = [r['vol_ma_5'] for r in results if r['vol_ma_5'] is not None]
            
            return {
                'success': True,
                'total_stocks': len(results),
                'updated_count': updated_count,
                'vcp_min': round(min(vcp_values), 3) if vcp_values else 0,
                'vcp_max': round(max(vcp_values), 3) if vcp_values else 0,
                'vcp_avg': round(sum(vcp_values) / len(vcp_values), 3) if vcp_values else 0,
                'vol_ma5_min': round(min(vol_ma5_values), 1) if vol_ma5_values else 0,
                'vol_ma5_max': round(max(vol_ma5_values), 1) if vol_ma5_values else 0,
                'vol_ma5_avg': round(sum(vol_ma5_values) / len(vol_ma5_values), 1) if vol_ma5_values else 0,
                'target_date': str(target_date)
            }
            
        except Exception as e:
            logger.error(f"计算VCP和vol_ma_5失败: {e}", exc_info=True)
            return {'success': False, 'message': str(e)}
    
    def _batch_update_vcp_vol_ma5(self, update_data: List[Dict], target_date: date) -> int:
        """
        批量更新VCP和vol_ma_5数据到数据库
        
        Args:
            update_data: 包含sheep_code、vcp_factor、vol_ma_5的字典列表
            target_date: 目标日期
            
        Returns:
            更新的记录数
        """
        from db.database import get_raw_connection
        
        if not update_data:
            return 0
        
        updated_count = 0
        batch_size = 1000
        
        try:
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                
                # 使用批量UPDATE语句更新VCP和vol_ma_5
                for i in range(0, len(update_data), batch_size):
                    batch = update_data[i:i + batch_size]
                    
                    if not batch:
                        continue
                    
                    # 使用批量INSERT ... ON DUPLICATE KEY UPDATE
                    values_placeholders = ','.join(['(%s, %s, %s, %s)'] * len(batch))
                    update_query = f"""
                        INSERT INTO sheep_daily (sheep_code, trade_date, vcp_factor, vol_ma_5)
                        VALUES {values_placeholders}
                        ON DUPLICATE KEY UPDATE 
                            vcp_factor = VALUES(vcp_factor),
                            vol_ma_5 = VALUES(vol_ma_5)
                    """
                    
                    params = []
                    for item in batch:
                        params.extend([item['sheep_code'], item['trade_date'], item['vcp_factor'], item['vol_ma_5']])
                    
                    cursor.execute(update_query, params)
                    updated_count += cursor.rowcount
                
                conn.commit()
                cursor.close()
                
        except Exception as e:
            logger.error(f"批量更新VCP和vol_ma_5失败: {e}", exc_info=True)
            raise
        
        return updated_count
    
    def backfill_vcp_vol_ma5_history(self, start_date: date = None, end_date: date = None, days: int = 365) -> Dict[str, any]:
        """
        批量补全历史VCP和vol_ma_5数据
        
        Args:
            start_date: 起始日期，默认为 end_date - days
            end_date: 结束日期，默认为最新交易日
            days: 补全天数（默认365天，即1年）
            
        Returns:
            补全结果统计
        """
        from datetime import timedelta
        
        logger.info(f"开始批量补全VCP和vol_ma_5历史数据...")
        
        try:
            # 确定日期范围
            if end_date is None:
                today = date.today()
                if not self.trade_date_adapter.is_trading_day(today):
                    end_date = self.trade_date_adapter.get_last_trading_day(today)
                else:
                    end_date = today
            
            if start_date is None:
                start_date = end_date - timedelta(days=days)
            
            # 获取日期范围内的所有交易日
            trading_days = self.trade_date_adapter.get_trading_days_in_range(start_date, end_date)
            
            if not trading_days:
                logger.warning(f"在 {start_date} 至 {end_date} 范围内没有找到交易日")
                return {'success': False, 'message': '没有找到交易日'}
            
            logger.info(f"VCP/vol_ma_5补全日期范围: {start_date} 至 {end_date}, 共 {len(trading_days)} 个交易日")
            
            # 逐日计算VCP和vol_ma_5（按时间倒序处理，从最近的日期到最远的日期）
            results = {
                'success': True,
                'start_date': str(start_date),
                'end_date': str(end_date),
                'total_days': len(trading_days),
                'success_days': 0,
                'failed_days': 0,
                'total_updated': 0,
                'details': []
            }
            
            # 按时间倒序处理（从最近的日期到最远的日期）
            trading_days_reversed = sorted(trading_days, reverse=True)
            
            for i, trade_date in enumerate(trading_days_reversed):
                try:
                    result = self.calculate_and_update_vcp_vol_ma5(target_date=trade_date)
                    
                    if result.get('success'):
                        results['success_days'] += 1
                        results['total_updated'] += result.get('updated_count', 0)
                        results['details'].append({
                            'date': str(trade_date),
                            'success': True,
                            'updated': result.get('updated_count', 0)
                        })
                    else:
                        results['failed_days'] += 1
                        results['details'].append({
                            'date': str(trade_date),
                            'success': False,
                            'message': result.get('message')
                        })
                    
                    # 进度日志
                    if (i + 1) % 5 == 0 or i == len(trading_days_reversed) - 1:
                        logger.info(f"VCP/vol_ma_5补全进度: {i + 1}/{len(trading_days_reversed)}, 成功: {results['success_days']}, 总更新: {results['total_updated']}")
                        
                except Exception as e:
                    logger.error(f"计算 {trade_date} 的VCP/vol_ma_5失败: {e}")
                    results['failed_days'] += 1
                    results['details'].append({
                        'date': str(trade_date),
                        'success': False,
                        'message': str(e)
                    })
            
            logger.info(f"VCP/vol_ma_5历史补全完成: 成功 {results['success_days']}/{results['total_days']} 天, 总更新 {results['total_updated']} 条")
            
            return results
            
        except Exception as e:
            logger.error(f"VCP/vol_ma_5历史补全失败: {e}", exc_info=True)
            return {'success': False, 'message': str(e)}
    

    def calculate_all_indicators_historical(self) -> Dict[str, any]:
        """
        计算所有指标的历史数据：rps_250, vol_ma_5, vcp_factor
        从最新的交易日开始，往回计算所有可用的历史数据
        """
        from datetime import timedelta, datetime
        from db.database import get_raw_connection
        import time
        
        start_time = time.time()
        logger.info("开始计算所有指标的历史数据 (rps_250, vol_ma_5, vcp_factor)...")
        
        try:
            # 获取所有交易日，从最新到最老
            today = date.today()
            end_date = self.trade_date_adapter.get_last_trading_day(today)
            # 获取数据库中最早的交易日
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT MIN(trade_date) FROM sheep_daily WHERE rps_250 IS NOT NULL OR vcp_factor IS NOT NULL OR vol_ma_5 IS NOT NULL")
                result = cursor.fetchone()
                cursor.close()
                
                if result and result[0]:
                    start_date = result[0] if isinstance(result[0], date) else datetime.strptime(str(result[0]), "%Y-%m-%d").date()
                else:
                    # 如果没有指标数据，使用最早的数据日期
                    cursor = conn.cursor()
                    cursor.execute("SELECT MIN(trade_date) FROM sheep_daily")
                    result = cursor.fetchone()
                    cursor.close()
                    
                    if result and result[0]:
                        start_date = result[0] if isinstance(result[0], date) else datetime.strptime(str(result[0]), "%Y-%m-%d").date()
                    else:
                        # 如果连数据都没有，使用一年前的日期
                        start_date = end_date - timedelta(days=365)
            
            logger.info(f"计算历史指标数据范围: {start_date} 到 {end_date}")
            
            trading_days = self.trade_date_adapter.get_trading_days_in_range(start_date, end_date)
            
            if not trading_days:
                logger.warning("未找到交易日")
                return {'success': False, 'message': '未找到交易日'}
            
            # 按时间倒序处理（从最新到最老）
            trading_days_reversed = sorted(trading_days, reverse=True)
            
            total_processed = 0
            total_rps_updated = 0
            total_vcp_vol_updated = 0
            failed_days = 0
            
            logger.info(f"开始处理 {len(trading_days_reversed)} 个交易日的历史指标数据")
            
            for i, trade_date in enumerate(trading_days_reversed):
                try:
                    # 计算RPS指标
                    rps_result = self.calculate_and_update_rps(target_date=trade_date)
                    if rps_result.get('success'):
                        total_rps_updated += rps_result.get('updated_count', 0)
                    
                    # 计算VCP和vol_ma_5指标
                    vcp_result = self.calculate_and_update_vcp_vol_ma5(target_date=trade_date)
                    if vcp_result.get('success'):
                        total_vcp_vol_updated += vcp_result.get('updated_count', 0)
                    
                    total_processed += 1
                    
                    # 每处理10天输出一次进度
                    if (i + 1) % 10 == 0 or i == len(trading_days_reversed) - 1:
                        elapsed = time.time() - start_time
                        logger.info(f"历史指标计算进度: {i + 1}/{len(trading_days_reversed)}, "
                                  f"RPS更新: {total_rps_updated}, VCP/vol_ma_5更新: {total_vcp_vol_updated}, "
                                  f"耗时: {elapsed:.1f}秒")
                
                except Exception as e:
                    logger.error(f"计算交易日 {trade_date} 的指标失败: {e}")
                    failed_days += 1
                    continue
            
            elapsed_time = time.time() - start_time
            
            result = {
                'success': True,
                'message': f"历史指标计算完成: 处理 {total_processed} 天, "
                         f"RPS更新 {total_rps_updated} 条, VCP/vol_ma_5更新 {total_vcp_vol_updated} 条, "
                         f"失败 {failed_days} 天",
                'total_processed_days': total_processed,
                'total_rps_updated': total_rps_updated,
                'total_vcp_vol_updated': total_vcp_vol_updated,
                'failed_days': failed_days,
                'elapsed_time': round(elapsed_time, 2)
            }
            
            logger.info(f"历史指标计算完成: {result['message']}")
            return result
            
        except Exception as e:
            error_msg = f"计算历史指标失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {'success': False, 'message': error_msg}

