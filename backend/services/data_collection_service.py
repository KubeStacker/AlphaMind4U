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
            days: 采集最近N天的数据，默认使用配置的保留天数
        """
        if days is None:
            days = Config.SHEEP_DATA_RETENTION_DAYS
        
        today = date.today()
        
        # 判断是否为交易日
        if not self.trade_date_adapter.is_trading_day(today):
            last_trading_day = self.trade_date_adapter.get_last_trading_day(today)
            logger.info(f"{today} 不是交易日，将采集上一个交易日 {last_trading_day} 的数据")
            target_date = last_trading_day
        else:
            target_date = today
        
        # 计算日期范围
        end_date = target_date.strftime('%Y%m%d')
        # 计算开始日期：从目标日期往前推N天，但至少保留3年数据
        start_date = (target_date - timedelta(days=days)).strftime('%Y%m%d')
        
        logger.info(f"开始采集肥羊日K数据，日期范围: {start_date} 至 {end_date}")
        
        # 获取所有肥羊代码
        sheep_list = self.sheep_adapter.get_all_sheep_codes()
        if not sheep_list:
            logger.error("未能获取到肥羊列表")
            return
        
        logger.info(f"共需处理 {len(sheep_list)} 只肥羊")
        
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
                    logger.debug(f"股票 {sheep_code} 已有数据至 {latest_date}，将从 {actual_start_date} 开始补全")
                else:
                    # 如果没有历史数据，从配置的起始日期开始采集
                    actual_start_str = start_date
                    logger.debug(f"股票 {sheep_code} 无历史数据，将从 {actual_start_str} 开始采集")
                
                # 获取日K数据
                df = self.sheep_adapter.get_sheep_daily_data(sheep_code, actual_start_str, end_date)
                
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
                    logger.info(f"进度: {idx + 1}/{len(sheep_list)}, 成功: {success_count}, 失败: {error_count}")
                
            except Exception as e:
                logger.warning(f"处理肥羊 {sheep_item.get('code', 'unknown')} 失败: {e}")
                error_count += 1
                continue
        
        logger.info(f"肥羊日K数据采集完成！成功: {success_count}, 失败: {error_count}")
    
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
            logger.info(f"当前不是交易时段，跳过刷新肥羊 {sheep_code} 的数据")
            return False
        
        today = date.today()
        
        # 判断是否为交易日
        if not self.trade_date_adapter.is_trading_day(today):
            logger.info(f"{today} 不是交易日，跳过刷新肥羊 {sheep_code} 的数据")
            return False
        
        try:
            logger.info(f"开始刷新肥羊 {sheep_code} 的最新数据...")
            
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
                logger.info(f"肥羊 {sheep_code} 日K数据刷新成功")
            
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
                    logger.info(f"肥羊 {sheep_code} 资金流向数据刷新成功")
            except Exception as e:
                logger.warning(f"刷新肥羊 {sheep_code} 资金流向数据失败: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"刷新肥羊 {sheep_code} 数据失败: {e}", exc_info=True)
            return False
    
    def collect_money_flow_data(self):
        """
        采集资金流向数据（仅在交易日执行）
        优先使用批量接口，如果失败则使用逐个获取的方式
        """
        today = date.today()
        
        if not self.trade_date_adapter.is_trading_day(today):
            logger.info(f"{today} 不是交易日，跳过资金流数据采集")
            return
        
        logger.info("开始采集资金流向数据...")
        
        # 方法1：尝试批量获取所有肥羊的资金流（更高效）
        flow_df = self.sheep_adapter.get_all_sheep_money_flow()
        
        if flow_df is not None and not flow_df.empty:
            # 转换为数据库格式（向量化操作）
            def format_flow_row(row):
                return {
                    'code': row['sheep_code'],
                    'date': today,
                    'main': float(row.get('main_net_inflow', 0)),
                    'super_large': float(row.get('super_large_inflow', 0)),
                    'large': float(row.get('large_inflow', 0)),
                    'medium': float(row.get('medium_inflow', 0)),
                    'small': float(row.get('small_inflow', 0)),
                }
            
            data_list = flow_df.apply(format_flow_row, axis=1).tolist()
            
            # 批量保存
            MoneyFlowRepository.batch_upsert_money_flow(data_list)
            
            logger.info(f"资金流向数据采集完成（批量方式）！共 {len(data_list)} 条")
            return
        
        # 方法2：批量接口失败，使用逐个获取的方式（更可靠）
        logger.warning("批量获取资金流数据失败，改用逐个获取方式...")
        self._collect_money_flow_data_individual(today)
    
    def _collect_money_flow_data_individual(self, target_date: date):
        """
        逐个获取每只股票的资金流数据（备用方法）
        
        Args:
            target_date: 目标日期
        """
        # 获取所有股票代码（使用适配器的方法）
        sheep_list = self.sheep_adapter.get_all_sheep_codes()
        if not sheep_list:
            logger.error("未能获取到股票列表")
            return
        
        # 提取股票代码列表
        sheep_codes = [item['code'] for item in sheep_list]
        
        logger.info(f"开始逐个获取 {len(sheep_codes)} 只股票的资金流数据...")
        
        success_count = 0
        error_count = 0
        total_records = 0
        
        for idx, sheep_code in enumerate(sheep_codes):
            try:
                # 获取单只股票的资金流数据（近100个交易日，包含今日）
                flow_df = self.sheep_adapter.get_sheep_money_flow_history(sheep_code)
                
                if flow_df is None or flow_df.empty:
                    continue
                
                # 筛选出今日的数据
                today_data = flow_df[flow_df['trade_date'] == target_date]
                
                if today_data.empty:
                    # 如果没有今日数据，跳过
                    continue
                
                # 格式化数据
                row = today_data.iloc[0]
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
                    logger.info(f"进度: {idx + 1}/{len(sheep_codes)}, 成功: {success_count}, 失败: {error_count}, 记录: {total_records}")
                
                # 延迟，避免请求过快
                import time
                time.sleep(0.1)  # 100ms延迟
                
            except Exception as e:
                logger.debug(f"处理股票 {sheep_code} 失败: {e}")
                error_count += 1
                continue
        
        logger.info(f"资金流向数据采集完成（逐个方式）！成功: {success_count}, 失败: {error_count}, 总记录: {total_records}")
    
    def cleanup_old_money_flow_data(self):
        """
        清理资金流旧数据（保留最近3年）
        """
        logger.info("开始清理资金流旧数据...")
        try:
            deleted_count = MoneyFlowRepository.cleanup_old_data(Config.MONEY_FLOW_RETENTION_DAYS)
            logger.info(f"资金流数据清理完成，删除了 {deleted_count} 条旧数据")
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
                logger.info(f"{today} 不是交易日，将采集上一个交易日 {target_date} 的热度榜数据")
            else:
                target_date = today
        
        logger.info(f"开始采集热度榜数据（日期: {target_date}）...")
        
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
            logger.info(f"热度榜数据采集完成！共 {len(all_data)} 条")
        else:
            logger.warning("未获取到任何热度榜数据")
    
    def collect_concept_data(self):
        """
        采集概念板块数据（自然日数据，每天执行）
        """
        logger.info("开始采集概念板块数据...")
        
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
                    logger.info(f"进度: {idx + 1}/{len(concept_list)}, 成功: {success_count}, 失败: {error_count}")
                
            except Exception as e:
                logger.warning(f"处理概念失败: {e}")
                error_count += 1
                continue
        
        logger.info(f"概念板块数据采集完成！成功: {success_count}, 失败: {error_count}")
    
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
            logger.info("开始同步概念元数据...")
            
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
            
            logger.info(f"从EastMoney获取到 {len(eastmoney_concepts)} 个概念")
            
            # 步骤2: 从数据库获取现有概念列表
            existing_concepts = self._get_existing_concepts()
            logger.info(f"数据库中现有 {len(existing_concepts)} 个概念")
            
            # 步骤3: 计算差异 - 找出新增/缺失的概念
            new_concepts = eastmoney_concepts - existing_concepts
            missing_concepts = new_concepts  # 新增的概念就是缺失的概念
            
            logger.info(f"发现 {len(missing_concepts)} 个新概念需要同步")
            
            # 步骤4: 增量更新 - 只处理缺失的概念
            synced_count = 0
            total_stocks_synced = 0
            
            for concept_name in missing_concepts:
                try:
                    logger.info(f"正在同步概念: {concept_name}")
                    
                    # 获取概念下的股票列表（带重试机制）
                    stock_codes = self.concept_adapter.fetch_concept_constituents(concept_name)
                    
                    if stock_codes is None or len(stock_codes) == 0:
                        logger.warning(f"概念 {concept_name} 没有股票数据，跳过")
                        continue
                    
                    # 插入或更新概念
                    concept_id = ConceptRepository.upsert_concept(
                        concept_name=concept_name,
                        source='em',  # EastMoney
                        description=f'从EastMoney同步，包含{len(stock_codes)}只股票'
                    )
                    
                    if concept_id > 0:
                        # 批量插入股票-概念关联
                        ConceptRepository.batch_upsert_sheep_concept_mapping(
                            concept_id=concept_id,
                            sheep_codes=stock_codes
                        )
                        
                        synced_count += 1
                        total_stocks_synced += len(stock_codes)
                        
                        logger.info(f"成功同步概念: {concept_name} ({len(stock_codes)} 只股票)")
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
            
            logger.info(f"概念元数据同步完成: 新增 {synced_count} 个概念，共 {total_stocks_synced} 只股票")
            
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
        logger.info(f"开始采集指数数据: {index_code}")
        
        try:
            # 获取数据库中已有的最新日期
            latest_date = IndexRepository.get_latest_trade_date(index_code)
            
            # 确定采集日期范围
            end_date = date.today()
            if latest_date:
                # 如果已有数据，从最新日期+1天开始采集
                from datetime import timedelta
                start_date = latest_date + timedelta(days=1)
                logger.info(f"指数 {index_code} 已有数据至 {latest_date}，将从 {start_date} 开始采集")
            else:
                # 如果没有数据，采集最近3年
                from datetime import timedelta
                start_date = end_date - timedelta(days=1095)  # 3年
                logger.info(f"指数 {index_code} 无历史数据，将采集最近3年数据")
            
            # 如果start_date >= end_date，说明数据已是最新
            if start_date >= end_date:
                logger.info(f"指数 {index_code} 数据已是最新，无需更新")
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
            
            logger.info(f"指数数据采集完成: {index_code}, 共 {len(df)} 条记录")
            
        except Exception as e:
            logger.error(f"采集指数数据失败: {index_code}, 错误: {e}", exc_info=True)
    
    def collect_concept_money_flow_data(self, target_date=None):
        """
        采集概念资金流向数据（仅在交易日执行）
        
        Args:
            target_date: 目标日期，如果为None则使用今天
        """
        from services.concept_money_flow_service import ConceptMoneyFlowService
        from datetime import date
        
        if target_date is None:
            target_date = date.today()
        
        # 判断是否为交易日
        if not self.trade_date_adapter.is_trading_day(target_date):
            logger.info(f"{target_date} 不是交易日，跳过概念资金流数据采集")
            return
        
        logger.info(f"开始采集概念资金流向数据（日期: {target_date}）...")
        
        try:
            ConceptMoneyFlowService.collect_concept_money_flow_data(target_date)
            logger.info("概念资金流向数据采集完成")
        except Exception as e:
            logger.error(f"采集概念资金流向数据失败: {e}", exc_info=True)
    
    def cleanup_old_concept_money_flow_data(self):
        """
        清理概念资金流旧数据（保留最近3个月）
        """
        from services.concept_money_flow_service import ConceptMoneyFlowService
        logger.info("开始清理概念资金流旧数据...")
        try:
            deleted_count = ConceptMoneyFlowService.cleanup_old_data(Config.SECTOR_MONEY_FLOW_RETENTION_DAYS)
            logger.info(f"概念资金流数据清理完成，删除了 {deleted_count} 条旧数据")
        except Exception as e:
            logger.error(f"清理概念资金流旧数据失败: {e}", exc_info=True)
    
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
        
        logger.info("=" * 60)
        logger.info("开始批量采集所有数据")
        logger.info("=" * 60)
        
        today = date.today()
        is_trading_day = self.trade_date_adapter.is_trading_day(today)
        
        # 1. 采集肥羊日K数据
        try:
            if is_trading_day or force_trading_day:
                logger.info("[1/7] 开始采集肥羊日K数据...")
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
                logger.info("[2/7] 开始采集资金流向数据...")
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
                logger.info("[3/7] 开始采集概念资金流向数据...")
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
            logger.info("[4/7] 开始采集热度榜数据...")
            self.collect_hot_rank_data()
            results['hot_rank'] = {'success': True, 'message': '采集成功'}
        except Exception as e:
            error_msg = f"采集失败: {str(e)}"
            logger.error(f"热度榜数据{error_msg}", exc_info=True)
            results['hot_rank'] = {'success': False, 'message': error_msg}
        
        # 5. 采集概念板块数据（自然日数据，总是执行）
        try:
            logger.info("[5/7] 开始采集概念板块数据...")
            self.collect_concept_data()
            results['concept_data'] = {'success': True, 'message': '采集成功'}
        except Exception as e:
            error_msg = f"采集失败: {str(e)}"
            logger.error(f"概念板块数据{error_msg}", exc_info=True)
            results['concept_data'] = {'success': False, 'message': error_msg}
        
        # 6. 采集大盘指数数据
        try:
            if is_trading_day or force_trading_day:
                logger.info("[6/7] 开始采集大盘指数数据...")
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
            logger.info("[7/7] 开始同步概念元数据...")
            sync_result = self.sync_concept_metadata()
            if sync_result['success']:
                results['concept_metadata_sync'] = {
                    'success': True, 
                    'message': f"同步成功：新增 {sync_result['new_concepts']} 个概念，共 {sync_result['total_stocks']} 只股票"
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
        
        logger.info("=" * 60)
        logger.info(f"批量采集完成！成功: {success_count}/{total_count}，总耗时: {total_time:.2f}秒")
        logger.info("=" * 60)
        
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
            }
        }
        
        if data_type not in data_type_map:
            return {
                'success': False,
                'message': f'不支持的数据类型: {data_type}',
                'data_type': data_type
            }
        
        config = data_type_map[data_type]
        today = date.today()
        is_trading_day = self.trade_date_adapter.is_trading_day(today)
        force = kwargs.get('force', False)
        
        # 检查是否需要交易日
        if config['requires_trading_day'] and not is_trading_day and not force:
            return {
                'success': False,
                'message': f'{config["name"]}需要在交易日执行，当前不是交易日',
                'data_type': data_type
            }
        
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
        days = kwargs.get('days', None)
        self.collect_sheep_daily_data(days=days)
        return {'message': '采集成功'}
    
    def _collect_money_flow_wrapper(self, **kwargs):
        """资金流向数据采集包装器"""
        self.collect_money_flow_data()
        return {'message': '采集成功'}
    
    def _collect_concept_money_flow_wrapper(self, **kwargs):
        """概念资金流向数据采集包装器"""
        target_date = kwargs.get('target_date', date.today())
        self.collect_concept_money_flow_data(target_date=target_date)
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
        index_code = kwargs.get('index_code', 'CSI1000')
        days = kwargs.get('days', None)
        self.collect_index_data(index_code=index_code, days=days)
        return {'message': '采集成功'}
    
    def _sync_concept_metadata_wrapper(self, **kwargs):
        """概念元数据同步包装器"""
        result = self.sync_concept_metadata()
        if result['success']:
            return {
                'message': f"同步成功：新增 {result['new_concepts']} 个概念，共 {result['total_stocks']} 只股票"
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
    
