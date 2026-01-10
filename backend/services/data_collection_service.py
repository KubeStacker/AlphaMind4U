"""
数据采集服务层
"""
from datetime import date, datetime, timedelta
from typing import Optional
import logging
from etl.trade_date_adapter import TradeDateAdapter
from etl.stock_adapter import StockAdapter
from etl.concept_adapter import ConceptAdapter
from etl.hot_rank_adapter import HotRankAdapter
from etl.concept_filter import should_filter_concept
from db.stock_repository import StockRepository
from db.money_flow_repository import MoneyFlowRepository
from db.concept_repository import ConceptRepository
from db.hot_rank_repository import HotRankRepository
from config import Config
import pandas as pd

logger = logging.getLogger(__name__)

class DataCollectionService:
    """数据采集服务"""
    
    def __init__(self):
        self.trade_date_adapter = TradeDateAdapter
        self.stock_adapter = StockAdapter
        self.concept_adapter = ConceptAdapter
        self.hot_rank_adapter = HotRankAdapter
    
    def collect_stock_daily_data(self, days: int = None):
        """
        采集股票日K数据（仅在交易日执行）
        
        Args:
            days: 采集最近N天的数据，默认使用配置的保留天数
        """
        if days is None:
            days = Config.STOCK_DATA_RETENTION_DAYS
        
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
        start_date = (target_date - timedelta(days=days)).strftime('%Y%m%d')
        
        logger.info(f"开始采集股票日K数据，日期范围: {start_date} 至 {end_date}")
        
        # 获取所有股票代码
        stock_list = self.stock_adapter.get_all_stock_codes()
        if not stock_list:
            logger.error("未能获取到股票列表")
            return
        
        logger.info(f"共需处理 {len(stock_list)} 只股票")
        
        success_count = 0
        error_count = 0
        
        for idx, stock_item in enumerate(stock_list):
            try:
                stock_code = stock_item['code']
                stock_name = stock_item['name']
                
                # 获取日K数据
                df = self.stock_adapter.get_stock_daily_data(stock_code, start_date, end_date)
                
                if df is None or df.empty:
                    continue
                
                # 转换为数据库格式
                data_list = []
                for _, row in df.iterrows():
                    data_list.append({
                        'code': stock_code,
                        'date': row['date'],
                        'open': float(row.get('open', 0)) if pd.notna(row.get('open')) else None,
                        'close': float(row.get('close', 0)) if pd.notna(row.get('close')) else None,
                        'high': float(row.get('high', 0)) if pd.notna(row.get('high')) else None,
                        'low': float(row.get('low', 0)) if pd.notna(row.get('low')) else None,
                        'volume': int(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0,
                        'amount': float(row.get('amount', 0)) if pd.notna(row.get('amount')) else 0,
                        'turnover_rate': float(row.get('turnover_rate', 0)) if pd.notna(row.get('turnover_rate')) else None,
                        'change_pct': float(row.get('change_pct', 0)) if pd.notna(row.get('change_pct')) else None,
                        'ma5': float(row.get('ma5', 0)) if pd.notna(row.get('ma5')) else None,
                        'ma10': float(row.get('ma10', 0)) if pd.notna(row.get('ma10')) else None,
                        'ma20': float(row.get('ma20', 0)) if pd.notna(row.get('ma20')) else None,
                        'ma30': float(row.get('ma30', 0)) if pd.notna(row.get('ma30')) else None,
                        'ma60': float(row.get('ma60', 0)) if pd.notna(row.get('ma60')) else None,
                    })
                
                # 批量保存
                StockRepository.batch_upsert_stock_daily(data_list)
                
                # 更新股票基本信息
                market = 'SH' if stock_code.startswith('6') else 'SZ'
                StockRepository.upsert_stock_basic(stock_code, stock_name, market=market)
                
                success_count += 1
                
                if (idx + 1) % 100 == 0:
                    logger.info(f"进度: {idx + 1}/{len(stock_list)}, 成功: {success_count}, 失败: {error_count}")
                
            except Exception as e:
                logger.warning(f"处理股票 {stock_item.get('code', 'unknown')} 失败: {e}")
                error_count += 1
                continue
        
        logger.info(f"股票日K数据采集完成！成功: {success_count}, 失败: {error_count}")
    
    def collect_money_flow_data(self):
        """
        采集资金流向数据（仅在交易日执行）
        """
        today = date.today()
        
        if not self.trade_date_adapter.is_trading_day(today):
            logger.info(f"{today} 不是交易日，跳过资金流数据采集")
            return
        
        logger.info("开始采集资金流向数据...")
        
        # 批量获取所有股票的资金流
        flow_df = self.stock_adapter.get_all_stocks_money_flow()
        
        if flow_df is None or flow_df.empty:
            logger.warning("未获取到资金流数据")
            return
        
        # 转换为数据库格式
        data_list = []
        for _, row in flow_df.iterrows():
            data_list.append({
                'code': row['stock_code'],
                'date': today,
                'main': float(row.get('main_net_inflow', 0)),
                'super_large': float(row.get('super_large_inflow', 0)),
                'large': float(row.get('large_inflow', 0)),
                'medium': float(row.get('medium_inflow', 0)),
                'small': float(row.get('small_inflow', 0)),
            })
        
        # 批量保存
        MoneyFlowRepository.batch_upsert_money_flow(data_list)
        
        logger.info(f"资金流向数据采集完成！共 {len(data_list)} 条")
    
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
                        # 转换为数据库格式，添加数据验证
                        for _, row in normalized_df.iterrows():
                            stock_code = str(row['stock_code']).strip()
                            stock_name = str(row['stock_name']).strip()[:50]  # 限制长度
                            
                            # 验证股票代码：必须是6位数字
                            if not stock_code or len(stock_code) != 6 or not stock_code.isdigit():
                                logger.warning(f"跳过无效股票代码: {stock_code}")
                                continue
                            
                            # 验证排名：必须是正整数
                            try:
                                rank = int(row['rank'])
                                if rank <= 0:
                                    continue
                            except (ValueError, TypeError):
                                continue
                            
                            # 验证成交量
                            try:
                                volume = int(row.get('volume', 0))
                                if volume < 0:
                                    volume = 0
                            except (ValueError, TypeError):
                                volume = 0
                            
                            all_data.append({
                                'code': stock_code,
                                'name': stock_name,
                                'rank': rank,
                                'source': source,
                                'date': target_date,
                                'score': None,  # 可以后续计算
                                'volume': volume
                            })
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
        
        for idx, row in concept_list.iterrows():
            try:
                # 提取概念名称和代码
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
                
                # 获取概念下的股票
                stock_codes = self.concept_adapter.get_concept_stocks(
                    concept_name, concept_code, source='ths'
                )
                
                if stock_codes:
                    # 批量保存股票-概念关联
                    ConceptRepository.batch_upsert_stock_concept_mapping(
                        concept_id, stock_codes
                    )
                    success_count += 1
                
                if (idx + 1) % 10 == 0:
                    logger.info(f"进度: {idx + 1}/{len(concept_list)}, 成功: {success_count}, 失败: {error_count}")
                
            except Exception as e:
                logger.warning(f"处理概念 {row.get('name', 'unknown')} 失败: {e}")
                error_count += 1
                continue
        
        logger.info(f"概念板块数据采集完成！成功: {success_count}, 失败: {error_count}")
