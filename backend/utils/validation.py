"""
Falcon Data Engine 数据校验模块
实现"三道防线"数据准确性校验
"""
from typing import Dict, Any, Optional, List
from datetime import date
from config import Config
from utils.logger import get_logger
from etl.trade_date_adapter import TradeDateAdapter

logger = get_logger(__name__)

class DataValidator:
    """数据校验器 - 三道防线"""
    
    @staticmethod
    def validate_atomic(data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        原子性校验：检查数据的基本合理性
        
        校验规则：
        - high >= low
        - volume >= 0
        - amount >= 0
        
        Args:
            data: 数据字典
            
        Returns:
            (是否通过, 错误信息)
        """
        if not Config.enable_atomic_check():
            return True, None
        
        # 检查价格合理性
        high = data.get('high') or data.get('high_price')
        low = data.get('low') or data.get('low_price')
        
        if high is not None and low is not None:
            if high < low:
                return False, f"价格异常: high({high}) < low({low})"
        
        # 检查成交量
        volume = data.get('volume')
        if volume is not None and volume < 0:
            return False, f"成交量异常: volume({volume}) < 0"
        
        # 检查成交额
        amount = data.get('amount')
        if amount is not None and amount < 0:
            return False, f"成交额异常: amount({amount}) < 0"
        
        return True, None
    
    @staticmethod
    def validate_money_flow(data: Dict[str, Any]) -> tuple[bool, Optional[str], bool]:
        """
        资金流校验：检查主力净流入是否合理
        
        校验规则：
        - abs(主力净流入) <= 成交额 * 1.1（允许10%误差）
        
        Args:
            data: 资金流数据字典
            
        Returns:
            (是否通过, 错误信息, 是否标记为无效)
        """
        if not Config.enable_money_flow_check():
            return True, None, False
        
        main_net_inflow = data.get('main') or data.get('main_net_inflow')
        amount = data.get('amount') or data.get('turnover_amount')
        
        if main_net_inflow is None or amount is None:
            return True, None, False
        
        # 检查主力净流入是否超过成交额（允许10%误差）
        abs_inflow = abs(main_net_inflow)
        max_allowed = abs(amount) * 1.1
        
        if abs_inflow > max_allowed:
            error_msg = f"资金流异常: 主力净流入({main_net_inflow:.2f}) > 成交额({amount:.2f}) * 1.1"
            logger.error(error_msg)
            return False, error_msg, True  # 标记为无效
        
        return True, None, False
    
    @staticmethod
    def validate_trading_calendar(check_date: date) -> bool:
        """
        交易日历检查：验证是否为交易日
        
        Args:
            check_date: 要检查的日期
            
        Returns:
            True表示是交易日，False表示非交易日
        """
        if not Config.enable_trading_calendar_check():
            return True
        
        return TradeDateAdapter.is_trading_day(check_date)
    
    @staticmethod
    def validate_sheep_daily(data: Dict[str, Any], trade_date: date) -> tuple[bool, Optional[str], bool]:
        """
        肥羊日K数据校验（综合校验）
        
        Args:
            data: 日K数据字典
            trade_date: 交易日期
            
        Returns:
            (是否通过, 错误信息, 是否标记为无效)
        """
        # 1. 交易日历检查
        if not DataValidator.validate_trading_calendar(trade_date):
            return False, f"非交易日: {trade_date}", False
        
        # 2. 原子性校验
        atomic_ok, atomic_error = DataValidator.validate_atomic(data)
        if not atomic_ok:
            return False, atomic_error, True
        
        return True, None, False
    
    @staticmethod
    def validate_money_flow_data(data: Dict[str, Any], trade_date: date) -> tuple[bool, Optional[str], bool]:
        """
        资金流数据校验（综合校验）
        
        Args:
            data: 资金流数据字典
            trade_date: 交易日期
            
        Returns:
            (是否通过, 错误信息, 是否标记为无效)
        """
        # 1. 交易日历检查
        if not DataValidator.validate_trading_calendar(trade_date):
            return False, f"非交易日: {trade_date}", False
        
        # 2. 原子性校验
        atomic_ok, atomic_error = DataValidator.validate_atomic(data)
        if not atomic_ok:
            return False, atomic_error, True
        
        # 3. 资金流校验
        flow_ok, flow_error, is_invalid = DataValidator.validate_money_flow(data)
        if not flow_ok:
            return False, flow_error, is_invalid
        
        return True, None, False
    
    @staticmethod
    def batch_validate(data_list: List[Dict[str, Any]], trade_date: date, 
                      data_type: str = 'sheep_daily') -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        批量校验数据
        
        Args:
            data_list: 数据列表
            trade_date: 交易日期
            data_type: 数据类型（'sheep_daily' 或 'money_flow'）
            
        Returns:
            (有效数据列表, 无效数据列表)
        """
        valid_data = []
        invalid_data = []
        
        for data in data_list:
            if data_type == 'sheep_daily':
                is_valid, error, mark_invalid = DataValidator.validate_sheep_daily(data, trade_date)
            elif data_type == 'money_flow':
                is_valid, error, mark_invalid = DataValidator.validate_money_flow_data(data, trade_date)
            else:
                # 默认只做原子性校验
                is_valid, error = DataValidator.validate_atomic(data)
                mark_invalid = False
            
            if is_valid:
                valid_data.append(data)
            else:
                if mark_invalid:
                    data['is_valid'] = 0
                    data['validation_error'] = error
                invalid_data.append(data)
                logger.debug(f"数据校验失败: {error}, 数据: {data.get('code', 'unknown')}")
        
        return valid_data, invalid_data
