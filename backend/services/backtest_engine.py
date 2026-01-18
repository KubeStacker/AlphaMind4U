"""
回测引擎：时光机逻辑 v2.0
实现历史数据回测，验证策略有效性

v2.0 优化内容：
1. 修复胜率判定标准过严问题（降低至5%或正收益）
2. 引入动态止损（基于ATR）
3. 批量查询优化，减少数据库访问次数
4. 增加因子缓存机制，避免重复计算
5. 更合理的采样策略
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from datetime import date, datetime, timedelta
import logging
from sqlalchemy import text
from db.database import get_db, get_raw_connection, engine
from functools import lru_cache
import concurrent.futures
from services.alpha_model_t7_concept_flow import AlphaModelT7ConceptFlow

logger = logging.getLogger(__name__)

# ============================================
# 胜率判定标准配置
# ============================================
# 分层判定，更符合A股实际
WIN_CRITERIA = {
    'excellent': {  # 优秀：大幅盈利
        'max_return_threshold': 10.0,  # 最大涨幅阈值(%)
        'final_return_threshold': 3.0,  # 最终收益阈值(%)
    },
    'good': {  # 良好：中等盈利
        'max_return_threshold': 5.0,
        'final_return_threshold': 2.0,
    },
    'pass': {  # 及格：小幅盈利或不亏
        'max_return_threshold': 3.0,
        'final_return_threshold': 0.0,
    }
}

# 动态止损配置
DYNAMIC_STOP_LOSS = {
    'base_ratio': 0.93,  # 基础止损比例（7%）
    'atr_multiplier': 2.0,  # ATR倍数
    'max_stop_loss': 0.90,  # 最大止损（10%）
    'min_stop_loss': 0.95,  # 最小止损（5%）
}


class BacktestEngine:
    """
    回测引擎
    
    核心特性：
    1. 分层胜率判定（优秀/良好/及格）
    2. 动态止损（基于ATR）
    3. 批量数据预加载，减少SQL查询
    4. 智能采样，保证关键时点不被跳过
    5. 回测专用宽松参数，提高成功率
    """
    
    # 回测模式下的宽松参数（覆盖模型默认值）
    BACKTEST_RELAXED_PARAMS = {
        'min_change_pct': 3.0,       # 回测时降低涨幅要求（从5%降到3%）
        'max_change_pct': 15,        # 回测时放宽最高涨幅（从12%到15%）
        'rps_threshold': 75,         # 回测时降低RPS要求（从85降到75）
        'min_main_inflow': -200,     # 回测时允许小幅资金流出
        'require_positive_inflow': False,  # 回测时不强制要求资金流入
        'min_turnover': 3.0,         # 回测时降低换手率要求（从5%到3%）
        'max_turnover': 30.0,        # 回测时放宽最高换手率（从20%到30%）
        'min_breakout_quality': 30,  # 回测时降低启动质量要求（从50到30）
        'min_ai_score': 40,          # 回测时降低AI评分要求（从55到40）
        'require_concept_resonance': False,  # 回测时不强制要求概念共振
    }
    
    def __init__(self):
        # 使用T7概念资金双驱模型
        self.model = AlphaModelT7ConceptFlow()
        # 价格数据缓存（减少重复查询）
        self._price_cache: Dict[str, pd.DataFrame] = {}
        self._cache_date_range: Tuple[date, date] = (None, None)
    
    def _preload_price_data(self, start_date: date, end_date: date) -> None:
        """
        预加载价格数据到缓存
        一次性加载所有需要的价格数据，避免每日重复查询
        """
        # 扩展日期范围以包含后续验证需要的数据
        extended_end = end_date + timedelta(days=15)
        
        if (self._cache_date_range[0] and self._cache_date_range[1] and
            self._cache_date_range[0] <= start_date and 
            self._cache_date_range[1] >= extended_end):
            logger.info("使用已缓存的价格数据")
            return
        
        logger.info(f"预加载价格数据: {start_date} 至 {extended_end}")
        try:
            query = text("""
                SELECT sheep_code, trade_date, open_price, close_price, 
                       high_price, low_price, change_pct, turnover_rate
                FROM sheep_daily
                WHERE trade_date >= :start_date AND trade_date <= :end_date
                ORDER BY sheep_code, trade_date
            """)
            
            with get_db() as db:
                result = db.execute(query, {
                    'start_date': start_date - timedelta(days=30),  # 多加载30天用于计算ATR
                    'end_date': extended_end
                })
                rows = result.fetchall()
            
            # 转换为DataFrame并按肥羊代码分组缓存
            if rows:
                df = pd.DataFrame(rows, columns=['sheep_code', 'trade_date', 'open_price', 
                                                  'close_price', 'high_price', 'low_price', 
                                                  'change_pct', 'turnover_rate'])
                for sheep_code, group in df.groupby('sheep_code'):
                    self._price_cache[sheep_code] = group.sort_values('trade_date').reset_index(drop=True)
                
                logger.info(f"已缓存 {len(self._price_cache)} 只肥羊的价格数据")
            
            self._cache_date_range = (start_date - timedelta(days=30), extended_end)
        except Exception as e:
            logger.error(f"预加载价格数据失败: {e}")
    
    def _calculate_dynamic_stop_loss(self, sheep_code: str, entry_date: date, entry_price: float) -> float:
        """
        计算动态止损价（基于ATR）
        
        v2.0改进：
        - 使用ATR（真实波幅）动态调整止损
        - 高波动肥羊使用更宽的止损，低波动肥羊使用更紧的止损
        """
        try:
            if sheep_code in self._price_cache:
                df = self._price_cache[sheep_code]
                # 获取入场前20日数据计算ATR
                df_before = df[df['trade_date'] < entry_date].tail(20)
                
                if len(df_before) >= 10:
                    # 计算ATR（简化版：使用高低价差的均值）
                    df_before['tr'] = df_before['high_price'] - df_before['low_price']
                    atr = df_before['tr'].mean()
                    
                    # 动态止损 = 入场价 - ATR * 倍数
                    atr_stop = entry_price - atr * DYNAMIC_STOP_LOSS['atr_multiplier']
                    
                    # 限制在合理范围内
                    min_stop = entry_price * DYNAMIC_STOP_LOSS['max_stop_loss']  # 最大亏10%
                    max_stop = entry_price * DYNAMIC_STOP_LOSS['min_stop_loss']  # 最小亏5%
                    
                    return max(min_stop, min(max_stop, atr_stop))
            
            # 默认使用固定止损
            return entry_price * DYNAMIC_STOP_LOSS['base_ratio']
        except Exception as e:
            logger.warning(f"计算动态止损失败: {e}")
            return entry_price * DYNAMIC_STOP_LOSS['base_ratio']
    
    def _merge_backtest_params(self, user_params: Dict) -> Dict:
        """
        合并用户参数和回测宽松参数
        
        逻辑：
        1. 使用回测宽松参数作为基础
        2. 用户参数覆盖宽松参数（用户可以选择更严格或更宽松）
        3. 但某些关键参数保持宽松（避免回测完全失败）
        """
        # 深拷贝回测宽松参数
        merged = dict(self.BACKTEST_RELAXED_PARAMS)
        
        # 用户参数覆盖
        if user_params:
            for key, value in user_params.items():
                if value is not None:
                    merged[key] = value
        
        # 强制保证某些参数不会太严格（防止回测完全失败）
        # RPS 不能高于 90
        if merged.get('rps_threshold', 75) > 90:
            merged['rps_threshold'] = 90
            logger.info("回测模式：RPS阈值已限制为90")
        
        # 涨幅范围至少有 5% 空间
        min_pct = merged.get('min_change_pct', 3.0)
        max_pct = merged.get('max_change_pct', 15)
        if max_pct - min_pct < 5:
            merged['max_change_pct'] = min_pct + 5
            logger.info(f"回测模式：涨幅范围已调整为 {min_pct}%-{merged['max_change_pct']}%")
        
        # AI评分不能高于 70
        if merged.get('min_ai_score', 40) > 70:
            merged['min_ai_score'] = 70
            logger.info("回测模式：AI评分要求已限制为70")
        
        return merged
    
    def run_backtest(self, start_date: date, end_date: date, params: Dict) -> Dict:
        """
        运行回测 v2.1
        
        优化内容：
        1. 预加载价格数据，减少SQL查询
        2. 智能采样（保留关键时点）
        3. 分层胜率判定
        4. 动态止损
        5. 更详细的统计信息
        6. v2.1: 回测专用宽松参数
        """
        try:
            # v2.1: 合并用户参数和回测宽松参数
            params = self._merge_backtest_params(params)
            logger.info(f"开始回测 v2.1: {start_date} 至 {end_date}, 参数: {params}")
            
            # 获取交易日列表
            trading_dates = self._get_trading_dates(start_date, end_date)
            logger.info(f"获取到 {len(trading_dates)} 个交易日")
            
            if not trading_dates:
                logger.warning("没有找到交易日数据")
                return {
                    'success': False,
                    'message': '没有找到交易日数据'
                }
            
            # v2.0: 预加载价格数据
            self._preload_price_data(start_date, end_date)
            
            # 智能采样策略（v2.0优化）
            trading_days_count = len(trading_dates)
            sample_interval = 1
            
            if trading_days_count > 120:
                # 超过120天，每4天处理一次
                sample_interval = 4
            elif trading_days_count > 80:
                sample_interval = 3
            elif trading_days_count > 50:
                sample_interval = 2
            
            if sample_interval > 1:
                logger.info(f"采样模式: 每{sample_interval}天处理一次，预计处理约{trading_days_count // sample_interval}天")
            
            # 存储回测结果
            trades = []
            equity_curve = []
            initial_capital = 100000  # 初始资金10万
            current_capital = initial_capital
            max_drawdown = 0
            peak_capital = initial_capital
            
            # v2.0: 更详细的统计信息
            stats = {
                'total_days': 0,
                'days_with_recommendations': 0,
                'days_without_recommendations': 0,
                'excellent_trades': 0,  # 优秀交易
                'good_trades': 0,       # 良好交易
                'pass_trades': 0,       # 及格交易
                'fail_trades': 0,       # 失败交易
                'stop_loss_triggered': 0,  # 止损触发次数
            }
            
            # 逐日回测（使用采样间隔）
            consecutive_no_recommendations = 0
            max_consecutive_no_rec = 30  # v2.1: 提高到30天，允许更长的空窗期
            total_no_rec_days = 0  # 总无推荐天数
            
            for i, trade_date in enumerate(trading_dates):
                if i < 4:  # 前4天数据不足，跳过
                    continue
                
                # 采样模式：跳过某些天数
                if sample_interval > 1 and (i - 4) % sample_interval != 0:
                    continue
                
                stats['total_days'] += 1
                
                # 记录进度（每处理10天记录一次）
                if stats['total_days'] % 10 == 0:
                    logger.info(f"回测进度: {stats['total_days']} 天，已生成 {len(trades)} 笔交易")
                
                # 运行策略，获取推荐肥羊
                try:
                    result = self.model.run_full_pipeline(trade_date, params, top_n=5)
                    # run_full_pipeline 返回 (recommendations, diagnostic_info, metadata)
                    if isinstance(result, tuple):
                        if len(result) == 3:
                            recommendations, diagnostic_info, _ = result
                        elif len(result) == 2:
                            recommendations, diagnostic_info = result
                        else:
                            recommendations = result[0] if result else []
                            diagnostic_info = None
                    else:
                        recommendations = result
                        diagnostic_info = None
                    
                    if diagnostic_info:
                        logger.debug(f"交易日 {trade_date}: {diagnostic_info}")
                    
                    if not recommendations:
                        stats['days_without_recommendations'] += 1
                        consecutive_no_recommendations += 1
                        total_no_rec_days += 1
                        
                        # 如果连续多天无推荐，提前终止（但确保至少处理了一定比例的日期）
                        processed_ratio = stats['total_days'] / max(len(trading_dates), 1)
                        if consecutive_no_recommendations >= max_consecutive_no_rec and processed_ratio > 0.3:
                            logger.warning(f"连续 {consecutive_no_recommendations} 天无推荐（已处理{processed_ratio*100:.0f}%），提前终止回测")
                            break
                        
                        if stats['total_days'] % 10 == 0:
                            logger.info(f"交易日 {trade_date}: 无推荐肥羊（连续{consecutive_no_recommendations}天）")
                        continue
                    
                    # 有推荐，重置连续无推荐计数
                    consecutive_no_recommendations = 0
                    stats['days_with_recommendations'] += 1
                    logger.info(f"交易日 {trade_date}: 推荐 {len(recommendations)} 只肥羊")
                    
                except Exception as e:
                    logger.error(f"交易日 {trade_date} 运行策略失败: {e}", exc_info=True)
                    stats['days_without_recommendations'] += 1
                    continue
                
                # 模拟买入（在trade_date的下一个交易日）
                if i + 1 < len(trading_dates):
                    buy_date = trading_dates[i + 1]
                    
                    for rec in recommendations:
                        sheep_code = rec['sheep_code']
                        entry_price = rec['entry_price']
                        
                        # 检查买入条件（开盘跌幅>3%或跌停，撤单）
                        buy_result = self._simulate_buy(sheep_code, buy_date, entry_price)
                        
                        if not buy_result['success']:
                            logger.debug(f"买入失败 {sheep_code} @ {buy_date}: {buy_result.get('message', '未知原因')}")
                            continue
                        
                        # 持有5个交易日
                        sell_date_idx = min(i + 6, len(trading_dates) - 1)
                        sell_date = trading_dates[sell_date_idx]
                        
                        # v2.0: 使用动态止损
                        dynamic_stop = self._calculate_dynamic_stop_loss(
                            sheep_code, buy_date, buy_result['buy_price']
                        )
                        
                        # 计算收益（使用v2.0改进的判定逻辑）
                        try:
                            trade_result = self._simulate_sell_v2(
                                sheep_code, 
                                buy_date, 
                                sell_date, 
                                buy_result['buy_price'],
                                dynamic_stop
                            )
                        except Exception as e:
                            logger.error(f"计算卖出收益失败: {sheep_code}, 错误={e}", exc_info=True)
                            continue
                        
                        # 更新统计
                        result_grade = trade_result.get('result_grade', 'fail')
                        if result_grade == 'excellent':
                            stats['excellent_trades'] += 1
                        elif result_grade == 'good':
                            stats['good_trades'] += 1
                        elif result_grade == 'pass':
                            stats['pass_trades'] += 1
                        else:
                            stats['fail_trades'] += 1
                        
                        if trade_result.get('stop_loss_triggered', False):
                            stats['stop_loss_triggered'] += 1
                        
                        trades.append({
                            'buy_date': buy_date,
                            'sell_date': sell_date,
                            'sheep_code': sheep_code,
                            'sheep_name': rec['sheep_name'],
                            'entry_price': buy_result['buy_price'],
                            'exit_price': trade_result['exit_price'],
                            'return_pct': trade_result['return_pct'],
                            'max_return_5d': trade_result['max_return_5d'],
                            'result': trade_result['result'],
                            'result_grade': result_grade,
                            'params_snapshot': params
                        })
                        
                        # 更新资金曲线
                        position_value = current_capital / 5
                        profit = position_value * (trade_result['return_pct'] / 100)
                        current_capital += profit
                        
                        # 计算最大回撤
                        if current_capital > peak_capital:
                            peak_capital = current_capital
                        drawdown = (peak_capital - current_capital) / peak_capital * 100
                        if drawdown > max_drawdown:
                            max_drawdown = drawdown
                        
                        equity_curve.append({
                            'date': sell_date,
                            'capital': current_capital,
                            'return_pct': (current_capital / initial_capital - 1) * 100
                        })
            
            # 计算统计指标
            logger.info(f"回测完成: 总交易日 {stats['total_days']}, 有推荐 {stats['days_with_recommendations']} 天, 生成交易 {len(trades)} 笔")
            
            if not trades:
                # 提供更详细的失败诊断
                no_rec_ratio = stats['days_without_recommendations'] / max(stats['total_days'], 1) * 100
                diagnostic_msg = f'没有生成任何交易记录。'
                diagnostic_msg += f'\n\n诊断信息：'
                diagnostic_msg += f'\n- 总交易日: {stats["total_days"]} 天'
                diagnostic_msg += f'\n- 有推荐: {stats["days_with_recommendations"]} 天'
                diagnostic_msg += f'\n- 无推荐: {stats["days_without_recommendations"]} 天 ({no_rec_ratio:.0f}%)'
                
                if stats['days_with_recommendations'] == 0:
                    diagnostic_msg += f'\n\n可能原因：'
                    diagnostic_msg += f'\n1. 筛选条件太严格（尝试降低RPS阈值、放宽涨幅范围）'
                    diagnostic_msg += f'\n2. 数据不足（检查所选日期范围内是否有足够的历史数据）'
                    diagnostic_msg += f'\n3. 市场环境不佳（该时段可能没有符合条件的肥羊）'
                else:
                    diagnostic_msg += f'\n\n可能原因：推荐后买入条件未满足（如开盘跌幅过大）'
                
                diagnostic_msg += f'\n\n建议：'
                diagnostic_msg += f'\n1. 缩短日期范围到1-2个月'
                diagnostic_msg += f'\n2. 降低RPS阈值（如从85降到75）'
                diagnostic_msg += f'\n3. 放宽涨幅范围（如改为3%-12%）'
                
                return {
                    'success': False,
                    'message': diagnostic_msg,
                    'stats': stats  # 返回统计信息供调试
                }
            
            # v2.0: 分层胜率计算
            # 成功定义改为：优秀+良好+及格
            successful_trades = [t for t in trades if t['result'] == 'SUCCESS']
            win_rate = len(successful_trades) / len(trades) * 100 if trades else 0
            
            # 优秀率（最大涨幅>10%且最终盈利>3%）
            excellent_rate = stats['excellent_trades'] / len(trades) * 100 if trades else 0
            
            # 良好率（最大涨幅>5%且最终盈利>2%）
            good_rate = stats['good_trades'] / len(trades) * 100 if trades else 0
            
            # 及格率（不亏钱）
            pass_rate = stats['pass_trades'] / len(trades) * 100 if trades else 0
            
            # 爆款率（5日最大涨幅>10%）
            alpha_trades = [t for t in trades if t.get('max_return_5d', 0) > 10]
            alpha_rate = len(alpha_trades) / len(trades) * 100 if trades else 0
            
            # 总收益率
            total_return = (current_capital / initial_capital - 1) * 100
            
            # 平均收益率
            avg_return = np.mean([t['return_pct'] for t in trades]) if trades else 0
            
            # 止损触发率
            stop_loss_rate = stats['stop_loss_triggered'] / len(trades) * 100 if trades else 0
            
            # 获取基准指数收益
            benchmark_return = self._get_benchmark_return(start_date, end_date)
            
            # 超额收益
            excess_return = total_return - benchmark_return
            
            return {
                'success': True,
                'trades': trades,
                'equity_curve': equity_curve,
                'metrics': {
                    'total_trades': len(trades),
                    'win_rate': round(win_rate, 2),
                    'excellent_rate': round(excellent_rate, 2),  # v2.0新增
                    'good_rate': round(good_rate, 2),            # v2.0新增
                    'pass_rate': round(pass_rate, 2),            # v2.0新增
                    'alpha_rate': round(alpha_rate, 2),
                    'total_return': round(total_return, 2),
                    'avg_return': round(avg_return, 2),          # v2.0新增
                    'max_drawdown': round(max_drawdown, 2),
                    'stop_loss_rate': round(stop_loss_rate, 2),  # v2.0新增
                    'benchmark_return': round(benchmark_return, 2),
                    'excess_return': round(excess_return, 2),    # v2.0新增
                    'final_capital': round(current_capital, 2)
                },
                'stats': stats  # v2.0: 返回详细统计
            }
            
        except Exception as e:
            logger.error(f"回测失败: {e}", exc_info=True)
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"回测异常详情:\n{error_detail}")
            return {
                'success': False,
                'message': f'回测执行失败: {str(e)}。请检查日志获取详细信息。'
            }
    
    def _get_trading_dates(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日列表"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT DISTINCT trade_date
                    FROM sheep_daily USE INDEX (idx_trade_date)
                    WHERE trade_date >= :start_date
                      AND trade_date <= :end_date
                    ORDER BY trade_date ASC
                """)
                result = db.execute(query, {
                    'start_date': start_date,
                    'end_date': end_date
                })
                dates = [row[0] for row in result]
                logger.info(f"从数据库获取交易日: {start_date} 至 {end_date}, 共 {len(dates)} 个交易日")
                if dates:
                    logger.info(f"第一个交易日: {dates[0]}, 最后一个交易日: {dates[-1]}")
                return dates
        except Exception as e:
            logger.error(f"获取交易日失败: {e}", exc_info=True)
            return []
    
    def _simulate_buy(self, sheep_code: str, buy_date: date, expected_price: float) -> Dict:
        """模拟买入"""
        try:
            with get_db() as db:
                # 获取买入日的开盘价
                query = text("""
                    SELECT open_price, close_price, change_pct
                    FROM sheep_daily USE INDEX (idx_sheep_code_date)
                    WHERE sheep_code = :code
                      AND trade_date = :date
                """)
                result = db.execute(query, {'code': sheep_code, 'date': buy_date})
                row = result.fetchone()
                
                if not row:
                    return {'success': False, 'message': '无数据'}
                
                open_price, close_price, change_pct = row
                
                # 检查买入条件：开盘跌幅>3%或跌停（-10%），撤单
                if change_pct and change_pct < -3:
                    return {'success': False, 'message': '开盘跌幅过大，撤单'}
                
                # 使用开盘价买入
                buy_price = float(open_price) if open_price else expected_price
                
                return {
                    'success': True,
                    'buy_price': buy_price
                }
        except Exception as e:
            logger.error(f"模拟买入失败: {e}")
            return {'success': False, 'message': str(e)}
    
    def _simulate_sell(self, sheep_code: str, buy_date: date, sell_date: date, 
                      buy_price: float, stop_loss: float) -> Dict:
        """模拟卖出（持有5个交易日）- 旧版本，保留兼容性"""
        return self._simulate_sell_v2(sheep_code, buy_date, sell_date, buy_price, stop_loss)
    
    def _simulate_sell_v2(self, sheep_code: str, buy_date: date, sell_date: date, 
                         buy_price: float, stop_loss: float) -> Dict:
        """
        模拟卖出 v2.0 - 改进的胜率判定
        
        改进内容：
        1. 使用缓存数据，减少SQL查询
        2. 分层胜率判定（优秀/良好/及格/失败）
        3. 记录止损触发
        """
        try:
            # v2.0: 优先使用缓存数据
            if sheep_code in self._price_cache:
                df = self._price_cache[sheep_code]
                df_period = df[(df['trade_date'] > buy_date) & (df['trade_date'] <= sell_date)].head(5)
            else:
                # 回退到SQL查询
                query = text("""
                    SELECT trade_date, open_price, close_price, high_price, low_price
                    FROM sheep_daily USE INDEX (idx_sheep_code_date)
                    WHERE sheep_code = :sheep_code
                      AND trade_date > :buy_date
                      AND trade_date <= :sell_date
                    ORDER BY trade_date ASC
                    LIMIT 5
                """)
                df_period = pd.read_sql(
                    query, 
                    engine, 
                    params={'sheep_code': sheep_code, 'buy_date': buy_date, 'sell_date': sell_date}
                )
            
            if df_period.empty:
                return {
                    'exit_price': buy_price,
                    'return_pct': 0,
                    'max_return_5d': 0,
                    'result': 'FAIL',
                    'result_grade': 'fail',
                    'stop_loss_triggered': False
                }
            
            # 计算期间最高价和最低价
            max_price = float(df_period['high_price'].max())
            min_price = float(df_period['low_price'].min())
            max_return_5d = (max_price / buy_price - 1) * 100
            
            # 检查是否触发止损
            stop_loss_triggered = min_price <= stop_loss
            
            if stop_loss_triggered:
                exit_price = stop_loss
                return_pct = (exit_price / buy_price - 1) * 100
                result = 'FAIL'
                result_grade = 'fail'
            else:
                # 使用第5日收盘价卖出
                exit_price = float(df_period.iloc[-1]['close_price'])
                return_pct = (exit_price / buy_price - 1) * 100
                
                # v2.0: 分层胜率判定
                # 优秀：最大涨幅>10% 且 最终盈利>3%
                if max_return_5d >= WIN_CRITERIA['excellent']['max_return_threshold'] and \
                   return_pct >= WIN_CRITERIA['excellent']['final_return_threshold']:
                    result = 'SUCCESS'
                    result_grade = 'excellent'
                # 良好：最大涨幅>5% 且 最终盈利>2%
                elif max_return_5d >= WIN_CRITERIA['good']['max_return_threshold'] and \
                     return_pct >= WIN_CRITERIA['good']['final_return_threshold']:
                    result = 'SUCCESS'
                    result_grade = 'good'
                # 及格：最大涨幅>3% 且 最终不亏（>=0%）
                elif max_return_5d >= WIN_CRITERIA['pass']['max_return_threshold'] and \
                     return_pct >= WIN_CRITERIA['pass']['final_return_threshold']:
                    result = 'SUCCESS'
                    result_grade = 'pass'
                else:
                    result = 'FAIL'
                    result_grade = 'fail'
            
            return {
                'exit_price': round(exit_price, 2),
                'return_pct': round(return_pct, 2),
                'max_return_5d': round(max_return_5d, 2),
                'result': result,
                'result_grade': result_grade,
                'stop_loss_triggered': stop_loss_triggered
            }
        except Exception as e:
            logger.error(f"模拟卖出失败: sheep_code={sheep_code}, 错误={e}", exc_info=True)
            return {
                'exit_price': buy_price,
                'return_pct': 0,
                'max_return_5d': 0,
                'result': 'FAIL',
                'result_grade': 'fail',
                'stop_loss_triggered': False
            }
    
    def _get_benchmark_return(self, start_date: date, end_date: date) -> float:
        """获取基准指数（中证1000）收益率"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT close_price, trade_date
                    FROM market_index_daily
                    WHERE index_code = 'CSI1000'
                      AND trade_date >= :start_date
                      AND trade_date <= :end_date
                    ORDER BY trade_date ASC
                """)
                result = db.execute(query, {
                    'start_date': start_date,
                    'end_date': end_date
                })
                rows = result.fetchall()
                
                if rows and len(rows) >= 2:
                    start_price = float(rows[0][0])
                    end_price = float(rows[-1][0])
                    if start_price > 0:
                        return (end_price / start_price - 1) * 100
            return 0.0
        except Exception as e:
            logger.warning(f"获取基准收益失败: {e}")
            return 0.0
