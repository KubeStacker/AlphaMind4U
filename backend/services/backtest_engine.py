"""
回测引擎：时光机逻辑
实现历史数据回测，验证策略有效性
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import date, datetime, timedelta
import logging
from sqlalchemy import text
from db.database import get_db, get_raw_connection, engine
from services.alpha_model_t6_resonance import AlphaModelT6Resonance
from services.alpha_model_t7_concept_flow import AlphaModelT7ConceptFlow

logger = logging.getLogger(__name__)


class BacktestEngine:
    """回测引擎"""
    
    def __init__(self):
        # 使用T7概念资金双驱模型
        self.model = AlphaModelT7ConceptFlow()
    
    def run_backtest(self, start_date: date, end_date: date, params: Dict) -> Dict:
        """
        运行回测
        在指定日期范围内，每日运行策略，模拟交易
        
        优化策略：
        1. 如果交易日太多（>100天），使用采样模式（每N天处理一次）
        2. 添加进度日志
        3. 提前终止条件（如果连续多天无推荐）
        """
        try:
            logger.info(f"开始回测: {start_date} 至 {end_date}, 参数: {params}")
            
            # 获取交易日列表
            trading_dates = self._get_trading_dates(start_date, end_date)
            logger.info(f"获取到 {len(trading_dates)} 个交易日")
            
            # 如果交易日太多，使用采样模式（加速回测）
            # 优化采样策略：更早启用采样，避免超时
            sample_interval = 1  # 默认每天处理
            trading_days_count = len(trading_dates)
            
            if trading_days_count > 80:
                # 超过80个交易日（约3个月），每3天处理一次
                sample_interval = 3
                logger.info(f"交易日较多({trading_days_count}天)，启用采样模式（每{sample_interval}天处理一次），预计处理约{trading_days_count // sample_interval}天")
            elif trading_days_count > 60:
                # 60-80个交易日（约2-3个月），每2天处理一次
                sample_interval = 2
                logger.info(f"交易日较多({trading_days_count}天)，启用采样模式（每{sample_interval}天处理一次），预计处理约{trading_days_count // sample_interval}天")
            elif trading_days_count > 40:
                # 40-60个交易日（约1.5-2个月），保持每天处理，但优化性能
                sample_interval = 1
                logger.info(f"交易日数量: {trading_days_count}天，将每天处理")
            else:
                # 少于40个交易日，每天处理
                sample_interval = 1
            
            if not trading_dates:
                logger.warning("没有找到交易日数据")
                return {
                    'success': False,
                    'message': '没有找到交易日数据'
                }
            
            # 存储回测结果
            trades = []
            equity_curve = []
            initial_capital = 100000  # 初始资金10万
            current_capital = initial_capital
            max_drawdown = 0
            peak_capital = initial_capital
            
            # 统计信息
            total_days = 0
            days_with_recommendations = 0
            days_without_recommendations = 0
            
            # 逐日回测（使用采样间隔）
            consecutive_no_recommendations = 0  # 连续无推荐天数
            max_consecutive_no_rec = 20  # 如果连续20天无推荐，提前终止（节省时间）
            
            for i, trade_date in enumerate(trading_dates):
                if i < 4:  # 前4天数据不足，跳过
                    continue
                
                # 采样模式：跳过某些天数
                if sample_interval > 1 and (i - 4) % sample_interval != 0:
                    continue
                
                total_days += 1
                
                # 记录进度（每处理10天记录一次）
                if total_days % 10 == 0:
                    logger.info(f"回测进度: {total_days} 天，已生成 {len(trades)} 笔交易")
                
                # 运行策略，获取推荐肥羊
                try:
                    result = self.model.run_full_pipeline(trade_date, params, top_n=5)
                    recommendations, diagnostic_info = result if isinstance(result, tuple) else (result, None)
                    
                    if diagnostic_info:
                        logger.debug(f"交易日 {trade_date}: {diagnostic_info}")
                    
                    if not recommendations:
                        days_without_recommendations += 1
                        consecutive_no_recommendations += 1
                        
                        # 如果连续多天无推荐，提前终止（节省时间）
                        if consecutive_no_recommendations >= max_consecutive_no_rec:
                            logger.warning(f"连续 {consecutive_no_recommendations} 天无推荐，提前终止回测（节省时间）")
                            break
                        
                        if total_days % 10 == 0:  # 每10天记录一次
                            logger.info(f"交易日 {trade_date}: 无推荐肥羊 (累计 {days_without_recommendations}/{total_days} 天无推荐)")
                        continue
                    
                    # 有推荐，重置连续无推荐计数
                    consecutive_no_recommendations = 0
                    
                    days_with_recommendations += 1
                    logger.info(f"交易日 {trade_date}: 推荐 {len(recommendations)} 只肥羊")
                    
                except Exception as e:
                    logger.error(f"交易日 {trade_date} 运行策略失败: {e}", exc_info=True)
                    days_without_recommendations += 1
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
                        
                        if buy_result['success']:
                            # 持有5个交易日
                            sell_date_idx = min(i + 6, len(trading_dates) - 1)
                            sell_date = trading_dates[sell_date_idx]
                            
                            # 计算收益
                            try:
                                trade_result = self._simulate_sell(
                                    sheep_code, 
                                    buy_date, 
                                    sell_date, 
                                    buy_result['buy_price'],
                                    rec.get('stop_loss_price', entry_price * 0.93)
                                )
                            except Exception as e:
                                logger.error(f"计算卖出收益失败: sheep_code={sheep_code}, buy_date={buy_date}, sell_date={sell_date}, 错误={e}", exc_info=True)
                                continue
                            
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
                                'params_snapshot': params
                            })
                            
                            # 更新资金曲线（简化：每只肥羊投入1/5资金）
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
            logger.info(f"回测完成: 总交易日 {total_days}, 有推荐 {days_with_recommendations} 天, 无推荐 {days_without_recommendations} 天, 生成交易 {len(trades)} 笔")
            
            if not trades:
                return {
                    'success': False,
                    'message': f'没有生成任何交易记录。总交易日: {total_days}, 有推荐: {days_with_recommendations} 天, 无推荐: {days_without_recommendations} 天。可能原因：筛选条件太严格或数据不足。'
                }
            
            # 胜率
            successful_trades = [t for t in trades if t['result'] == 'SUCCESS']
            win_rate = len(successful_trades) / len(trades) * 100 if trades else 0
            
            # 爆款率（5日涨幅>10%）
            alpha_trades = [t for t in trades if t.get('max_return_5d', 0) > 10]
            alpha_rate = len(alpha_trades) / len(trades) * 100 if trades else 0
            
            # 总收益率
            total_return = (current_capital / initial_capital - 1) * 100
            
            # 获取基准指数收益（使用中证1000指数）
            # 注意：基准收益计算需要从market_index_daily表获取，当前简化处理
            benchmark_return = 0
            
            return {
                'success': True,
                'trades': trades,
                'equity_curve': equity_curve,
                'metrics': {
                    'total_trades': len(trades),
                    'win_rate': round(win_rate, 2),
                    'alpha_rate': round(alpha_rate, 2),
                    'total_return': round(total_return, 2),
                    'max_drawdown': round(max_drawdown, 2),
                    'benchmark_return': round(benchmark_return, 2),
                    'final_capital': round(current_capital, 2)
                }
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
        """模拟卖出（持有5个交易日）"""
        try:
            # 使用SQLAlchemy engine替代原始连接，避免pandas警告
            query = text("""
                SELECT trade_date, open_price, close_price, high_price, low_price
                FROM sheep_daily USE INDEX (idx_sheep_code_date)
                WHERE sheep_code = :sheep_code
                  AND trade_date > :buy_date
                  AND trade_date <= :sell_date
                ORDER BY trade_date ASC
                LIMIT 5
            """)
            df = pd.read_sql(
                query, 
                engine, 
                params={'sheep_code': sheep_code, 'buy_date': buy_date, 'sell_date': sell_date}
            )
            
            if df.empty:
                return {
                    'exit_price': buy_price,
                    'return_pct': 0,
                    'max_return_5d': 0,
                    'result': 'FAIL'
                }
            
            # 计算期间最高价（最大涨幅）
            max_price = df['high_price'].max()
            max_return_5d = (max_price / buy_price - 1) * 100
            
            # 检查是否触发止损
            min_price = df['low_price'].min()
            if min_price <= stop_loss:
                exit_price = stop_loss
                return_pct = (exit_price / buy_price - 1) * 100
                result = 'FAIL'
            else:
                # 使用第5日收盘价卖出
                exit_price = float(df.iloc[-1]['close_price'])
                return_pct = (exit_price / buy_price - 1) * 100
                
                # 判定成功：最大涨幅>10% 且 最终盈利
                if max_return_5d > 10 and return_pct > 0:
                    result = 'SUCCESS'
                else:
                    result = 'FAIL'
            
            return {
                'exit_price': exit_price,
                'return_pct': round(return_pct, 2),
                'max_return_5d': round(max_return_5d, 2),
                'result': result
            }
        except Exception as e:
            logger.error(f"模拟卖出失败: sheep_code={sheep_code}, buy_date={buy_date}, sell_date={sell_date}, 错误={e}", exc_info=True)
            return {
                'exit_price': buy_price,
                'return_pct': 0,
                'max_return_5d': 0,
                'result': 'FAIL'
            }
