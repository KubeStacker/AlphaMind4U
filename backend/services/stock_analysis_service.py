"""
肥羊深度分析服务 - 走势预判 + 形态识别 + 止盈止损
专注于A股市场，尤其是科创板和创业板

核心功能：
1. 走势预判：基于多因子模型预测3日、5日、10日走势
2. 形态识别：趋势翻转、顶部翻转、震荡洗盘、快速拉升等
3. 止盈止损：基于ATR和支撑阻力位计算
4. 综合评估：给出操作建议和风险评级
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import date, timedelta
import logging
from db.database import get_raw_connection, get_db
from sqlalchemy import text
import warnings

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)


class StockAnalysisService:
    """
    肥羊深度分析服务
    
    基于量化因子进行走势预判和形态识别
    """
    
    # ============================================
    # 预测模型参数
    # ============================================
    
    # 走势预判权重配置
    PREDICTION_WEIGHTS = {
        'momentum': 0.25,        # 动量因子
        'trend': 0.20,           # 趋势因子
        'volume': 0.15,          # 量能因子
        'money_flow': 0.20,      # 资金流因子
        'technical': 0.20,       # 技术指标因子
    }
    
    # 形态识别参数
    PATTERN_PARAMS = {
        # 趋势翻转
        'trend_reversal_threshold': 0.7,      # RSRS阈值
        'volume_expansion_ratio': 1.5,        # 放量倍数
        
        # 顶部翻转
        'top_reversal_ma_deviation': 0.08,    # 偏离均线幅度(8%)
        'top_reversal_vol_shrink': 0.7,       # 缩量比例
        
        # 震荡洗盘
        'consolidation_range': 0.05,          # 震荡幅度(5%)
        'consolidation_days': 5,              # 最少震荡天数
        
        # 快速拉升
        'rapid_rise_threshold': 0.05,         # 单日涨幅阈值(5%)
        'rapid_rise_days': 3,                 # 连续天数
    }
    
    # ATR止损参数
    ATR_PARAMS = {
        'period': 14,                # ATR计算周期
        'stop_loss_multiplier': 2.0, # 止损ATR倍数
        'take_profit_multiplier': 3.0, # 止盈ATR倍数
    }
    
    @classmethod
    def analyze_stock(cls, sheep_code: str, trade_date: date = None) -> Dict:
        """
        综合分析肥羊
        
        返回：
        - 走势预判（3日/5日/10日）
        - 形态识别
        - 止盈止损位
        - 操作建议
        """
        if trade_date is None:
            trade_date = date.today()
        
        try:
            # 获取历史数据
            stock_data = cls._get_stock_data(sheep_code, trade_date, days=120)
            
            if stock_data.empty:
                return {
                    'success': False,
                    'message': f'无法获取肥羊 {sheep_code} 的历史数据'
                }
            
            # 获取资金流数据
            money_flow = cls._get_money_flow(sheep_code, trade_date, days=30)
            
            # 1. 计算技术因子
            factors = cls._calculate_factors(stock_data, money_flow)
            
            # 2. 走势预判
            predictions = cls._predict_trend(stock_data, factors)
            
            # 3. 形态识别
            pattern = cls._identify_pattern(stock_data, factors)
            
            # 4. 止盈止损计算
            stop_levels = cls._calculate_stop_levels(stock_data, factors)
            
            # 5. 综合评估
            assessment = cls._generate_assessment(
                predictions, pattern, stop_levels, factors
            )
            
            return {
                'success': True,
                'sheep_code': sheep_code,
                'trade_date': trade_date.isoformat(),
                'current_price': float(stock_data.iloc[-1]['close_price']),
                'predictions': predictions,
                'pattern': pattern,
                'stop_levels': stop_levels,
                'factors': factors,
                'assessment': assessment
            }
            
        except Exception as e:
            logger.error(f"肥羊分析失败 {sheep_code}: {e}", exc_info=True)
            return {
                'success': False,
                'message': f'分析失败: {str(e)}'
            }
    
    @classmethod
    def _get_stock_data(cls, sheep_code: str, trade_date: date, days: int = 120) -> pd.DataFrame:
        """获取肥羊历史数据"""
        try:
            with get_raw_connection() as conn:
                query = """
                    SELECT 
                        trade_date,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        amount,
                        turnover_rate,
                        change_pct,
                        ma5, ma10, ma20, ma30, ma60
                    FROM sheep_daily
                    WHERE sheep_code = %s
                      AND trade_date <= %s
                    ORDER BY trade_date DESC
                    LIMIT %s
                """
                df = pd.read_sql(query, conn, params=[sheep_code, trade_date, days])
                
                if df.empty:
                    return pd.DataFrame()
                
                # 按日期升序排列
                df = df.sort_values('trade_date').reset_index(drop=True)
                return df
                
        except Exception as e:
            logger.error(f"获取肥羊数据失败: {e}")
            return pd.DataFrame()
    
    @classmethod
    def _get_money_flow(cls, sheep_code: str, trade_date: date, days: int = 30) -> pd.DataFrame:
        """获取资金流向数据"""
        try:
            with get_raw_connection() as conn:
                query = """
                    SELECT 
                        trade_date,
                        main_net_inflow,
                        super_large_inflow,
                        large_inflow,
                        medium_inflow,
                        small_inflow
                    FROM sheep_money_flow
                    WHERE sheep_code = %s
                      AND trade_date <= %s
                    ORDER BY trade_date DESC
                    LIMIT %s
                """
                df = pd.read_sql(query, conn, params=[sheep_code, trade_date, days])
                
                if df.empty:
                    return pd.DataFrame()
                
                df = df.sort_values('trade_date').reset_index(drop=True)
                return df
                
        except Exception as e:
            logger.error(f"获取资金流数据失败: {e}")
            return pd.DataFrame()
    
    @classmethod
    def _calculate_factors(cls, stock_data: pd.DataFrame, money_flow: pd.DataFrame) -> Dict:
        """计算技术因子"""
        if stock_data.empty or len(stock_data) < 20:
            return {}
        
        factors = {}
        
        try:
            # 当前价格和成交量
            current_price = float(stock_data.iloc[-1]['close_price'])
            current_volume = float(stock_data.iloc[-1]['volume'])
            
            # ========== 动量因子 ==========
            # 1. RSI (6日)
            factors['rsi_6'] = cls._calculate_rsi(stock_data['close_price'], 6)
            
            # 2. RSI (14日)
            factors['rsi_14'] = cls._calculate_rsi(stock_data['close_price'], 14)
            
            # 3. 价格动量 (5日涨幅)
            if len(stock_data) >= 5:
                price_5d_ago = float(stock_data.iloc[-5]['close_price'])
                factors['momentum_5d'] = ((current_price / price_5d_ago) - 1) * 100
            else:
                factors['momentum_5d'] = 0.0
            
            # 4. 价格动量 (10日涨幅)
            if len(stock_data) >= 10:
                price_10d_ago = float(stock_data.iloc[-10]['close_price'])
                factors['momentum_10d'] = ((current_price / price_10d_ago) - 1) * 100
            else:
                factors['momentum_10d'] = 0.0
            
            # 5. 价格动量 (20日涨幅)
            if len(stock_data) >= 20:
                price_20d_ago = float(stock_data.iloc[-20]['close_price'])
                factors['momentum_20d'] = ((current_price / price_20d_ago) - 1) * 100
            else:
                factors['momentum_20d'] = 0.0
            
            # ========== 趋势因子 ==========
            # 1. 均线排列评分
            factors['ma_alignment'] = cls._calculate_ma_alignment(stock_data)
            
            # 2. 均线偏离度 (乖离率)
            ma20 = stock_data.iloc[-1].get('ma20')
            if ma20 and ma20 > 0:
                factors['bias_20'] = ((current_price / float(ma20)) - 1) * 100
            else:
                factors['bias_20'] = 0.0
            
            # 3. MACD (简化版)
            factors['macd_signal'] = cls._calculate_macd_signal(stock_data['close_price'])
            
            # 4. 趋势强度 (ADX简化版)
            factors['trend_strength'] = cls._calculate_trend_strength(stock_data)
            
            # ========== 量能因子 ==========
            # 1. 量比 (相对5日均量)
            if len(stock_data) >= 5:
                vol_ma5 = stock_data['volume'].tail(5).mean()
                factors['volume_ratio'] = current_volume / vol_ma5 if vol_ma5 > 0 else 1.0
            else:
                factors['volume_ratio'] = 1.0
            
            # 2. 量比 (相对20日均量)
            if len(stock_data) >= 20:
                vol_ma20 = stock_data['volume'].tail(20).mean()
                factors['volume_ratio_20'] = current_volume / vol_ma20 if vol_ma20 > 0 else 1.0
            else:
                factors['volume_ratio_20'] = 1.0
            
            # 3. 量能趋势 (5日量能变化)
            if len(stock_data) >= 10:
                vol_5d_avg = stock_data['volume'].tail(5).mean()
                vol_prev_5d_avg = stock_data['volume'].iloc[-10:-5].mean()
                factors['volume_trend'] = (vol_5d_avg / vol_prev_5d_avg - 1) * 100 if vol_prev_5d_avg > 0 else 0.0
            else:
                factors['volume_trend'] = 0.0
            
            # 4. OBV斜率 (能量潮)
            factors['obv_slope'] = cls._calculate_obv_slope(stock_data)
            
            # ========== 资金流因子 ==========
            if not money_flow.empty:
                # 1. 近5日主力净流入
                if len(money_flow) >= 5:
                    factors['main_inflow_5d'] = money_flow['main_net_inflow'].tail(5).sum() / 10000  # 转换为万元
                else:
                    factors['main_inflow_5d'] = money_flow['main_net_inflow'].sum() / 10000
                
                # 2. 近10日主力净流入
                if len(money_flow) >= 10:
                    factors['main_inflow_10d'] = money_flow['main_net_inflow'].tail(10).sum() / 10000
                else:
                    factors['main_inflow_10d'] = money_flow['main_net_inflow'].sum() / 10000
                
                # 3. 主力资金趋势 (近5日 vs 前5日)
                if len(money_flow) >= 10:
                    recent_5d = money_flow['main_net_inflow'].tail(5).sum()
                    prev_5d = money_flow['main_net_inflow'].iloc[-10:-5].sum()
                    factors['main_inflow_trend'] = recent_5d - prev_5d
                else:
                    factors['main_inflow_trend'] = 0.0
                
                # 4. 大单净流入占比
                if len(money_flow) >= 5:
                    total_large = money_flow['super_large_inflow'].tail(5).sum() + money_flow['large_inflow'].tail(5).sum()
                    total_small = money_flow['medium_inflow'].tail(5).sum() + money_flow['small_inflow'].tail(5).sum()
                    factors['large_order_ratio'] = total_large / (abs(total_large) + abs(total_small) + 1) * 100
                else:
                    factors['large_order_ratio'] = 0.0
            else:
                factors['main_inflow_5d'] = 0.0
                factors['main_inflow_10d'] = 0.0
                factors['main_inflow_trend'] = 0.0
                factors['large_order_ratio'] = 0.0
            
            # ========== 技术指标因子 ==========
            # 1. ATR (真实波幅)
            factors['atr'] = cls._calculate_atr(stock_data)
            
            # 2. ATR百分比 (波动率)
            factors['atr_pct'] = (factors['atr'] / current_price) * 100
            
            # 3. 布林带位置
            factors['bollinger_position'] = cls._calculate_bollinger_position(stock_data)
            
            # 4. 连续涨跌天数
            factors['consecutive_days'] = cls._calculate_consecutive_days(stock_data)
            
            # 5. K线形态评分
            factors['candle_score'] = cls._calculate_candle_score(stock_data)
            
            # 6. 上影线比例 (当日)
            high = float(stock_data.iloc[-1]['high_price'])
            low = float(stock_data.iloc[-1]['low_price'])
            close = float(stock_data.iloc[-1]['close_price'])
            price_range = high - low
            if price_range > 0:
                factors['upper_shadow_ratio'] = (high - close) / price_range
            else:
                factors['upper_shadow_ratio'] = 0.0
            
            # 7. 下影线比例 (当日)
            open_price = float(stock_data.iloc[-1]['open_price'])
            if price_range > 0:
                factors['lower_shadow_ratio'] = (min(open_price, close) - low) / price_range
            else:
                factors['lower_shadow_ratio'] = 0.0
            
        except Exception as e:
            logger.error(f"计算技术因子失败: {e}", exc_info=True)
        
        return factors
    
    @classmethod
    def _predict_trend(cls, stock_data: pd.DataFrame, factors: Dict) -> Dict:
        """
        走势预判 - 预测3日、5日、10日走势
        
        基于多因子加权模型进行预测
        """
        predictions = {
            '3d': {'direction': '持平', 'probability': 50, 'expected_change': 0.0, 'risk_level': '中'},
            '5d': {'direction': '持平', 'probability': 50, 'expected_change': 0.0, 'risk_level': '中'},
            '10d': {'direction': '持平', 'probability': 50, 'expected_change': 0.0, 'risk_level': '中'},
        }
        
        if not factors:
            return predictions
        
        try:
            # ========== 3日预测 (短线) ==========
            score_3d = cls._calculate_short_term_score(factors)
            pred_3d = cls._score_to_prediction(score_3d, 'short')
            predictions['3d'] = pred_3d
            
            # ========== 5日预测 (中短线) ==========
            score_5d = cls._calculate_mid_term_score(factors)
            pred_5d = cls._score_to_prediction(score_5d, 'mid')
            predictions['5d'] = pred_5d
            
            # ========== 10日预测 (中线) ==========
            score_10d = cls._calculate_long_term_score(factors)
            pred_10d = cls._score_to_prediction(score_10d, 'long')
            predictions['10d'] = pred_10d
            
            # 添加预测理由
            predictions['3d']['reasons'] = cls._generate_prediction_reasons(factors, 'short')
            predictions['5d']['reasons'] = cls._generate_prediction_reasons(factors, 'mid')
            predictions['10d']['reasons'] = cls._generate_prediction_reasons(factors, 'long')
            
        except Exception as e:
            logger.error(f"走势预判失败: {e}", exc_info=True)
        
        return predictions
    
    @classmethod
    def _calculate_short_term_score(cls, factors: Dict) -> float:
        """计算短期评分 (3日)"""
        score = 50.0  # 基准分
        
        # RSI因子 (超买超卖)
        rsi_6 = factors.get('rsi_6', 50)
        if rsi_6 > 80:
            score -= 15  # 严重超买
        elif rsi_6 > 70:
            score -= 8
        elif rsi_6 < 20:
            score += 15  # 严重超卖
        elif rsi_6 < 30:
            score += 8
        
        # 量能因子 (短期更看重量能)
        vol_ratio = factors.get('volume_ratio', 1.0)
        if vol_ratio > 2.0:
            score += 10  # 放量
        elif vol_ratio > 1.5:
            score += 5
        elif vol_ratio < 0.5:
            score -= 8  # 缩量
        
        # K线形态
        candle_score = factors.get('candle_score', 0)
        score += candle_score * 0.5
        
        # 上影线风险
        upper_shadow = factors.get('upper_shadow_ratio', 0)
        if upper_shadow > 0.4:
            score -= 12  # 长上影线
        
        # 连续涨跌
        consecutive_days = factors.get('consecutive_days', 0)
        if consecutive_days >= 5:
            score -= 10  # 连涨5天，短期回调风险
        elif consecutive_days >= 3:
            score -= 5
        elif consecutive_days <= -3:
            score += 8  # 连跌3天，短期反弹
        
        # 主力资金 (近5日)
        main_inflow_5d = factors.get('main_inflow_5d', 0)
        if main_inflow_5d > 500:  # 500万以上流入
            score += 8
        elif main_inflow_5d > 0:
            score += 4
        elif main_inflow_5d < -500:  # 500万以上流出
            score -= 8
        
        return max(0, min(100, score))
    
    @classmethod
    def _calculate_mid_term_score(cls, factors: Dict) -> float:
        """计算中期评分 (5日)"""
        score = 50.0
        
        # 动量因子
        momentum_5d = factors.get('momentum_5d', 0)
        if momentum_5d > 10:
            score += 10
        elif momentum_5d > 5:
            score += 6
        elif momentum_5d < -10:
            score -= 10
        elif momentum_5d < -5:
            score -= 6
        
        # 趋势强度
        trend_strength = factors.get('trend_strength', 50)
        if trend_strength > 70:
            score += 8
        elif trend_strength > 50:
            score += 4
        elif trend_strength < 30:
            score -= 6
        
        # 均线排列
        ma_alignment = factors.get('ma_alignment', 0)
        score += ma_alignment * 0.3
        
        # 乖离率
        bias_20 = factors.get('bias_20', 0)
        if bias_20 > 15:
            score -= 10  # 偏离过高
        elif bias_20 > 10:
            score -= 5
        elif bias_20 < -10:
            score += 8  # 超跌
        elif bias_20 < -5:
            score += 4
        
        # 主力资金趋势
        main_trend = factors.get('main_inflow_trend', 0)
        if main_trend > 200:  # 200万以上增加
            score += 8
        elif main_trend > 0:
            score += 4
        elif main_trend < -200:
            score -= 8
        
        # MACD信号
        macd_signal = factors.get('macd_signal', 0)
        score += macd_signal * 5
        
        return max(0, min(100, score))
    
    @classmethod
    def _calculate_long_term_score(cls, factors: Dict) -> float:
        """计算长期评分 (10日)"""
        score = 50.0
        
        # 20日动量
        momentum_20d = factors.get('momentum_20d', 0)
        if momentum_20d > 20:
            score += 12
        elif momentum_20d > 10:
            score += 8
        elif momentum_20d < -20:
            score -= 12
        elif momentum_20d < -10:
            score -= 8
        
        # 均线系统
        ma_alignment = factors.get('ma_alignment', 0)
        score += ma_alignment * 0.4
        
        # 10日主力资金
        main_inflow_10d = factors.get('main_inflow_10d', 0)
        if main_inflow_10d > 1000:  # 1000万以上
            score += 12
        elif main_inflow_10d > 500:
            score += 8
        elif main_inflow_10d > 0:
            score += 4
        elif main_inflow_10d < -1000:
            score -= 12
        elif main_inflow_10d < -500:
            score -= 8
        
        # 大单占比
        large_order = factors.get('large_order_ratio', 0)
        if large_order > 30:
            score += 8
        elif large_order > 0:
            score += 4
        elif large_order < -30:
            score -= 8
        
        # OBV斜率
        obv_slope = factors.get('obv_slope', 0)
        if obv_slope > 0.5:
            score += 6
        elif obv_slope > 0:
            score += 3
        elif obv_slope < -0.5:
            score -= 6
        
        # 波动率风险
        atr_pct = factors.get('atr_pct', 0)
        if atr_pct > 8:
            score -= 5  # 高波动风险
        
        return max(0, min(100, score))
    
    @classmethod
    def _score_to_prediction(cls, score: float, term: str) -> Dict:
        """将评分转换为预测结果"""
        # 根据周期调整预期涨幅倍数
        term_multiplier = {
            'short': 1.0,   # 3日
            'mid': 1.5,     # 5日
            'long': 2.5,    # 10日
        }
        
        multiplier = term_multiplier.get(term, 1.0)
        
        if score >= 75:
            direction = '看涨'
            probability = min(85, 50 + (score - 50) * 0.7)
            expected_change = (score - 50) * 0.15 * multiplier
            risk_level = '低'
        elif score >= 60:
            direction = '偏多'
            probability = min(70, 50 + (score - 50) * 0.6)
            expected_change = (score - 50) * 0.12 * multiplier
            risk_level = '中低'
        elif score >= 45:
            direction = '持平'
            probability = 50 + abs(score - 50) * 0.3
            expected_change = (score - 50) * 0.08 * multiplier
            risk_level = '中'
        elif score >= 30:
            direction = '偏空'
            probability = min(70, 50 + (50 - score) * 0.6)
            expected_change = (score - 50) * 0.12 * multiplier
            risk_level = '中高'
        else:
            direction = '看跌'
            probability = min(85, 50 + (50 - score) * 0.7)
            expected_change = (score - 50) * 0.15 * multiplier
            risk_level = '高'
        
        return {
            'direction': direction,
            'probability': round(probability, 1),
            'expected_change': round(expected_change, 2),
            'risk_level': risk_level,
            'score': round(score, 1)
        }
    
    @classmethod
    def _generate_prediction_reasons(cls, factors: Dict, term: str) -> List[str]:
        """
        生成预测理由 - 针对不同周期生成差异化分析
        
        短期(3日): 关注RSI、K线形态、量能、连续涨跌等短期技术信号
        中期(5日): 关注趋势强度、MACD、均线排列、乖离率、资金趋势
        长期(10日): 关注长期动量、均线系统、累计资金、大单占比、OBV趋势
        """
        reasons = []
        
        if term == 'short':
            # ========== 短期(3日)：关注超短线技术信号 ==========
            
            # 1. RSI超买超卖 (短期最敏感)
            rsi_6 = factors.get('rsi_6', 50)
            if rsi_6 > 80:
                reasons.append(f'【超买警告】RSI(6)={rsi_6:.1f}严重超买，3日内回调概率极高')
            elif rsi_6 > 70:
                reasons.append(f'【超买】RSI(6)={rsi_6:.1f}进入超买区，短线有调整压力')
            elif rsi_6 < 20:
                reasons.append(f'【超卖机会】RSI(6)={rsi_6:.1f}严重超卖，3日反弹概率大')
            elif rsi_6 < 30:
                reasons.append(f'【超卖】RSI(6)={rsi_6:.1f}进入超卖区，关注反弹信号')
            elif 45 <= rsi_6 <= 55:
                reasons.append(f'RSI(6)={rsi_6:.1f}处于中性区间，短线方向不明')
            
            # 2. K线形态 (上影线、下影线)
            upper_shadow = factors.get('upper_shadow_ratio', 0)
            lower_shadow = factors.get('lower_shadow_ratio', 0)
            candle_score = factors.get('candle_score', 0)
            
            if upper_shadow > 0.5:
                reasons.append(f'【冲高回落】上影线占比{upper_shadow:.0%}，上方抛压沉重')
            elif upper_shadow > 0.35:
                reasons.append(f'上影线较长({upper_shadow:.0%})，短期上攻动能减弱')
            
            if lower_shadow > 0.4:
                reasons.append(f'【下影线支撑】下影线占比{lower_shadow:.0%}，下方有买盘承接')
            
            if candle_score > 20:
                reasons.append('K线形态强势（大阳线/光头阳），短线做多信号')
            elif candle_score < -20:
                reasons.append('K线形态弱势（大阴线/光脚阴），短线做空信号')
            
            # 3. 连续涨跌天数 (短期最重要)
            consecutive = factors.get('consecutive_days', 0)
            if consecutive >= 5:
                reasons.append(f'【回调警告】连涨{consecutive}天，获利盘积累，短线回调压力大')
            elif consecutive >= 3:
                reasons.append(f'连涨{consecutive}天，短期有技术性回调需求')
            elif consecutive <= -4:
                reasons.append(f'【超跌反弹】连跌{abs(consecutive)}天，技术性反弹随时可能出现')
            elif consecutive <= -2:
                reasons.append(f'连跌{abs(consecutive)}天，短线关注止跌企稳信号')
            
            # 4. 短期量能 (当日量比)
            vol_ratio = factors.get('volume_ratio', 1.0)
            if vol_ratio > 3.0:
                reasons.append(f'【异动放量】量比{vol_ratio:.1f}倍，短期波动加剧，注意方向选择')
            elif vol_ratio > 2.0:
                reasons.append(f'量比{vol_ratio:.1f}倍放量，短线活跃度提升')
            elif vol_ratio < 0.5:
                reasons.append(f'量比{vol_ratio:.2f}缩量，短线缺乏交易热情')
            
            # 5. 近5日主力资金
            main_inflow_5d = factors.get('main_inflow_5d', 0)
            if main_inflow_5d > 800:
                reasons.append(f'【资金加仓】近5日主力净流入{main_inflow_5d:.0f}万，短期做多意愿强')
            elif main_inflow_5d > 300:
                reasons.append(f'近5日主力流入{main_inflow_5d:.0f}万，资金小幅进场')
            elif main_inflow_5d < -800:
                reasons.append(f'【资金出逃】近5日主力净流出{abs(main_inflow_5d):.0f}万，短期承压')
            elif main_inflow_5d < -300:
                reasons.append(f'近5日主力流出{abs(main_inflow_5d):.0f}万，资金有撤离迹象')
            
            # 6. 布林带位置 (短期超买超卖)
            bollinger = factors.get('bollinger_position', 50)
            if bollinger > 90:
                reasons.append(f'股价位于布林带上轨附近({bollinger:.0f}%)，短期超买')
            elif bollinger < 10:
                reasons.append(f'股价位于布林带下轨附近({bollinger:.0f}%)，短期超卖')
            
        elif term == 'mid':
            # ========== 中期(5日)：关注趋势和MACD ==========
            
            # 1. 5日动量
            momentum_5d = factors.get('momentum_5d', 0)
            if momentum_5d > 15:
                reasons.append(f'【强势上涨】5日涨幅{momentum_5d:.1f}%，中短期动能强劲')
            elif momentum_5d > 8:
                reasons.append(f'5日涨幅{momentum_5d:.1f}%，上涨动量较好')
            elif momentum_5d < -15:
                reasons.append(f'【急跌】5日跌幅{abs(momentum_5d):.1f}%，中短期弱势明显')
            elif momentum_5d < -8:
                reasons.append(f'5日跌幅{abs(momentum_5d):.1f}%，下跌动量增强')
            else:
                reasons.append(f'5日涨跌幅{momentum_5d:.1f}%，短期震荡整理')
            
            # 2. MACD信号 (中期趋势最重要)
            macd_signal = factors.get('macd_signal', 0)
            if macd_signal >= 2:
                reasons.append('【金叉信号】MACD金叉出现，中期趋势转多')
            elif macd_signal >= 1:
                reasons.append('MACD多头运行，柱状图扩大，中期向好')
            elif macd_signal <= -2:
                reasons.append('【死叉信号】MACD死叉出现，中期趋势转空')
            elif macd_signal <= -1:
                reasons.append('MACD空头运行，柱状图扩大，中期偏弱')
            elif macd_signal > 0:
                reasons.append('MACD多头但动能减弱，关注是否形成顶背离')
            elif macd_signal < 0:
                reasons.append('MACD空头但动能减弱，关注是否形成底背离')
            
            # 3. 趋势强度 (ADX)
            trend_strength = factors.get('trend_strength', 50)
            if trend_strength > 70:
                reasons.append(f'趋势强度{trend_strength:.0f}，当前趋势明确且强劲')
            elif trend_strength > 50:
                reasons.append(f'趋势强度{trend_strength:.0f}，趋势正在形成中')
            elif trend_strength < 30:
                reasons.append(f'趋势强度{trend_strength:.0f}，市场处于震荡无趋势状态')
            
            # 4. 均线排列
            ma_alignment = factors.get('ma_alignment', 0)
            if ma_alignment > 50:
                reasons.append('【多头排列】均线呈多头排列(MA5>MA10>MA20)，中期趋势向上')
            elif ma_alignment > 20:
                reasons.append('均线偏多排列，中期趋势偏强')
            elif ma_alignment < -50:
                reasons.append('【空头排列】均线呈空头排列，中期趋势向下')
            elif ma_alignment < -20:
                reasons.append('均线偏空排列，中期趋势偏弱')
            
            # 5. 乖离率 (中期回归压力)
            bias_20 = factors.get('bias_20', 0)
            if bias_20 and bias_20 > 12:
                reasons.append(f'【偏离过高】偏离MA20达{bias_20:.1f}%，存在向均线回归压力')
            elif bias_20 and bias_20 > 8:
                reasons.append(f'偏离MA20为{bias_20:.1f}%，注意追高风险')
            elif bias_20 and bias_20 < -12:
                reasons.append(f'【超跌机会】偏离MA20达{bias_20:.1f}%，存在超跌反弹机会')
            elif bias_20 and bias_20 < -8:
                reasons.append(f'偏离MA20为{bias_20:.1f}%，关注反弹时机')
            
            # 6. 主力资金趋势 (资金方向变化)
            main_trend = factors.get('main_inflow_trend', 0)
            if main_trend > 500:
                reasons.append(f'【资金加速】主力资金较前期增加{main_trend:.0f}万，看多情绪升温')
            elif main_trend > 200:
                reasons.append(f'主力资金趋势向好，较前期增加{main_trend:.0f}万')
            elif main_trend < -500:
                reasons.append(f'【资金撤退】主力资金较前期减少{abs(main_trend):.0f}万，需警惕')
            elif main_trend < -200:
                reasons.append(f'主力资金趋势转弱，较前期减少{abs(main_trend):.0f}万')
            
            # 7. RSI(14)中期超买超卖
            rsi_14 = factors.get('rsi_14', 50)
            if rsi_14 > 75:
                reasons.append(f'RSI(14)={rsi_14:.1f}中期超买，5日内有回调风险')
            elif rsi_14 < 25:
                reasons.append(f'RSI(14)={rsi_14:.1f}中期超卖，关注反弹机会')
            
        else:  # long (10日)
            # ========== 长期(10日)：关注趋势和资金累计 ==========
            
            # 1. 20日动量 (长期动能)
            momentum_20d = factors.get('momentum_20d', 0)
            if momentum_20d > 25:
                reasons.append(f'【强势趋势】20日涨幅{momentum_20d:.1f}%，中期上升趋势确立')
            elif momentum_20d > 15:
                reasons.append(f'20日涨幅{momentum_20d:.1f}%，中期动能充沛')
            elif momentum_20d > 5:
                reasons.append(f'20日涨幅{momentum_20d:.1f}%，中期稳步上行')
            elif momentum_20d < -25:
                reasons.append(f'【弱势趋势】20日跌幅{abs(momentum_20d):.1f}%，中期下降趋势明确')
            elif momentum_20d < -15:
                reasons.append(f'20日跌幅{abs(momentum_20d):.1f}%，中期弱势运行')
            elif momentum_20d < -5:
                reasons.append(f'20日跌幅{abs(momentum_20d):.1f}%，中期承压')
            else:
                reasons.append(f'20日涨跌幅{momentum_20d:.1f}%，中期震荡格局')
            
            # 2. 10日动量对比5日动量 (趋势加速/减速)
            momentum_10d = factors.get('momentum_10d', 0)
            momentum_5d = factors.get('momentum_5d', 0)
            if momentum_10d > 0 and momentum_5d > momentum_10d * 0.6:
                reasons.append(f'上涨趋势加速中（5日{momentum_5d:.1f}% vs 10日{momentum_10d:.1f}%）')
            elif momentum_10d > 0 and momentum_5d < momentum_10d * 0.3:
                reasons.append(f'上涨动能减弱（5日{momentum_5d:.1f}% vs 10日{momentum_10d:.1f}%），注意趋势反转')
            elif momentum_10d < 0 and momentum_5d < momentum_10d * 0.6:
                reasons.append(f'下跌趋势加速中（5日{momentum_5d:.1f}% vs 10日{momentum_10d:.1f}%）')
            elif momentum_10d < 0 and momentum_5d > momentum_10d * 0.3:
                reasons.append(f'下跌动能减弱（5日{momentum_5d:.1f}% vs 10日{momentum_10d:.1f}%），关注企稳')
            
            # 3. 均线系统 (长期趋势)
            ma_alignment = factors.get('ma_alignment', 0)
            if ma_alignment > 60:
                reasons.append('【完美多头】均线系统完美多头排列，10日内趋势大概率延续')
            elif ma_alignment > 30:
                reasons.append('均线多头排列，中期趋势向好')
            elif ma_alignment < -60:
                reasons.append('【完美空头】均线系统空头排列，10日内下行压力持续')
            elif ma_alignment < -30:
                reasons.append('均线空头排列，中期趋势偏弱')
            else:
                reasons.append('均线交织，中期方向待定')
            
            # 4. 10日累计主力资金 (最重要)
            main_inflow_10d = factors.get('main_inflow_10d', 0)
            if main_inflow_10d > 2000:
                reasons.append(f'【主力深度介入】10日累计净流入{main_inflow_10d:.0f}万，机构看好')
            elif main_inflow_10d > 1000:
                reasons.append(f'【主力建仓】10日累计净流入{main_inflow_10d:.0f}万，资金持续进场')
            elif main_inflow_10d > 500:
                reasons.append(f'10日累计净流入{main_inflow_10d:.0f}万，资金面偏多')
            elif main_inflow_10d < -2000:
                reasons.append(f'【主力撤离】10日累计净流出{abs(main_inflow_10d):.0f}万，机构大规模减仓')
            elif main_inflow_10d < -1000:
                reasons.append(f'【资金外流】10日累计净流出{abs(main_inflow_10d):.0f}万，资金持续撤离')
            elif main_inflow_10d < -500:
                reasons.append(f'10日累计净流出{abs(main_inflow_10d):.0f}万，资金面偏空')
            
            # 5. 大单占比 (机构行为)
            large_order = factors.get('large_order_ratio', 0)
            if large_order > 40:
                reasons.append(f'大单净流入占比{large_order:.0f}%，机构主导行情')
            elif large_order > 20:
                reasons.append(f'大单净流入占比{large_order:.0f}%，机构有参与')
            elif large_order < -40:
                reasons.append(f'大单净流出占比{abs(large_order):.0f}%，机构在减仓')
            elif large_order < -20:
                reasons.append(f'大单净流出占比{abs(large_order):.0f}%，机构有撤退迹象')
            
            # 6. OBV趋势 (量价配合)
            obv_slope = factors.get('obv_slope', 0)
            if obv_slope > 0.8:
                reasons.append('OBV上升斜率较大，量价配合良好，10日趋势健康')
            elif obv_slope > 0.3:
                reasons.append('OBV温和上升，量价正相关')
            elif obv_slope < -0.8:
                reasons.append('OBV下降明显，量价背离，10日趋势堪忧')
            elif obv_slope < -0.3:
                reasons.append('OBV走弱，量能支撑不足')
            
            # 7. 波动率风险
            atr_pct = factors.get('atr_pct', 0)
            if atr_pct > 8:
                reasons.append(f'【高波动】ATR波动率{atr_pct:.1f}%，10日内波动风险较大')
            elif atr_pct > 5:
                reasons.append(f'波动率{atr_pct:.1f}%适中，有一定交易空间')
            elif atr_pct < 2:
                reasons.append(f'波动率{atr_pct:.1f}%偏低，可能即将选择方向')
        
        # 确保至少有一条理由
        if not reasons:
            if term == 'short':
                reasons.append('短期技术指标中性，建议观望或轻仓试探')
            elif term == 'mid':
                reasons.append('中短期趋势不明朗，等待明确信号')
            else:
                reasons.append('中期技术面震荡，建议耐心等待趋势确立')
        
        return reasons[:6]  # 最多返回6条理由
    
    @classmethod
    def _identify_pattern(cls, stock_data: pd.DataFrame, factors: Dict) -> Dict:
        """
        形态识别 - 识别技术形态
        
        形态类型：
        1. 趋势翻转 (底部反转)
        2. 顶部翻转 (顶部反转)
        3. 震荡洗盘
        4. 快速拉升
        5. 缩量整理
        6. 放量突破
        """
        pattern = {
            'type': '无明显形态',
            'confidence': 0,
            'description': '',
            'signals': [],
            'operation_hint': ''
        }
        
        if stock_data.empty or len(stock_data) < 20:
            return pattern
        
        try:
            # 获取最近数据
            recent_20 = stock_data.tail(20)
            recent_10 = stock_data.tail(10)
            recent_5 = stock_data.tail(5)
            
            current_price = float(stock_data.iloc[-1]['close_price'])
            current_vol = float(stock_data.iloc[-1]['volume'])
            vol_ma20 = recent_20['volume'].mean()
            
            # 计算均线
            ma5 = recent_5['close_price'].mean()
            ma10 = recent_10['close_price'].mean()
            ma20 = recent_20['close_price'].mean()
            
            # 价格区间
            high_20 = recent_20['high_price'].max()
            low_20 = recent_20['low_price'].min()
            price_range = (high_20 - low_20) / low_20
            
            patterns_detected = []
            
            # ========== 1. 检测底部反转 (趋势翻转) ==========
            bottom_score = 0
            bottom_signals = []
            
            # 条件1: 近期跌幅较大
            momentum_10d = factors.get('momentum_10d', 0)
            if momentum_10d < -10:
                bottom_score += 25
                bottom_signals.append(f'近10日跌幅{momentum_10d:.1f}%')
            
            # 条件2: RSI超卖
            rsi_6 = factors.get('rsi_6', 50)
            if rsi_6 < 30:
                bottom_score += 25
                bottom_signals.append(f'RSI(6)={rsi_6:.1f}超卖')
            
            # 条件3: 放量阳线
            change_pct = float(stock_data.iloc[-1].get('change_pct', 0))
            vol_ratio = factors.get('volume_ratio', 1.0)
            if change_pct > 3 and vol_ratio > 1.5:
                bottom_score += 30
                bottom_signals.append(f'放量{vol_ratio:.1f}倍阳线涨{change_pct:.1f}%')
            
            # 条件4: 价格接近20日低点
            if (current_price - low_20) / low_20 < 0.05:
                bottom_score += 20
                bottom_signals.append('价格接近20日低点')
            
            if bottom_score >= 50:
                patterns_detected.append({
                    'type': '底部反转',
                    'confidence': min(90, bottom_score),
                    'signals': bottom_signals,
                    'description': '股价经过一段下跌后出现企稳反弹信号，可能形成底部反转',
                    'operation_hint': '可考虑逢低分批建仓，设好止损位'
                })
            
            # ========== 2. 检测顶部反转 ==========
            top_score = 0
            top_signals = []
            
            # 条件1: 近期涨幅较大
            if momentum_10d > 15:
                top_score += 25
                top_signals.append(f'近10日涨幅{momentum_10d:.1f}%')
            
            # 条件2: RSI超买
            if rsi_6 > 70:
                top_score += 25
                top_signals.append(f'RSI(6)={rsi_6:.1f}超买')
            
            # 条件3: 长上影线
            upper_shadow = factors.get('upper_shadow_ratio', 0)
            if upper_shadow > 0.4:
                top_score += 25
                top_signals.append(f'上影线比例{upper_shadow:.1%}')
            
            # 条件4: 价格接近20日高点
            if (high_20 - current_price) / high_20 < 0.03:
                top_score += 15
                top_signals.append('价格接近20日高点')
            
            # 条件5: 高位放量滞涨
            if vol_ratio > 2 and abs(change_pct) < 2 and momentum_10d > 10:
                top_score += 25
                top_signals.append('高位放量滞涨')
            
            if top_score >= 50:
                patterns_detected.append({
                    'type': '顶部翻转',
                    'confidence': min(90, top_score),
                    'signals': top_signals,
                    'description': '股价经过一段上涨后出现见顶信号，短期可能回调',
                    'operation_hint': '建议逢高减仓或离场观望'
                })
            
            # ========== 3. 检测震荡洗盘 ==========
            consolidation_score = 0
            consolidation_signals = []
            
            # 条件1: 近期涨跌幅在5%以内
            if abs(momentum_10d) < 5:
                consolidation_score += 30
                consolidation_signals.append(f'10日波动{momentum_10d:.1f}%')
            
            # 条件2: 价格在均线附近震荡
            ma_deviation = abs(current_price - ma20) / ma20
            if ma_deviation < 0.03:
                consolidation_score += 25
                consolidation_signals.append('价格贴近20日均线')
            
            # 条件3: 成交量萎缩
            if vol_ratio < 0.8:
                consolidation_score += 25
                consolidation_signals.append(f'量比{vol_ratio:.2f}缩量')
            
            # 条件4: 主力资金小幅流入
            main_inflow = factors.get('main_inflow_5d', 0)
            if 0 < main_inflow < 200:
                consolidation_score += 20
                consolidation_signals.append('主力资金小幅流入')
            
            if consolidation_score >= 50:
                patterns_detected.append({
                    'type': '震荡洗盘',
                    'confidence': min(90, consolidation_score),
                    'signals': consolidation_signals,
                    'description': '股价在一定区间内窄幅震荡，成交量萎缩，可能是主力洗盘吸筹',
                    'operation_hint': '持股者可继续持有，新进者等待放量突破信号'
                })
            
            # ========== 4. 检测快速拉升 ==========
            rapid_rise_score = 0
            rapid_rise_signals = []
            
            # 条件1: 单日涨幅大
            if change_pct > 5:
                rapid_rise_score += 30
                rapid_rise_signals.append(f'单日涨幅{change_pct:.1f}%')
            
            # 条件2: 连续上涨
            consecutive = factors.get('consecutive_days', 0)
            if consecutive >= 3:
                rapid_rise_score += 25
                rapid_rise_signals.append(f'连涨{consecutive}天')
            
            # 条件3: 放量
            if vol_ratio > 2:
                rapid_rise_score += 25
                rapid_rise_signals.append(f'量比{vol_ratio:.1f}倍放量')
            
            # 条件4: 突破20日高点
            if current_price > high_20 * 0.98:
                rapid_rise_score += 20
                rapid_rise_signals.append('突破20日高点')
            
            if rapid_rise_score >= 50:
                patterns_detected.append({
                    'type': '快速拉升',
                    'confidence': min(90, rapid_rise_score),
                    'signals': rapid_rise_signals,
                    'description': '股价快速拉升，成交量放大，短期动能强劲',
                    'operation_hint': '持股者可享受主升浪，但需设置移动止盈；新进者谨慎追高'
                })
            
            # ========== 5. 检测放量突破 ==========
            breakout_score = 0
            breakout_signals = []
            
            # 条件1: 突破20日高点
            prev_high_20 = recent_20.iloc[:-1]['high_price'].max()
            if current_price > prev_high_20:
                breakout_score += 30
                breakout_signals.append('突破前期高点')
            
            # 条件2: 放量
            if vol_ratio > 1.8:
                breakout_score += 30
                breakout_signals.append(f'量比{vol_ratio:.1f}倍')
            
            # 条件3: 阳线实体饱满
            open_price = float(stock_data.iloc[-1]['open_price'])
            if change_pct > 3 and (current_price - open_price) / open_price > 0.025:
                breakout_score += 25
                breakout_signals.append('阳线实体饱满')
            
            # 条件4: 站上均线
            if current_price > ma5 > ma10 > ma20:
                breakout_score += 15
                breakout_signals.append('均线多头排列')
            
            if breakout_score >= 50:
                patterns_detected.append({
                    'type': '放量突破',
                    'confidence': min(90, breakout_score),
                    'signals': breakout_signals,
                    'description': '股价放量突破前期压力位，可能开启新一轮上涨行情',
                    'operation_hint': '可考虑跟进，注意突破后的回踩确认'
                })
            
            # 选择置信度最高的形态
            if patterns_detected:
                patterns_detected.sort(key=lambda x: x['confidence'], reverse=True)
                pattern = patterns_detected[0]
                pattern['all_patterns'] = [p['type'] for p in patterns_detected]
            
        except Exception as e:
            logger.error(f"形态识别失败: {e}", exc_info=True)
        
        return pattern
    
    @classmethod
    def _calculate_stop_levels(cls, stock_data: pd.DataFrame, factors: Dict) -> Dict:
        """
        计算止盈止损位
        
        方法：
        1. ATR止损：基于波动率
        2. 支撑阻力止损：基于历史高低点
        3. 均线止损：基于均线支撑
        """
        if stock_data.empty:
            return {}
        
        try:
            current_price = float(stock_data.iloc[-1]['close_price'])
            atr = factors.get('atr', current_price * 0.03)
            
            # ========== 止损位计算 ==========
            # 方法1: ATR止损 (2倍ATR)
            atr_stop_loss = current_price - atr * cls.ATR_PARAMS['stop_loss_multiplier']
            
            # 方法2: 近期低点止损
            recent_low = stock_data.tail(10)['low_price'].min()
            support_stop_loss = float(recent_low) * 0.99  # 破位后再留1%缓冲
            
            # 方法3: 均线止损 (MA20)
            ma20 = stock_data.iloc[-1].get('ma20')
            ma_stop_loss = float(ma20) * 0.98 if ma20 and ma20 > 0 else current_price * 0.93
            
            # 综合止损位 (取最高的止损位，更保守)
            stop_loss_price = max(atr_stop_loss, support_stop_loss, ma_stop_loss)
            stop_loss_price = min(stop_loss_price, current_price * 0.95)  # 最大止损5%
            stop_loss_pct = (stop_loss_price / current_price - 1) * 100
            
            # ========== 止盈位计算 ==========
            # 方法1: ATR止盈 (3倍ATR)
            atr_take_profit = current_price + atr * cls.ATR_PARAMS['take_profit_multiplier']
            
            # 方法2: 近期高点止盈
            recent_high = stock_data.tail(20)['high_price'].max()
            resistance_take_profit = float(recent_high) * 1.02
            
            # 方法3: 风险收益比止盈 (1:2)
            risk = current_price - stop_loss_price
            rr_take_profit = current_price + risk * 2
            
            # 综合止盈位
            # 第一止盈位 (保守)
            take_profit_1 = min(atr_take_profit, resistance_take_profit, rr_take_profit)
            take_profit_1_pct = (take_profit_1 / current_price - 1) * 100
            
            # 第二止盈位 (激进)
            take_profit_2 = current_price + risk * 3
            take_profit_2_pct = (take_profit_2 / current_price - 1) * 100
            
            # 计算盈亏比
            risk_reward_ratio = abs(take_profit_1 - current_price) / abs(current_price - stop_loss_price) if stop_loss_price < current_price else 0
            
            return {
                'stop_loss': {
                    'price': round(stop_loss_price, 2),
                    'percentage': round(stop_loss_pct, 2),
                    'method': '综合止损(ATR+支撑位+均线)',
                    'details': {
                        'atr_stop': round(atr_stop_loss, 2),
                        'support_stop': round(support_stop_loss, 2),
                        'ma_stop': round(ma_stop_loss, 2)
                    }
                },
                'take_profit_1': {
                    'price': round(take_profit_1, 2),
                    'percentage': round(take_profit_1_pct, 2),
                    'description': '第一止盈位(建议减半仓)'
                },
                'take_profit_2': {
                    'price': round(take_profit_2, 2),
                    'percentage': round(take_profit_2_pct, 2),
                    'description': '第二止盈位(建议清仓)'
                },
                'risk_reward_ratio': round(risk_reward_ratio, 2),
                'atr': round(atr, 2),
                'atr_pct': round((atr / current_price) * 100, 2)
            }
            
        except Exception as e:
            logger.error(f"计算止盈止损失败: {e}", exc_info=True)
            return {}
    
    @classmethod
    def _generate_assessment(cls, predictions: Dict, pattern: Dict, 
                            stop_levels: Dict, factors: Dict) -> Dict:
        """
        生成综合评估和操作建议
        """
        assessment = {
            'overall_rating': '中性',
            'risk_level': '中',
            'operation_advice': [],
            'key_points': [],
            'attention_items': []
        }
        
        try:
            # 综合评分
            scores = []
            for period in ['3d', '5d', '10d']:
                if period in predictions and 'score' in predictions[period]:
                    scores.append(predictions[period]['score'])
            
            avg_score = sum(scores) / len(scores) if scores else 50
            
            # 确定整体评级
            if avg_score >= 70:
                assessment['overall_rating'] = '强烈看好'
                assessment['risk_level'] = '低'
            elif avg_score >= 60:
                assessment['overall_rating'] = '看好'
                assessment['risk_level'] = '中低'
            elif avg_score >= 45:
                assessment['overall_rating'] = '中性'
                assessment['risk_level'] = '中'
            elif avg_score >= 35:
                assessment['overall_rating'] = '看淡'
                assessment['risk_level'] = '中高'
            else:
                assessment['overall_rating'] = '强烈看淡'
                assessment['risk_level'] = '高'
            
            # 操作建议
            if avg_score >= 65:
                if pattern.get('type') == '底部反转':
                    assessment['operation_advice'].append('建议：底部反转信号明确，可考虑分批建仓')
                elif pattern.get('type') == '放量突破':
                    assessment['operation_advice'].append('建议：放量突破形态良好，可跟进做多')
                else:
                    assessment['operation_advice'].append('建议：技术面偏多，可考虑逢低布局')
            elif avg_score >= 50:
                assessment['operation_advice'].append('建议：持股观望，等待更明确的方向信号')
            elif avg_score >= 40:
                assessment['operation_advice'].append('建议：谨慎持有，注意控制仓位')
            else:
                if pattern.get('type') == '顶部翻转':
                    assessment['operation_advice'].append('建议：顶部翻转信号出现，建议逢高减仓')
                else:
                    assessment['operation_advice'].append('建议：技术面偏空，建议减仓或观望')
            
            # 止损建议
            if stop_levels.get('stop_loss'):
                sl = stop_levels['stop_loss']
                assessment['operation_advice'].append(
                    f"止损参考：{sl['price']}元（{sl['percentage']:.1f}%）"
                )
            
            # 止盈建议
            if stop_levels.get('take_profit_1'):
                tp1 = stop_levels['take_profit_1']
                assessment['operation_advice'].append(
                    f"第一止盈：{tp1['price']}元（{tp1['percentage']:.1f}%）"
                )
            
            # 关键点位
            assessment['key_points'].append(f"当前评分：{avg_score:.0f}/100")
            
            if factors.get('ma20'):
                assessment['key_points'].append(f"20日均线支撑：{factors.get('ma20', 0):.2f}")
            
            rr_ratio = stop_levels.get('risk_reward_ratio', 0)
            if rr_ratio > 0:
                assessment['key_points'].append(f"盈亏比：1:{rr_ratio:.1f}")
            
            # 注意事项
            rsi = factors.get('rsi_6', 50)
            if rsi > 80:
                assessment['attention_items'].append('⚠️ RSI超买，短期有回调风险')
            elif rsi < 20:
                assessment['attention_items'].append('💡 RSI超卖，关注反弹机会')
            
            vol_ratio = factors.get('volume_ratio', 1.0)
            if vol_ratio > 3:
                assessment['attention_items'].append('⚠️ 成交量异常放大，注意追高风险')
            elif vol_ratio < 0.5:
                assessment['attention_items'].append('📉 成交萎缩，市场关注度低')
            
            upper_shadow = factors.get('upper_shadow_ratio', 0)
            if upper_shadow > 0.4:
                assessment['attention_items'].append('⚠️ 出现长上影线，上方抛压较重')
            
            consecutive = factors.get('consecutive_days', 0)
            if consecutive >= 5:
                assessment['attention_items'].append('⚠️ 连续上涨天数过多，注意调整风险')
            elif consecutive <= -4:
                assessment['attention_items'].append('💡 连续下跌，关注超跌反弹')
            
            main_inflow = factors.get('main_inflow_5d', 0)
            if main_inflow > 1000:
                assessment['attention_items'].append('✅ 主力资金持续流入，做多氛围浓厚')
            elif main_inflow < -1000:
                assessment['attention_items'].append('⚠️ 主力资金大幅流出，谨慎操作')
            
        except Exception as e:
            logger.error(f"生成评估失败: {e}", exc_info=True)
        
        return assessment
    
    # ========== 辅助计算函数 ==========
    
    @staticmethod
    def _calculate_rsi(prices: pd.Series, period: int = 14) -> float:
        """计算RSI"""
        try:
            if len(prices) < period + 1:
                return 50.0
            
            delta = prices.diff()
            gain = delta.clip(lower=0)
            loss = (-delta.clip(upper=0))
            
            avg_gain = gain.rolling(window=period).mean().iloc[-1]
            avg_loss = loss.rolling(window=period).mean().iloc[-1]
            
            if avg_loss == 0:
                return 100.0 if avg_gain > 0 else 50.0
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            return float(rsi)
        except:
            return 50.0
    
    @staticmethod
    def _calculate_ma_alignment(stock_data: pd.DataFrame) -> float:
        """
        计算均线排列评分
        多头排列返回正分，空头排列返回负分
        """
        try:
            current = stock_data.iloc[-1]
            ma5 = float(current.get('ma5', 0))
            ma10 = float(current.get('ma10', 0))
            ma20 = float(current.get('ma20', 0))
            ma60 = float(current.get('ma60', 0))
            close = float(current['close_price'])
            
            if not all([ma5, ma10, ma20]):
                return 0.0
            
            score = 0.0
            
            # 完美多头排列: close > ma5 > ma10 > ma20 > ma60
            if close > ma5:
                score += 20
            if ma5 > ma10:
                score += 20
            if ma10 > ma20:
                score += 20
            if ma60 and ma20 > ma60:
                score += 15
            
            # 完美空头排列: close < ma5 < ma10 < ma20 < ma60
            if close < ma5:
                score -= 20
            if ma5 < ma10:
                score -= 20
            if ma10 < ma20:
                score -= 20
            if ma60 and ma20 < ma60:
                score -= 15
            
            return score
        except:
            return 0.0
    
    @staticmethod
    def _calculate_macd_signal(prices: pd.Series) -> float:
        """计算MACD信号 (简化版)"""
        try:
            if len(prices) < 26:
                return 0.0
            
            exp1 = prices.ewm(span=12, adjust=False).mean()
            exp2 = prices.ewm(span=26, adjust=False).mean()
            macd = exp1 - exp2
            signal = macd.ewm(span=9, adjust=False).mean()
            
            hist = macd.iloc[-1] - signal.iloc[-1]
            prev_hist = macd.iloc[-2] - signal.iloc[-2]
            
            # 金叉/死叉信号
            if hist > 0 and prev_hist <= 0:
                return 2.0  # 金叉
            elif hist < 0 and prev_hist >= 0:
                return -2.0  # 死叉
            elif hist > 0:
                return 1.0 if hist > prev_hist else 0.5
            else:
                return -1.0 if hist < prev_hist else -0.5
        except:
            return 0.0
    
    @staticmethod
    def _calculate_trend_strength(stock_data: pd.DataFrame) -> float:
        """计算趋势强度 (ADX简化版)"""
        try:
            if len(stock_data) < 14:
                return 50.0
            
            recent = stock_data.tail(14)
            
            # 计算方向变化
            up_moves = []
            down_moves = []
            
            for i in range(1, len(recent)):
                high_diff = float(recent.iloc[i]['high_price']) - float(recent.iloc[i-1]['high_price'])
                low_diff = float(recent.iloc[i-1]['low_price']) - float(recent.iloc[i]['low_price'])
                
                if high_diff > low_diff and high_diff > 0:
                    up_moves.append(high_diff)
                    down_moves.append(0)
                elif low_diff > high_diff and low_diff > 0:
                    up_moves.append(0)
                    down_moves.append(low_diff)
                else:
                    up_moves.append(0)
                    down_moves.append(0)
            
            # 计算趋势强度
            avg_up = np.mean(up_moves)
            avg_down = np.mean(down_moves)
            total = avg_up + avg_down
            
            if total > 0:
                dx = abs(avg_up - avg_down) / total * 100
                return min(100, dx)
            return 50.0
        except:
            return 50.0
    
    @staticmethod
    def _calculate_atr(stock_data: pd.DataFrame, period: int = 14) -> float:
        """计算ATR (真实波幅)"""
        try:
            if len(stock_data) < period:
                recent = stock_data
            else:
                recent = stock_data.tail(period)
            
            tr_list = []
            for i in range(len(recent)):
                high = float(recent.iloc[i]['high_price'])
                low = float(recent.iloc[i]['low_price'])
                
                if i > 0:
                    prev_close = float(recent.iloc[i-1]['close_price'])
                    tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                else:
                    tr = high - low
                
                tr_list.append(tr)
            
            return np.mean(tr_list)
        except:
            return 0.0
    
    @staticmethod
    def _calculate_obv_slope(stock_data: pd.DataFrame) -> float:
        """计算OBV斜率"""
        try:
            if len(stock_data) < 10:
                return 0.0
            
            recent = stock_data.tail(10)
            obv = 0
            obv_list = []
            
            for i in range(len(recent)):
                if i > 0:
                    if float(recent.iloc[i]['close_price']) > float(recent.iloc[i-1]['close_price']):
                        obv += float(recent.iloc[i]['volume'])
                    elif float(recent.iloc[i]['close_price']) < float(recent.iloc[i-1]['close_price']):
                        obv -= float(recent.iloc[i]['volume'])
                obv_list.append(obv)
            
            # 计算OBV的斜率 (归一化)
            if len(obv_list) >= 2:
                obv_array = np.array(obv_list)
                x = np.arange(len(obv_array))
                slope = np.polyfit(x, obv_array, 1)[0]
                # 归一化
                return slope / (np.std(obv_array) + 1) 
            return 0.0
        except:
            return 0.0
    
    @staticmethod
    def _calculate_bollinger_position(stock_data: pd.DataFrame) -> float:
        """计算布林带位置 (0-100)"""
        try:
            if len(stock_data) < 20:
                return 50.0
            
            recent = stock_data.tail(20)
            close = recent['close_price']
            
            middle = close.mean()
            std = close.std()
            upper = middle + 2 * std
            lower = middle - 2 * std
            
            current_price = float(stock_data.iloc[-1]['close_price'])
            
            if upper == lower:
                return 50.0
            
            position = (current_price - lower) / (upper - lower) * 100
            return max(0, min(100, position))
        except:
            return 50.0
    
    @staticmethod
    def _calculate_consecutive_days(stock_data: pd.DataFrame) -> int:
        """计算连续涨跌天数"""
        try:
            if len(stock_data) < 2:
                return 0
            
            consecutive = 0
            for i in range(len(stock_data) - 1, 0, -1):
                change = float(stock_data.iloc[i].get('change_pct', 0))
                
                if consecutive == 0:
                    consecutive = 1 if change > 0 else -1
                elif consecutive > 0 and change > 0:
                    consecutive += 1
                elif consecutive < 0 and change < 0:
                    consecutive -= 1
                else:
                    break
            
            return consecutive
        except:
            return 0
    
    @staticmethod
    def _calculate_candle_score(stock_data: pd.DataFrame) -> float:
        """计算K线形态评分"""
        try:
            if stock_data.empty:
                return 0.0
            
            current = stock_data.iloc[-1]
            open_price = float(current['open_price'])
            close_price = float(current['close_price'])
            high_price = float(current['high_price'])
            low_price = float(current['low_price'])
            
            price_range = high_price - low_price
            if price_range == 0:
                return 0.0
            
            body = abs(close_price - open_price)
            body_ratio = body / price_range
            
            is_bullish = close_price > open_price
            
            # 上影线和下影线
            if is_bullish:
                upper_shadow = high_price - close_price
                lower_shadow = open_price - low_price
            else:
                upper_shadow = high_price - open_price
                lower_shadow = close_price - low_price
            
            upper_ratio = upper_shadow / price_range
            lower_ratio = lower_shadow / price_range
            
            score = 0.0
            
            # 阳线加分
            if is_bullish:
                score += 10
                if body_ratio > 0.7:  # 大阳线
                    score += 15
                if lower_ratio > 0.3:  # 下影线长（承接力强）
                    score += 10
            else:
                score -= 10
                if body_ratio > 0.7:  # 大阴线
                    score -= 15
            
            # 上影线扣分
            if upper_ratio > 0.4:
                score -= 15
            
            return score
        except:
            return 0.0
