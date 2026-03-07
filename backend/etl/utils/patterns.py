"""
股票技术形态识别器 — 兼容层

底层调用 kline_patterns 向量化引擎，对外保持 PatternRecognizer /
get_professional_commentary 接口不变，供 admin.py 等模块复用。
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

from etl.utils.kline_patterns import (
    DEFAULT_CALIBRATION_PATH,
    detect_all_patterns,
    get_latest_signals,
    load_pattern_calibration,
    PATTERN_CN_MAP,
    BULLISH_PATTERNS,
    BEARISH_PATTERNS,
)

logger = logging.getLogger(__name__)
_PATTERN_CALIBRATION_CACHE: dict | None = None
_PATTERN_CALIBRATION_MTIME: float | None = None


def _get_pattern_calibration() -> dict:
    global _PATTERN_CALIBRATION_CACHE, _PATTERN_CALIBRATION_MTIME

    target = Path(DEFAULT_CALIBRATION_PATH)
    current_mtime = target.stat().st_mtime if target.exists() else None
    should_reload = (
        _PATTERN_CALIBRATION_CACHE is None
        or current_mtime != _PATTERN_CALIBRATION_MTIME
    )

    if should_reload:
        _PATTERN_CALIBRATION_CACHE = load_pattern_calibration()
        _PATTERN_CALIBRATION_MTIME = current_mtime
    return _PATTERN_CALIBRATION_CACHE or {}


class PatternRecognizer:
    """
    股票技术形态识别器（兼容旧接口）

    用法:
        recognizer = PatternRecognizer(df)
        patterns = recognizer.recognize()  # -> ["红三兵", "仙人指路", ...]
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy() if df is not None else pd.DataFrame()
        self.signals = []

        if len(self.df) < 5:
            return

        # 统一 volume 列名
        if 'volume' not in self.df.columns and 'vol' in self.df.columns:
            self.df = self.df.rename(columns={'vol': 'volume'})

        # 确保基本均线存在
        for ma in [5, 10, 20, 60]:
            col = f'ma{ma}'
            if col not in self.df.columns:
                self.df[col] = self.df['close'].rolling(ma, min_periods=ma).mean()

    def recognize(self, min_confidence: float = 0.5) -> list:
        """
        识别最新一个交易日的形态，返回中文名列表。

        参数:
            min_confidence: 最低置信度阈值 (0.0~1.0)
        返回:
            ["红三兵", "仙人指路", ...]
        """
        if len(self.df) < 5:
            return []

        try:
            result_df = detect_all_patterns(self.df)
            self.signals = get_latest_signals(
                result_df,
                min_confidence=min_confidence,
                calibration=_get_pattern_calibration(),
            )
            return [s['pattern'] for s in self.signals]
        except Exception as e:
            logger.error(f"形态识别异常: {e}", exc_info=True)
            return []

    def recognize_detailed(self, min_confidence: float = 0.5) -> list:
        """
        返回详细信号列表 (含置信度和方向)。

        返回:
            [{"pattern": "红三兵", "code": "THREE_WHITE_SOLDIERS",
              "confidence": 1.0, "direction": "bullish"}, ...]
        """
        if len(self.df) < 5:
            return []

        try:
            result_df = detect_all_patterns(self.df)
            self.signals = get_latest_signals(
                result_df,
                min_confidence=min_confidence,
                calibration=_get_pattern_calibration(),
            )
            return self.signals
        except Exception as e:
            logger.error(f"形态识别异常: {e}", exc_info=True)
            return []


def get_professional_commentary(df: pd.DataFrame, patterns: list) -> str:
    """
    根据形态和最近行情，给出专业的点评分析（机构/游资视角）。
    返回: 简洁的汇总字符串 (兼容旧接口)
    """
    detail = get_professional_commentary_detailed(df, patterns)
    return detail.get("summary", "暂无明显信号，观望为主。")


def get_professional_commentary_detailed(df: pd.DataFrame, patterns: list) -> dict:
    """
    根据形态和最近行情，给出专业的点评分析（机构/游资视角）。
    返回详细的结构化数据，供前端展示。
    """
    if df is None or df.empty:
        return {
            "summary": "暂无数据分析",
            "institution": [],
            "hotmoney": [],
            "patterns": [],
            "risk_alert": []
        }

    last = df.iloc[-1]
    last_5 = df.tail(5)
    last_10 = df.tail(10)
    last_20 = df.tail(20)
    last_60 = df.tail(60) if len(df) >= 60 else df

    # 统一列名
    vol_col = 'volume' if 'volume' in df.columns else 'vol'
    pct_col = 'pct_chg' if 'pct_chg' in df.columns else None

    close = last['close']
    open_price = last.get('open', close)
    high = last.get('high', close)
    low = last.get('low', close)
    pct_today = last.get(pct_col, 0) if pct_col else 0
    vol_today = last[vol_col]

    # 均线数据
    ma5 = last.get('ma5', None)
    ma10 = last.get('ma10', None)
    ma20 = last.get('ma20', None)
    ma60 = last.get('ma60', None)

    # 量能分析
    vol_5_avg = last_5[vol_col].mean()
    vol_20_avg = last_20[vol_col].mean() if len(last_20) >= 20 else vol_5_avg
    vol_ratio_5 = (vol_today / vol_5_avg - 1) if vol_5_avg > 0 else 0
    vol_ratio_20 = (vol_today / vol_20_avg - 1) if vol_20_avg > 0 else 0

    # 涨跌幅统计
    pct_5_avg = last_5[pct_col].mean() if pct_col and len(last_5) >= 5 else 0
    pct_10_sum = last_10[pct_col].sum() if pct_col and len(last_10) >= 10 else 0
    pct_20_sum = last_20[pct_col].sum() if pct_col and len(last_20) >= 20 else 0

    # 振幅分析
    if 'high' in df.columns and 'low' in df.columns:
        amplitude_today = ((last['high'] - last['low']) / last['low'] * 100) if last['low'] > 0 else 0
        amplitude_5_avg = ((last_5['high'] - last_5['low']) / last_5['low'] * 100).mean() if len(last_5) >= 5 else 0
    else:
        amplitude_today = 0
        amplitude_5_avg = 0

    # 换手率（如果有）
    turnover = last.get('turnover_rate', None)

    # 融资数据（如果有）
    rzye = last.get('rzye', None)
    rzmre = last.get('rzmre', None)
    rzche = last.get('rzche', None)

    # 资金流向
    amount_today = last.get('amount', None)
    amount_5_avg = last_5['amount'].mean() if 'amount' in last_5.columns else None

    institution_view = []
    hotmoney_view = []
    pattern_view = []
    risk_alert = []

    # === 1. 机构视角分析 ===
    
    # 趋势结构（核心）
    if ma20 is not None and ma60 is not None and not (pd.isna(ma20) or pd.isna(ma60)):
        if close > ma20 > ma60 and ma20 > ma60 * 1.02:
            if ma5 is not None and ma10 is not None and close > ma5 > ma10:
                institution_view.append({
                    "type": "trend",
                    "level": "strong",
                    "title": "上升趋势确立",
                    "desc": "均线多头排列，短期中期趋势共振向上，机构资金配置窗口打开"
                })
            else:
                institution_view.append({
                    "type": "trend",
                    "level": "medium",
                    "title": "中期趋势向好",
                    "desc": "价格站上20/60日均线，中期上升趋势初步形成"
                })
        elif close < ma20 < ma60 and ma20 < ma60 * 0.98:
            institution_view.append({
                "type": "trend",
                "level": "bearish",
                "title": "下降趋势中",
                "desc": "均线空头排列，趋势未扭转，机构资金维持观望"
            })
        elif close > ma20 and ma20 < ma60:
            institution_view.append({
                "type": "trend",
                "level": "neutral",
                "title": "突破关键均线",
                "desc": "价格突破20日线，但中期均线仍在下行，需观察量能持续性"
            })
        elif close < ma20 and ma20 > ma60:
            institution_view.append({
                "type": "trend",
                "level": "warning",
                "title": "反弹遇阻",
                "desc": "价格跌破20日线，短期反弹结束，需警惕进一步回落"
            })
        else:
            institution_view.append({
                "type": "trend",
                "level": "neutral",
                "title": "横盘整理",
                "desc": "均线粘连缠绕，趋势方向不明，等待突破信号"
            })

    # 均线支撑/压力判断
    if ma5 is not None and ma10 is not None and ma20 is not None:
        if close > ma5 * 1.05:
            if close > ma20 * 1.05:
                institution_view.append({
                    "type": "support",
                    "level": "strong",
                    "title": "强势站上均线",
                    "desc": f"价格距20日均线{((close/ma20-1)*100):.1f}%，上涨动能充沛"
                })
            else:
                institution_view.append({
                    "type": "support",
                    "level": "medium",
                    "title": "依托5日均线上涨",
                    "desc": "短期走势健康，回调不破5日均线可继续持有"
                })
        elif close < ma5 * 0.95:
            institution_view.append({
                "type": "support",
                "level": "warning",
                "title": "跌破短期均线",
                "desc": "价格失守5日均线，短期走势转弱，支撑位下移至10日均线"
            })

    # 量能结构（机构关注持续性）
    if vol_ratio_20 > 1.0:
        institution_view.append({
            "type": "volume",
            "level": "strong",
            "title": "量能显著放大",
            "desc": f"成交量较20日均量放大{(vol_ratio_20*100):.0f}%，增量资金入场明显，关注持续性"
        })
    elif vol_ratio_20 > 0.3:
        institution_view.append({
            "type": "volume",
            "level": "medium",
            "title": "量能温和放大",
            "desc": "成交量稳步缓慢放大，增量资金入场，趋势可持续"
        })
    elif vol_ratio_20 < -0.4:
        institution_view.append({
            "type": "volume",
            "level": "weak",
            "title": "量能萎缩",
            "desc": "成交量持续萎缩，市场参与度不足，需等待催化剂"
        })

    # 量价配合分析
    if vol_ratio_5 > 0.5 and pct_today > 2:
        institution_view.append({
            "type": "volume_price",
            "level": "strong",
            "title": "价量齐升",
            "desc": "上涨伴随放量，量价配合健康，资金持续流入"
        })
    elif vol_ratio_5 < -0.3 and abs(pct_today) < 1:
        institution_view.append({
            "type": "volume_price",
            "level": "neutral",
            "title": "缩量横盘",
            "desc": "波动率降低，观望情绪浓厚，等待方向选择"
        })

    # 融资融券（机构杠杆资金）
    if rzye is not None and rzmre is not None and not pd.isna(rzye) and not pd.isna(rzmre):
        rz_ratio = rzmre / rzye if rzye > 0 else 0
        if rz_ratio > 0.03:
            institution_view.append({
                "type": "margin",
                "level": "strong",
                "title": "融资活跃",
                "desc": f"融资买入占比{rz_ratio*100:.1f}%，杠杆资金强烈看多，后市看涨"
            })
        elif rz_ratio > 0.015:
            institution_view.append({
                "type": "margin",
                "level": "medium",
                "title": "融资买入回升",
                "desc": "融资情绪回暖，杠杆资金参与度提升"
            })
        elif rz_ratio < 0.005:
            institution_view.append({
                "type": "margin",
                "level": "weak",
                "title": "融资观望",
                "desc": "融资买入低迷，杠杆资金谨慎观望"
            })
        
        # 融资余额变化
        if rzche is not None and not pd.isna(rzche):
            if rzche > 0:
                institution_view.append({
                    "type": "margin",
                    "level": "medium",
                    "title": "融资余额增加",
                    "desc": "融资偿还额小于买入额，杠杆资金净流入"
                })
            elif rzche < -rzye * 0.01:
                institution_view.append({
                    "type": "margin",
                    "level": "warning",
                    "title": "融资偿还加速",
                    "desc": "杠杆资金出现偿还压力，需警惕抛压"
                })

    # 波动率（机构风控）
    if amplitude_today > amplitude_5_avg * 1.5 and amplitude_today > 5:
        risk_alert.append({
            "type": "volatility",
            "level": "high",
            "title": "波动率上升",
            "desc": f"日内振幅{amplitude_today:.1f}%，较5日均值放大{(amplitude_today/amplitude_5_avg-1)*100:.0f}%，需控制仓位"
        })

    # 20日累计涨跌幅
    if pct_20_sum > 30:
        risk_alert.append({
            "type": "accumulate",
            "level": "medium",
            "title": "短期涨幅较大",
            "desc": f"20日累计上涨{pct_20_sum:.1f}%，注意短期回调风险"
        })
    elif pct_20_sum < -20:
        risk_alert.append({
            "type": "accumulate",
            "level": "low",
            "title": "短期超跌",
            "desc": f"20日累计下跌{abs(pct_20_sum):.1f}%，存在超跌反弹机会"
        })

    # === 2. 游资视角分析 ===
    
    # 短期爆发力
    if pct_today >= 9.5:
        hotmoney_view.append({
            "type": "momentum",
            "level": "extreme",
            "title": "涨停板",
            "desc": "强势涨停，情绪达到高潮，关注次日溢价和封板强度"
        })
    elif pct_today >= 7:
        hotmoney_view.append({
            "type": "momentum",
            "level": "strong",
            "title": "大涨突破",
            "desc": "涨幅超7%，短线资金抢筹明显，关注能否封板"
        })
    elif pct_today >= 5:
        hotmoney_view.append({
            "type": "momentum",
            "level": "medium",
            "title": "强势上涨",
            "desc": "大阳线突破，短线资金积极入场"
        })
    elif pct_today >= 3:
        hotmoney_view.append({
            "type": "momentum",
            "level": "light",
            "title": "日内强势",
            "desc": "走势强于大盘，有短线资金关注"
        })
    elif pct_today <= -9.5:
        hotmoney_view.append({
            "type": "momentum",
            "level": "extreme",
            "title": "跌停板",
            "desc": "恐慌性跌停，短线资金踩踏离场，短期回避"
        })
    elif pct_today <= -7:
        hotmoney_view.append({
            "type": "momentum",
            "level": "strong",
            "title": "大跌",
            "desc": "跌幅超7%，恐慌盘涌出，短线风险极大"
        })
    elif pct_today <= -5:
        hotmoney_view.append({
            "type": "momentum",
            "level": "medium",
            "title": "下跌调整",
            "desc": "大阴线杀跌，短线情绪转弱，观望为主"
        })

    # 换手率（游资最关注）
    if turnover is not None and not pd.isna(turnover):
        if turnover > 30:
            hotmoney_view.append({
                "type": "turnover",
                "level": "extreme",
                "title": "极高换手",
                "desc": f"换手率{turnover:.1f}%，筹码充分换手，注意游资对倒出货风险"
            })
        elif turnover > 20:
            hotmoney_view.append({
                "type": "turnover",
                "level": "strong",
                "title": "高换手",
                "desc": f"换手率{turnover:.1f}%，筹码高度活跃，游资接力积极"
            })
        elif turnover > 10:
            hotmoney_view.append({
                "type": "turnover",
                "level": "medium",
                "title": "活跃换手",
                "desc": f"换手率{turnover:.1f}%，资金进出活跃，适合短线操作"
            })
        elif turnover > 5:
            hotmoney_view.append({
                "type": "turnover",
                "level": "light",
                "title": "温和换手",
                "desc": f"换手率{turnover:.1f}%，流动性适中"
            })
        elif turnover < 2:
            hotmoney_view.append({
                "type": "turnover",
                "level": "weak",
                "title": "低换手",
                "desc": f"换手率{turnover:.1f}%，筹码锁定，缺乏流动性，观望"
            })

    # 量价配合（游资看爆发）
    if vol_ratio_5 > 2.0 and pct_today > 5:
        hotmoney_view.append({
            "type": "volume_price",
            "level": "strong",
            "title": "放量启动",
            "desc": "放量大涨，游资抢筹信号，可能进入主升浪"
        })
    elif vol_ratio_5 > 1.5 and pct_today > 3:
        hotmoney_view.append({
            "type": "volume_price",
            "level": "medium",
            "title": "量价齐升",
            "desc": "量价配合良好，短线保持强势"
        })
    elif vol_ratio_5 > 1.0 and pct_today < -3:
        hotmoney_view.append({
            "type": "volume_price",
            "level": "warning",
            "title": "放量下跌",
            "desc": "放量下跌，游资出逃，短线风险加大"
        })
    elif vol_ratio_5 > 0.5 and pct_today < -5:
        hotmoney_view.append({
            "type": "volume_price",
            "level": "danger",
            "title": "恐慌抛售",
            "desc": "放量暴跌，恐慌盘涌出，短期回避"
        })
    elif vol_ratio_5 < -0.3 and abs(pct_today) < 1.5:
        hotmoney_view.append({
            "type": "volume_price",
            "level": "neutral",
            "title": "缩量横盘",
            "desc": "缩量盘整，游资观望，等待方向选择"
        })

    # 连板高度（如果10日累计涨幅大）
    if pct_10_sum > 40:
        hotmoney_view.append({
            "type": "continue_rise",
            "level": "danger",
            "title": "高位风险",
            "desc": f"10日累计涨幅{pct_10_sum:.0f}%，高位分歧加大，注意补跌风险"
        })
    elif pct_10_sum > 30:
        hotmoney_view.append({
            "type": "continue_rise",
            "level": "warning",
            "title": "注意分歧",
            "desc": f"10日累计涨幅{pct_10_sum:.0f}%，获利盘丰厚，注意短线回调"
        })
    elif pct_10_sum > 20:
        hotmoney_view.append({
            "type": "continue_rise",
            "level": "medium",
            "title": "已有涨幅",
            "desc": f"10日累计涨幅{pct_10_sum:.0f}%，短线可逐步兑现收益"
        })

    # 日内分型（上下影线）
    if high > low:
        up_shadow = (high - max(close, open_price)) / (high - low) * 100 if high > low else 0
        down_shadow = (min(close, open_price) - low) / (high - low) * 100 if high > low else 0
        
        if up_shadow > 60 and pct_today > 2:
            hotmoney_view.append({
                "type": "candle",
                "level": "warning",
                "title": "长上影",
                "desc": f"上影线占比{up_shadow:.0f}%，上方抛压重，警惕冲高回落"
            })
        elif down_shadow > 60 and pct_today < -2:
            hotmoney_view.append({
                "type": "candle",
                "level": "medium",
                "title": "长下影",
                "desc": f"下影线占比{down_shadow:.0f}%，有资金抄底，关注反弹力度"
            })

    # === 3. 形态信号 ===
    if patterns:
        p_set = set(patterns)

        # 强势形态
        if p_set & {'老鸭头'}:
            pattern_view.append({
                "type": "bullish",
                "level": "strong",
                "pattern": "老鸭头",
                "desc": "主力洗盘结束，控盘度高，经典拉升形态，后市看涨"
            })
        if p_set & {'仙人指路'}:
            pattern_view.append({
                "type": "bullish",
                "level": "medium",
                "pattern": "仙人指路",
                "desc": "上影线试盘，主力试探上方抛压，后市看涨"
            })
        if p_set & {'红三兵'}:
            pattern_view.append({
                "type": "bullish",
                "level": "strong",
                "pattern": "红三兵",
                "desc": "多头稳步推进，底部逐步抬升，趋势向好"
            })
        if p_set & {'曙光初现'}:
            pattern_view.append({
                "type": "bullish",
                "level": "medium",
                "pattern": "曙光初现",
                "desc": "空头衰竭后资金回补，属于经典止跌转强信号"
            })
        if p_set & {'放量突破'}:
            pattern_view.append({
                "type": "bullish",
                "level": "strong",
                "pattern": "放量突破",
                "desc": "突破关键压力位，量能配合，有效性强"
            })
        if p_set & {'量价齐升'}:
            pattern_view.append({
                "type": "bullish",
                "level": "medium",
                "pattern": "量价齐升",
                "desc": "资金持续流入，健康上涨态势"
            })
        if p_set & {'出水芙蓉'}:
            pattern_view.append({
                "type": "bullish",
                "level": "strong",
                "pattern": "出水芙蓉",
                "desc": "一阳穿多线，短期转强信号"
            })
        if p_set & {'多方炮'}:
            pattern_view.append({
                "type": "bullish",
                "level": "medium",
                "pattern": "多方炮",
                "desc": "两阳夹一阴，多头进攻形态，后市看涨"
            })

        # 反转形态
        if p_set & {'锤子线', '启明星', '早晨之星', '曙光初现', '看涨吞没'}:
            pattern_view.append({
                "type": "reversal_bull",
                "level": "medium",
                "pattern": "底部反转",
                "desc": "出现底部反转信号，空头动能衰竭，多头开始反攻"
            })
        if p_set & {'三只乌鸦', '黄昏星', '顶分型', '乌云盖顶', '看跌吞没'}:
            pattern_view.append({
                "type": "reversal_bear",
                "level": "medium",
                "pattern": "顶部反转",
                "desc": "出现顶部反转信号，多头力竭，注意减仓"
            })
        if p_set & {'射击之星', '吊颈线'}:
            pattern_view.append({
                "type": "reversal_bear",
                "level": "light",
                "pattern": "射击之星",
                "desc": "上影线较长，警惕短期回调"
            })
        if p_set & {'乌云盖顶'}:
            pattern_view.append({
                "type": "warning",
                "level": "warning",
                "pattern": "乌云盖顶",
                "desc": "强势区被空头反压，若次日继续走弱宜减仓控制风险"
            })

        # 警示形态
        if p_set & {'量价背离'}:
            risk_alert.append({
                "type": "warning",
                "level": "medium",
                "title": "量价背离",
                "desc": "价格上涨但成交量萎缩，主力可能借势出货"
            })
            pattern_view.append({
                "type": "warning",
                "level": "warning",
                "pattern": "量价背离",
                "desc": "价涨量缩，需警惕量价配合失效"
            })
        if p_set & {'地量地价'}:
            pattern_view.append({
                "type": "special",
                "level": "neutral",
                "pattern": "地量地价",
                "desc": "成交极度萎缩，变盘窗口临近"
            })
        if p_set & {'天量天价'}:
            risk_alert.append({
                "type": "warning",
                "level": "high",
                "title": "天量天价",
                "desc": "成交量创近期新高，需警惕短期头部"
            })
            pattern_view.append({
                "type": "warning",
                "level": "warning",
                "pattern": "天量天价",
                "desc": "放量滞涨信号，注意风险"
            })

    # === 组装最终汇总 ===
    summary_parts = []
    
    # 机构视角取最重要的1-2条
    if institution_view:
        top_institution = sorted(institution_view, key=lambda x: {'strong': 0, 'medium': 1, 'light': 2, 'neutral': 3, 'weak': 4, 'warning': 5, 'bearish': 6}.get(x.get('level', 'neutral'), 3))
        summary_parts.append(f"【机构】{top_institution[0]['title']}")
        if len(top_institution) > 1 and top_institution[1].get('level') in ['strong', 'medium', 'warning']:
            summary_parts.append(f"机构关注：{top_institution[1]['title']}")
    
    # 游资视角取最重要的1-2条
    if hotmoney_view:
        level_order = {'extreme': 0, 'strong': 1, 'medium': 2, 'light': 3, 'neutral': 4, 'weak': 5, 'warning': 6, 'danger': 7}
        top_hotmoney = sorted(hotmoney_view, key=lambda x: level_order.get(x.get('level', 'neutral'), 4))
        summary_parts.append(f"【游资】{top_hotmoney[0]['title']}")
        if len(top_hotmoney) > 1 and top_hotmoney[1].get('level') in ['extreme', 'strong', 'medium', 'warning', 'danger']:
            summary_parts.append(f"游资动向：{top_hotmoney[1]['title']}")
    
    # 形态信号取1条
    if pattern_view:
        top_pattern = pattern_view[0]
        summary_parts.append(f"【形态】{top_pattern['pattern']}: {top_pattern['desc'][:10]}")

    if not summary_parts:
        summary_parts.append("暂无明显信号，观望为主")

    return {
        "summary": " | ".join(summary_parts),
        "institution": institution_view,
        "hotmoney": hotmoney_view,
        "patterns": pattern_view,
        "risk_alert": risk_alert,
        "technical": {
            "close": close,
            "pct_today": pct_today,
            "volume": vol_today,
            "vol_ratio_5": vol_ratio_5,
            "vol_ratio_20": vol_ratio_20,
            "turnover": turnover,
            "amplitude": amplitude_today,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma60": ma60,
        }
    }
