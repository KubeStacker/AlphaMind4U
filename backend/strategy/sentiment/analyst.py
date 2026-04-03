# /backend/strategy/sentiment/analyst.py

import json
import logging
import math

import numpy as np
import arrow

from db.connection import get_db_connection, fetch_df
from strategy.sentiment.config import SENTIMENT_CONFIG

logger = logging.getLogger(__name__)


class SentimentAnalyst:
    """
    情绪分析器 (Sentiment Analyst) - 简化版
    
    只保留基本情绪分数计算，不涉及交易信号和回测。
    """

    def _finite_number(self, value, default=0.0):
        try:
            v = float(value)
            if math.isnan(v) or math.isinf(v):
                return float(default)
            return v
        except Exception:
            return float(default)

    def _sanitize_payload(self, value):
        if isinstance(value, dict):
            return {k: self._sanitize_payload(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._sanitize_payload(v) for v in value]
        if isinstance(value, float):
            return self._finite_number(value, 0.0)
        return value

    def analyze(self, trade_date: str):
        """计算指定日期的市场情绪分数"""
        logger.info(f"正在计算情绪分数: {trade_date}...")
        try:
            df_today = self._get_daily_data(trade_date)
            if df_today.empty:
                return None
            
            fingerprint = self._calculate_fingerprint(df_today, trade_date)
            current_score = self._calculate_continuous_score(fingerprint)
            
            # 获取历史情绪用于计算动量
            history = self._get_recent_sentiments(trade_date, limit=10)
            v1 = 0.0
            if history:
                prev_score = history[-1]['score']
                v1 = current_score - prev_score
            
            # 确定情绪标签
            label = self._score_to_label(current_score)
            
            # 保存结果
            self._save_result(trade_date, current_score, label, fingerprint, v1)
            
            return {
                "trade_date": trade_date,
                "score": current_score,
                "label": label,
                "v1": round(v1, 1)
            }
        except Exception as e:
            logger.error(f"情绪计算失败: {e}", exc_info=True)
            return None

    def _score_to_label(self, score: float) -> str:
        """将分数转换为情绪标签"""
        if score >= 85:
            return "沸腾"
        if score >= 70:
            return "高热"
        if score >= 55:
            return "修复"
        if score >= 42:
            return "拉锯"
        if score >= 25:
            return "低温"
        return "冰点"

    def _get_daily_data(self, date_str):
        return fetch_df(f"SELECT ts_code, open, high, low, close, pre_close, pct_chg, amount, vol FROM daily_price WHERE trade_date = '{date_str}' AND vol > 0")

    def _get_recent_sentiments(self, date_str, limit=5):
        df = fetch_df(f"SELECT trade_date, score FROM market_sentiment WHERE trade_date < '{date_str}' ORDER BY trade_date DESC LIMIT {limit}")
        return df.sort_values('trade_date').to_dict('records') if not df.empty else []

    def _calculate_continuous_score(self, fp):
        """计算情绪分数 (0-100)"""
        w = SENTIMENT_CONFIG['weights']
        score = 50.0
        p = fp.get('factor_percentiles', {})
        
        # 1. 涨跌停动能
        limit_score = (fp['limit_up_count'] - fp['limit_down_count']) * w['limit_diff']
        score += np.clip(limit_score, -30, 30)
        
        # 2. 赚钱效应
        promo_score = (fp['promotion_rate'] - 0.3) * w['promotion']
        score += np.clip(promo_score, -20, 20)
        
        # 3. 恐慌度
        broken_score = (0.25 - fp['broken_ratio']) * w['broken']
        score += np.clip(broken_score, -15, 15)
        
        # 4. 指数共振
        index_score = fp['index_pct_chg'] * w['index_chg']
        score += np.clip(index_score, -10, 10)
        
        # 5. 修复力度
        repair_score = fp['repair_count'] * w['repair']
        score += np.clip(repair_score, 0, 10)

        # 6. 市场广度
        breadth_score = (p.get('breadth', 0.5) - 0.5) * w.get('breadth', 32.0)
        score += np.clip(breadth_score, -12, 12)

        # 7. 成交活跃度
        turnover_score = (p.get('turnover_activity', 0.5) - 0.5) * w.get('turnover_activity', 8.0)
        score += np.clip(turnover_score, -8, 8)

        # 8. 融资杠杆情绪
        margin_score = (p.get('margin_financing_delta5', 0.5) - 0.5) * w.get('margin_delta', 120.0)
        score += np.clip(margin_score, -10, 10)

        # 9. 资金流向强弱
        flow_score = (p.get('net_mf_ratio', 0.5) - 0.5) * w.get('net_mf_ratio', 18.0)
        score += np.clip(flow_score, -8, 8)

        # 10. 新高/新低结构
        nhnl_score = (p.get('new_high_low_ratio', 0.5) - 0.5) * w.get('new_high_low', 6.0)
        score += np.clip(nhnl_score, -8, 8)

        # 11. 连板高度
        board_score = (p.get('max_limit_up_streak', 0.5) - 0.5) * w.get('board_height', 2.0)
        score += np.clip(board_score, -6, 6)

        # 12. 波动恐慌代理
        iv_score = -(p.get('iv_proxy_z', 0.5) - 0.5) * w.get('iv_proxy', 5.5)
        score += np.clip(iv_score, -10, 10)
        
        score = self._finite_number(score, 50.0)
        return round(float(np.clip(score, 0, 100)), 1)

    def _calculate_fingerprint(self, df_today, trade_date):
        """计算当日市场指纹数据"""
        stats = {}
        limit_ups = df_today[df_today['pct_chg'] >= 9.5]
        limit_downs = df_today[df_today['pct_chg'] <= -9.5]
        stats['limit_up_count'], stats['limit_down_count'] = len(limit_ups), len(limit_downs)
        stats['up_count'], stats['down_count'] = len(df_today[df_today['pct_chg'] > 0]), len(df_today[df_today['pct_chg'] < 0])
        total_amt = df_today['amount'].sum()
        stats['total_amount'] = round(float(total_amt), 2)
        total_stocks = len(df_today)
        stats['breadth_ratio'] = round(stats['up_count'] / total_stocks, 4) if total_stocks > 0 else 0.5
        
        # 涨停晋级率
        stats['promotion_rate'] = 0.3
        try:
            prev_date_df = fetch_df(f"SELECT trade_date FROM market_index WHERE ts_code='000300.SH' AND trade_date < '{trade_date}' ORDER BY trade_date DESC LIMIT 1")
            if not prev_date_df.empty:
                prev_date = prev_date_df.iloc[0, 0]
                prev_limit_ups = fetch_df(f"SELECT ts_code FROM daily_price WHERE trade_date = '{prev_date}' AND pct_chg >= 9.5")
                if not prev_limit_ups.empty:
                    promoted = limit_ups[limit_ups['ts_code'].isin(prev_limit_ups['ts_code'])]
                    stats['promotion_rate'] = round(len(promoted) / len(prev_limit_ups), 2)
        except Exception as e:
            logger.debug(f"Promotion rate error: {e}")

        stats['repair_count'] = len(df_today[((df_today[['open', 'close']].min(axis=1) - df_today['low']) / df_today['close'] > 0.03)])
        stats['broken_count'] = len(df_today[(df_today['high'] >= df_today['pre_close'] * 1.095) & (df_today['pct_chg'] < 9.5)])
        stats['broken_ratio'] = round(stats['broken_count'] / (len(limit_ups) + stats['broken_count']), 1) if (len(limit_ups) + stats['broken_count']) > 0 else 0
        
        # 指数涨跌幅
        stats['index_pct_chg'] = 0.0
        try:
            indices = fetch_df(
                f"""
                SELECT ts_code, pct_chg, close, pre_close
                FROM market_index
                WHERE trade_date = '{trade_date}'
                  AND ts_code IN ('000300.SH', '000001.SH', '399001.SZ')
                """
            )
            if not indices.empty:
                for code in ['000300.SH', '000001.SH', '399001.SZ']:
                    tmp = indices[indices['ts_code'] == code]
                    if not tmp.empty:
                        row = tmp.iloc[0]
                        pct = self._finite_number(row.get('pct_chg', 0.0), 0.0)
                        if abs(pct) < 1e-9:
                            close = self._finite_number(row.get('close', 0.0), 0.0)
                            pre_close = self._finite_number(row.get('pre_close', 0.0), 0.0)
                            if pre_close > 0:
                                pct = (close - pre_close) / pre_close * 100.0
                        stats['index_pct_chg'] = round(pct, 1)
                        break
        except:
            pass

        # 成交活跃度
        stats['turnover_activity'] = 1.0
        try:
            amt_hist = fetch_df(
                f"""
                SELECT trade_date, SUM(amount) AS total_amount
                FROM daily_price
                WHERE trade_date <= '{trade_date}'
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT 25
                """
            )
            if not amt_hist.empty:
                amt_hist = amt_hist.sort_values('trade_date')
                current_amt = float(amt_hist.iloc[-1]['total_amount']) if len(amt_hist) > 0 else float(total_amt)
                ma20_amt = float(amt_hist['total_amount'].tail(20).mean()) if len(amt_hist) > 0 else current_amt
                if ma20_amt > 0:
                    stats['turnover_activity'] = round(current_amt / ma20_amt, 4)
        except Exception as e:
            logger.debug(f"Turnover activity error: {e}")

        # 融资融券情绪
        margin_stats = self._get_margin_stats(trade_date)
        stats.update(margin_stats)

        # 资金流向情绪
        flow_stats = self._get_moneyflow_stats(trade_date, total_amt)
        stats.update(flow_stats)

        # 新高/新低结构
        nh_nl_stats = self._get_new_high_low_stats(trade_date)
        stats.update(nh_nl_stats)

        # 连板高度
        stats['max_limit_up_streak'] = self._get_max_limit_up_streak(trade_date)

        # 波动率恐慌代理
        vol_stats = self._get_index_volatility_proxy(trade_date)
        stats.update(vol_stats)

        # 因子分位数标准化
        stats['factor_percentiles'] = self._get_factor_percentiles(trade_date, stats)
        stats['median_pct_chg'] = round(self._finite_number(df_today['pct_chg'].median(), 0.0), 1)
        
        return self._sanitize_payload(stats)

    def _calc_percentile(self, current: float, history: list[float]) -> float:
        clean = [float(x) for x in history if x is not None]
        if len(clean) < 20:
            return 0.5
        arr = np.array(clean, dtype=float)
        return float(np.mean(arr <= float(current)))

    def _get_factor_percentiles(self, trade_date: str, stats: dict, lookback_days: int = 365) -> dict:
        result = {
            'breadth': 0.5,
            'turnover_activity': 0.5,
            'margin_financing_delta5': 0.5,
            'net_mf_ratio': 0.5,
            'new_high_low_ratio': 0.5,
            'max_limit_up_streak': 0.5,
            'iv_proxy_z': 0.5
        }
        try:
            df = fetch_df(
                f"""
                SELECT
                    TRY_CAST(json_extract(details, '$.factors.breadth') AS DOUBLE) AS breadth,
                    TRY_CAST(json_extract(details, '$.factors.turnover_activity') AS DOUBLE) AS turnover_activity,
                    TRY_CAST(json_extract(details, '$.factors.margin_financing_delta5') AS DOUBLE) AS margin_financing_delta5,
                    TRY_CAST(json_extract(details, '$.factors.net_mf_ratio') AS DOUBLE) AS net_mf_ratio,
                    TRY_CAST(json_extract(details, '$.factors.new_high_low_ratio') AS DOUBLE) AS new_high_low_ratio,
                    TRY_CAST(json_extract(details, '$.factors.max_limit_up_streak') AS DOUBLE) AS max_limit_up_streak,
                    TRY_CAST(json_extract(details, '$.factors.iv_proxy_z') AS DOUBLE) AS iv_proxy_z
                FROM market_sentiment
                WHERE trade_date < '{trade_date}'
                ORDER BY trade_date DESC
                LIMIT {int(lookback_days)}
                """
            )
            if df.empty:
                return result

            result['breadth'] = round(
                self._calc_percentile(stats.get('breadth_ratio', 0.5) * 100, df['breadth'].tolist()), 4
            )
            result['turnover_activity'] = round(
                self._calc_percentile(stats.get('turnover_activity', 1.0), df['turnover_activity'].tolist()), 4
            )
            result['margin_financing_delta5'] = round(
                self._calc_percentile(stats.get('margin_financing_delta5', 0.0), df['margin_financing_delta5'].tolist()), 4
            )
            result['net_mf_ratio'] = round(
                self._calc_percentile(stats.get('net_mf_ratio', 0.0), df['net_mf_ratio'].tolist()), 4
            )
            result['new_high_low_ratio'] = round(
                self._calc_percentile(stats.get('new_high_low_ratio', 1.0), df['new_high_low_ratio'].tolist()), 4
            )
            result['max_limit_up_streak'] = round(
                self._calc_percentile(stats.get('max_limit_up_streak', 0), df['max_limit_up_streak'].tolist()), 4
            )
            result['iv_proxy_z'] = round(
                self._calc_percentile(stats.get('iv_proxy_z', 0.0), df['iv_proxy_z'].tolist()), 4
            )
        except Exception as e:
            logger.debug(f"Factor percentile error: {e}")
        return result

    def _get_margin_stats(self, trade_date: str) -> dict:
        stats = {'margin_financing_delta5': 0.0}
        try:
            df = fetch_df(
                f"""
                SELECT trade_date, SUM(rzye) AS rzye
                FROM stock_margin
                WHERE trade_date <= '{trade_date}'
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT 8
                """
            )
            if df.empty or len(df) < 6:
                return stats
            df = df.sort_values('trade_date')
            current_rzye = float(df.iloc[-1]['rzye']) if df.iloc[-1]['rzye'] is not None else 0.0
            base_rzye = float(df.iloc[-6]['rzye']) if df.iloc[-6]['rzye'] is not None else 0.0
            if base_rzye > 0:
                stats['margin_financing_delta5'] = round((current_rzye - base_rzye) / base_rzye, 4)
        except Exception as e:
            logger.debug(f"Margin stats error: {e}")
        return stats

    def _get_moneyflow_stats(self, trade_date: str, total_amt: float) -> dict:
        stats = {'net_mf_ratio': 0.0}
        try:
            df = fetch_df(
                f"""
                SELECT SUM(net_mf_amount) AS net_mf_amount
                FROM stock_moneyflow
                WHERE trade_date = '{trade_date}'
                """
            )
            if df.empty:
                return stats
            net_mf_amount = float(df.iloc[0]['net_mf_amount']) if df.iloc[0]['net_mf_amount'] is not None else 0.0
            if total_amt > 0:
                stats['net_mf_ratio'] = round(net_mf_amount / total_amt, 4)
        except Exception as e:
            logger.debug(f"Moneyflow stats error: {e}")
        return stats

    def _get_new_high_low_stats(self, trade_date: str, window: int = 60) -> dict:
        stats = {'new_high_low_ratio': 1.0}
        try:
            df = fetch_df(
                f"""
                WITH latest AS (
                    SELECT ts_code, close
                    FROM daily_price
                    WHERE trade_date = '{trade_date}'
                ),
                hist AS (
                    SELECT ts_code, MAX(close) AS max_close, MIN(close) AS min_close
                    FROM daily_price
                    WHERE trade_date < '{trade_date}'
                      AND trade_date >= DATE '{trade_date}' - INTERVAL {int(window * 2)} DAY
                    GROUP BY ts_code
                )
                SELECT
                    SUM(CASE WHEN l.close >= h.max_close THEN 1 ELSE 0 END) AS new_high_count,
                    SUM(CASE WHEN l.close <= h.min_close THEN 1 ELSE 0 END) AS new_low_count
                FROM latest l
                JOIN hist h ON l.ts_code = h.ts_code
                """
            )
            if df.empty:
                return stats
            new_high_count = int(df.iloc[0]['new_high_count'] or 0)
            new_low_count = int(df.iloc[0]['new_low_count'] or 0)
            stats['new_high_low_ratio'] = round((new_high_count + 1) / (new_low_count + 1), 4)
        except Exception as e:
            logger.debug(f"New high/low stats error: {e}")
        return stats

    def _get_max_limit_up_streak(self, trade_date: str, lookback_days: int = 15) -> int:
        try:
            today_limit_df = fetch_df(
                f"""
                SELECT ts_code
                FROM daily_price
                WHERE trade_date = '{trade_date}' AND pct_chg >= 9.5
                """
            )
            if today_limit_df.empty:
                return 0

            ts_codes = [c for c in today_limit_df['ts_code'].tolist() if c]
            if not ts_codes:
                return 0
            codes_str = ",".join([f"'{c}'" for c in ts_codes])

            df = fetch_df(
                f"""
                SELECT ts_code, trade_date, pct_chg
                FROM daily_price
                WHERE trade_date <= '{trade_date}'
                  AND trade_date >= DATE '{trade_date}' - INTERVAL {int(lookback_days * 2)} DAY
                  AND ts_code IN ({codes_str})
                ORDER BY trade_date DESC
                """
            )
            if df.empty:
                return 0

            df['trade_date'] = df['trade_date'].astype(str)
            max_streak = 1
            for ts_code in ts_codes:
                stock_df = df[df['ts_code'] == ts_code].sort_values('trade_date')
                flags = (stock_df['pct_chg'] >= 9.5).tolist()
                streak = 0
                for is_limit in reversed(flags):
                    if is_limit:
                        streak += 1
                    else:
                        break
                if streak > max_streak:
                    max_streak = streak
            return int(max_streak)
        except Exception as e:
            logger.debug(f"Limit-up streak error: {e}")
            return 0

    def _get_index_volatility_proxy(self, trade_date: str, ts_code: str = '000300.SH') -> dict:
        stats = {'iv_proxy_z': 0.0}
        try:
            df = fetch_df(
                f"""
                SELECT trade_date, close
                FROM market_index
                WHERE ts_code = '{ts_code}' AND trade_date <= '{trade_date}'
                ORDER BY trade_date DESC
                LIMIT 180
                """
            )
            if df.empty or len(df) < 25:
                return stats
            df = df.sort_values('trade_date')
            df['ret'] = df['close'].pct_change()
            if df['ret'].dropna().empty:
                return stats

            vol20 = float(df['ret'].tail(20).std() * np.sqrt(252) * 100)
            rolling = df['ret'].rolling(window=20).std() * np.sqrt(252) * 100
            hist = rolling.dropna().tail(120)
            if len(hist) >= 20:
                mean_v = float(hist.mean())
                std_v = float(hist.std())
                if std_v > 1e-6:
                    stats['iv_proxy_z'] = round((vol20 - mean_v) / std_v, 4)
        except Exception as e:
            logger.debug(f"Volatility proxy error: {e}")
        return stats

    def _save_result(self, trade_date, score, label, fingerprint, v1):
        """保存情绪计算结果"""
        details = {
            "factors": {
                "breadth": round(fingerprint.get('breadth_ratio', 0.5) * 100, 1),
                "median_chg": round(fingerprint.get('median_pct_chg', 0), 1),
                "index_chg": round(fingerprint.get('index_pct_chg', 0), 1),
                "limit": fingerprint.get('limit_up_count', 0),
                "limit_down": fingerprint.get('limit_down_count', 0),
                "failure": fingerprint.get('broken_count', 0),
                "repair": fingerprint.get('repair_count', 0),
                "promotion_rate": round(fingerprint.get('promotion_rate', 0.0), 4),
                "turnover_activity": round(fingerprint.get('turnover_activity', 1.0), 4),
                "margin_financing_delta5": round(fingerprint.get('margin_financing_delta5', 0.0), 4),
                "net_mf_ratio": round(fingerprint.get('net_mf_ratio', 0.0), 4),
                "new_high_low_ratio": round(fingerprint.get('new_high_low_ratio', 1.0), 4),
                "iv_proxy_z": round(fingerprint.get('iv_proxy_z', 0.0), 4),
                "max_limit_up_streak": int(fingerprint.get('max_limit_up_streak', 0)),
                "factor_percentiles": fingerprint.get('factor_percentiles', {})
            },
            "v1": round(v1, 1)
        }
        details = self._sanitize_payload(details)
        score = round(self._finite_number(score, 50.0), 1)

        sql = "INSERT INTO market_sentiment (trade_date, score, label, details) VALUES (?, ?, ?, ?) ON CONFLICT (trade_date) DO UPDATE SET score=excluded.score, label=excluded.label, details=excluded.details"
        with get_db_connection() as con:
            con.execute(sql, (trade_date, score, label, json.dumps(details, ensure_ascii=False)))
        logger.info(f"情绪结果已保存: {label} | Score: {score:.1f}")

    def calculate(self, days=365):
        """批量计算历史情绪数据"""
        date_query = f"SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date DESC LIMIT {int(days)}"
        dates_df = fetch_df(date_query)
        if dates_df.empty:
            return
        target_dates = sorted(dates_df['trade_date'].tolist())
        for d in target_dates:
            date_str = d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)
            self.analyze(date_str)

    def get_history(self, days=30):
        """获取情绪历史数据"""
        query = f"SELECT trade_date, score, label, details FROM market_sentiment ORDER BY trade_date DESC LIMIT {int(days)}"
        df_sent = fetch_df(query)
        if df_sent.empty:
            return {"dates": [], "sentiment": [], "index": []}
        
        df_sent = df_sent.sort_values('trade_date')
        dates, sentiment_data = [], []
        for _, row in df_sent.iterrows():
            d_str = row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date'])
            dates.append(d_str)
            details = json.loads(row['details']) if isinstance(row['details'], str) else row['details']
            sentiment_data.append({"value": round(row['score'], 1), "label": row['label'], "details": details})
        
        min_date, max_date = dates[0], dates[-1]
        idx_query = f"SELECT trade_date, close FROM market_index WHERE ts_code = '000001.SH' AND trade_date BETWEEN '{min_date}' AND '{max_date}' ORDER BY trade_date ASC"
        df_idx = fetch_df(idx_query)
        idx_map = {(d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)): round(c, 1) for d, c in zip(df_idx['trade_date'], df_idx['close'])}
        return {"dates": dates, "sentiment": sentiment_data, "index": [idx_map.get(d) for d in dates]}


sentiment_analyst = SentimentAnalyst()
