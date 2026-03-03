# /backend/strategy/mainline/analyst.py

import pandas as pd
import numpy as np
import logging
import arrow
from functools import lru_cache
from db.connection import fetch_df, fetch_df_read_only, get_db_connection
from .config import CONCEPT_MAPPING, CATEGORY_WEIGHTS
from core.constants import CONCEPT_BLACKLIST

logger = logging.getLogger(__name__)

class MainlineAnalyst:
    """
    主线分析器 (Mainline Analyst)
    功能：基于板块共识与资金热度，识别市场的核心主线板块。
    核心逻辑：
    1. 统计各概念板块的涨停数、平均涨幅、成交额等数据。
    2. 计算多维评分 (Score)，包括涨停强度、板块广度、资金热度。
    3. 引入"行业共识"算法，解决一只股票对应多个概念时的归属问题，将资金聚焦到最核心的主线。
    """
    def __init__(self, concept_mapping=None, category_weights=None):
        # 概念映射表，定义了"大板块"包含哪些"子概念"
        self.concept_mapping = concept_mapping if concept_mapping else CONCEPT_MAPPING
        # 板块权重配置，用于在多重概念冲突时决定优先归属
        self.category_weights = category_weights if category_weights else CATEGORY_WEIGHTS
        self._concept_score_cache = {} # Pre-calculated scores for each concept
        self._sector_mapping_cache = {} # Cache for ts_code -> primary_sector

    def _get_concept_scores(self, concept_name: str):
        """ 预计算并缓存单个概念对各板块的得分 """
        if concept_name in self._concept_score_cache:
            return self._concept_score_cache[concept_name]
        
        scores = []
        for sector, keywords in self.concept_mapping.items():
            weight = self.category_weights.get(sector, 1.0)
            match_score = 0
            if sector in concept_name: match_score = len(sector) * 1.5
            
            max_kw_len = 0
            for kw in keywords:
                if kw in concept_name:
                    max_kw_len = max(max_kw_len, len(kw))
            
            final_score = max(match_score, max_kw_len) * weight
            if final_score > 0:
                scores.append({'sector': sector, 'score': final_score})
        
        self._concept_score_cache[concept_name] = scores
        return scores

    def _get_mapped_concept(self, original_concept: str) -> str:
        """
        根据打分系统将原始概念映射到核心大板块。
        优化版：复用 _get_concept_scores。
        """
        scores = self._get_concept_scores(original_concept)
        if not scores:
            return original_concept
        # 返回得分最高的板块名
        best = max(scores, key=lambda x: x['score'])
        return best['sector']

    def _clean_concept_name(self, concept_name: str) -> str:
        if not concept_name:
            return ""
        c = str(concept_name).strip()
        c = c.replace("_THS", "")
        c = c.replace("（", "(").replace("）", ")")
        return c

    def _build_stock_mainline_map(self, min_date: str, max_date: str) -> pd.DataFrame:
        """
        为每只股票构建一个稳定的主线归属:
        1. 优先使用 stock_concept_details（过滤黑名单）
        2. 如概念缺失，则回退 stock_basic.industry
        """
        concept_df = fetch_df(
            """
            SELECT DISTINCT ts_code, concept_name
            FROM stock_concept_details
            WHERE concept_name IS NOT NULL
            """
        )
        records = []
        if not concept_df.empty:
            for _, row in concept_df.iterrows():
                raw = self._clean_concept_name(row["concept_name"])
                if not raw or raw in CONCEPT_BLACKLIST:
                    continue
                scores = self._get_concept_scores(raw)
                if scores:
                    best = max(scores, key=lambda x: x["score"])
                    mapped = best["sector"]
                    score = float(best["score"])
                else:
                    mapped = raw
                    score = 0.5
                records.append(
                    {
                        "ts_code": row["ts_code"],
                        "mapped_name": mapped,
                        "map_score": score,
                    }
                )

        df_map = pd.DataFrame(records) if records else pd.DataFrame(columns=["ts_code", "mapped_name", "map_score"])
        if not df_map.empty:
            df_map = (
                df_map.sort_values(["ts_code", "map_score"], ascending=[True, False])
                .drop_duplicates(subset=["ts_code"])
                .drop(columns=["map_score"])
            )

        # 回退行业映射，保证主线结果可用
        fallback_df = fetch_df(
            """
            SELECT ts_code, industry
            FROM stock_basic
            WHERE industry IS NOT NULL AND industry <> ''
            """
        )
        if fallback_df.empty:
            return df_map

        fallback_rows = []
        for _, row in fallback_df.iterrows():
            industry = str(row["industry"]).strip()
            if not industry:
                continue
            mapped = self._get_mapped_concept(industry)
            fallback_rows.append({"ts_code": row["ts_code"], "mapped_name": mapped or industry})

        fallback_map = pd.DataFrame(fallback_rows)
        if df_map.empty:
            return fallback_map.drop_duplicates(subset=["ts_code"])

        # 用概念映射优先，行业映射兜底
        merged = fallback_map.merge(df_map, on="ts_code", how="left", suffixes=("_fallback", ""))
        merged["mapped_name"] = merged["mapped_name"].fillna(merged["mapped_name_fallback"])
        merged = merged[["ts_code", "mapped_name"]].drop_duplicates(subset=["ts_code"])
        return merged

    def _identify_stock_sectors(self, df_concepts: pd.DataFrame) -> pd.DataFrame:
        """
        [V9 核心算法] 基于概念共识的股票行业聚类。
        优化版：使用预计算缓存减少循环开销。
        """
        if df_concepts.empty:
            return pd.DataFrame(columns=['ts_code', 'primary_sector'])

        # 1. 为每个概念获取得分 (使用缓存)
        records = []
        for concept in df_concepts['concept_name'].unique():
            scores = self._get_concept_scores(concept)
            for s in scores:
                records.append({'concept_name': concept, 'sector': s['sector'], 'score': s['score']})
        
        if not records:
            return pd.DataFrame(columns=['ts_code', 'primary_sector'])
            
        df_scores = pd.DataFrame(records)
        df_merged = df_concepts.merge(df_scores, on='concept_name')
        df_stock_sector = df_merged.groupby(['ts_code', 'sector'])['score'].sum().reset_index()
        df_stock_sector = df_stock_sector.sort_values(['ts_code', 'score'], ascending=[True, False])
        df_primary = df_stock_sector.drop_duplicates(subset=['ts_code']).rename(columns={'sector': 'primary_sector'})
        
        return df_primary[['ts_code', 'primary_sector']]

    def _safe_float(self, value, default=0.0):
        try:
            v = float(value)
            if np.isnan(v) or np.isinf(v):
                return default if default is None else float(default)
            return v
        except Exception:
            return default if default is None else float(default)

    def _extract_rt_pct(self, row: pd.Series):
        row_map = {str(k).lower(): row[k] for k in row.index}
        for key in ("pct_chg", "pct_change", "changepercent"):
            if key in row_map and row_map[key] is not None:
                v = self._safe_float(row_map[key], None)
                if v is not None:
                    return v
        price = None
        pre_close = None
        for k in ("price", "current", "close"):
            if k in row_map:
                tmp = self._safe_float(row_map[k], None)
                if tmp is not None:
                    price = tmp
                    break
        for k in ("pre_close", "yclose"):
            if k in row_map:
                tmp = self._safe_float(row_map[k], None)
                if tmp is not None:
                    pre_close = tmp
                    break
        if price is not None and pre_close is not None and pre_close > 0:
            return (price - pre_close) / pre_close * 100.0
        return None

    def preview_intraday(self, provider, limit: int = 5, leaders_per_mainline: int = 8, src: str = "dc"):
        """
        盘中主线预估（不落库）：
        - 基于最近交易日的主线归属和龙头池
        - 用 realtime_quote 替换最新涨跌幅，输出盘中主线强弱
        """
        date_df = fetch_df(
            "SELECT trade_date FROM daily_price GROUP BY trade_date HAVING COUNT(*) > 1000 ORDER BY trade_date DESC LIMIT 1"
        )
        if date_df.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        latest_trade_date = (
            date_df.iloc[0]["trade_date"].strftime("%Y-%m-%d")
            if hasattr(date_df.iloc[0]["trade_date"], "strftime")
            else str(date_df.iloc[0]["trade_date"])
        )

        stock_map = self._build_stock_mainline_map(latest_trade_date, latest_trade_date)
        if stock_map.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        universe_df = fetch_df(
            """
            SELECT d.ts_code, d.pct_chg, d.amount, b.name AS stock_name
            FROM daily_price d
            LEFT JOIN stock_basic b ON d.ts_code = b.ts_code
            WHERE d.trade_date = ?
            """,
            params=[latest_trade_date]
        )
        if universe_df.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        merged = universe_df.merge(stock_map, on="ts_code", how="left")
        merged = merged[merged["mapped_name"].notna()].copy()
        if merged.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        # 先按昨日结构选主线与龙头池，再用实时行情打分
        by_line = (
            merged.groupby("mapped_name")
            .agg(total_amt=("amount", "sum"), avg_ret=("pct_chg", "mean"), stock_count=("ts_code", "nunique"))
            .reset_index()
            .sort_values(["total_amt", "avg_ret"], ascending=[False, False])
            .head(max(limit * 2, 10))
        )
        selected_lines = by_line["mapped_name"].tolist()
        leader_pool = []
        for line in selected_lines:
            g = merged[merged["mapped_name"] == line].sort_values(["pct_chg", "amount"], ascending=[False, False]).head(max(3, leaders_per_mainline))
            for _, r in g.iterrows():
                leader_pool.append({
                    "mapped_name": line,
                    "ts_code": r["ts_code"],
                    "stock_name": r.get("stock_name") or r["ts_code"]
                })
        if not leader_pool:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        leader_df = pd.DataFrame(leader_pool).drop_duplicates(subset=["ts_code"])
        codes = leader_df["ts_code"].tolist()

        rt_df = pd.DataFrame()
        try:
            rt_df = provider.realtime_quote(ts_code=",".join(codes), src=src)
        except Exception as e:
            logger.warning(f"mainline preview realtime bulk quote failed: {e}")

        if rt_df is None or rt_df.empty:
            rows = []
            for code in codes:
                try:
                    q = provider.realtime_quote(ts_code=code, src=src)
                    if q is not None and not q.empty:
                        rows.append(q.iloc[0].to_dict())
                except Exception:
                    continue
            rt_df = pd.DataFrame(rows) if rows else pd.DataFrame()

        if rt_df.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        key_col = "ts_code" if "ts_code" in rt_df.columns else ("code" if "code" in rt_df.columns else None)
        if not key_col:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        rt_df["ts_code"] = rt_df[key_col].astype(str)
        rt_df["rt_pct_chg"] = rt_df.apply(self._extract_rt_pct, axis=1)
        rt_df = rt_df[rt_df["rt_pct_chg"].notna()][["ts_code", "rt_pct_chg"]]
        if rt_df.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        joined = leader_df.merge(rt_df, on="ts_code", how="inner")
        if joined.empty:
            return {"as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"), "data": []}

        result_rows = []
        for line, g in joined.groupby("mapped_name"):
            n = len(g)
            if n < 2:
                continue
            avg_ret = float(g["rt_pct_chg"].mean())
            up_ratio = float((g["rt_pct_chg"] > 0).sum() / n)
            strong_ratio = float((g["rt_pct_chg"] >= 2.0).sum() / n)
            score = avg_ret * 2.0 + up_ratio * 24.0 + strong_ratio * 22.0 + np.log(n + 1.0) * 1.2
            tops = (
                g.sort_values("rt_pct_chg", ascending=False)
                .head(5)[["stock_name", "rt_pct_chg"]]
                .to_dict("records")
            )
            result_rows.append({
                "name": line,
                "score": round(float(score), 2),
                "avg_ret": round(avg_ret, 2),
                "up_ratio": round(up_ratio, 4),
                "strong_ratio": round(strong_ratio, 4),
                "sample_size": int(n),
                "top_stocks": [
                    {"name": x["stock_name"], "pct_chg": round(float(x["rt_pct_chg"]), 2)}
                    for x in tops
                ],
            })

        result_rows = sorted(result_rows, key=lambda x: x["score"], reverse=True)[:max(1, int(limit))]
        return {
            "as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
            "baseline_trade_date": latest_trade_date,
            "data": result_rows
        }

    @lru_cache(maxsize=32)
    def analyze(self, days=3, limit=5, trade_date: str = None):
        """
        主线分析入口函数。
        
        参数:
        - days: 分析的时间窗口 (默认最近 3 个交易日)。
        - limit: 返回排名靠前的主线数量。
        - trade_date: 指定分析日期 (分析将以该日期为终点，向前追溯 days 个交易日)。
        
        返回:
        - 主线分析结果列表，包含板块名、评分、涨停数、龙头股等。
        """
        # 1. 确定分析的时间范围 (Min Date, Max Date)
        if trade_date:
            # 如果指定了日期，寻找该日期及其之前的交易日
            date_query = """
                SELECT trade_date FROM daily_price 
                WHERE trade_date <= ? 
                GROUP BY trade_date 
                HAVING COUNT(*) > 1000 
                ORDER BY trade_date DESC LIMIT ?
            """
            try:
                dates_df = fetch_df(date_query, params=[trade_date, days])
                if dates_df.empty: return []
                
                recent_dates = dates_df['trade_date'].tolist()
                min_date = min(recent_dates).strftime('%Y-%m-%d')
                max_date = max(recent_dates).strftime('%Y-%m-%d')
            except Exception as e:
                logger.error(f"Failed to fetch dates for trade_date {trade_date}: {e}")
                return []
        else:
            # 如果未指定，自动查找最近有数据的 N 个交易日
            date_query = "SELECT trade_date FROM daily_price GROUP BY trade_date HAVING COUNT(*) > 1000 ORDER BY trade_date DESC LIMIT ?"
            try:
                dates_df = fetch_df(date_query, params=[days])
                if dates_df.empty: return []
                
                recent_dates = dates_df['trade_date'].tolist()
                min_date = min(recent_dates).strftime('%Y-%m-%d')
                max_date = max(recent_dates).strftime('%Y-%m-%d')
            except Exception as e:
                logger.error(f"Failed to fetch dates: {e}")
                return []

        try:
            # 2. 获取期间板块聚合数据
            # 统计维度：平均涨幅、成交额、个股数、涨停数、上涨数、强势股数(>5%)
            query = """
            SELECT 
                c.concept_name, d.trade_date,
                AVG(d.pct_chg) as avg_ret,
                SUM(d.amount) as total_amt,
                COUNT(d.ts_code) as stock_count,
                SUM(CASE WHEN d.pct_chg > 9.8 THEN 1 ELSE 0 END) as limit_ups,
                SUM(CASE WHEN d.pct_chg > 0 THEN 1 ELSE 0 END) as up_count,
                SUM(CASE WHEN d.pct_chg > 5.0 THEN 1 ELSE 0 END) as strong_count
            FROM daily_price d
            JOIN stock_concept_details c ON d.ts_code = c.ts_code
            WHERE d.trade_date BETWEEN ? AND ?
            GROUP BY c.concept_name, d.trade_date
            HAVING stock_count >= 5
            """
            df = fetch_df(query, params=[min_date, max_date])
            if df.empty: return []

            # 优化：初步映射 (Mapping)
            # 先将原始概念名 (concept_name) 映射到大类 (mapped_name)
            unique_concepts = df['concept_name'].unique()
            mapping_dict = {c: self._get_mapped_concept(c) for c in unique_concepts}
            df['mapped_name'] = df['concept_name'].map(mapping_dict)
            
            # 3. V8 因子计算：引入共振过滤 (Resonance Filter)
            df['lu_ratio'] = df['limit_ups'] / df['stock_count']       # 涨停比率
            df['breadth'] = df['up_count'] / df['stock_count']         # 上涨广度
            df['strong_ratio'] = df['strong_count'] / df['stock_count'] # 强势股比率
            
            # 核心评分公式 (Daily Score Formula)
            # 权重：涨停比率(60) > 广度(20) > 平均涨幅(2) > 成交额对数(0.5)
            # 逻辑：主线必须有极强的赚钱效应 (涨停潮)
            df['daily_score'] = (
                df['avg_ret'] * 2.0 + 
                df['lu_ratio'] * 60.0 + 
                df['breadth'] * 20.0 +
                np.log(df['total_amt'] + 1) * 0.5
            )

            # 惩罚因子：如果没有普涨 (breadth < 10%) 且缺乏强势股，大幅降低得分，适应弱势市场。
            mask_weak_resonance = (df['breadth'] < 0.1) & (df['strong_ratio'] < 0.03)
            df.loc[mask_weak_resonance, 'daily_score'] *= 0.3

            # 4. 聚合到 mapped_name (大类板块)
            mainlines_agg = df.groupby('mapped_name').agg({
                'daily_score': 'mean',
                'limit_ups': 'sum',
                'stock_count': 'sum'
            }).reset_index().sort_values(by='daily_score', ascending=False).head(limit)

            # --- [V9 优化] 获取最强标的并进行聚类校验 ---
            top_mainlines = mainlines_agg['mapped_name'].tolist()
            if not top_mainlines: return []

            # 仅查询当前主线且涨幅较大的股票
            latest_date = df['trade_date'].max().strftime('%Y-%m-%d')
            stock_query = """
            SELECT c.concept_name, c.ts_code, c.name as stock_name, d.pct_chg
            FROM daily_price d
            JOIN stock_concept_details c ON d.ts_code = c.ts_code
            WHERE d.trade_date = ? AND d.pct_chg > 3.0
            """
            stock_df_raw = fetch_df(stock_query, params=[latest_date])
            
            # 调用共识算法进行行业判定
            df_stock_sectors = self._identify_stock_sectors(stock_df_raw)
            stock_df = stock_df_raw.merge(df_stock_sectors, on='ts_code')
            
            # 5. 生成最终结果
            results = []
            for _, row in mainlines_agg.iterrows():
                name = row['mapped_name']
                score = round(row['daily_score'], 2)
                lu_sum = int(row['limit_ups'])
                
                # 获取该主线的 Top 3 股票 (必须通过行业共识校验：primary_sector 必须等于当前主线名)
                m_stocks = stock_df[stock_df['primary_sector'] == name].nlargest(3, 'pct_chg')
                top_stocks = [
                    {"name": r['stock_name'], "pct_chg": round(float(r['pct_chg']), 2)} 
                    for _, r in m_stocks.iterrows()
                ]
                
                results.append({
                    "name": name,
                    "score": score,
                    "stock_count": int(row['stock_count']),
                    "limit_ups": lu_sum,
                    "reason": f"资金高度共振，当日涨停{lu_sum}家" if lu_sum > 0 else f"板块协同性强，平均涨幅显著",
                    "top_stocks": top_stocks
                })
            
            return results
        except Exception as e:
            logger.error(f"HotConcept plugin error: {e}")
            return []

    def save_results(self, trade_date: str):
        """
        执行指定日期的分析并将结果持久化到数据库。
        表：mainline_scores
        """
        try:
            # 1. 获取分析结果
            results = self.analyze(days=1, limit=100, trade_date=trade_date)
            
            if not results:
                logger.warning(f"save_results: {trade_date} 没有分析结果")
                return

            with get_db_connection() as con:
                for res in results:
                    import json
                    con.execute("""
                        INSERT INTO mainline_scores (trade_date, mapped_name, score, limit_ups, stock_count, top_stocks)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT (trade_date, mapped_name) DO UPDATE SET
                            score = EXCLUDED.score,
                            limit_ups = EXCLUDED.limit_ups,
                            stock_count = EXCLUDED.stock_count,
                            top_stocks = EXCLUDED.top_stocks
                    """, (
                        trade_date, 
                        res['name'], 
                        res['score'], 
                        res.get('limit_ups', 0),
                        res.get('stock_count', 0),
                        json.dumps(res['top_stocks'])
                    ))
            logger.info(f"已成功持久化 {trade_date} 的主线评分数据")
        except Exception as e:
            logger.error(f"持久化主线数据失败: {e}")

    def get_history(self, days=30):
        """
        获取历史主线演变数据，用于前端可视化展示。
        逻辑：
        1. 获取最近 N 天的数据。
        2. 计算每天各板块的得分。
        3. 筛选出全周期内最活跃的 Top 8 主线。
        4. 生成时间序列数据，展示这些主线的分数变化。
        """
        date_query = f"""
            SELECT trade_date
            FROM daily_price
            GROUP BY trade_date
            HAVING COUNT(*) > 1000
            ORDER BY trade_date DESC
            LIMIT {int(days)}
        """
        try:
            dates_df = fetch_df(date_query)
            if dates_df.empty:
                return {"dates": [], "series": [], "analysis": {"phase": "No_Data", "deduction": "无可用行情数据"}}

            recent_dates = sorted(pd.to_datetime(dates_df["trade_date"]).tolist())
            min_date = recent_dates[0].strftime("%Y-%m-%d")
            max_date = recent_dates[-1].strftime("%Y-%m-%d")

            stock_map = self._build_stock_mainline_map(min_date, max_date)
            if stock_map.empty:
                return {
                    "dates": [d.strftime("%m-%d") for d in recent_dates],
                    "series": [],
                    "analysis": {"phase": "No_Map", "deduction": "概念与行业映射为空，无法识别主线"},
                }

            market_df = fetch_df(
                """
                SELECT p.trade_date, p.ts_code, p.pct_chg, p.amount, b.name AS stock_name,
                       COALESCE(m.net_mf_amount, 0) AS net_mf_amount
                FROM daily_price p
                LEFT JOIN stock_moneyflow m
                  ON p.ts_code = m.ts_code AND p.trade_date = m.trade_date
                LEFT JOIN stock_basic b
                  ON p.ts_code = b.ts_code
                WHERE p.trade_date BETWEEN ? AND ?
                """,
                params=[min_date, max_date],
            )
            if market_df.empty:
                return {
                    "dates": [d.strftime("%m-%d") for d in recent_dates],
                    "series": [],
                    "analysis": {"phase": "No_Data", "deduction": "区间内无可用板块数据"},
                }

            market_df["trade_date"] = pd.to_datetime(market_df["trade_date"])
            merged = market_df.merge(stock_map, on="ts_code", how="left")
            merged = merged[merged["mapped_name"].notna()]
            if merged.empty:
                return {
                    "dates": [d.strftime("%m-%d") for d in recent_dates],
                    "series": [],
                    "analysis": {"phase": "No_Map", "deduction": "无股票可归属到主线"},
                }

            grouped = (
                merged.groupby(["trade_date", "mapped_name"])
                .agg(
                    avg_ret=("pct_chg", "mean"),
                    total_amt=("amount", "sum"),
                    stock_count=("ts_code", "nunique"),
                    limit_ups=("pct_chg", lambda x: int((x >= 9.5).sum())),
                    up_count=("pct_chg", lambda x: int((x > 0).sum())),
                    strong_count=("pct_chg", lambda x: int((x >= 5).sum())),
                    leader_count=("pct_chg", lambda x: int((x >= 7).sum())),
                    net_mf=("net_mf_amount", "sum"),
                )
                .reset_index()
            )
            if grouped.empty:
                return {"dates": [], "series": [], "analysis": {"phase": "No_Data", "deduction": "主线聚合结果为空"}}

            grouped["lu_ratio"] = grouped["limit_ups"] / grouped["stock_count"].clip(lower=1)
            grouped["breadth"] = grouped["up_count"] / grouped["stock_count"].clip(lower=1)
            grouped["strong_ratio"] = grouped["strong_count"] / grouped["stock_count"].clip(lower=1)
            grouped["leader_ratio"] = grouped["leader_count"] / grouped["stock_count"].clip(lower=1)
            grouped["net_mf_ratio"] = grouped["net_mf"] / grouped["total_amt"].replace(0, np.nan)
            grouped["net_mf_ratio"] = grouped["net_mf_ratio"].fillna(0.0)

            # 多因子融合：赚钱效应 + 广度 + 龙头 + 资金 + 成交热度
            grouped["score"] = (
                grouped["avg_ret"] * 1.6
                + grouped["lu_ratio"] * 65.0
                + grouped["breadth"] * 22.0
                + grouped["strong_ratio"] * 16.0
                + grouped["leader_ratio"] * 12.0
                + np.log(grouped["total_amt"] + 1.0) * 0.35
                + grouped["net_mf_ratio"].clip(-0.02, 0.02) * 800.0
            )

            weak_mask = (
                (grouped["stock_count"] < 8)
                | ((grouped["breadth"] < 0.15) & (grouped["strong_ratio"] < 0.02))
            )
            grouped.loc[weak_mask, "score"] *= 0.35
            grouped["score"] = grouped["score"].fillna(0.0).round(2)

            grouped["rank"] = grouped.groupby("trade_date")["score"].rank(method="first", ascending=False)
            top_daily = grouped[grouped["rank"] <= 10].copy()
            if top_daily.empty:
                return {
                    "dates": [d.strftime("%m-%d") for d in recent_dates],
                    "series": [],
                    "analysis": {"phase": "Chaos", "deduction": "近期无通过多因子筛选的主线"},
                }

            top_concepts = (
                top_daily.groupby("mapped_name")["score"].sum().sort_values(ascending=False).head(8).index.tolist()
            )
            unique_dates = sorted(grouped["trade_date"].unique())
            dates = [pd.to_datetime(d).strftime("%m-%d") for d in unique_dates]

            leader_df = merged[(merged["pct_chg"] >= 3.0) & (merged["mapped_name"].isin(top_concepts))].copy()
            top_stocks_map = {}
            if not leader_df.empty:
                leader_df = leader_df.sort_values(["trade_date", "mapped_name", "pct_chg"], ascending=[True, True, False])
                for (d_obj, c), g in leader_df.groupby(["trade_date", "mapped_name"]):
                    key = f"{pd.to_datetime(d_obj).strftime('%m-%d')}_{c}"
                    top_stocks_map[key] = [
                        {"name": (r["stock_name"] or r["ts_code"]), "pct_chg": round(float(r["pct_chg"]), 2)}
                        for _, r in g.drop_duplicates(subset=["ts_code"]).head(5).iterrows()
                    ]

            latest_date = unique_dates[-1]
            latest_top = top_daily[top_daily["trade_date"] == latest_date].sort_values("score", ascending=False)
            best_name = "混沌"
            best_score = 0.0
            best_reason = "市场无明确主线"
            if not latest_top.empty:
                row = latest_top.iloc[0]
                best_name = row["mapped_name"]
                best_score = float(row["score"])
                best_reason = (
                    f"涨停{int(row['limit_ups'])}家，上涨占比{row['breadth']*100:.1f}%，"
                    f"强势股占比{row['strong_ratio']*100:.1f}%，净流入占比{row['net_mf_ratio']*100:.2f}%"
                )

            series = []
            for concept in top_concepts:
                data = []
                for d_obj in unique_dates:
                    date_slice = grouped[(grouped["trade_date"] == d_obj) & (grouped["mapped_name"] == concept)]
                    if date_slice.empty:
                        val = 0.0
                        limit_ups = 0
                        breadth = 0.0
                    else:
                        rr = date_slice.iloc[0]
                        val = float(rr["score"])
                        limit_ups = int(rr["limit_ups"])
                        breadth = float(rr["breadth"])
                    date_str = pd.to_datetime(d_obj).strftime("%m-%d")
                    data.append(
                        {
                            "value": round(val, 2),
                            "limit_ups": limit_ups,
                            "breadth": round(breadth, 4),
                            "top_stocks": top_stocks_map.get(f"{date_str}_{concept}", []),
                        }
                    )
                series.append({"name": concept, "data": data})

            return {
                "dates": dates,
                "series": series,
                "analysis": {
                    "phase": "Mainline_MultiFactor",
                    "top_mainline": {
                        "name": best_name,
                        "score": round(best_score, 2),
                        "reason": best_reason,
                    },
                    "deduction": f"【核心主线：{best_name}】\n评分：{best_score:.2f}\n依据：{best_reason}",
                },
            }
        except Exception as e:
            logger.error(f"HotConcept history plugin error: {e}", exc_info=True)
            return {"dates": [], "series": [], "analysis": {}}

# 导出单例
mainline_analyst = MainlineAnalyst()
