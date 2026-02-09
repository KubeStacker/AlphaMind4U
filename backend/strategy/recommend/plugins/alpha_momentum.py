# /backend/strategy/recommend/plugins/alpha_momentum.py

import pandas as pd
import numpy as np
import logging
import math
from db.connection import fetch_df, get_db_connection
from strategy.mainline.config import CONCEPT_MAPPING, CATEGORY_WEIGHTS
from strategy.recommend.plugins.base import BaseStrategyPlugin
from strategy.mainline import MainlineAnalyst

logger = logging.getLogger(__name__)

class AlphaMomentumStrategy(BaseStrategyPlugin):
    """
    Alpha 动量增强选股插件 (Alpha Momentum Strategy)
    
    核心逻辑：
    结合"价格动量 (Momentum)"和"业绩稳定性 (Sharpe)"进行选股，聚焦于强势主线中的龙头标的。
    
    选股因子：
    1. RPS (Relative Price Strength): 股价相对强度，要求年线(250日) RPS > 90 或月线(20日) RPS > 85。
    2. Sharpe Ratio: 60日夏普比率，衡量收益风险比，优先选择稳健上涨的个股。
    3. Bias (乖离率): 限制 20日乖离率在 -2% 到 10% 之间，避免追高。
    4. Sector Alignment: 必须属于当前市场的主线板块。
    """
    def __init__(self, concept_mapping=None, category_weights=None):
        self.concept_mapping = concept_mapping if concept_mapping else CONCEPT_MAPPING
        self.category_weights = category_weights if category_weights else CATEGORY_WEIGHTS
        self.negative_mapping = {}
        # 用于自动寻找主线
        self.mainline_analyst = MainlineAnalyst(self.concept_mapping, self.category_weights)

    @property
    def name(self):
        return "alpha_momentum"

    def run(self, target_date=None, concept=None, **kwargs):
        """
        执行选股流程
        
        参数:
        - target_date: 选股日期 (默认最新)
        - concept: 指定主线板块 (如果为空，则自动计算当日最强主线)
        """
        # 1. 确定日期
        if not target_date:
            with get_db_connection() as con:
                res = con.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()
                target_date = res[0] if res else None
        
        if not target_date:
            return {"status": "error", "message": "数据库中无行情数据"}

        # 2. 自动匹配主线 (如果未指定)
        if not concept:
            mainlines = self.mainline_analyst.analyze(days=3, limit=1)
            if mainlines:
                concept = mainlines[0]['name']

        # 3. 执行选股
        stocks = self.recommend(target_date, concept=concept)
        
        # 4. 封装结果
        return {
            "status": "success",
            "date": str(target_date),
            "strategy": self.name,
            "mainline": concept,
            "data": stocks,
            "conclusion": f"【Alpha 动量】当前市场主线聚焦于「{concept}」，已筛选出动量最强标的。" if concept else "当前市场暂无明显主线。"
        }

    def _get_stock_primary_sectors(self, ts_codes: list) -> pd.DataFrame:
        """ 
        基于多维概念共识算法判定股票的主行业 
        (复用 MainlineAnalyst 的逻辑，确保归类一致性)
        """
        if not ts_codes: return pd.DataFrame(columns=['ts_code', 'primary_sector'])
        
        query = f"SELECT ts_code, concept_name FROM stock_concept_details WHERE ts_code IN {str(tuple(ts_codes)).replace(',)', ')')}"
        df_concepts = fetch_df(query)
        if df_concepts.empty: return pd.DataFrame(columns=['ts_code', 'primary_sector'])

        records = []
        for concept in df_concepts['concept_name'].unique():
            for sector, keywords in self.concept_mapping.items():
                weight = self.category_weights.get(sector, 1.0)
                match_score = 0
                if sector in concept: match_score = len(sector) * 1.5
                
                max_kw_len = 0
                for kw in keywords:
                    if kw in concept:
                        max_kw_len = max(max_kw_len, len(kw))
                
                final_score = max(match_score, max_kw_len) * weight
                if final_score > 0:
                    records.append({'concept_name': concept, 'sector': sector, 'score': final_score})
        
        if not records: return pd.DataFrame(columns=['ts_code', 'primary_sector'])
        df_scores = pd.DataFrame(records)
        df_merged = df_concepts.merge(df_scores, on='concept_name')
        df_stock_sector = df_merged.groupby(['ts_code', 'sector'])['score'].sum().reset_index()
        df_stock_sector = df_stock_sector.sort_values(['ts_code', 'score'], ascending=[True, False])
        df_primary = df_stock_sector.drop_duplicates(subset=['ts_code']).rename(columns={'sector': 'primary_sector'})
        
        return df_primary[['ts_code', 'primary_sector']]

    def recommend(self, target_date, concept=None):
        """
        核心推荐算法实现
        """
        target_date_str = str(target_date)
        
        # SQL 查询：计算基础因子 (RPS, MA, Volatility)
        query = """
        WITH Analysis AS (
            SELECT 
                d.ts_code, d.trade_date, d.close, d.pct_chg, d.amount, d.vol,
                CAST(json_extract(d.factors, '$.rps_250') AS DOUBLE) as rps_250,
                CAST(json_extract(d.factors, '$.rps_20') AS DOUBLE) as rps_20,
                CAST(json_extract(d.factors, '$.ma20') AS DOUBLE) as ma20,
                AVG(d.pct_chg) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as avg_ret_60,
                STDDEV(d.pct_chg) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as vol_60
            FROM daily_price d
            WHERE d.trade_date >= CAST(? AS DATE) - INTERVAL 90 DAY
        )
        SELECT 
            a.ts_code, b.name, b.industry, a.close, a.pct_chg, a.rps_250, a.rps_20,
            a.avg_ret_60, a.vol_60, ((a.close - a.ma20) / a.ma20 * 100) as bias_20
        FROM Analysis a
        JOIN stock_basic b ON a.ts_code = b.ts_code
        WHERE a.trade_date = ? AND a.close > 0 AND (a.rps_250 > 80 OR a.rps_20 > 85)
        """
        
        final_query = query.replace("CAST(? AS DATE) - INTERVAL 90 DAY", f"CAST('{target_date_str}' AS DATE) - INTERVAL 90 DAY")
        
        # 概念过滤逻辑
        if concept:
            keywords = self.concept_mapping.get(concept, [concept])
            like_clauses = " OR ".join(["concept_name LIKE ?"] * len(keywords))
            neg_clauses = ""
            neg_params = []
            if concept in self.negative_mapping:
                neg_clauses = " AND " + " AND ".join(["concept_name NOT LIKE ?"] * len(self.negative_mapping[concept]))
                neg_params = [f"%{nk}%" for nk in self.negative_mapping[concept]]

            c_query = f"SELECT DISTINCT ts_code FROM stock_concept_details WHERE ({like_clauses}) {neg_clauses}"
            c_df = fetch_df(c_query, params=[f"%{k}%" for k in keywords] + neg_params)
            if not c_df.empty:
                final_query += f" AND a.ts_code IN {str(tuple(c_df['ts_code'].tolist())).replace(',)', ')')}"
            else:
                return []

        try:
            df = fetch_df(final_query, params=[target_date_str])
            if df.empty: return []

            ts_codes = df['ts_code'].tolist()
            # 再次确认主行业归属
            df_sectors = self._get_stock_primary_sectors(ts_codes)
            df = df.merge(df_sectors, on='ts_code', how='left')
            
            if concept:
                df = df[df['primary_sector'] == concept]

            results = []
            sqrt_250 = math.sqrt(250)
            
            for _, row in df.iterrows():
                # 计算年化夏普比率
                sharpe = (row['avg_ret_60'] / row['vol_60']) * sqrt_250 if row['vol_60'] > 0 else 0
                
                # 综合打分公式
                score = 0
                # 过滤条件：RPS 极强 (Top 10%) 且 乖离率适中 (不追高)
                if row['rps_250'] > 90 and -2 <= row['bias_20'] <= 10:
                    # 评分权重：Sharpe (稳定性) > RPS (爆发力)
                    score = row['rps_250'] * 0.6 + sharpe * 10
                
                if score > 0:
                    results.append({
                        "ts_code": row['ts_code'],
                        "name": row['name'],
                        "industry": row['primary_sector'] if not pd.isna(row['primary_sector']) else row['industry'],
                        "score": round(float(score), 2),
                        "close": row['close'],
                        "pct_chg": row['pct_chg'],
                        "sharpe": round(sharpe, 2)
                    })
            
            # 返回得分最高的 Top 12
            return sorted(results, key=lambda x: x['score'], reverse=True)[:12]
        except Exception as e:
            logger.error(f"Alpha recommendation error: {e}")
            return []
