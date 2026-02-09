# /backend/strategy/mainline/analyst.py

import pandas as pd
import numpy as np
import logging
from db.connection import fetch_df, fetch_df_read_only, get_db_connection
from .config import CONCEPT_MAPPING, CATEGORY_WEIGHTS

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

    def _get_mapped_concept(self, original_concept: str) -> str:
        """
        [DEPRECATED in V9] 以前是针对单一概念名进行映射。
        现在改用 _get_stock_primary_sector 进行多维共识判定。
        保留此方法用于向后兼容或初步筛选。
        """
        best_category = original_concept
        max_score = 0.0
        
        for target, keywords in self.concept_mapping.items():
            current_score = 0.0
            weight = self.category_weights.get(target, 1.0)
            
            if target in original_concept:
                current_score = len(target) * weight * 1.5
            
            matched_len = 0
            for k in keywords:
                if k in original_concept:
                    if len(k) > matched_len:
                        matched_len = len(k)
            
            if matched_len > 0:
                current_score = max(current_score, matched_len * weight)
            
            if current_score > max_score:
                max_score = current_score
                best_category = target
                
        return best_category if max_score > 0 else original_concept

    def _identify_stock_sectors(self, df_concepts: pd.DataFrame) -> pd.DataFrame:
        """
        [V9 核心算法] 基于概念共识的股票行业聚类。
        
        背景：一只股票可能同时属于"光伏"、"HJT电池"、"半导体"等多个概念。
        目标：确定该股票当前最核心的"主行业" (Primary Sector)。
        
        算法逻辑：
        1. 输入：股票及其所属的所有概念列表。
        2. 打分：遍历每个概念，如果它属于某个配置的大板块 (Sector)，则根据关键词匹配度和板块权重进行打分。
        3. 聚合：将同一股票在同一 Sector 下的所有得分累加。
           例如：协鑫集成
           - 概念"光伏概念" -> 归属 Sector "新能源" (得分 2)
           - 概念"太阳能"   -> 归属 Sector "新能源" (得分 3)
           - 概念"集成电路" -> 归属 Sector "半导体" (得分 2)
           - 结果：新能源(5) > 半导体(2)，故主行业判定为"新能源"。
        4. 决策：取得分最高的 Sector 作为该股票的 Primary Sector。
        
        输入：包含 ts_code, concept_name 的 DataFrame
        输出：ts_code, primary_sector 的映射表
        """
        # 1. 为每个概念打分 (Score per Concept-Sector pair)
        records = []
        for concept in df_concepts['concept_name'].unique():
            for sector, keywords in self.concept_mapping.items():
                weight = self.category_weights.get(sector, 1.0)
                # 关键词匹配度计算
                match_score = 0
                if sector in concept: match_score = len(sector) * 1.5
                
                max_kw_len = 0
                for kw in keywords:
                    if kw in concept:
                        max_kw_len = max(max_kw_len, len(kw))
                
                final_score = max(match_score, max_kw_len) * weight
                if final_score > 0:
                    records.append({'concept_name': concept, 'sector': sector, 'score': final_score})
        
        if not records:
            return pd.DataFrame(columns=['ts_code', 'primary_sector'])
            
        df_scores = pd.DataFrame(records)
        
        # 2. 将分数合并回股票数据
        df_merged = df_concepts.merge(df_scores, on='concept_name')
        
        # 3. 聚合：计算每只股票在每个 Sector 下的累积得分
        df_stock_sector = df_merged.groupby(['ts_code', 'sector'])['score'].sum().reset_index()
        
        # 4. 取最高分作为主行业
        df_stock_sector = df_stock_sector.sort_values(['ts_code', 'score'], ascending=[True, False])
        df_primary = df_stock_sector.drop_duplicates(subset=['ts_code']).rename(columns={'sector': 'primary_sector'})
        
        return df_primary[['ts_code', 'primary_sector']]

    def analyze(self, days=3, limit=5, trade_date: str = None):
        """
        主线分析入口函数。
        
        参数:
        - days: 分析的时间窗口 (默认最近 3 个交易日，用于寻找近期日期，实际计算通常聚焦于单日或短期窗口)。
        - limit: 返回排名靠前的主线数量。
        - trade_date: 指定分析日期。
        
        返回:
        - 主线分析结果列表，包含板块名、评分、涨停数、龙头股等。
        """
        # 1. 确定分析的时间范围 (Min Date, Max Date)
        if trade_date:
            min_date = trade_date
            max_date = trade_date
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
        # 1. 先获取最近的 N 个交易日 (排除非交易日)
        # 避免参数化查询在某些 DuckDB 版本下的潜在问题，且 days 是受控的 int
        date_query = f"SELECT trade_date FROM daily_price GROUP BY trade_date HAVING COUNT(*) > 1000 ORDER BY trade_date DESC LIMIT {int(days)}"
        try:
            # 直接使用 fetch_df 确保获取全局连接
            dates_df = fetch_df(date_query)
            if dates_df.empty:
                logger.warning("get_history: dates_df is empty")
                return {"dates": [], "series": [], "analysis": {"phase": "No_Data", "deduction": "数据库中无行情数据"}}
            
            # 转换为日期字符串列表
            recent_dates = sorted(dates_df['trade_date'].tolist())
            min_date = recent_dates[0].strftime('%Y-%m-%d')
            max_date = recent_dates[-1].strftime('%Y-%m-%d')
            
            # 2. 获取期间板块聚合数据
            query = f"""
            WITH ValidDates AS (
                SELECT trade_date 
                FROM daily_price 
                WHERE trade_date >= '{min_date}' AND trade_date <= '{max_date}'
                GROUP BY trade_date 
                HAVING COUNT(*) > 1000
            )
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
            JOIN ValidDates v ON d.trade_date = v.trade_date
            GROUP BY 1, 2
            HAVING stock_count >= 5
            """
            df = fetch_df(query)
            
            if df.empty: 
                logger.warning(f"get_history: df is empty for {min_date} to {max_date}")
                return {
                    "dates": [d.strftime('%m-%d') for d in recent_dates], 
                    "series": [], 
                    "analysis": { "phase": "No_Data", "deduction": "当前周期内无符合标准的板块共振数据。" }
                }
            
            # 基础数据处理
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['date_str'] = df['trade_date'].dt.strftime('%m-%d')
            
            # 优化映射速度
            unique_concepts = df['concept_name'].unique()
            mapping_dict = {c: self._get_mapped_concept(c) for c in unique_concepts}
            df['mapped_name'] = df['concept_name'].map(mapping_dict)
            
            # V8 评分计算
            df['lu_ratio'] = df['limit_ups'] / df['stock_count']
            df['breadth'] = df['up_count'] / df['stock_count']
            df['strong_ratio'] = df['strong_count'] / df['stock_count']
            
            df['score'] = (
                df['avg_ret'] * 2.0 + 
                df['lu_ratio'] * 80.0 + 
                df['breadth'] * 20.0
            ).fillna(0) # 填充可能存在的 NaN
            
            # 共振过滤：大幅惩罚无板块效应的概念 (放宽阈值)
            mask_weak = (df['breadth'] < 0.1) & (df['strong_ratio'] < 0.03)
            df.loc[mask_weak, 'score'] *= 0.3
            
            # 聚合到 mapped_name
            # 这里先生成包含所有数据的完整聚合表，用于后续查分
            df_full_agg = df.groupby(['trade_date', 'mapped_name']).agg({
                'score': 'max', 
                'limit_ups': 'sum', # 汇总大类的涨停数
                'strong_count': 'sum' # 汇总大类的强势股数
            }).reset_index()

            # 计算排名并过滤出 Top 10 用于确定"谁是主线"
            df_ranked = df_full_agg.copy()
            df_ranked['rnk'] = df_ranked.groupby('trade_date')['score'].rank(method='first', ascending=False)
            df_top10 = df_ranked[df_ranked['rnk'] <= 10].sort_values('trade_date')
            
            # 时间轴处理
            unique_dates = sorted(df_full_agg['trade_date'].unique())
            dates = [pd.to_datetime(d).strftime('%m-%d') for d in unique_dates]
            
            # 提取前8大主线 (基于 Top 10 数据集的总影响力，避免长尾噪音干扰主线选择)
            concept_influence = df_top10.groupby('mapped_name')['score'].sum().sort_values(ascending=False).head(8).index.tolist()

            # --- 优化后的龙头股提取逻辑 (覆盖全量 30 日日期) ---
            if not concept_influence or not unique_dates:
                 return {
                    "dates": dates, 
                    "series": [], 
                    "analysis": { "phase": "Chaos", "deduction": "近期市场极度弱势，无符合共振标准的主线板块。" }
                }

            # 关键修复：确保 min_date 覆盖 30 天
            min_date = pd.to_datetime(unique_dates[0]).strftime('%Y-%m-%d')
            max_date = pd.to_datetime(unique_dates[-1]).strftime('%Y-%m-%d')
            
            # 仅查询 Top 8 概念且涨幅 > 4% 的股票
            stock_query = """
            SELECT d.trade_date, c.concept_name, c.ts_code, c.name as stock_name, d.pct_chg
            FROM daily_price d
            JOIN stock_concept_details c ON d.ts_code = c.ts_code
            WHERE d.trade_date >= ? AND d.trade_date <= ? AND d.pct_chg > 4.0
            """
            stock_df_raw = fetch_df_read_only(stock_query, params=[min_date, max_date])
            stock_df_raw['trade_date'] = pd.to_datetime(stock_df_raw['trade_date'])
            
            # [V9 优化] 执行多维行业判定
            df_stock_sectors = self._identify_stock_sectors(stock_df_raw)
            stock_df = stock_df_raw.merge(df_stock_sectors, on='ts_code')
            
            # 允许一只标的出现在多个相关板块中，但其主行业必须与判定的一致
            top_stocks_map = {}
            
            for (d_date, d_sector), group in stock_df.groupby(['trade_date', 'primary_sector']):
                if d_sector not in concept_influence: continue
                
                # 取涨幅前 5 的标的
                unique_stocks = group.drop_duplicates(subset=['stock_name']).nlargest(5, 'pct_chg')
                
                stocks_list = [
                    {"name": r['stock_name'], "pct_chg": round(float(r['pct_chg']), 2)} 
                    for _, r in unique_stocks.iterrows()
                ]
                
                d_str = d_date.strftime('%m-%d')
                top_stocks_map[f"{d_str}_{d_sector}"] = stocks_list
            
            
            # 3. 组装返回数据
            series_data = []
            current_best_concept = "混沌"
            current_best_score = 0
            current_reason = "市场无明显主线"

            for c in concept_influence:
                data = []
                for d_obj in unique_dates:
                    d_obj = pd.to_datetime(d_obj)
                    d_str = d_obj.strftime('%m-%d')
                    
                    # 关键修改：从 df_full_agg (全量数据) 中查询分数，而不是 filtered 的 df_top10
                    row = df_full_agg[(df_full_agg['trade_date'] == d_obj) & (df_full_agg['mapped_name'] == c)]
                    
                    if not row.empty:
                        val = round(float(row['score'].values[0]), 2)
                        lu_sum = int(row['limit_ups'].iloc[0])
                        # 更新最新日期的最强主线判断
                        if d_obj == unique_dates[-1] and val > current_best_score:
                            current_best_score = val
                            current_best_concept = c
                            current_reason = f"板块爆发，当日涨停{lu_sum}家，资金共振强烈。"
                    else:
                        val = 0
                        
                    data.append({
                        "value": val, 
                        "top_stocks": top_stocks_map.get(f"{d_str}_{c}", [])
                    })
                series_data.append({"name": c, "data": data})

            return {
                "dates": dates,
                "series": series_data,
                "analysis": { 
                    "phase": "Quant_V8_Resonance", 
                    "deduction": f"【核心主线：{current_best_concept}】\n评分：{current_best_score}\n依据：{current_reason}" 
                }
            }
        except Exception as e:
            logger.error(f"HotConcept history plugin error: {e}")
            return {"dates": [], "series": [], "analysis": {}}

# 导出单例
mainline_analyst = MainlineAnalyst()