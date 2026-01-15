"""
T-4 级联火箭模型核心算法
实现4级过滤逻辑：特征提取 -> 硬约束 -> 打分 -> AI修正
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import date
import logging
from sqlalchemy import text
from db.database import get_raw_connection
from etl.trade_date_adapter import TradeDateAdapter

logger = logging.getLogger(__name__)


class AlphaModelT4:
    """T-4级联火箭模型"""
    
    # 常量定义
    MIN_HISTORY_DAYS = 90  # 最少需要90天历史数据
    HISTORY_DAYS_FOR_FACTORS = 90  # 用于计算因子的历史数据天数
    STOP_LOSS_RATIO = 0.93  # 止损比例（93%）
    DEFAULT_TOP_N = 5  # 默认返回肥羊数量
    
    def __init__(self):
        self.model_version = "T4_Model"
    
    def level1_extract_features(self, trade_date: date) -> pd.DataFrame:
        """
        第1级：离线特征提取（ETL Layer）
        计算高维因子：VCP、均线粘合度、主力潜伏、套牢盘等
        优化：分步查询，避免复杂窗口函数，提升性能
        注意：不限制数量，处理全部数据，通过索引优化查询性能
        """
        try:
            with get_raw_connection() as conn:
                # 步骤1：快速获取每个肥羊的最新交易日数据（不限制数量，处理全部数据）
                # 使用索引优化：idx_sheep_code_date 用于 GROUP BY 和 JOIN
                query1 = """
                    SELECT 
                        sd.sheep_code,
                        sd.trade_date,
                        sd.close_price,
                        sd.high_price,
                        sd.low_price,
                        sd.open_price,
                        sd.volume,
                        sd.amount,
                        sd.turnover_rate,
                        sd.change_pct,
                        sd.ma5,
                        sd.ma10,
                        sd.ma20,
                        sd.ma30,
                        sd.ma60,
                        sb.sheep_name,
                        sb.list_date,
                        sb.industry
                    FROM (
                        SELECT sheep_code, MAX(trade_date) as max_date
                        FROM sheep_daily USE INDEX (idx_stock_code_date)
                        WHERE trade_date <= %s
                        GROUP BY sheep_code
                        HAVING COUNT(*) >= %s
                    ) latest
                    INNER JOIN sheep_daily sd 
                        ON sd.sheep_code = latest.sheep_code AND sd.trade_date = latest.max_date
                    INNER JOIN sheep_basic sb ON sd.sheep_code = sb.sheep_code
                    WHERE sb.is_active = 1
                """
                
                df_latest = pd.read_sql(query1, conn, params=[trade_date, self.MIN_HISTORY_DAYS])
                
                if df_latest.empty:
                    logger.warning(f"Level 1: 无法获取最新数据 (trade_date={trade_date})")
                    return pd.DataFrame()
                
                logger.info(f"Level 1: 获取到 {len(df_latest)} 只肥羊的最新数据")
                
                # 步骤2：只对有效肥羊获取历史数据（最近90天，用于计算向量因子）
                valid_sheep = df_latest['sheep_code'].tolist()
                if not valid_sheep:
                    return pd.DataFrame()
                
                # 不再限制肥羊数量，处理全部数据
                
                # 获取最近90天的历史数据（用于计算向量因子：vol_ma5、vcp等）
                # 使用索引优化：idx_sheep_code_date 用于肥羊代码和日期范围查询
                # 对于大量肥羊，分批查询以提高性能，避免IN子句过长导致查询计划不佳
                batch_size = 500  # 每批处理500只肥羊
                df_history_list = []
                
                for i in range(0, len(valid_sheep), batch_size):
                    batch_sheep = valid_sheep[i:i + batch_size]
                    sheep_placeholders = ','.join(['%s'] * len(batch_sheep))
                    query2 = f"""
                        SELECT 
                            sheep_code,
                            trade_date,
                            close_price,
                            high_price,
                            low_price,
                            volume
                        FROM sheep_daily USE INDEX (idx_stock_code_date)
                        WHERE sheep_code IN ({sheep_placeholders})
                          AND trade_date <= %s
                          AND trade_date >= DATE_SUB(%s, INTERVAL %s DAY)
                        ORDER BY sheep_code, trade_date
                    """
                    
                    batch_history = pd.read_sql(query2, conn, params=batch_sheep + [trade_date, trade_date, self.HISTORY_DAYS_FOR_FACTORS])
                    if not batch_history.empty:
                        df_history_list.append(batch_history)
                
                # 合并所有批次的结果
                if df_history_list:
                    df_history = pd.concat(df_history_list, ignore_index=True)
                else:
                    df_history = pd.DataFrame()
                
                # 步骤3：使用pandas向量化计算因子（比SQL窗口函数快）
                if df_history.empty:
                    logger.warning("Level 1: 无法获取历史数据，因子计算将使用默认值")
                    df_latest['vol_ma_5'] = df_latest['volume']
                    df_latest['vcp_factor'] = None
                    df_latest['rps_250'] = None
                    return df_latest.reset_index(drop=True)
                
                # 按肥羊分组计算因子（向量化优化）
                # 确保按日期排序
                df_history = df_history.sort_values(['sheep_code', 'trade_date'])
                
                # 获取唯一肥羊数量，用于进度日志
                unique_stocks = df_history['sheep_code'].nunique()
                logger.info(f"Level 1: 开始计算 {unique_stocks} 只肥羊的因子")
                
                # 使用pandas的transform和agg进行向量化计算
                factors_list = []
                processed_count = 0
                
                for sheep_code, group in df_history.groupby('sheep_code'):
                    processed_count += 1
                    # 每处理100只肥羊记录一次进度
                    if processed_count % 100 == 0:
                        logger.info(f"Level 1: 因子计算进度 {processed_count}/{unique_stocks}")
                    
                    group = group.sort_values('trade_date').reset_index(drop=True)
                    factor_dict = {'sheep_code': sheep_code}
                    
                    # 计算5日均量（使用tail更高效）
                    if len(group) >= 5:
                        factor_dict['vol_ma_5'] = group['volume'].tail(5).mean()
                    elif len(group) > 0:
                        factor_dict['vol_ma_5'] = group['volume'].mean()
                    else:
                        factor_dict['vol_ma_5'] = None
                    
                    # 计算20日均量
                    if len(group) >= 20:
                        factor_dict['vol_ma_20'] = group['volume'].tail(20).mean()
                    elif len(group) > 0:
                        factor_dict['vol_ma_20'] = group['volume'].mean()
                    else:
                        factor_dict['vol_ma_20'] = None
                    
                    # 计算VCP（最近20日）
                    if len(group) >= 20:
                        recent_20 = group.tail(20)
                        high_max = recent_20['high_price'].max()
                        low_min = recent_20['low_price'].min()
                        close_mean = recent_20['close_price'].mean()
                        factor_dict['vcp_factor'] = (high_max - low_min) / close_mean if close_mean > 0 else None
                    else:
                        factor_dict['vcp_factor'] = None
                    
                    # RPS需要250天数据
                    if len(group) >= 250:
                        price_250d_ago = group.iloc[0]['close_price']
                        current_price = group.iloc[-1]['close_price']
                        factor_dict['rps_250'] = (current_price / price_250d_ago - 1) * 100 if price_250d_ago and price_250d_ago > 0 else None
                    else:
                        factor_dict['rps_250'] = None
                    
                    factors_list.append(factor_dict)
                
                logger.info(f"Level 1: 因子计算完成，共处理 {processed_count} 只肥羊")
                
                # 转换为DataFrame（比apply快）
                factors = pd.DataFrame(factors_list)
                
                # 合并因子到最新数据
                df_latest = df_latest.merge(factors, on='sheep_code', how='left')
                
                # 填充缺失值
                df_latest['vol_ma_5'] = df_latest['vol_ma_5'].fillna(df_latest['volume'])
                df_latest['vol_ma_20'] = df_latest['vol_ma_20'].fillna(df_latest['volume'])
                df_latest['vcp_factor'] = df_latest['vcp_factor'].fillna(1.0)
                df_latest['rps_250'] = df_latest['rps_250'].fillna(0)
                
                # 计算量比因子：Volume / MA20_Volume（避免除零）
                df_latest['vol_ratio_ma20'] = np.where(
                    (df_latest['vol_ma_20'].notna()) & (df_latest['vol_ma_20'] > 0),
                    df_latest['volume'] / df_latest['vol_ma_20'],
                    1.0  # 如果MA20为0或None，默认量比为1.0
                )
                df_latest['vol_ratio_ma20'] = df_latest['vol_ratio_ma20'].fillna(1.0)
                
                # 计算上影线比例：(High - Close) / (High - Low)，用于判断是否为光头阳线
                price_range = df_latest['high_price'] - df_latest['low_price']
                upper_shadow = df_latest['high_price'] - df_latest['close_price']
                df_latest['upper_shadow_ratio'] = np.where(
                    price_range > 0,
                    upper_shadow / price_range,
                    0
                )
                
                # 识别板块类型
                df_latest['is_star_market'] = df_latest['sheep_code'].str.startswith('688').astype(int)  # 科创板（688开头）
                df_latest['is_gem'] = df_latest['sheep_code'].str.startswith('300').astype(int)  # 创业板（300开头）
                
                logger.info(f"Level 1: 成功提取 {len(df_latest)} 只肥羊的特征")
                
                return df_latest.reset_index(drop=True)
                
        except Exception as e:
            logger.error(f"Level 1特征提取失败: {e}", exc_info=True)
            import traceback
            logger.error(f"Level 1异常详情:\n{traceback.format_exc()}")
            return pd.DataFrame()
    
    def level2_dynamic_filter(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        """
        第2级：动态硬约束漏斗
        根据用户参数过滤肥羊池
        新增优化：
        1. 涨幅阈值 >= 7%（适配科创板20%涨跌幅限制）
        2. 量比因子：Volume / MA20_Volume > 2.5（确保爆量形态）
        3. K线形态：上影线比例 < 0.1（光头阳线或接近光头阳线）
        """
        if df.empty:
            return df
        
        filtered_df = df.copy()
        
        # 1. 涨幅阈值筛选：>= 7%
        # 科创板（688开头）涨跌幅限制为20%，7%仍然适用
        min_change_pct = params.get('min_change_pct', 7.0)  # 默认7%
        if 'change_pct' in filtered_df.columns:
            change_mask = filtered_df['change_pct'].notna()
            filtered_df = filtered_df[change_mask & (filtered_df['change_pct'] >= min_change_pct)]
            logger.info(f"Level 2: 涨幅阈值筛选 (>= {min_change_pct}%)，剩余 {len(filtered_df)} 只肥羊")
        else:
            logger.warning("Level 2: change_pct字段不存在，跳过涨幅筛选")
        
        # 2. 量比因子筛选：Volume / MA20_Volume > 2.5
        vol_ratio_threshold = params.get('vol_ratio_ma20_threshold', 2.5)  # 默认2.5
        if 'vol_ratio_ma20' in filtered_df.columns:
            vol_ratio_mask = filtered_df['vol_ratio_ma20'].notna()
            filtered_df = filtered_df[vol_ratio_mask & (filtered_df['vol_ratio_ma20'] > vol_ratio_threshold)]
            logger.info(f"Level 2: 量比因子筛选 (Volume/MA20 > {vol_ratio_threshold})，剩余 {len(filtered_df)} 只肥羊")
        else:
            logger.warning("Level 2: vol_ratio_ma20字段不存在，跳过量比筛选")
        
        # 3. K线形态约束：上影线比例 < 0.1（确保是光头阳线或接近光头阳线）
        max_upper_shadow_ratio = params.get('max_upper_shadow_ratio', 0.1)  # 默认0.1（10%）
        if 'upper_shadow_ratio' in filtered_df.columns:
            shadow_mask = filtered_df['upper_shadow_ratio'].notna()
            filtered_df = filtered_df[shadow_mask & (filtered_df['upper_shadow_ratio'] <= max_upper_shadow_ratio)]
            logger.info(f"Level 2: K线形态筛选 (上影线比例 <= {max_upper_shadow_ratio})，剩余 {len(filtered_df)} 只肥羊")
        else:
            logger.warning("Level 2: upper_shadow_ratio字段不存在，跳过K线形态筛选")
        
        # 市值门槛（需要从外部获取市值数据，这里先跳过）
        # min_mv = params.get('min_mv', 50)  # 50亿
        # max_mv = params.get('max_mv', 300)  # 300亿
        
        # 趋势护城河：股价 > MA(N)（处理MA为None的情况）
        ma_support = params.get('ma_support', 'MA60')  # MA20/MA60/MA120
        if ma_support == 'MA20':
            # 只过滤MA20不为None的肥羊
            ma20_mask = filtered_df['ma20'].notna()
            filtered_df = filtered_df[ma20_mask & (filtered_df['close_price'] > filtered_df['ma20'])]
        elif ma_support == 'MA60':
            # 只过滤MA60不为None的肥羊
            ma60_mask = filtered_df['ma60'].notna()
            filtered_df = filtered_df[ma60_mask & (filtered_df['close_price'] > filtered_df['ma60'])]
        elif ma_support == 'MA120':
            # 需要计算MA120，这里先跳过
            pass
        
        # 负面剔除：上市<60天（处理list_date为None的情况）
        if 'list_date' in filtered_df.columns:
            # 只对list_date不为None的肥羊进行过滤
            has_list_date = filtered_df['list_date'].notna()
            filtered_df['days_since_list'] = None
            
            if has_list_date.any():
                # 确保日期列为datetime类型
                trade_date_col = pd.to_datetime(filtered_df.loc[has_list_date, 'trade_date'], errors='coerce')
                list_date_col = pd.to_datetime(filtered_df.loc[has_list_date, 'list_date'], errors='coerce')
                
                # 计算日期差（天）
                date_diff = trade_date_col - list_date_col
                filtered_df.loc[has_list_date, 'days_since_list'] = date_diff.dt.days
            
            # 保留list_date为None的肥羊（可能是数据缺失），只过滤明确上市<60天的
            filtered_df = filtered_df[
                filtered_df['days_since_list'].isna() | (filtered_df['days_since_list'] >= 60)
            ]
        
        # 剔除ST肥羊（名称包含ST）
        if 'sheep_name' in filtered_df.columns:
            filtered_df = filtered_df[~filtered_df['sheep_name'].str.contains('ST', na=False)]
        
        # 剔除涨停的肥羊
        # 主板/中小板：涨跌幅 >= 9.95%（接近10%涨停）
        # 创业板（300开头）和科创板（688开头）：涨跌幅 >= 19.95%（接近20%涨停）
        if 'change_pct' in filtered_df.columns and 'is_star_market' in filtered_df.columns and 'is_gem' in filtered_df.columns:
            change_mask = filtered_df['change_pct'].notna()
            
            # 主板/中小板涨停：>= 9.95%
            main_board_limit_up = (filtered_df['is_star_market'] == 0) & (filtered_df['is_gem'] == 0) & (filtered_df['change_pct'] >= 9.95)
            
            # 创业板和科创板涨停：>= 19.95%
            gem_star_limit_up = ((filtered_df['is_star_market'] == 1) | (filtered_df['is_gem'] == 1)) & (filtered_df['change_pct'] >= 19.95)
            
            # 剔除所有涨停的肥羊
            limit_up_mask = main_board_limit_up | gem_star_limit_up
            filtered_df = filtered_df[~limit_up_mask]
            
            logger.info(f"Level 2: 剔除涨停肥羊，剩余 {len(filtered_df)} 只肥羊")
        else:
            # 如果没有板块标识，使用保守策略：剔除 >= 9.95% 的
            if 'change_pct' in filtered_df.columns:
                change_mask = filtered_df['change_pct'].notna()
                filtered_df = filtered_df[change_mask & (filtered_df['change_pct'] < 9.95)]
                logger.info(f"Level 2: 剔除涨停肥羊（保守策略 >= 9.95%），剩余 {len(filtered_df)} 只肥羊")
        
        return filtered_df
    
    def level3_scoring_engine(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        """
        第3级：量价爆发打分引擎
        计算总分：爆发力 + 结构美感 + 板块协同
        """
        if df.empty:
            return df
        
        scored_df = df.copy()
        
        # 权重参数
        w_tech = params.get('w_tech', 0.4)  # 技术权重
        w_trend = params.get('w_trend', 0.4)  # 趋势权重
        w_hot = params.get('w_hot', 0.2)  # 热度权重
        
        # 1. 爆发力得分 (S_Explosion)
        vol_ratio = params.get('vol_ratio', 2.0)  # 倍量阈值
        vol_threshold = params.get('vol_threshold', 2.0)
        
        # 倍量攻击（处理vol_ma_5为None的情况）
        # 如果vol_ma_5为None或0，使用当日成交量作为基准（避免除零）
        scored_df['vol_ma_5'] = scored_df['vol_ma_5'].fillna(scored_df['volume'])
        # 使用mask方法替换0值，不能使用replace(0, Series)
        scored_df['vol_ma_5'] = scored_df['vol_ma_5'].mask(scored_df['vol_ma_5'] == 0, scored_df['volume'])
        scored_df['vol_ratio'] = scored_df['volume'] / scored_df['vol_ma_5']
        scored_df['vol_ratio'] = scored_df['vol_ratio'].fillna(1.0)  # 如果仍有NaN，设为1.0
        scored_df['vol_explosion'] = (scored_df['vol_ratio'] >= vol_threshold).astype(int) * 50
        
        # 口袋支点（当日量 > 过去10日最大阴量）- 简化处理
        scored_df['pocket_pivot'] = 0  # 需要历史数据，暂时简化
        
        s_explosion = scored_df['vol_explosion'] + scored_df['pocket_pivot']
        
        # 2. 结构美感得分 (S_Structure)
        rps_threshold = params.get('rps_threshold', 90)
        if 'rps_250' in scored_df.columns:
            # 处理rps_250为None的情况
            scored_df['rps_250'] = scored_df['rps_250'].fillna(0)
            scored_df['rps_score'] = (scored_df['rps_250'] >= rps_threshold).astype(int) * 30
        else:
            scored_df['rps_score'] = 0
        
        # VCP得分（波动率越低分越高）
        if 'vcp_factor' in scored_df.columns:
            # 处理vcp_factor为None的情况
            scored_df['vcp_factor'] = scored_df['vcp_factor'].fillna(1.0)  # 默认值1.0
            vcp_median = scored_df['vcp_factor'].median()
            if vcp_median > 0:
                scored_df['vcp_score'] = (1 - scored_df['vcp_factor'] / (vcp_median * 2)).clip(0, 1) * 20
            else:
                scored_df['vcp_score'] = 10  # 如果中位数为0，给默认分
        else:
            scored_df['vcp_score'] = 0
        
        s_structure = scored_df['rps_score'] + scored_df['vcp_score']
        
        # 3. 板块协同得分 (S_Sector)
        sector_boost = params.get('sector_boost', True)
        if sector_boost:
            # 获取板块热度（简化：从概念映射获取）
            scored_df['sector_score'] = 0  # 需要板块涨幅数据，暂时简化
        else:
            scored_df['sector_score'] = 0
        
        s_sector = scored_df['sector_score']
        
        # 计算基础总分
        scored_df['total_score'] = (
            w_tech * s_explosion +
            w_trend * s_structure +
            w_hot * s_sector
        )
        
        # 创业板和科创板权重适度提高（权重加成）
        # 为创业板和科创板添加额外的权重加成，提高它们被推荐的概率
        if 'is_star_market' in scored_df.columns and 'is_gem' in scored_df.columns:
            # 创业板和科创板权重加成系数（可配置，默认1.15，即提高15%）
            gem_star_weight_boost = params.get('gem_star_weight_boost', 1.15)
            
            # 识别创业板和科创板
            gem_star_mask = (scored_df['is_star_market'] == 1) | (scored_df['is_gem'] == 1)
            
            # 对创业板和科创板的总分进行加权加成
            scored_df.loc[gem_star_mask, 'total_score'] = scored_df.loc[gem_star_mask, 'total_score'] * gem_star_weight_boost
            
            gem_star_count = gem_star_mask.sum()
            if gem_star_count > 0:
                logger.info(f"Level 3: 创业板和科创板权重加成（{gem_star_weight_boost}x），共 {gem_star_count} 只肥羊获得加成")
        
        # 排序
        scored_df = scored_df.sort_values('total_score', ascending=False)
        
        return scored_df
    
    def level4_ai_enhancement(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        """
        第4级：AI概率修正
        使用LightGBM模型过滤假突破（简化版：基于规则）
        """
        if df.empty:
            return df
        
        ai_filter = params.get('ai_filter', True)
        enhanced_df = df.copy()
        
        # 计算胜率（简化规则，向量化操作）
        # 条件：换手率适中、量比合理、VCP低、RPS高
        # 处理NaN值
        enhanced_df['turnover_rate'] = enhanced_df['turnover_rate'].fillna(0)
        enhanced_df['vol_ratio'] = enhanced_df['vol_ratio'].fillna(1.0)
        
        conditions = (
            (enhanced_df['turnover_rate'] > 1.0) &  # 换手率 > 1%
            (enhanced_df['turnover_rate'] < 20.0) &  # 换手率 < 20%
            (enhanced_df['vol_ratio'] >= 1.5)  # 量比 >= 1.5
        )
        
        # 添加VCP条件（如果列存在且不为空）
        if 'vcp_factor' in enhanced_df.columns:
            enhanced_df['vcp_factor'] = enhanced_df['vcp_factor'].fillna(1.0)  # 默认值1.0
            conditions = conditions & (enhanced_df['vcp_factor'] < 0.3)
        
        enhanced_df['win_probability'] = np.where(conditions, 70, 40)  # 简化：符合条件70%，否则40%
        
        # 如果AI过滤开启，只保留胜率 >= 60% 的；否则保留所有（但仍计算胜率）
        if ai_filter:
            before_count = len(enhanced_df)
            enhanced_df = enhanced_df[enhanced_df['win_probability'] >= 60]
            after_count = len(enhanced_df)
            logger.info(f"Level 4 AI过滤: {before_count} -> {after_count} (胜率 >= 60%)")
            
            # 如果过滤后为空，放宽条件到50%
            if enhanced_df.empty and before_count > 0:
                logger.warning("Level 4: 胜率>=60%过滤后无数据，放宽到>=50%")
                enhanced_df = df.copy()  # 使用Level 3的结果
                # 重新计算胜率
                enhanced_df['turnover_rate'] = enhanced_df['turnover_rate'].fillna(0)
                enhanced_df['vol_ratio'] = enhanced_df['vol_ratio'].fillna(1.0)
                conditions = (
                    (enhanced_df['turnover_rate'] > 1.0) &
                    (enhanced_df['turnover_rate'] < 20.0) &
                    (enhanced_df['vol_ratio'] >= 1.5)
                )
                if 'vcp_factor' in enhanced_df.columns:
                    enhanced_df['vcp_factor'] = enhanced_df['vcp_factor'].fillna(1.0)
                    conditions = conditions & (enhanced_df['vcp_factor'] < 0.3)
                enhanced_df['win_probability'] = np.where(conditions, 70, 40)
                enhanced_df = enhanced_df[enhanced_df['win_probability'] >= 50]
        
        return enhanced_df
    
    def run_full_pipeline(self, trade_date: date, params: Dict, top_n: Optional[int] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        运行完整的T-4级联流程
        
        Returns:
            (推荐结果列表, 诊断信息)
        """
        diagnostic_info = []
        
        try:
            # Level 1: 特征提取
            logger.info(f"Level 1: 特征提取 (trade_date={trade_date})")
            df = self.level1_extract_features(trade_date)
            
            if df.empty:
                msg = "Level 1返回空数据 - 可能原因：数据库中没有足够的历史数据（需要至少90天）"
                logger.warning(msg)
                diagnostic_info.append(f"Level 1: 提取失败 - 数据库中没有 {trade_date} 之前至少90天的肥羊数据")
                return [], " | ".join(diagnostic_info)
            
            logger.info(f"Level 1: 成功提取 {len(df)} 只肥羊的特征")
            diagnostic_info.append(f"Level 1: 提取了 {len(df)} 只肥羊")
            
            # Level 2: 硬约束过滤
            logger.info(f"Level 2: 硬约束过滤 (过滤前: {len(df)} 只肥羊)")
            before_level2 = len(df)
            df = self.level2_dynamic_filter(df, params)
            after_level2 = len(df)
            logger.info(f"Level 2: 过滤后: {len(df)} 只肥羊")
            # 构建过滤条件描述
            filter_conditions = [
                f"涨幅>={params.get('min_change_pct', 7.0)}%",
                f"量比MA20>{params.get('vol_ratio_ma20_threshold', 2.5)}",
                f"上影线<={params.get('max_upper_shadow_ratio', 0.1)}",
                f"MA{params.get('ma_support', 'MA60')}",
                "上市>=60天",
                "排除ST"
            ]
            diagnostic_info.append(f"Level 2: {before_level2} -> {after_level2} 只肥羊 (过滤条件: {', '.join(filter_conditions)})")
            
            if df.empty:
                msg = f"Level 2过滤后无数据 - 可能原因：过滤条件太严格（涨幅>={params.get('min_change_pct', 7.0)}%、量比>{params.get('vol_ratio_ma20_threshold', 2.5)}、上影线限制、MA{params.get('ma_support', 'MA60')}、上市天数、ST肥羊等）"
                logger.warning(msg)
                diagnostic_info.append(f"Level 2: 所有肥羊被过滤 - 建议：放宽涨幅阈值、量比阈值或上影线限制")
                return [], " | ".join(diagnostic_info)
            
            # Level 3: 打分排序
            logger.info(f"Level 3: 打分排序 (当前: {len(df)} 只肥羊)")
            df = self.level3_scoring_engine(df, params)
            max_score = df['total_score'].max() if not df.empty else 0
            logger.info(f"Level 3: 打分完成，最高分: {max_score:.2f}")
            diagnostic_info.append(f"Level 3: {len(df)} 只肥羊，最高分 {max_score:.2f}")
            
            # Level 4: AI修正
            logger.info(f"Level 4: AI修正 (过滤前: {len(df)} 只肥羊)")
            before_level4 = len(df)
            df = self.level4_ai_enhancement(df, params)
            after_level4 = len(df)
            logger.info(f"Level 4: AI修正后: {len(df)} 只肥羊")
            ai_filter_enabled = params.get('ai_filter', True)
            diagnostic_info.append(f"Level 4: {before_level4} -> {after_level4} 只肥羊 (AI过滤: {'开启' if ai_filter_enabled else '关闭'})")
            
            if df.empty:
                msg = f"Level 4过滤后无数据 - 可能原因：AI过滤条件太严格（胜率要求 >= 60%）"
                logger.warning(msg)
                diagnostic_info.append(f"Level 4: 所有肥羊被过滤 - 建议：关闭AI过滤或降低胜率要求")
                return [], " | ".join(diagnostic_info)
            
            # 确保数据按total_score降序排序（即使level3已排序，这里再次确保）
            df = df.sort_values('total_score', ascending=False).reset_index(drop=True)
            
            # 取Top N（如果top_n为None，则返回所有符合条件的肥羊）
            if top_n is not None and top_n > 0:
                top_stocks = df.head(top_n).copy()
                logger.info(f"最终推荐: {len(top_stocks)} 只肥羊（限制Top {top_n}，按分数降序排序）")
            else:
                top_stocks = df.copy()
                logger.info(f"最终推荐: {len(top_stocks)} 只肥羊（全部符合条件的肥羊，按分数降序排序）")
            
            if top_stocks.empty:
                logger.warning("Top N肥羊为空，无法生成推荐结果")
                return []
            
            # 检查必要字段是否存在
            required_fields = ['sheep_code', 'sheep_name', 'close_price', 'total_score', 'win_probability', 'vol_ratio']
            missing_fields = [f for f in required_fields if f not in top_stocks.columns]
            if missing_fields:
                logger.error(f"缺少必要字段: {missing_fields}")
                logger.info(f"可用字段: {list(top_stocks.columns)}")
                return []
            
            # 向量化计算（完全避免apply和循环）
            vol_threshold = params.get('vol_threshold', 2.0)
            rps_threshold = params.get('rps_threshold', 90)
            
            # 准备数据列
            top_stocks = top_stocks.copy()
            top_stocks['entry_price'] = top_stocks['close_price'].astype(float)
            top_stocks['stop_loss_price'] = (top_stocks['entry_price'] * self.STOP_LOSS_RATIO).round(2)
            
            # 填充缺失值并确保类型
            vol_ratio_col = top_stocks.get('vol_ratio', pd.Series([0.0] * len(top_stocks), index=top_stocks.index)).fillna(0.0).astype(float)
            rps_250_col = top_stocks.get('rps_250', pd.Series([0.0] * len(top_stocks), index=top_stocks.index)).fillna(0.0).astype(float)
            vcp_factor_col = top_stocks.get('vcp_factor', pd.Series([1.0] * len(top_stocks), index=top_stocks.index)).fillna(1.0).astype(float)
            
            # 向量化生成推荐理由（使用列表推导，比apply快）
            def build_reasons(idx):
                reasons = []
                vol_ratio = vol_ratio_col.loc[idx] if idx in vol_ratio_col.index else 0.0
                rps_250 = rps_250_col.loc[idx] if idx in rps_250_col.index else 0.0
                vcp_factor = vcp_factor_col.loc[idx] if idx in vcp_factor_col.index else 1.0
                
                if vol_ratio >= vol_threshold:
                    reasons.append(f"倍量{vol_ratio:.1f}倍")
                if rps_250 >= rps_threshold:
                    reasons.append("RPS强势")
                if vcp_factor < 0.3:
                    reasons.append("VCP收敛")
                
                return " + ".join(reasons) if reasons else "技术突破"
            
            # 使用列表推导比apply快
            top_stocks['reason_tags'] = [build_reasons(idx) for idx in top_stocks.index]
            
            try:
                # 转换为字典列表（使用to_dict('records')最快）
                # 先选择需要的列，确保类型正确
                result_df = top_stocks[[
                    'sheep_code', 'sheep_name', 'entry_price', 'total_score',
                    'win_probability', 'reason_tags', 'stop_loss_price'
                ]].copy()
                
                # 添加可选列
                if 'vol_ratio' in top_stocks.columns:
                    result_df['vol_ratio'] = top_stocks['vol_ratio'].fillna(0.0).astype(float)
                else:
                    result_df['vol_ratio'] = 0.0
                
                if 'rps_250' in top_stocks.columns:
                    result_df['rps_250'] = top_stocks['rps_250'].where(pd.notna(top_stocks['rps_250']), None)
                else:
                    result_df['rps_250'] = None
                
                if 'vcp_factor' in top_stocks.columns:
                    result_df['vcp_factor'] = top_stocks['vcp_factor'].where(pd.notna(top_stocks['vcp_factor']), None)
                else:
                    result_df['vcp_factor'] = None
                
                # 重命名列以匹配API格式
                result_df = result_df.rename(columns={
                    'total_score': 'ai_score'
                })
                
                # 计算涨幅信息（根据推荐日期距离今天的交易日数量）
                # 获取今天的日期和推荐日期之间的交易日数量
                today = date.today()
                trading_days_between = TradeDateAdapter.get_trading_days_in_range(trade_date, today)
                trading_days_count = len(trading_days_between)
                
                # 交易日列表是从trade_date到today的升序列表
                # 例如：[2026-01-10, 2026-01-11, 2026-01-12, ..., 2026-01-15]
                # 要获取5天前：trading_days_between[trading_days_count - 5]
                # 要获取10天前：trading_days_between[trading_days_count - 10]
                
                # 批量获取涨幅信息（优化：避免循环查询数据库）
                sheep_codes_list = result_df['sheep_code'].tolist()
                return_data = {}  # {sheep_code: {'return_5d': ..., 'return_10d': ..., 'return_nd': ...}}
                
                if sheep_codes_list and trading_days_count > 0:
                    try:
                        with get_raw_connection() as conn:
                            # 批量获取最新价格
                            sheep_placeholders = ','.join(['%s'] * len(sheep_codes_list))
                            latest_prices_query = f"""
                                SELECT sheep_code, close_price, trade_date
                                FROM sheep_daily
                                WHERE sheep_code IN ({sheep_placeholders})
                                  AND trade_date <= %s
                                  AND (sheep_code, trade_date) IN (
                                      SELECT sheep_code, MAX(trade_date)
                                      FROM sheep_daily
                                      WHERE sheep_code IN ({sheep_placeholders})
                                        AND trade_date <= %s
                                      GROUP BY sheep_code
                                  )
                            """
                            latest_prices_df = pd.read_sql(
                                latest_prices_query, 
                                conn, 
                                params=sheep_codes_list + [today] + sheep_codes_list + [today]
                            )
                            
                            # 根据交易日数量决定计算哪些涨幅
                            # trading_days_between 是从 trade_date 到 today 的交易日列表（升序）
                            # 要获取5天前和10天前的日期，需要从列表末尾往前数
                            if trading_days_count >= 10:
                                # 早于10个交易日：计算5日涨幅和10日涨幅
                                # 从列表末尾往前数：-5表示倒数第5个交易日，-10表示倒数第10个交易日
                                date_5d_ago = trading_days_between[trading_days_count - 5] if trading_days_count >= 5 else None
                                date_10d_ago = trading_days_between[trading_days_count - 10] if trading_days_count >= 10 else None
                                
                                # 批量获取5天前和10天前的价格
                                if date_5d_ago:
                                    prices_5d_query = f"""
                                        SELECT sheep_code, close_price
                                        FROM sheep_daily
                                        WHERE sheep_code IN ({sheep_placeholders})
                                          AND trade_date = %s
                                    """
                                    prices_5d_df = pd.read_sql(
                                        prices_5d_query,
                                        conn,
                                        params=sheep_codes_list + [date_5d_ago]
                                    )
                                    prices_5d_dict = dict(zip(prices_5d_df['sheep_code'], prices_5d_df['close_price']))
                                else:
                                    prices_5d_dict = {}
                                
                                if date_10d_ago:
                                    prices_10d_query = f"""
                                        SELECT sheep_code, close_price
                                        FROM sheep_daily
                                        WHERE sheep_code IN ({sheep_placeholders})
                                          AND trade_date = %s
                                    """
                                    prices_10d_df = pd.read_sql(
                                        prices_10d_query,
                                        conn,
                                        params=sheep_codes_list + [date_10d_ago]
                                    )
                                    prices_10d_dict = dict(zip(prices_10d_df['sheep_code'], prices_10d_df['close_price']))
                                else:
                                    prices_10d_dict = {}
                                
                                # 计算涨幅
                                latest_prices_dict = dict(zip(latest_prices_df['sheep_code'], latest_prices_df['close_price']))
                                for sheep_code in sheep_codes_list:
                                    return_data[sheep_code] = {
                                        'return_5d': None,
                                        'return_10d': None,
                                        'return_nd': None
                                    }
                                    
                                    latest_price = latest_prices_dict.get(sheep_code)
                                    if latest_price is None or pd.isna(latest_price):
                                        continue
                                    latest_price = float(latest_price)
                                    
                                    # 计算5日涨幅
                                    if sheep_code in prices_5d_dict and prices_5d_dict[sheep_code] is not None and not pd.isna(prices_5d_dict[sheep_code]):
                                        price_5d = float(prices_5d_dict[sheep_code])
                                        if price_5d > 0:
                                            return_data[sheep_code]['return_5d'] = ((latest_price - price_5d) / price_5d) * 100
                                    
                                    # 计算10日涨幅
                                    if sheep_code in prices_10d_dict and prices_10d_dict[sheep_code] is not None and not pd.isna(prices_10d_dict[sheep_code]):
                                        price_10d = float(prices_10d_dict[sheep_code])
                                        if price_10d > 0:
                                            return_data[sheep_code]['return_10d'] = ((latest_price - price_10d) / price_10d) * 100
                            else:
                                # 10个交易日内的：计算5日涨幅和最近N天涨幅（N=trading_days_count）
                                # 从列表末尾往前数：-5表示倒数第5个交易日
                                date_5d_ago = trading_days_between[trading_days_count - 5] if trading_days_count >= 5 else None
                                
                                # 批量获取5天前的价格
                                if date_5d_ago:
                                    prices_5d_query = f"""
                                        SELECT sheep_code, close_price
                                        FROM sheep_daily
                                        WHERE sheep_code IN ({sheep_placeholders})
                                          AND trade_date = %s
                                    """
                                    prices_5d_df = pd.read_sql(
                                        prices_5d_query,
                                        conn,
                                        params=sheep_codes_list + [date_5d_ago]
                                    )
                                    prices_5d_dict = dict(zip(prices_5d_df['sheep_code'], prices_5d_df['close_price']))
                                else:
                                    prices_5d_dict = {}
                                
                                # 计算涨幅（最近N天涨幅 = 从推荐日期的entry_price到最新价格的涨幅）
                                latest_prices_dict = dict(zip(latest_prices_df['sheep_code'], latest_prices_df['close_price']))
                                entry_prices_dict = dict(zip(result_df['sheep_code'], result_df['entry_price']))
                                
                                for sheep_code in sheep_codes_list:
                                    return_data[sheep_code] = {
                                        'return_5d': None,
                                        'return_10d': None,
                                        'return_nd': None
                                    }
                                    
                                    latest_price = latest_prices_dict.get(sheep_code)
                                    entry_price = entry_prices_dict.get(sheep_code)
                                    
                                    if latest_price is None or pd.isna(latest_price) or entry_price is None or pd.isna(entry_price):
                                        continue
                                    
                                    latest_price = float(latest_price)
                                    entry_price = float(entry_price)
                                    
                                    # 计算5日涨幅
                                    if sheep_code in prices_5d_dict and prices_5d_dict[sheep_code] is not None and not pd.isna(prices_5d_dict[sheep_code]):
                                        price_5d = float(prices_5d_dict[sheep_code])
                                        if price_5d > 0:
                                            return_data[sheep_code]['return_5d'] = ((latest_price - price_5d) / price_5d) * 100
                                    
                                    # 计算最近N天涨幅（从推荐日期到最新日期）
                                    if entry_price > 0:
                                        return_data[sheep_code]['return_nd'] = ((latest_price - entry_price) / entry_price) * 100
                                
                    except Exception as e:
                        logger.warning(f"批量计算涨幅信息失败: {e}", exc_info=True)
                        # 如果批量计算失败，返回空的涨幅数据
                        for sheep_code in sheep_codes_list:
                            return_data[sheep_code] = {'return_5d': None, 'return_10d': None, 'return_nd': None}
                
                # 转换为字典列表（最快的方方法）
                results = result_df.to_dict('records')
                
                # 确保类型正确并添加涨幅信息
                for r in results:
                    r['sheep_code'] = str(r['sheep_code'])
                    r['sheep_name'] = str(r['sheep_name'])
                    r['entry_price'] = float(r['entry_price'])
                    r['ai_score'] = float(r['ai_score'])
                    r['win_probability'] = float(r['win_probability'])
                    r['reason_tags'] = str(r['reason_tags'])
                    r['stop_loss_price'] = float(r['stop_loss_price'])
                    r['vol_ratio'] = float(r['vol_ratio']) if r['vol_ratio'] is not None else 0.0
                    if r['rps_250'] is not None:
                        r['rps_250'] = float(r['rps_250'])
                    if r['vcp_factor'] is not None:
                        r['vcp_factor'] = float(r['vcp_factor'])
                    
                    # 添加涨幅信息
                    sheep_code = r['sheep_code']
                    return_info = return_data.get(sheep_code, {'return_5d': None, 'return_10d': None, 'return_nd': None})
                    r['return_5d'] = return_info['return_5d']
                    r['return_10d'] = return_info['return_10d']
                    r['return_nd'] = return_info['return_nd']
                
                logger.info(f"成功生成 {len(results)} 条推荐结果")
                diagnostic_info.append(f"最终: 生成 {len(results)} 条推荐")
                return results, " | ".join(diagnostic_info)
            except Exception as e:
                logger.error(f"生成推荐结果时出错: {e}", exc_info=True)
                logger.info(f"Top stocks columns: {list(top_stocks.columns)}")
                logger.info(f"Top stocks shape: {top_stocks.shape}")
                diagnostic_info.append(f"错误: {str(e)}")
                return [], " | ".join(diagnostic_info)
            
        except Exception as e:
            logger.error(f"T-4模型运行失败: {e}", exc_info=True)
            diagnostic_info.append(f"异常: {str(e)}")
            return [], " | ".join(diagnostic_info)
