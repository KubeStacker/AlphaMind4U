"""
热度榜数据仓储层
"""
from typing import List, Dict, Optional
from datetime import date
from sqlalchemy import text
from db.database import get_db

class HotRankRepository:
    """热度榜数据仓储"""
    
    @staticmethod
    def batch_upsert_hot_rank(data_list: List[Dict], trade_date: date):
        """批量插入或更新热度榜数据"""
        if not data_list:
            return
        
        with get_db() as db:
            # 先删除该日期的数据
            delete_query = text("""
                DELETE FROM market_hot_rank 
                WHERE trade_date = :date AND source = :source
            """)
            for source in set(d['source'] for d in data_list):
                db.execute(delete_query, {'date': trade_date, 'source': source})
            
            # 批量插入
            query = text("""
                INSERT INTO market_hot_rank 
                (sheep_code, sheep_name, `rank`, source, trade_date, hot_score, volume)
                VALUES (:code, :name, :rank, :source, :date, :score, :volume)
            """)
            
            # 优化批量插入性能
            batch_size = 500  # 增大批次大小
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                db.execute(query, batch)
                db.commit()  # 每批次提交
    
    @staticmethod
    def get_hot_sheep(source: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """获取热度榜数据（包含7天涨幅、所属板块、连续上榜天数）"""
        with get_db() as db:
            # 先获取最新交易日
            max_date_query = text("""
                SELECT MAX(trade_date) as max_date 
                FROM market_hot_rank
                WHERE (:source IS NULL OR source = :source)
            """)
            max_date_result = db.execute(max_date_query, {'source': source}).fetchone()
            if not max_date_result or not max_date_result[0]:
                return []
            
            max_date = max_date_result[0]
            
            # 主查询：获取最新价格（总是使用sheep_daily表中的最新交易日价格）
            # 前一个交易日价格用于计算涨跌幅
            # 优先使用sheep_basic表中的名称，如果为空或看起来像代码，则使用代码作为后备
            query = text("""
                SELECT 
                    hr.sheep_code, 
                    sb.sheep_name as sheep_name_from_basic,
                    hr.sheep_name as sheep_name_from_rank,
                    hr.source, hr.`rank`, hr.trade_date,
                    hr.volume, hr.hot_score,
                    sd_latest.close_price as current_price,
                    sd_prev.close_price as prev_price,
                    sd_latest.trade_date as price_date
                FROM market_hot_rank hr
                LEFT JOIN sheep_basic sb ON hr.sheep_code = sb.sheep_code AND sb.is_active = 1
                LEFT JOIN (
                    SELECT 
                        sheep_code,
                        MAX(trade_date) as max_date
                    FROM sheep_daily
                    GROUP BY sheep_code
                ) sd_max ON sd_max.sheep_code = hr.sheep_code
                LEFT JOIN sheep_daily sd_latest ON 
                    sd_latest.sheep_code = hr.sheep_code
                    AND sd_latest.trade_date = sd_max.max_date
                LEFT JOIN sheep_daily sd_prev ON 
                    sd_prev.sheep_code = hr.sheep_code 
                    AND sd_prev.trade_date = (
                        SELECT MAX(sd2.trade_date)
                        FROM sheep_daily sd2
                        WHERE sd2.sheep_code = hr.sheep_code
                          AND sd2.trade_date < sd_max.max_date
                    )
                WHERE hr.trade_date = :max_date
                  AND (:source IS NULL OR hr.source = :source)
                ORDER BY hr.`rank` ASC
                LIMIT :limit
            """)
            
            result = db.execute(query, {
                'source': source,
                'max_date': max_date,
                'limit': limit
            })
            
            stocks_data = []
            sheep_codes = []
            
            # 辅助函数：安全地格式化日期
            def safe_strftime(date_value, default=None):
                """安全地将日期对象转换为字符串"""
                if date_value is None:
                    return default
                # 检查是否是日期对象
                if hasattr(date_value, 'strftime'):
                    try:
                        return date_value.strftime('%Y-%m-%d')
                    except (AttributeError, TypeError):
                        pass
                # 如果是字符串，直接返回
                if isinstance(date_value, str):
                    return date_value
                # 如果是整数或其他类型，返回默认值
                return default
            
            for row in result:
                sheep_code = row[0]
                sheep_codes.append(sheep_code)
                # 字段索引：0=sheep_code, 1=sheep_name_from_basic, 2=sheep_name_from_rank, 3=source, 4=rank, 5=trade_date, 6=volume, 7=hot_score, 8=current_price, 9=prev_price, 10=price_date
                current_price = float(row[8]) if row[8] is not None else None
                prev_price = float(row[9]) if row[9] is not None else None
                price_date = safe_strftime(row[10])
                change_pct = None
                if current_price and prev_price and prev_price > 0:
                    change_pct = round((current_price - prev_price) / prev_price * 100, 2)
                
                # 优先使用sheep_basic表中的名称，如果为空或看起来像代码（6位数字），则尝试使用market_hot_rank表中的名称，最后才使用代码
                sheep_name_from_basic = row[1] if row[1] else None
                sheep_name_from_rank = row[2] if row[2] else None
                
                # 判断名称是否有效（不是代码格式）
                def is_valid_name(name):
                    if not name or not name.strip():
                        return False
                    name_clean = name.strip()
                    # 如果是6位纯数字，认为是代码
                    if len(name_clean) == 6 and name_clean.isdigit():
                        return False
                    # 如果以SH或SZ开头后跟6位数字，也认为是代码
                    if (name_clean.startswith('SH') or name_clean.startswith('SZ')) and len(name_clean) == 8 and name_clean[2:].isdigit():
                        return False
                    return True
                
                # 选择有效的名称
                if is_valid_name(sheep_name_from_basic):
                    sheep_name = sheep_name_from_basic.strip()
                elif is_valid_name(sheep_name_from_rank):
                    sheep_name = sheep_name_from_rank.strip()
                else:
                    # 如果都没有有效名称，使用代码
                    sheep_name = sheep_code
                
                stocks_data.append({
                    'sheep_code': sheep_code,
                    'sheep_name': sheep_name,
                    'source': row[3],
                    'rank': row[4],
                    'trade_date': safe_strftime(row[5]),
                    'volume': int(row[6]) if row[6] else 0,
                    'hot_score': float(row[7]) if row[7] is not None else None,
                    'current_price': current_price,
                    'change_pct': change_pct,
                    'price_date': price_date  # 价格对应的日期，用于调试
                })
            
            if not sheep_codes:
                return stocks_data
            
            # 批量查询7天涨幅
            avg_change_7d_map = {}
            if sheep_codes:
                # 构建参数化查询
                params = {'max_date': max_date}
                placeholders = []
                for i, code in enumerate(sheep_codes):
                    param_name = f'code_{i}'
                    placeholders.append(f':{param_name}')
                    params[param_name] = code
                
                avg_change_query = text(f"""
                    SELECT 
                        sd.sheep_code,
                        sd.close_price as current_price,
                        sd_7d.close_price as price_7d_ago
                    FROM sheep_daily sd
                    LEFT JOIN sheep_daily sd_7d ON 
                        sd_7d.sheep_code = sd.sheep_code
                        AND sd_7d.trade_date = DATE_SUB(sd.trade_date, INTERVAL 7 DAY)
                    WHERE sd.sheep_code IN ({','.join(placeholders)})
                      AND sd.trade_date = :max_date
                """)
                
                avg_result = db.execute(avg_change_query, params)
                for avg_row in avg_result:
                    sheep_code_avg = avg_row[0]
                    current_price_avg = float(avg_row[1]) if avg_row[1] else None
                    price_7d_ago = float(avg_row[2]) if avg_row[2] else None
                    if current_price_avg and price_7d_ago and price_7d_ago > 0:
                        avg_change_7d_map[sheep_code_avg] = round((current_price_avg - price_7d_ago) / price_7d_ago * 100, 2)
            
            # 批量查询连续上榜天数
            consecutive_days_map = {}
            if sheep_codes:
                params = {'max_date': max_date, 'source': source}
                placeholders = []
                for i, code in enumerate(sheep_codes):
                    param_name = f'code_{i}'
                    placeholders.append(f':{param_name}')
                    params[param_name] = code
                
                source_filter = "AND (:source IS NULL OR source = :source)" if source else ""
                consecutive_query = text(f"""
                    SELECT 
                        sheep_code,
                        COUNT(DISTINCT trade_date) as consecutive_days
                    FROM market_hot_rank
                    WHERE sheep_code IN ({','.join(placeholders)})
                      AND trade_date >= DATE_SUB(:max_date, INTERVAL 7 DAY)
                      AND trade_date <= :max_date
                      {source_filter}
                    GROUP BY sheep_code
                """)
                
                consecutive_result = db.execute(consecutive_query, params)
                for row in consecutive_result:
                    consecutive_days_map[row[0]] = row[1]
            
            # 批量查询所属板块（使用聚类后的板块映射）
            sectors_map = {}
            if sheep_codes:
                from services.clustered_sector_mapping_service import ClusteredSectorMappingService
                
                params = {}
                placeholders = []
                for i, code in enumerate(sheep_codes):
                    param_name = f'code_{i}'
                    placeholders.append(f':{param_name}')
                    params[param_name] = code
                
                # 查询原始概念名
                sectors_query = text(f"""
                    SELECT 
                        scm.sheep_code,
                        ct.concept_name,
                        MAX(scm.weight) as max_weight
                    FROM sheep_concept_mapping scm
                    INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                    WHERE scm.sheep_code IN ({','.join(placeholders)})
                      AND ct.is_active = 1
                    GROUP BY scm.sheep_code, ct.concept_name
                    ORDER BY scm.sheep_code, max_weight DESC
                """)
                
                sectors_result = db.execute(sectors_query, params)
                
                # 将原始概念名映射到聚类后的板块名
                for row in sectors_result:
                    sheep_code_sector = row[0]
                    original_concept_name = row[1]
                    
                    # 映射到聚类后的板块名
                    clustered_sector = ClusteredSectorMappingService.map_concept_to_clustered_sector(original_concept_name)
                    
                    if sheep_code_sector not in sectors_map:
                        sectors_map[sheep_code_sector] = []
                    
                    # 去重并限制最多3个板块
                    if clustered_sector not in sectors_map[sheep_code_sector]:
                        if len(sectors_map[sheep_code_sector]) < 3:
                            sectors_map[sheep_code_sector].append(clustered_sector)
            
            # 合并数据
            for stock in stocks_data:
                sheep_code = stock['sheep_code']
                stock['avg_change_7d'] = avg_change_7d_map.get(sheep_code)
                stock['sectors'] = sectors_map.get(sheep_code, [])
                stock['consecutive_days'] = consecutive_days_map.get(sheep_code, 0)
            
            return stocks_data
    
    @staticmethod
    def cleanup_old_data(retention_days: int):
        """清理热度榜旧数据（保留最近N天）"""
        from datetime import date, timedelta
        
        cutoff_date = date.today() - timedelta(days=retention_days)
        
        with get_db() as db:
            delete_query = text("""
                DELETE FROM market_hot_rank 
                WHERE trade_date < :cutoff_date
            """)
            result = db.execute(delete_query, {'cutoff_date': cutoff_date})
            db.commit()
            return result.rowcount
