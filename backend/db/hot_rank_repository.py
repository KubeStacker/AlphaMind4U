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
                (stock_code, stock_name, `rank`, source, trade_date, hot_score, volume)
                VALUES (:code, :name, :rank, :source, :date, :score, :volume)
            """)
            
            batch_size = 100
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                db.execute(query, batch)
    
    @staticmethod
    def get_hot_stocks(source: Optional[str] = None, limit: int = 100) -> List[Dict]:
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
            
            # 主查询
            query = text("""
                SELECT 
                    hr.stock_code, hr.stock_name, hr.source, hr.`rank`, hr.trade_date,
                    hr.volume, hr.hot_score,
                    sd.close_price as current_price,
                    sd_prev.close_price as prev_price
                FROM market_hot_rank hr
                LEFT JOIN stock_daily sd ON 
                    sd.stock_code = hr.stock_code AND sd.trade_date = hr.trade_date
                LEFT JOIN stock_daily sd_prev ON 
                    sd_prev.stock_code = hr.stock_code 
                    AND sd_prev.trade_date = (
                        SELECT MAX(sd2.trade_date)
                        FROM stock_daily sd2
                        WHERE sd2.stock_code = hr.stock_code
                          AND sd2.trade_date < hr.trade_date
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
            stock_codes = []
            for row in result:
                stock_code = row[0]
                stock_codes.append(stock_code)
                current_price = float(row[7]) if row[7] else None
                prev_price = float(row[8]) if row[8] else None
                change_pct = None
                if current_price and prev_price and prev_price > 0:
                    change_pct = round((current_price - prev_price) / prev_price * 100, 2)
                
                stocks_data.append({
                    'stock_code': stock_code,
                    'stock_name': row[1],
                    'source': row[2],
                    'rank': row[3],
                    'trade_date': row[4].strftime('%Y-%m-%d') if row[4] else None,
                    'volume': int(row[5]) if row[5] else 0,
                    'hot_score': float(row[6]) if row[6] else None,
                    'current_price': current_price,
                    'change_pct': change_pct
                })
            
            if not stock_codes:
                return stocks_data
            
            # 批量查询7天涨幅
            avg_change_7d_map = {}
            if stock_codes:
                # 构建参数化查询
                params = {'max_date': max_date}
                placeholders = []
                for i, code in enumerate(stock_codes):
                    param_name = f'code_{i}'
                    placeholders.append(f':{param_name}')
                    params[param_name] = code
                
                avg_change_query = text(f"""
                    SELECT 
                        sd.stock_code,
                        sd.close_price as current_price,
                        sd_7d.close_price as price_7d_ago
                    FROM stock_daily sd
                    LEFT JOIN stock_daily sd_7d ON 
                        sd_7d.stock_code = sd.stock_code
                        AND sd_7d.trade_date = DATE_SUB(sd.trade_date, INTERVAL 7 DAY)
                    WHERE sd.stock_code IN ({','.join(placeholders)})
                      AND sd.trade_date = :max_date
                """)
                
                avg_result = db.execute(avg_change_query, params)
                for avg_row in avg_result:
                    stock_code_avg = avg_row[0]
                    current_price_avg = float(avg_row[1]) if avg_row[1] else None
                    price_7d_ago = float(avg_row[2]) if avg_row[2] else None
                    if current_price_avg and price_7d_ago and price_7d_ago > 0:
                        avg_change_7d_map[stock_code_avg] = round((current_price_avg - price_7d_ago) / price_7d_ago * 100, 2)
            
            # 批量查询连续上榜天数
            consecutive_days_map = {}
            if stock_codes:
                params = {'max_date': max_date, 'source': source}
                placeholders = []
                for i, code in enumerate(stock_codes):
                    param_name = f'code_{i}'
                    placeholders.append(f':{param_name}')
                    params[param_name] = code
                
                source_filter = "AND (:source IS NULL OR source = :source)" if source else ""
                consecutive_query = text(f"""
                    SELECT 
                        stock_code,
                        COUNT(DISTINCT trade_date) as consecutive_days
                    FROM market_hot_rank
                    WHERE stock_code IN ({','.join(placeholders)})
                      AND trade_date >= DATE_SUB(:max_date, INTERVAL 7 DAY)
                      AND trade_date <= :max_date
                      {source_filter}
                    GROUP BY stock_code
                """)
                
                consecutive_result = db.execute(consecutive_query, params)
                for row in consecutive_result:
                    consecutive_days_map[row[0]] = row[1]
            
            # 批量查询所属板块（通过概念映射到虚拟板块）
            sectors_map = {}
            if stock_codes:
                params = {}
                placeholders = []
                for i, code in enumerate(stock_codes):
                    param_name = f'code_{i}'
                    placeholders.append(f':{param_name}')
                    params[param_name] = code
                
                sectors_query = text(f"""
                    SELECT 
                        scm.stock_code,
                        vba.virtual_board_name,
                        MAX(scm.weight) as max_weight
                    FROM stock_concept_mapping scm
                    INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                    INNER JOIN virtual_board_aggregation vba ON ct.concept_name = vba.source_concept_name
                    WHERE scm.stock_code IN ({','.join(placeholders)})
                      AND ct.is_active = 1
                      AND vba.is_active = 1
                    GROUP BY scm.stock_code, vba.virtual_board_name
                    ORDER BY scm.stock_code, max_weight DESC
                """)
                
                sectors_result = db.execute(sectors_query, params)
                for row in sectors_result:
                    stock_code_sector = row[0]
                    sector_name = row[1]
                    if stock_code_sector not in sectors_map:
                        sectors_map[stock_code_sector] = []
                    if len(sectors_map[stock_code_sector]) < 3:  # 最多3个板块
                        sectors_map[stock_code_sector].append(sector_name)
            
            # 合并数据
            for stock in stocks_data:
                stock_code = stock['stock_code']
                stock['avg_change_7d'] = avg_change_7d_map.get(stock_code)
                stock['sectors'] = sectors_map.get(stock_code, [])
                stock['consecutive_days'] = consecutive_days_map.get(stock_code, 0)
            
            return stocks_data
