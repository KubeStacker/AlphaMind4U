"""
肥羊数据仓储层
"""
from typing import List, Dict, Optional
from datetime import date
from sqlalchemy import text
from db.database import get_db
import logging
try:
    from pypinyin import lazy_pinyin, Style
except ImportError:
    # 如果pypinyin未安装，使用空函数
    def lazy_pinyin(text, style=None):
        return []
    Style = None

logger = logging.getLogger(__name__)

class SheepRepository:
    """肥羊数据仓储"""
    
    @staticmethod
    def upsert_sheep_basic(sheep_code: str, sheep_name: str, market: Optional[str] = None, 
                          industry: Optional[str] = None, list_date: Optional[date] = None):
        """插入或更新肥羊基本信息"""
        with get_db() as db:
            query = text("""
                INSERT INTO sheep_basic (sheep_code, sheep_name, market, industry, list_date)
                VALUES (:code, :name, :market, :industry, :list_date)
                ON DUPLICATE KEY UPDATE
                    sheep_name = VALUES(sheep_name),
                    market = VALUES(market),
                    industry = VALUES(industry),
                    list_date = VALUES(list_date)
            """)
            db.execute(query, {
                'code': sheep_code,
                'name': sheep_name,
                'market': market,
                'industry': industry,
                'list_date': list_date
            })
    
    @staticmethod
    def batch_upsert_sheep_daily(data_list: List[Dict]):
        """批量插入或更新肥羊日K数据"""
        if not data_list:
            return
        
        with get_db() as db:
            query = text("""
                INSERT INTO sheep_daily 
                (sheep_code, trade_date, open_price, close_price, high_price, low_price,
                 volume, amount, turnover_rate, change_pct, ma5, ma10, ma20, ma30, ma60)
                VALUES 
                (:code, :date, :open, :close, :high, :low, :volume, :amount, 
                 :turnover_rate, :change_pct, :ma5, :ma10, :ma20, :ma30, :ma60)
                ON DUPLICATE KEY UPDATE
                    open_price = VALUES(open_price),
                    close_price = VALUES(close_price),
                    high_price = VALUES(high_price),
                    low_price = VALUES(low_price),
                    volume = VALUES(volume),
                    amount = VALUES(amount),
                    turnover_rate = VALUES(turnover_rate),
                    change_pct = VALUES(change_pct),
                    ma5 = VALUES(ma5),
                    ma10 = VALUES(ma10),
                    ma20 = VALUES(ma20),
                    ma30 = VALUES(ma30),
                    ma60 = VALUES(ma60)
            """)
            
            # 分批插入（优化：使用executemany提高性能）
            batch_size = 2000  # 增大批次大小以提高性能
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                # 使用executemany进行批量插入，比循环execute快
                db.execute(query, batch)
                db.commit()  # 每批次提交一次，提高性能
    
    @staticmethod
    def search_sheep(keyword: str, limit: int = 20) -> List[Dict]:
        """搜索肥羊（支持代码、中文名称、拼音首字母）"""
        with get_db() as db:
            keyword_clean = keyword.strip()
            keyword_upper = keyword_clean.upper().replace("SZ", "").replace("SH", "")
            
            if keyword_upper.isdigit():
                # 代码搜索：优先从sheep_basic表搜索，因为有索引
                query = text(f"""
                    SELECT sb.sheep_code, sb.sheep_name, sb.industry
                    FROM sheep_basic sb
                    WHERE sb.sheep_code LIKE :kw AND sb.is_active = 1
                    ORDER BY 
                        CASE WHEN sb.sheep_code = :full_kw THEN 0 ELSE 1 END,
                        sb.sheep_code ASC
                    LIMIT {limit}
                """)
                result = db.execute(query, {
                    'kw': f"{keyword_upper}%",
                    'full_kw': keyword_upper
                })
                
                results_list = [
                    {'code': row[0], 'name': row[1] or row[0], 'industry': row[2]}
                    for row in result
                ]
                
                # 如果sheep_basic表中没有结果，从sheep_daily表中搜索代码
                if not results_list:
                    # 使用EXISTS优化性能 instead of DISTINCT
                    daily_query = text(f"""
                        SELECT sd.sheep_code, 
                               COALESCE(sb.sheep_name, mhr.sheep_name, sd.sheep_code) AS sheep_name,
                               sb.industry
                        FROM (SELECT DISTINCT sheep_code FROM sheep_daily WHERE sheep_code LIKE :kw LIMIT {limit}) temp
                        JOIN sheep_daily sd ON sd.sheep_code = temp.sheep_code
                        LEFT JOIN sheep_basic sb ON sd.sheep_code = sb.sheep_code
                        LEFT JOIN (
                            SELECT DISTINCT sheep_code, sheep_name
                            FROM market_hot_rank
                            WHERE sheep_code LIKE :kw
                        ) mhr ON sd.sheep_code = mhr.sheep_code
                        ORDER BY 
                            CASE WHEN sd.sheep_code = :full_kw THEN 0 ELSE 1 END,
                            sd.sheep_code ASC
                        LIMIT {limit}
                    """)
                    daily_result = db.execute(daily_query, {
                        'kw': f"{keyword_upper}%",
                        'full_kw': keyword_upper
                    })
                    results_list = [
                        {'code': row[0], 'name': row[1] or row[0], 'industry': row[2]}
                        for row in daily_result
                    ]
                
                return results_list
            else:
                # 名称搜索（支持中文和拼音首字母）
                # 1. 先尝试中文名称匹配（从sheep_basic表）
                query = text(f"""
                    SELECT sb.sheep_code, sb.sheep_name, sb.industry
                    FROM sheep_basic sb
                    WHERE sb.sheep_name LIKE :kw AND sb.is_active = 1
                    ORDER BY 
                        CASE WHEN sb.sheep_name = :full_kw THEN 0 
                             WHEN sb.sheep_name LIKE :prefix_kw THEN 1 
                             ELSE 2 END,
                        sb.sheep_code
                    LIMIT {limit}
                """)
                result = db.execute(query, {
                    'kw': f"%{keyword_clean}%",
                    'full_kw': keyword_clean,
                    'prefix_kw': f"{keyword_clean}%"
                })
                
                results_list = [
                    {'code': row[0], 'name': row[1] or row[0], 'industry': row[2]}
                    for row in result
                ]
                
                # 2. 如果sheep_basic表中没有结果，从market_hot_rank表中搜索（按名称）
                if not results_list:
                    try:
                        # Limit results to improve performance
                        hot_rank_query = text(f"""
                            SELECT DISTINCT mr.sheep_code, mr.sheep_name, NULL AS industry
                            FROM market_hot_rank mr
                            WHERE mr.sheep_name LIKE :kw
                            ORDER BY mr.trade_date DESC, mr.sheep_code ASC
                            LIMIT {limit}
                        """)
                        hot_rank_result = db.execute(hot_rank_query, {
                            'kw': f"%{keyword_clean}%"
                        })
                        
                        hot_rank_results = [
                            {'code': row[0], 'name': row[1] or row[0], 'industry': row[2]}
                            for row in hot_rank_result
                        ]
                        
                        if hot_rank_results:
                            results_list = hot_rank_results
                    except Exception as e:
                        logger.warning(f"从market_hot_rank表搜索失败: {e}")
                
                # 3. 如果中文匹配结果为空，尝试拼音首字母匹配
                if not results_list and keyword_clean:
                    try:
                        # 将关键词转换为拼音首字母
                        keyword_pinyin = ''.join(lazy_pinyin(keyword_clean, style=Style.FIRST_LETTER))
                        keyword_pinyin_upper = keyword_pinyin.upper()
                        
                        if keyword_pinyin_upper:
                            # 获取所有肥羊名称，在内存中匹配拼音首字母
                            all_query = text("""
                                SELECT sb.sheep_code, sb.sheep_name, sb.industry
                                FROM sheep_basic sb
                                WHERE sb.is_active = 1
                                LIMIT 10000
                            """)
                            all_result = db.execute(all_query, {})
                            
                            # 在内存中匹配拼音首字母
                            matched = []
                            for row in all_result:
                                sheep_name = row[1]
                                if sheep_name:
                                    # 生成肥羊名称的拼音首字母
                                    name_pinyin = ''.join(lazy_pinyin(sheep_name, style=Style.FIRST_LETTER)).upper()
                                    # 检查是否包含关键词的拼音首字母
                                    if keyword_pinyin_upper in name_pinyin:
                                        matched.append({
                                            'code': row[0],
                                            'name': row[1] or row[0],
                                            'industry': row[2],
                                            'match_score': name_pinyin.startswith(keyword_pinyin_upper)  # 前缀匹配优先
                                        })
                            
                            # 按匹配分数排序（前缀匹配优先）
                            matched.sort(key=lambda x: (not x.get('match_score', False), x['name']))
                            results_list = [{'code': m['code'], 'name': m['name'], 'industry': m['industry']} 
                                          for m in matched[:limit]]
                    except Exception as e:
                        logger.warning(f"拼音搜索失败: {e}")
                
                return results_list
    
    @staticmethod
    def get_max_trade_date() -> Optional[date]:
        """获取数据库中所有肥羊的最大交易日期"""
        with get_db() as db:
            query = text("""
                SELECT MAX(trade_date) as max_date
                FROM sheep_daily
            """)
            result = db.execute(query)
            row = result.fetchone()
            if row and row[0]:
                return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
            return None
    
    @staticmethod
    def get_latest_trade_date(sheep_code: str) -> Optional[date]:
        """获取肥羊在数据库中的最新交易日期"""
        with get_db() as db:
            query = text("""
                SELECT MAX(trade_date) as max_date
                FROM sheep_daily
                WHERE sheep_code = :code
            """)
            result = db.execute(query, {'code': sheep_code})
            row = result.fetchone()
            if row and row[0]:
                return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
            return None
    
    @staticmethod
    def get_earliest_trade_date(sheep_code: str) -> Optional[date]:
        """获取肥羊在数据库中的最早交易日期"""
        with get_db() as db:
            query = text("""
                SELECT MIN(trade_date) as min_date
                FROM sheep_daily
                WHERE sheep_code = :code
            """)
            result = db.execute(query, {'code': sheep_code})
            row = result.fetchone()
            if row and row[0]:
                return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
            return None
    
    @staticmethod
    def get_sheep_daily_count_for_date(trade_date: date) -> int:
        """获取某个交易日的肥羊日K数据数量"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT COUNT(*) as count
                    FROM (SELECT DISTINCT sheep_code FROM sheep_daily WHERE trade_date = :trade_date) as subquery
                """)
                result = db.execute(query, {'trade_date': trade_date})
                row = result.fetchone()
                if row and row[0]:
                    return int(row[0])
                return 0
        except Exception as e:
            logger.error(f"获取交易日 {trade_date} 的肥羊日K数据数量失败: {e}")
            return 0
    
    @staticmethod
    def get_sheep_daily(sheep_code: str, limit: int = 90) -> List[Dict]:
        """
        获取肥羊日K数据（默认返回最新的数据）
        
        Args:
            sheep_code: 肥羊代码
            limit: 返回的记录数，默认90条（最新的90条）
            
        Returns:
            按日期升序排列的日K数据列表（从旧到新）
        """
        with get_db() as db:
            # 直接按日期升序查询，避免在应用层排序
            query = text(f"""
                SELECT trade_date, open_price, close_price, high_price, low_price,
                       volume, amount, change_pct, ma5, ma10, ma20, ma30, ma60
                FROM (
                    SELECT trade_date, open_price, close_price, high_price, low_price,
                           volume, amount, change_pct, ma5, ma10, ma20, ma30, ma60
                    FROM sheep_daily
                    WHERE sheep_code = :code
                    ORDER BY trade_date DESC
                    LIMIT {limit}
                ) as subquery
                ORDER BY trade_date ASC
            """)
            result = db.execute(query, {'code': sheep_code})
            
            # 辅助函数：安全地格式化日期
            def safe_strftime(date_value, default=None):
                """安全地将日期对象转换为字符串"""
                if date_value is None:
                    return default
                if hasattr(date_value, 'strftime'):
                    try:
                        return date_value.strftime('%Y-%m-%d')
                    except (AttributeError, TypeError):
                        pass
                if isinstance(date_value, str):
                    return date_value
                return default
            
            # 获取数据并转换为列表
            data_list = [
                {
                    'trade_date': safe_strftime(row[0]),
                    'open_price': float(row[1]) if row[1] else None,
                    'close_price': float(row[2]) if row[2] else None,
                    'high_price': float(row[3]) if row[3] else None,
                    'low_price': float(row[4]) if row[4] else None,
                    'volume': int(row[5]) if row[5] else 0,
                    'amount': float(row[6]) if row[6] else 0,
                    'change_pct': float(row[7]) if row[7] is not None else None,
                    'ma5': float(row[8]) if row[8] else None,
                    'ma10': float(row[9]) if row[9] else None,
                    'ma20': float(row[10]) if row[10] else None,
                    'ma30': float(row[11]) if row[11] else None,
                    'ma60': float(row[12]) if row[12] else None,
                }
                for row in result
            ]
            
            return data_list
    
