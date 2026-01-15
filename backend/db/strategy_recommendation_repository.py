"""
策略推荐记录仓储层
"""
from typing import List, Dict, Optional
from datetime import date
from sqlalchemy import text
from db.database import get_db
import json
import logging

logger = logging.getLogger(__name__)


class StrategyRecommendationRepository:
    """策略推荐记录仓储"""
    
    @staticmethod
    def create_recommendation(
        user_id: int,
        run_date: date,
        sheep_code: str,
        sheep_name: str,
        params_snapshot: Dict,
        entry_price: float,
        ai_score: float,
        win_probability: float,
        reason_tags: str,
        stop_loss_price: float,
        strategy_version: str = "T4_Model"
    ) -> int:
        """创建推荐记录"""
        try:
            with get_db() as db:
                query = text("""
                    INSERT INTO strategy_recommendations
                    (user_id, run_date, strategy_version, ts_code, sheep_code, sheep_name,
                     params_snapshot, entry_price, ai_score, win_probability,
                     reason_tags, stop_loss_price)
                    VALUES
                    (:user_id, :run_date, :strategy_version, :ts_code, :sheep_code, :sheep_name,
                     :params_snapshot, :entry_price, :ai_score, :win_probability,
                     :reason_tags, :stop_loss_price)
                    ON DUPLICATE KEY UPDATE
                        sheep_name = VALUES(sheep_name),
                        params_snapshot = VALUES(params_snapshot),
                        entry_price = VALUES(entry_price),
                        ai_score = VALUES(ai_score),
                        win_probability = VALUES(win_probability),
                        reason_tags = VALUES(reason_tags),
                        stop_loss_price = VALUES(stop_loss_price)
                """)
                
                # 转换ts_code格式（6位数字 -> ts_code格式）
                ts_code = f"{sheep_code}.SH" if sheep_code.startswith('6') else f"{sheep_code}.SZ"
                
                result = db.execute(query, {
                    'user_id': user_id,
                    'run_date': run_date,
                    'strategy_version': strategy_version,
                    'ts_code': ts_code,
                    'sheep_code': sheep_code,
                    'sheep_name': sheep_name,
                    'params_snapshot': json.dumps(params_snapshot, ensure_ascii=False),
                    'entry_price': entry_price,
                    'ai_score': ai_score,
                    'win_probability': win_probability,
                    'reason_tags': reason_tags,
                    'stop_loss_price': stop_loss_price
                })
                
                return result.lastrowid
        except Exception as e:
            logger.error(f"创建推荐记录失败: {e}", exc_info=True)
            raise
    
    @staticmethod
    def get_recommendations(
        user_id: int,
        run_date: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
        is_verified: Optional[bool] = None
    ) -> List[Dict]:
        """获取推荐记录列表（仅返回指定用户的数据）"""
        try:
            with get_db() as db:
                conditions = ["user_id = :user_id"]  # 必须按用户ID过滤
                params = {'user_id': user_id}
                
                if run_date:
                    conditions.append("run_date = :run_date")
                    params['run_date'] = run_date
                
                if is_verified is not None:
                    conditions.append("is_verified = :is_verified")
                    params['is_verified'] = 1 if is_verified else 0
                
                where_clause = "WHERE " + " AND ".join(conditions)
                
                query = text(f"""
                    SELECT 
                        id, run_date, strategy_version, sheep_code, sheep_name,
                        params_snapshot, entry_price, ai_score, win_probability,
                        reason_tags, stop_loss_price,
                        is_verified, max_return_5d, final_return_5d, final_result,
                        create_time, update_time
                    FROM strategy_recommendations
                    {where_clause}
                    ORDER BY run_date DESC, ai_score DESC
                    LIMIT :limit OFFSET :offset
                """)
                
                params['limit'] = limit
                params['offset'] = offset
                
                result = db.execute(query, params)
                
                recommendations = []
                for row in result:
                    recommendations.append({
                        'id': row[0],
                        'run_date': row[1].strftime('%Y-%m-%d') if row[1] else None,
                        'strategy_version': row[2],
                        'sheep_code': row[3],
                        'sheep_name': row[4],
                        'params_snapshot': json.loads(row[5]) if row[5] else {},
                        'entry_price': float(row[6]) if row[6] else None,
                        'ai_score': float(row[7]) if row[7] else None,
                        'win_probability': float(row[8]) if row[8] else None,
                        'reason_tags': row[9],
                        'stop_loss_price': float(row[10]) if row[10] else None,
                        'is_verified': bool(row[11]),
                        'max_return_5d': float(row[12]) if row[12] else None,
                        'final_return_5d': float(row[13]) if row[13] else None,
                        'final_result': row[14],
                        'create_time': row[15].isoformat() if row[15] else None,
                        'update_time': row[16].isoformat() if row[16] else None
                    })
                
                return recommendations
        except Exception as e:
            logger.error(f"获取推荐记录失败: {e}", exc_info=True)
            return []
    
    @staticmethod
    def update_verification(
        user_id: int,
        run_date: date,
        sheep_code: str,
        max_return_5d: float,
        final_return_5d: float,
        final_result: str
    ) -> bool:
        """更新验证结果（仅更新指定用户的记录）"""
        try:
            with get_db() as db:
                query = text("""
                    UPDATE strategy_recommendations
                    SET is_verified = 1,
                        max_return_5d = :max_return_5d,
                        final_return_5d = :final_return_5d,
                        final_result = :final_result
                    WHERE user_id = :user_id
                      AND run_date = :run_date
                      AND sheep_code = :sheep_code
                """)
                
                db.execute(query, {
                    'user_id': user_id,
                    'run_date': run_date,
                    'sheep_code': sheep_code,
                    'max_return_5d': max_return_5d,
                    'final_return_5d': final_return_5d,
                    'final_result': final_result
                })
                
                return True
        except Exception as e:
            logger.error(f"更新验证结果失败: {e}", exc_info=True)
            return False
    
    @staticmethod
    def clear_all_history(user_id: int) -> int:
        """清空指定用户的所有历史记录"""
        try:
            with get_db() as db:
                query = text("DELETE FROM strategy_recommendations WHERE user_id = :user_id")
                result = db.execute(query, {'user_id': user_id})
                return result.rowcount
        except Exception as e:
            logger.error(f"清空历史记录失败: {e}", exc_info=True)
            return 0
    
    @staticmethod
    def clear_failed_history(user_id: int) -> int:
        """清空指定用户的失败记录"""
        try:
            with get_db() as db:
                query = text("""
                    DELETE FROM strategy_recommendations
                    WHERE user_id = :user_id
                      AND final_result = 'FAIL'
                """)
                result = db.execute(query, {'user_id': user_id})
                return result.rowcount
        except Exception as e:
            logger.error(f"清空失败记录失败: {e}", exc_info=True)
            return 0
