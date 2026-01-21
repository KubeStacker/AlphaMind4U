"""
板块信号快照数据仓储层
"""
from typing import List, Dict, Optional
from sqlalchemy import text
from db.database import get_db
import logging
from datetime import date

logger = logging.getLogger(__name__)

class SectorSignalSnapshotRepository:
    """板块信号快照数据仓储"""
    
    @staticmethod
    def save_signal_snapshot(data: Dict) -> int:
        """保存板块信号快照"""
        with get_db() as db:
            query = text("""
                INSERT INTO sector_signal_snapshot
                (trade_date, sector_name, signal_type, strategy_code, technical_context, confidence_score)
                VALUES 
                (:trade_date, :sector_name, :signal_type, :strategy_code, :technical_context, :confidence_score)
            """)
            
            result = db.execute(query, data)
            db.commit()
            return result.lastrowid
    
    @staticmethod
    def get_recent_signals_by_sector(sector_name: str, days: int = 30, limit: int = 100) -> List[Dict]:
        """获取指定板块最近的信号记录"""
        with get_db() as db:
            query = text("""
                SELECT id, trade_date, sector_name, signal_type, strategy_code, 
                       technical_context, confidence_score, created_at
                FROM sector_signal_snapshot
                WHERE sector_name = :sector_name
                  AND trade_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                ORDER BY trade_date DESC, created_at DESC
                LIMIT :limit
            """)
            
            result = db.execute(query, {
                'sector_name': sector_name,
                'days': days,
                'limit': limit
            })
            
            signals = []
            for row in result:
                signals.append({
                    'id': row[0],
                    'trade_date': row[1],
                    'sector_name': row[2],
                    'signal_type': row[3],
                    'strategy_code': row[4],
                    'technical_context': row[5],
                    'confidence_score': float(row[6]) if row[6] is not None else None,
                    'created_at': row[7]
                })
            
            return signals
    
    @staticmethod
    def get_signals_by_type(signal_type: str, days: int = 7, limit: int = 100) -> List[Dict]:
        """根据信号类型获取信号记录"""
        with get_db() as db:
            query = text("""
                SELECT id, trade_date, sector_name, signal_type, strategy_code, 
                       technical_context, confidence_score, created_at
                FROM sector_signal_snapshot
                WHERE signal_type = :signal_type
                  AND trade_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                ORDER BY confidence_score DESC, trade_date DESC
                LIMIT :limit
            """)
            
            result = db.execute(query, {
                'signal_type': signal_type,
                'days': days,
                'limit': limit
            })
            
            signals = []
            for row in result:
                signals.append({
                    'id': row[0],
                    'trade_date': row[1],
                    'sector_name': row[2],
                    'signal_type': row[3],
                    'strategy_code': row[4],
                    'technical_context': row[5],
                    'confidence_score': float(row[6]) if row[6] is not None else None,
                    'created_at': row[7]
                })
            
            return signals
    
    @staticmethod
    def get_sector_signals_summary(sector_name: str, start_date: date, end_date: date) -> Dict:
        """获取板块信号汇总统计"""
        with get_db() as db:
            # 统计各类型信号数量
            count_query = text("""
                SELECT signal_type, COUNT(*) as count, AVG(confidence_score) as avg_confidence
                FROM sector_signal_snapshot
                WHERE sector_name = :sector_name
                  AND trade_date BETWEEN :start_date AND :end_date
                GROUP BY signal_type
            """)
            
            count_result = db.execute(count_query, {
                'sector_name': sector_name,
                'start_date': start_date,
                'end_date': end_date
            })
            
            summary = {
                'signals_by_type': {},
                'total_signals': 0,
                'avg_confidence': 0.0
            }
            
            total_count = 0
            total_confidence = 0.0
            signal_count = 0
            
            for row in count_result:
                signal_type = row[0]
                count = row[1]
                avg_confidence = float(row[2]) if row[2] is not None else 0.0
                
                summary['signals_by_type'][signal_type] = {
                    'count': count,
                    'avg_confidence': avg_confidence
                }
                
                total_count += count
                if avg_confidence > 0:
                    total_confidence += avg_confidence
                    signal_count += 1
            
            summary['total_signals'] = total_count
            summary['avg_confidence'] = total_confidence / signal_count if signal_count > 0 else 0.0
            
            return summary