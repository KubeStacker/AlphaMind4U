"""
创建 sector_money_flow 表的迁移脚本

使用方法：
  docker-compose exec backend python scripts/create_sector_money_flow_table.py
  或者直接在 MySQL 容器中执行 SQL
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import get_db
from sqlalchemy import text
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_sector_money_flow_table():
    """创建 sector_money_flow 表"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `sector_money_flow` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `sector_name` VARCHAR(100) NOT NULL COMMENT '板块名称',
        `trade_date` DATE NOT NULL COMMENT '交易日期',
        `main_net_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '主力净流入（万元）',
        `super_large_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '超大单净流入（万元）',
        `large_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '大单净流入（万元）',
        `medium_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '中单净流入（万元）',
        `small_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '小单净流入（万元）',
        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_sector_date` (`sector_name`, `trade_date`),
        INDEX `idx_trade_date` (`trade_date`),
        INDEX `idx_sector_date` (`sector_name`, `trade_date`),
        INDEX `idx_main_inflow` (`main_net_inflow`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='板块资金流向表（保留3个月）';
    """
    
    try:
        with get_db() as db:
            # 检查表是否存在
            check_table_sql = text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                AND table_name = 'sector_money_flow'
            """)
            result = db.execute(check_table_sql)
            table_exists = result.scalar() > 0
            
            if table_exists:
                logger.info("表 sector_money_flow 已存在，跳过创建")
                return True
            
            # 创建表
            logger.info("开始创建表 sector_money_flow...")
            db.execute(text(create_table_sql))
            db.commit()
            logger.info("表 sector_money_flow 创建成功！")
            return True
            
    except Exception as e:
        logger.error(f"创建表 sector_money_flow 失败: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    try:
        success = create_sector_money_flow_table()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n用户中断，退出程序")
        sys.exit(1)
    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)
        sys.exit(1)
