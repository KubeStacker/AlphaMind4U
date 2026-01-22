-- 数据库表结构设计 - 完整版
-- 版本: v4.3
-- 日期: 2026-01-16
-- 说明：包含所有表结构和字段定义，使用1位小数精度，表名和字段名统一使用sheep_*前缀

-- ============================================
-- 1. 肥羊基本信息表
-- ============================================
CREATE TABLE IF NOT EXISTS `sheep_basic` (
    `sheep_code` VARCHAR(10) NOT NULL COMMENT '肥羊代码（6位数字）',
    `sheep_name` VARCHAR(50) NOT NULL COMMENT '肥羊名称',
    `market` VARCHAR(10) DEFAULT NULL COMMENT '所属市场（SH/SZ）',
    `industry` VARCHAR(50) DEFAULT NULL COMMENT '所属行业',
    `list_date` DATE DEFAULT NULL COMMENT '上市日期',
    `is_active` TINYINT(1) DEFAULT 1 COMMENT '是否有效（1=有效，0=退市）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`sheep_code`),
    INDEX `idx_sheep_name` (`sheep_name`),
    INDEX `idx_market` (`market`),
    INDEX `idx_is_active` (`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='肥羊基本信息表';

-- ============================================
-- 2. 肥羊交易日数据表（使用1位小数精度）
-- ============================================
CREATE TABLE IF NOT EXISTS `sheep_daily` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `sheep_code` VARCHAR(10) NOT NULL COMMENT '肥羊代码',
    `trade_date` DATE NOT NULL COMMENT '交易日期',
    `open_price` DECIMAL(10,1) DEFAULT NULL COMMENT '开盘价',
    `close_price` DECIMAL(10,1) DEFAULT NULL COMMENT '收盘价',
    `high_price` DECIMAL(10,1) DEFAULT NULL COMMENT '最高价',
    `low_price` DECIMAL(10,1) DEFAULT NULL COMMENT '最低价',
    `volume` BIGINT UNSIGNED DEFAULT 0 COMMENT '成交量（手）',
    `amount` DECIMAL(20,2) DEFAULT 0 COMMENT '成交额（元）',
    `turnover_rate` DECIMAL(5,1) DEFAULT NULL COMMENT '换手率（%）',
    `change_pct` DECIMAL(7,1) DEFAULT NULL COMMENT '涨跌幅（%）',
    `ma5` DECIMAL(10,1) DEFAULT NULL COMMENT '5日均价',
    `ma10` DECIMAL(10,1) DEFAULT NULL COMMENT '10日均价',
    `ma20` DECIMAL(10,1) DEFAULT NULL COMMENT '20日均价',
    `ma30` DECIMAL(10,1) DEFAULT NULL COMMENT '30日均价',
    `ma60` DECIMAL(10,1) DEFAULT NULL COMMENT '60日均价',
    -- 模型老K扩展字段
    `vol_ma_5` DECIMAL(20,1) DEFAULT NULL COMMENT '5日均量（倍量基准）',
    `rps_250` DECIMAL(5,1) DEFAULT NULL COMMENT '250日RPS强度',
    `vcp_factor` DECIMAL(10,1) DEFAULT NULL COMMENT '波动收敛系数（VCP）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_sheep_date` (`sheep_code`, `trade_date`),
    INDEX `idx_trade_date` (`trade_date`),
    INDEX `idx_sheep_code_date` (`sheep_code`, `trade_date`),
    INDEX `idx_date_code` (`trade_date`, `sheep_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='肥羊交易日数据表';

-- ============================================
-- 3. 概念主题表
-- ============================================
CREATE TABLE IF NOT EXISTS `concept_theme` (
    `concept_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `concept_name` VARCHAR(100) NOT NULL COMMENT '概念名称',
    `concept_code` VARCHAR(50) DEFAULT NULL COMMENT '概念代码',
    `source` VARCHAR(20) DEFAULT 'ths' COMMENT '数据来源（ths=同花顺，em=东财）',
    `description` TEXT DEFAULT NULL COMMENT '概念描述',
    `is_active` TINYINT(1) DEFAULT 1 COMMENT '是否有效',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`concept_id`),
    UNIQUE KEY `uk_concept_name_source` (`concept_name`, `source`),
    INDEX `idx_concept_name` (`concept_name`),
    INDEX `idx_is_active` (`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='概念主题表';

-- ============================================
-- 4. 肥羊与概念关联关系表（多对多）
-- ============================================
CREATE TABLE IF NOT EXISTS `sheep_concept_mapping` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `sheep_code` VARCHAR(10) NOT NULL COMMENT '肥羊代码',
    `concept_id` INT UNSIGNED NOT NULL COMMENT '概念ID',
    `weight` DECIMAL(5,2) DEFAULT 1.0 COMMENT '关联权重（用于计算虚拟板块）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_sheep_concept` (`sheep_code`, `concept_id`),
    INDEX `idx_sheep_code` (`sheep_code`),
    INDEX `idx_concept_id` (`concept_id`),
    INDEX `idx_weight` (`weight`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='肥羊与概念关联关系表';


-- ============================================
-- 6. 市场热度排名表
-- ============================================
CREATE TABLE IF NOT EXISTS `market_hot_rank` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `sheep_code` VARCHAR(10) NOT NULL COMMENT '肥羊代码',
    `sheep_name` VARCHAR(50) NOT NULL COMMENT '肥羊名称',
    `rank` INT UNSIGNED NOT NULL COMMENT '排名',
    `source` VARCHAR(20) NOT NULL COMMENT '数据来源（xueqiu/dongcai/ths等）',
    `trade_date` DATE NOT NULL COMMENT '交易日期',
    `hot_score` DECIMAL(10,2) DEFAULT NULL COMMENT '热度分数',
    `volume` BIGINT UNSIGNED DEFAULT 0 COMMENT '成交量（手）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_source_date_rank` (`source`, `trade_date`, `rank`),
    INDEX `idx_sheep_code` (`sheep_code`),
    INDEX `idx_trade_date` (`trade_date`),
    INDEX `idx_source_date` (`source`, `trade_date`),
    INDEX `idx_rank` (`rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='市场热度排名表';

-- ============================================
-- 7. 肥羊资金流向表
-- ============================================
CREATE TABLE IF NOT EXISTS `sheep_money_flow` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `sheep_code` VARCHAR(10) NOT NULL COMMENT '肥羊代码',
    `trade_date` DATE NOT NULL COMMENT '交易日期',
    `main_net_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '主力净流入（万元）',
    `super_large_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '超大单净流入（万元）',
    `large_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '大单净流入（万元）',
    `medium_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '中单净流入（万元）',
    `small_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '小单净流入（万元）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_sheep_date` (`sheep_code`, `trade_date`),
    INDEX `idx_trade_date` (`trade_date`),
    INDEX `idx_sheep_code_date` (`sheep_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='肥羊资金流向表';

-- ============================================
-- 8. 用户表
-- ============================================
CREATE TABLE IF NOT EXISTS `users` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `username` VARCHAR(50) NOT NULL,
    `password_hash` VARCHAR(255) NOT NULL,
    `is_active` TINYINT(1) DEFAULT 1,
    `can_use_ai_recommend` TINYINT(1) DEFAULT 0 COMMENT '是否允许使用AI推荐功能',
    `failed_login_attempts` INT DEFAULT 0,
    `locked_until` DATETIME DEFAULT NULL,
    `last_login` DATETIME DEFAULT NULL,
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_username` (`username`),
    INDEX `idx_is_active` (`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';

-- ============================================
-- 9. 登录日志表
-- ============================================
CREATE TABLE IF NOT EXISTS `login_logs` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `username` VARCHAR(50) NOT NULL,
    `ip_address` VARCHAR(50) DEFAULT NULL,
    `user_agent` TEXT DEFAULT NULL,
    `login_status` VARCHAR(20) NOT NULL,
    `failure_reason` VARCHAR(255) DEFAULT NULL,
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    INDEX `idx_username` (`username`),
    INDEX `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='登录日志表';

-- ============================================
-- 11. AI模型配置表
-- ============================================
CREATE TABLE IF NOT EXISTS `ai_model_config` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `model_name` VARCHAR(50) NOT NULL COMMENT '模型名称（如：gpt-4, claude-3等）',
    `model_display_name` VARCHAR(100) NOT NULL COMMENT '模型显示名称',
    `api_key` VARCHAR(255) DEFAULT NULL COMMENT 'API密钥',
    `api_url` VARCHAR(255) DEFAULT NULL COMMENT 'API地址',
    `sort_order` INT DEFAULT 0 COMMENT '排序顺序',
    `is_active` TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_model_name` (`model_name`),
    INDEX `idx_is_active` (`is_active`),
    INDEX `idx_sort_order` (`sort_order`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AI模型配置表';

-- ============================================
-- 12. AI分析结果缓存表
-- ============================================
CREATE TABLE IF NOT EXISTS `ai_analysis_cache` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `cache_key` VARCHAR(100) NOT NULL COMMENT '缓存键（sheep_code或recommend）',
    `cache_type` VARCHAR(20) NOT NULL COMMENT '缓存类型（analyze/recommend）',
    `content` TEXT NOT NULL COMMENT 'AI分析结果内容',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_cache_key_type` (`cache_key`, `cache_type`),
    INDEX `idx_cache_key` (`cache_key`),
    INDEX `idx_cache_type` (`cache_type`),
    INDEX `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AI分析结果缓存表';

-- ============================================
-- 13. 策略推荐记录表（模型老K）
-- ============================================
CREATE TABLE IF NOT EXISTS `strategy_recommendations` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `user_id` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '用户ID（用于用户数据隔离）',
    `run_date` DATE NOT NULL COMMENT '推荐计算日期',
    `strategy_version` VARCHAR(20) DEFAULT 'T4_Model' COMMENT '策略版本',
    `ts_code` VARCHAR(20) NOT NULL COMMENT '肥羊代码（兼容ts_code格式，如：000001.SZ）',
    `sheep_code` VARCHAR(10) NOT NULL COMMENT '肥羊代码（6位数字）',
    `sheep_name` VARCHAR(50) COMMENT '肥羊名称',
    
    -- 核心：保存当时用户设置的参数，用于复盘哪个参数组合最强
    `params_snapshot` JSON COMMENT '{"vol_ratio": 2.0, "rps": 90, "min_mv": 50, "max_mv": 300, ...}',
    
    -- 推荐时快照
    `entry_price` DECIMAL(10,1) COMMENT '推荐时价格（买入价）',
    `ai_score` DECIMAL(5,1) COMMENT 'T4模型最终打分',
    `win_probability` DECIMAL(5,1) COMMENT 'AI预测胜率',
    `reason_tags` VARCHAR(255) COMMENT '推荐理由标签（如：倍量2.5倍 + 机器人板块龙头）',
    `stop_loss_price` DECIMAL(10,1) COMMENT '止损建议价',
    
    -- 后验验证 (由每日定时任务回填)
    `is_verified` TINYINT DEFAULT 0 COMMENT '是否已验证（0=未验证，1=已验证）',
    `max_return_5d` DECIMAL(10,1) COMMENT '后5日最大涨幅（%）',
    `final_return_5d` DECIMAL(10,1) COMMENT '后5日最终涨幅（%）',
    `final_result` VARCHAR(10) COMMENT 'SUCCESS/FAIL',
    
    `create_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    UNIQUE KEY `idx_unique_rec` (`user_id`, `run_date`, `sheep_code`),
    INDEX `idx_user_id` (`user_id`),
    INDEX `idx_run_date` (`run_date`),
    INDEX `idx_sheep_code` (`sheep_code`),
    INDEX `idx_ts_code` (`ts_code`),
    INDEX `idx_is_verified` (`is_verified`),
    INDEX `idx_final_result` (`final_result`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='策略推荐记录表（按用户隔离，永久保留）';

-- ============================================
-- 14. 肥羊财务数据表（保留5年）
-- ============================================
CREATE TABLE IF NOT EXISTS `sheep_financials` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `sheep_code` VARCHAR(10) NOT NULL COMMENT '肥羊代码',
    `report_date` DATE NOT NULL COMMENT '报告期（季度/年度）',
    `rd_exp` DECIMAL(20,2) DEFAULT NULL COMMENT '研发费用（元）',
    `net_profit` DECIMAL(20,2) DEFAULT NULL COMMENT '净利润（元）',
    `net_profit_growth` DECIMAL(10,1) DEFAULT NULL COMMENT '净利润增长率（%）',
    `total_revenue` DECIMAL(20,2) DEFAULT NULL COMMENT '营业收入（元）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_sheep_report` (`sheep_code`, `report_date`),
    INDEX `idx_sheep_code` (`sheep_code`),
    INDEX `idx_report_date` (`report_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='肥羊财务数据表（保留5年）';

-- ============================================
-- 16. 板块资金流向表（保留3个月数据）
-- ============================================
CREATE TABLE IF NOT EXISTS `sector_money_flow` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `sector_name` VARCHAR(100) NOT NULL COMMENT '板块名称',
    `trade_date` DATE NOT NULL COMMENT '交易日期',
    -- Original money flow fields
    `main_net_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '主力净流入（万元）',
    `super_large_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '超大单净流入（万元）',
    `large_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '大单净流入（万元）',
    `medium_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '中单净流入（万元）',
    `small_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '小单净流入（万元）',
    -- New core factors
    `change_pct` DECIMAL(10,4) DEFAULT NULL COMMENT '板块指数加权涨跌幅',
    `avg_turnover` DECIMAL(10,4) DEFAULT NULL COMMENT '平均换手率',
    `limit_up_count` INT DEFAULT 0 COMMENT '涨停家数',
    `top_weight_stocks` JSON DEFAULT NULL COMMENT '前5大权重股代码',
    -- New derived trend indicators
    `sector_rps_20` DECIMAL(5,2) DEFAULT NULL COMMENT '20日相对强度',
    `sector_rps_50` DECIMAL(5,2) DEFAULT NULL COMMENT '50日相对强度',
    `ma_status` TINYINT DEFAULT 0 COMMENT '均线状态(1=多头,0=震荡,-1=空头)',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_sector_date` (`sector_name`, `trade_date`),
    INDEX `idx_trade_date` (`trade_date`),
    INDEX `idx_sector_date` (`sector_name`, `trade_date`),
    INDEX `idx_main_inflow` (`main_net_inflow`),
    INDEX `idx_sector_rps_20` (`sector_rps_20`),
    INDEX `idx_sector_rps_50` (`sector_rps_50`),
    INDEX `idx_limit_up_count` (`limit_up_count`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='板块资金流向表（保留3个月）';

-- ============================================
-- 15. 全市场指数数据表（用于RSRS牛熊市判断）
-- ============================================
CREATE TABLE IF NOT EXISTS `market_index_daily` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `index_code` VARCHAR(20) NOT NULL DEFAULT 'CSI1000' COMMENT '指数代码（CSI1000=中证1000，CSI300=沪深300等）',
    `index_name` VARCHAR(50) NOT NULL DEFAULT '中证1000' COMMENT '指数名称',
    `trade_date` DATE NOT NULL COMMENT '交易日期',
    `open_price` DECIMAL(10,2) DEFAULT NULL COMMENT '开盘价',
    `close_price` DECIMAL(10,2) DEFAULT NULL COMMENT '收盘价',
    `high_price` DECIMAL(10,2) DEFAULT NULL COMMENT '最高价',
    `low_price` DECIMAL(10,2) DEFAULT NULL COMMENT '最低价',
    `volume` BIGINT UNSIGNED DEFAULT 0 COMMENT '成交量',
    `amount` DECIMAL(20,2) DEFAULT 0 COMMENT '成交额',
    `change_pct` DECIMAL(7,2) DEFAULT NULL COMMENT '涨跌幅（%）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_index_date` (`index_code`, `trade_date`),
    INDEX `idx_trade_date` (`trade_date`),
    INDEX `idx_index_code_date` (`index_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全市场指数数据表（用于RSRS计算，保留3年）';

-- ============================================
-- 16. 板块信号快照表
-- ============================================
CREATE TABLE IF NOT EXISTS `sector_signal_snapshot` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `trade_date` DATE NOT NULL COMMENT '交易日期',
    `sector_name` VARCHAR(100) NOT NULL COMMENT '板块名称',
    `signal_type` VARCHAR(20) NOT NULL COMMENT '信号类型(OPPORTUNITY/RISK)',
    `strategy_code` VARCHAR(50) NOT NULL COMMENT '策略代码',
    `technical_context` JSON DEFAULT NULL COMMENT '技术背景快照',
    `confidence_score` DECIMAL(5,2) DEFAULT NULL COMMENT '信号置信度',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`),
    INDEX `idx_trade_date` (`trade_date`),
    INDEX `idx_sector_name` (`sector_name`),
    INDEX `idx_signal_type` (`signal_type`),
    INDEX `idx_strategy_code` (`strategy_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='板块信号快照表（记录系统发出的每一次机会或风险提示）';

