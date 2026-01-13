-- 数据库表结构设计 - 重构版
-- 1. stock_basic: 股票基本信息表
CREATE TABLE IF NOT EXISTS `stock_basic` (
    `stock_code` VARCHAR(10) NOT NULL COMMENT '股票代码（6位数字）',
    `stock_name` VARCHAR(50) NOT NULL COMMENT '股票名称',
    `market` VARCHAR(10) DEFAULT NULL COMMENT '所属市场（SH/SZ）',
    `industry` VARCHAR(50) DEFAULT NULL COMMENT '所属行业',
    `list_date` DATE DEFAULT NULL COMMENT '上市日期',
    `is_active` TINYINT(1) DEFAULT 1 COMMENT '是否有效（1=有效，0=退市）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`stock_code`),
    INDEX `idx_stock_name` (`stock_name`),
    INDEX `idx_market` (`market`),
    INDEX `idx_is_active` (`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票基本信息表';

-- 2. stock_daily: 股票交易日数据表
CREATE TABLE IF NOT EXISTS `stock_daily` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `stock_code` VARCHAR(10) NOT NULL COMMENT '股票代码',
    `trade_date` DATE NOT NULL COMMENT '交易日期',
    `open_price` DECIMAL(10,2) DEFAULT NULL COMMENT '开盘价',
    `close_price` DECIMAL(10,2) DEFAULT NULL COMMENT '收盘价',
    `high_price` DECIMAL(10,2) DEFAULT NULL COMMENT '最高价',
    `low_price` DECIMAL(10,2) DEFAULT NULL COMMENT '最低价',
    `volume` BIGINT UNSIGNED DEFAULT 0 COMMENT '成交量（手）',
    `amount` DECIMAL(20,2) DEFAULT 0 COMMENT '成交额（元）',
    `turnover_rate` DECIMAL(5,2) DEFAULT NULL COMMENT '换手率（%）',
    `change_pct` DECIMAL(5,2) DEFAULT NULL COMMENT '涨跌幅（%）',
    `ma5` DECIMAL(10,2) DEFAULT NULL COMMENT '5日均价',
    `ma10` DECIMAL(10,2) DEFAULT NULL COMMENT '10日均价',
    `ma20` DECIMAL(10,2) DEFAULT NULL COMMENT '20日均价',
    `ma30` DECIMAL(10,2) DEFAULT NULL COMMENT '30日均价',
    `ma60` DECIMAL(10,2) DEFAULT NULL COMMENT '60日均价',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_stock_date` (`stock_code`, `trade_date`),
    INDEX `idx_trade_date` (`trade_date`),
    INDEX `idx_stock_code_date` (`stock_code`, `trade_date`),
    INDEX `idx_date_code` (`trade_date`, `stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票交易日数据表';

-- 3. concept_theme: 概念主题表
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

-- 4. stock_concept_mapping: 股票与概念关联关系表（多对多）
CREATE TABLE IF NOT EXISTS `stock_concept_mapping` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `stock_code` VARCHAR(10) NOT NULL COMMENT '股票代码',
    `concept_id` INT UNSIGNED NOT NULL COMMENT '概念ID',
    `weight` DECIMAL(5,2) DEFAULT 1.0 COMMENT '关联权重（用于计算虚拟板块）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_stock_concept` (`stock_code`, `concept_id`),
    INDEX `idx_stock_code` (`stock_code`),
    INDEX `idx_concept_id` (`concept_id`),
    INDEX `idx_weight` (`weight`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票与概念关联关系表';

-- 5. virtual_board_aggregation: 虚拟聚合表（参考sector_mapping）
CREATE TABLE IF NOT EXISTS `virtual_board_aggregation` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `virtual_board_name` VARCHAR(100) NOT NULL COMMENT '虚拟板块名称',
    `source_concept_name` VARCHAR(100) NOT NULL COMMENT '源概念名称',
    `weight` DECIMAL(5,2) DEFAULT 1.0 COMMENT '聚合权重',
    `is_active` TINYINT(1) DEFAULT 1 COMMENT '是否有效',
    `description` TEXT DEFAULT NULL COMMENT '描述',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_virtual_source` (`virtual_board_name`, `source_concept_name`),
    INDEX `idx_virtual_board` (`virtual_board_name`),
    INDEX `idx_source_concept` (`source_concept_name`),
    INDEX `idx_is_active` (`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='虚拟板块聚合表';

-- 6. market_hot_rank: 热度数据表
CREATE TABLE IF NOT EXISTS `market_hot_rank` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `stock_code` VARCHAR(10) NOT NULL COMMENT '股票代码',
    `stock_name` VARCHAR(50) NOT NULL COMMENT '股票名称',
    `rank` INT UNSIGNED NOT NULL COMMENT '排名',
    `source` VARCHAR(20) NOT NULL COMMENT '数据来源（xueqiu/dongcai/ths等）',
    `trade_date` DATE NOT NULL COMMENT '交易日期',
    `hot_score` DECIMAL(10,2) DEFAULT NULL COMMENT '热度分数',
    `volume` BIGINT UNSIGNED DEFAULT 0 COMMENT '成交量（手）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_source_date_rank` (`source`, `trade_date`, `rank`),
    INDEX `idx_stock_code` (`stock_code`),
    INDEX `idx_trade_date` (`trade_date`),
    INDEX `idx_source_date` (`source`, `trade_date`),
    INDEX `idx_rank` (`rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='市场热度排名表';

-- 7. stock_money_flow: 资金流向数据表
CREATE TABLE IF NOT EXISTS `stock_money_flow` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `stock_code` VARCHAR(10) NOT NULL COMMENT '股票代码',
    `trade_date` DATE NOT NULL COMMENT '交易日期',
    `main_net_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '主力净流入（万元）',
    `super_large_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '超大单净流入（万元）',
    `large_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '大单净流入（万元）',
    `medium_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '中单净流入（万元）',
    `small_inflow` DECIMAL(20,2) DEFAULT 0 COMMENT '小单净流入（万元）',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_stock_date` (`stock_code`, `trade_date`),
    INDEX `idx_trade_date` (`trade_date`),
    INDEX `idx_stock_code_date` (`stock_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票资金流向表';

-- 保留用户表（登录逻辑需要）
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

-- 保留登录日志表
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

-- 8. ai_config: AI配置表
CREATE TABLE IF NOT EXISTS `ai_config` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `config_key` VARCHAR(50) NOT NULL COMMENT '配置键（api_key, prompt_recommend, prompt_analyze等）',
    `config_value` TEXT DEFAULT NULL COMMENT '配置值',
    `description` VARCHAR(255) DEFAULT NULL COMMENT '配置说明',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_config_key` (`config_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AI配置表';
