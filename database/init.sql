-- 股票数据库初始化脚本
-- 数据库名: stock
-- 字符集: utf8mb4

-- 股票基本信息表
CREATE TABLE IF NOT EXISTS stock_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    stock_name VARCHAR(100) NOT NULL COMMENT '股票名称',
    sector VARCHAR(100) COMMENT '所属板块',
    market VARCHAR(20) COMMENT '市场（沪深）',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_stock_code (stock_code),
    INDEX idx_sector (sector)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票基本信息表';

-- 股票日K数据表
CREATE TABLE IF NOT EXISTS stock_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    trade_date DATE NOT NULL COMMENT '交易日期',
    open_price DECIMAL(10, 2) NOT NULL COMMENT '开盘价',
    close_price DECIMAL(10, 2) NOT NULL COMMENT '收盘价',
    high_price DECIMAL(10, 2) NOT NULL COMMENT '最高价',
    low_price DECIMAL(10, 2) NOT NULL COMMENT '最低价',
    volume BIGINT NOT NULL COMMENT '成交量',
    amount DECIMAL(20, 2) COMMENT '成交额',
    ma5 DECIMAL(10, 2) COMMENT '5日均线',
    ma10 DECIMAL(10, 2) COMMENT '10日均线',
    ma20 DECIMAL(10, 2) COMMENT '20日均线',
    ma30 DECIMAL(10, 2) COMMENT '30日均线',
    ma60 DECIMAL(10, 2) COMMENT '60日均线',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_stock_date (stock_code, trade_date),
    INDEX idx_trade_date (trade_date),
    INDEX idx_stock_code (stock_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票日K数据表';

-- 主力资金流入表
CREATE TABLE IF NOT EXISTS stock_capital_flow (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    trade_date DATE NOT NULL COMMENT '交易日期',
    main_net_inflow DECIMAL(20, 2) COMMENT '主力净流入（万元）',
    super_large_inflow DECIMAL(20, 2) COMMENT '超大单净流入（万元）',
    large_inflow DECIMAL(20, 2) COMMENT '大单净流入（万元）',
    medium_inflow DECIMAL(20, 2) COMMENT '中单净流入（万元）',
    small_inflow DECIMAL(20, 2) COMMENT '小单净流入（万元）',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_stock_date_flow (stock_code, trade_date),
    INDEX idx_trade_date_flow (trade_date),
    INDEX idx_stock_code_flow (stock_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='主力资金流入表';

-- 板块信息表
CREATE TABLE IF NOT EXISTS sector_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sector_code VARCHAR(50) NOT NULL COMMENT '板块代码',
    sector_name VARCHAR(100) NOT NULL COMMENT '板块名称',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_sector_code (sector_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='板块信息表';

-- 板块日K数据表
CREATE TABLE IF NOT EXISTS sector_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    sector_code VARCHAR(50) NOT NULL COMMENT '板块代码',
    trade_date DATE NOT NULL COMMENT '交易日期',
    open_price DECIMAL(10, 2) NOT NULL COMMENT '开盘价',
    close_price DECIMAL(10, 2) NOT NULL COMMENT '收盘价',
    high_price DECIMAL(10, 2) NOT NULL COMMENT '最高价',
    low_price DECIMAL(10, 2) NOT NULL COMMENT '最低价',
    volume BIGINT NOT NULL COMMENT '成交量',
    amount DECIMAL(20, 2) COMMENT '成交额',
    change_pct DECIMAL(5, 2) COMMENT '涨跌幅(%)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_sector_date (sector_code, trade_date),
    INDEX idx_trade_date_sector (trade_date),
    INDEX idx_sector_code (sector_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='板块日K数据表';

-- 热度榜数据表
CREATE TABLE IF NOT EXISTS hot_stocks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    stock_name VARCHAR(100) NOT NULL COMMENT '股票名称',
    source VARCHAR(20) NOT NULL COMMENT '数据来源（xueqiu/dongcai）',
    `rank` INT NOT NULL COMMENT '排名',
    hot_score DECIMAL(10, 2) COMMENT '热度分数',
    volume BIGINT DEFAULT 0 COMMENT '成交量',
    trade_date DATE NOT NULL COMMENT '交易日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_source_date (source, trade_date),
    INDEX idx_stock_code_hot (stock_code),
    INDEX idx_trade_date_hot (trade_date),
    UNIQUE KEY uk_source_rank_date (source, `rank`, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='热度榜数据表';

-- 涨幅榜数据表
CREATE TABLE IF NOT EXISTS gainers (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    stock_name VARCHAR(100) NOT NULL COMMENT '股票名称',
    change_pct DECIMAL(5, 2) NOT NULL COMMENT '涨跌幅(%)',
    trade_date DATE NOT NULL COMMENT '交易日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_trade_date_gainers (trade_date),
    INDEX idx_stock_code_gainers (stock_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='涨幅榜数据表';

-- 用户表
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL COMMENT '用户名',
    password_hash VARCHAR(255) NOT NULL COMMENT '密码哈希',
    is_active TINYINT(1) DEFAULT 1 COMMENT '是否激活',
    failed_login_attempts INT DEFAULT 0 COMMENT '失败登录次数',
    locked_until DATETIME NULL COMMENT '锁定到期时间',
    last_login DATETIME NULL COMMENT '最后登录时间',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_username (username),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';

-- 登录日志表（用于安全审计和防暴力破解）
CREATE TABLE IF NOT EXISTS login_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL COMMENT '用户名',
    ip_address VARCHAR(45) COMMENT 'IP地址',
    user_agent TEXT COMMENT '用户代理',
    login_status ENUM('success', 'failed') NOT NULL COMMENT '登录状态',
    failure_reason VARCHAR(100) COMMENT '失败原因',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_username (username),
    INDEX idx_created_at (created_at),
    INDEX idx_login_status (login_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='登录日志表';

-- 插入默认管理员用户（用户名: admin, 密码: admin123）
-- 密码使用bcrypt加密，默认密码为 admin123
-- 注意：首次部署后需要运行脚本生成正确的密码哈希
INSERT INTO users (username, password_hash, is_active) 
VALUES ('admin', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW', 1)
ON DUPLICATE KEY UPDATE username=username;
