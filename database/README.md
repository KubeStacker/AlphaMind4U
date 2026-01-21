# 数据库表结构说明文档

## 概述

本文档详细说明了系统中所有数据库表的结构、字段含义和使用场景。

**数据库版本**: v4.3  
**最后更新**: 2026-01-16  
**数据库名**: stock  
**字符集**: utf8mb4  
**存储引擎**: InnoDB

---

## 表分类

### 1. 核心数据表（肥羊相关）
- `sheep_basic` - 肥羊基本信息表
- `sheep_daily` - 肥羊交易日数据表
- `sheep_money_flow` - 肥羊资金流向表
- `sheep_financials` - 肥羊财务数据表

### 2. 概念板块表
- `concept_theme` - 概念主题表
- `sheep_concept_mapping` - 肥羊与概念关联关系表

- `sector_money_flow` - 板块资金流向表

### 3. 市场数据表
- `market_hot_rank` - 市场热度排名表
- `market_index_daily` - 全市场指数数据表

### 4. 系统功能表
- `users` - 用户表
- `login_logs` - 登录日志表
- `ai_config` - AI配置表
- `ai_model_config` - AI模型配置表
- `ai_analysis_cache` - AI分析结果缓存表

### 5. 策略与信号表
- `strategy_recommendations` - 策略推荐记录表
- `sector_signal_snapshot` - 板块信号快照表
- `next_day_prediction_cache` - 下个交易日预测缓存表

---

## 详细表结构

### 1. sheep_basic - 肥羊基本信息表

**用途**: 存储肥羊（股票）的基本信息，包括代码、名称、市场、行业等。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| sheep_code | VARCHAR(10) | 肥羊代码（6位数字） | PRIMARY KEY |
| sheep_name | VARCHAR(50) | 肥羊名称 | NOT NULL |
| market | VARCHAR(10) | 所属市场（SH/SZ） | NULL |
| industry | VARCHAR(50) | 所属行业 | NULL |
| list_date | DATE | 上市日期 | NULL |
| is_active | TINYINT(1) | 是否有效（1=有效，0=退市） | DEFAULT 1 |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `sheep_code`
- INDEX: `idx_sheep_name`, `idx_market`, `idx_is_active`

**数据保留**: 永久保留

---

### 2. sheep_daily - 肥羊交易日数据表

**用途**: 存储肥羊每日交易数据，包括价格、成交量、技术指标等。用于K线图展示和技术分析。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| sheep_code | VARCHAR(10) | 肥羊代码 | NOT NULL |
| trade_date | DATE | 交易日期 | NOT NULL |
| open_price | DECIMAL(10,1) | 开盘价 | NULL |
| close_price | DECIMAL(10,1) | 收盘价 | NULL |
| high_price | DECIMAL(10,1) | 最高价 | NULL |
| low_price | DECIMAL(10,1) | 最低价 | NULL |
| volume | BIGINT UNSIGNED | 成交量（手） | DEFAULT 0 |
| amount | DECIMAL(20,2) | 成交额（元） | DEFAULT 0 |
| turnover_rate | DECIMAL(5,1) | 换手率（%） | NULL |
| change_pct | DECIMAL(7,1) | 涨跌幅（%） | NULL |
| ma5 | DECIMAL(10,1) | 5日均价 | NULL |
| ma10 | DECIMAL(10,1) | 10日均价 | NULL |
| ma20 | DECIMAL(10,1) | 20日均价 | NULL |
| ma30 | DECIMAL(10,1) | 30日均价 | NULL |
| ma60 | DECIMAL(10,1) | 60日均价 | NULL |
| vol_ma_5 | DECIMAL(20,1) | 5日均量（倍量基准） | NULL |
| rps_250 | DECIMAL(5,1) | 250日RPS强度 | NULL |
| vcp_factor | DECIMAL(10,1) | 波动收敛系数（VCP） | NULL |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_sheep_date` (`sheep_code`, `trade_date`)
- INDEX: `idx_trade_date`, `idx_sheep_code_date`, `idx_date_code`

**数据保留**: 3年（1095天），用于模型老K回测

---

### 3. concept_theme - 概念主题表

**用途**: 存储概念主题信息，支持多数据源（同花顺、东财等）。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| concept_id | INT UNSIGNED | 概念ID | PRIMARY KEY, AUTO_INCREMENT |
| concept_name | VARCHAR(100) | 概念名称 | NOT NULL |
| concept_code | VARCHAR(50) | 概念代码 | NULL |
| source | VARCHAR(20) | 数据来源（ths=同花顺，em=东财） | DEFAULT 'ths' |
| description | TEXT | 概念描述 | NULL |
| is_active | TINYINT(1) | 是否有效 | DEFAULT 1 |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `concept_id`
- UNIQUE KEY: `uk_concept_name_source` (`concept_name`, `source`)
- INDEX: `idx_concept_name`, `idx_is_active`

**数据保留**: 永久保留

---

### 4. sheep_concept_mapping - 肥羊与概念关联关系表

**用途**: 多对多关系表，记录肥羊与概念的关联关系及权重。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| sheep_code | VARCHAR(10) | 肥羊代码 | NOT NULL |
| concept_id | INT UNSIGNED | 概念ID | NOT NULL |
| weight | DECIMAL(5,2) | 关联权重（用于计算虚拟板块） | DEFAULT 1.0 |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_sheep_concept` (`sheep_code`, `concept_id`)
- INDEX: `idx_sheep_code`, `idx_concept_id`, `idx_weight`

**数据保留**: 永久保留

---

### 6. market_hot_rank - 市场热度排名表

**用途**: 存储各数据源（雪球、东财、同花顺等）的热度排名数据。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| sheep_code | VARCHAR(10) | 肥羊代码 | NOT NULL |
| sheep_name | VARCHAR(50) | 肥羊名称 | NOT NULL |
| rank | INT UNSIGNED | 排名 | NOT NULL |
| source | VARCHAR(20) | 数据来源（xueqiu/dongcai/ths等） | NOT NULL |
| trade_date | DATE | 交易日期 | NOT NULL |
| hot_score | DECIMAL(10,2) | 热度分数 | NULL |
| volume | BIGINT UNSIGNED | 成交量（手） | DEFAULT 0 |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_source_date_rank` (`source`, `trade_date`, `rank`)
- INDEX: `idx_sheep_code`, `idx_trade_date`, `idx_source_date`, `idx_rank`

**数据保留**: 30天

---

### 7. sheep_money_flow - 肥羊资金流向表

**用途**: 存储肥羊的资金流向数据，包括主力、超大单、大单、中单、小单的净流入。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| sheep_code | VARCHAR(10) | 肥羊代码 | NOT NULL |
| trade_date | DATE | 交易日期 | NOT NULL |
| main_net_inflow | DECIMAL(20,2) | 主力净流入（万元） | DEFAULT 0 |
| super_large_inflow | DECIMAL(20,2) | 超大单净流入（万元） | DEFAULT 0 |
| large_inflow | DECIMAL(20,2) | 大单净流入（万元） | DEFAULT 0 |
| medium_inflow | DECIMAL(20,2) | 中单净流入（万元） | DEFAULT 0 |
| small_inflow | DECIMAL(20,2) | 小单净流入（万元） | DEFAULT 0 |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_sheep_date` (`sheep_code`, `trade_date`)
- INDEX: `idx_trade_date`, `idx_sheep_code_date`

**数据保留**: 3年（1095天）

---

### 8. users - 用户表

**用途**: 存储系统用户信息，支持用户认证和权限管理。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | INT UNSIGNED | 用户ID | PRIMARY KEY, AUTO_INCREMENT |
| username | VARCHAR(50) | 用户名 | NOT NULL, UNIQUE |
| password_hash | VARCHAR(255) | 密码哈希 | NOT NULL |
| is_active | TINYINT(1) | 是否激活 | DEFAULT 1 |
| can_use_ai_recommend | TINYINT(1) | 是否允许使用AI推荐功能 | DEFAULT 0 |
| failed_login_attempts | INT | 失败登录次数 | DEFAULT 0 |
| locked_until | DATETIME | 锁定截止时间 | NULL |
| last_login | DATETIME | 最后登录时间 | NULL |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_username` (`username`)
- INDEX: `idx_is_active`

**数据保留**: 永久保留

---

### 9. login_logs - 登录日志表

**用途**: 记录用户登录日志，用于安全审计和异常检测。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| username | VARCHAR(50) | 用户名 | NOT NULL |
| ip_address | VARCHAR(50) | IP地址 | NULL |
| user_agent | TEXT | 用户代理 | NULL |
| login_status | VARCHAR(20) | 登录状态 | NOT NULL |
| failure_reason | VARCHAR(255) | 失败原因 | NULL |
| created_at | TIMESTAMP | 创建时间 | AUTO |

**索引**:
- PRIMARY KEY: `id`
- INDEX: `idx_username`, `idx_created_at`

**数据保留**: 永久保留

---

### 10. ai_config - AI配置表

**用途**: 存储AI相关的配置信息，如API密钥、提示词等。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | INT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| config_key | VARCHAR(50) | 配置键（api_key, prompt_recommend, prompt_analyze等） | NOT NULL, UNIQUE |
| config_value | TEXT | 配置值 | NULL |
| description | VARCHAR(255) | 配置说明 | NULL |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_config_key` (`config_key`)

**数据保留**: 永久保留

---

### 11. ai_model_config - AI模型配置表

**用途**: 存储多个AI模型的配置信息，支持多模型切换。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | INT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| model_name | VARCHAR(50) | 模型名称（如：gpt-4, claude-3等） | NOT NULL, UNIQUE |
| model_display_name | VARCHAR(100) | 模型显示名称 | NOT NULL |
| api_key | VARCHAR(255) | API密钥 | NULL |
| api_url | VARCHAR(255) | API地址 | NULL |
| sort_order | INT | 排序顺序 | DEFAULT 0 |
| is_active | TINYINT(1) | 是否启用 | DEFAULT 1 |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_model_name` (`model_name`)
- INDEX: `idx_is_active`, `idx_sort_order`

**数据保留**: 永久保留

---

### 12. ai_analysis_cache - AI分析结果缓存表

**用途**: 缓存AI分析结果，避免重复调用API，提高响应速度。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| cache_key | VARCHAR(100) | 缓存键（sheep_code或recommend） | NOT NULL |
| cache_type | VARCHAR(20) | 缓存类型（analyze/recommend） | NOT NULL |
| content | TEXT | AI分析结果内容 | NOT NULL |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_cache_key_type` (`cache_key`, `cache_type`)
- INDEX: `idx_cache_key`, `idx_cache_type`, `idx_created_at`

**数据保留**: 永久保留（通过缓存策略自动清理）

---

### 13. strategy_recommendations - 策略推荐记录表

**用途**: 存储模型老K的策略推荐记录，包括推荐时的快照和后验验证结果。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | INT | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| user_id | INT UNSIGNED | 用户ID（用于用户数据隔离） | NOT NULL, DEFAULT 0 |
| run_date | DATE | 推荐计算日期 | NOT NULL |
| strategy_version | VARCHAR(20) | 策略版本 | DEFAULT 'T4_Model' |
| ts_code | VARCHAR(20) | 肥羊代码（兼容ts_code格式，如：000001.SZ） | NOT NULL |
| sheep_code | VARCHAR(10) | 肥羊代码（6位数字） | NOT NULL |
| sheep_name | VARCHAR(50) | 肥羊名称 | NULL |
| params_snapshot | JSON | 参数快照（用于复盘） | NULL |
| entry_price | DECIMAL(10,1) | 推荐时价格（买入价） | NULL |
| ai_score | DECIMAL(5,1) | T4模型最终打分 | NULL |
| win_probability | DECIMAL(5,1) | AI预测胜率 | NULL |
| reason_tags | VARCHAR(255) | 推荐理由标签 | NULL |
| stop_loss_price | DECIMAL(10,1) | 止损建议价 | NULL |
| is_verified | TINYINT | 是否已验证（0=未验证，1=已验证） | DEFAULT 0 |
| max_return_5d | DECIMAL(10,1) | 后5日最大涨幅（%） | NULL |
| final_return_5d | DECIMAL(10,1) | 后5日最终涨幅（%） | NULL |
| final_result | VARCHAR(10) | 最终结果（SUCCESS/FAIL） | NULL |
| create_time | TIMESTAMP | 创建时间 | AUTO |
| update_time | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `idx_unique_rec` (`user_id`, `run_date`, `sheep_code`)
- INDEX: `idx_user_id`, `idx_run_date`, `idx_sheep_code`, `idx_ts_code`, `idx_is_verified`, `idx_final_result`

**数据保留**: 永久保留（按用户隔离）

---

### 14. sheep_financials - 肥羊财务数据表

**用途**: 存储肥羊的财务数据，包括研发费用、净利润、营业收入等。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| sheep_code | VARCHAR(10) | 肥羊代码 | NOT NULL |
| report_date | DATE | 报告期（季度/年度） | NOT NULL |
| rd_exp | DECIMAL(20,2) | 研发费用（元） | NULL |
| net_profit | DECIMAL(20,2) | 净利润（元） | NULL |
| net_profit_growth | DECIMAL(10,1) | 净利润增长率（%） | NULL |
| total_revenue | DECIMAL(20,2) | 营业收入（元） | NULL |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_sheep_report` (`sheep_code`, `report_date`)
- INDEX: `idx_sheep_code`, `idx_report_date`

**数据保留**: 5年

---

### 15. sector_money_flow - 板块资金流向表

**用途**: 存储板块的资金流向数据，包括资金流入、涨跌幅、涨停数、RPS等指标。用于猎鹰雷达和信号雷达功能。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| sector_name | VARCHAR(100) | 板块名称 | NOT NULL |
| trade_date | DATE | 交易日期 | NOT NULL |
| main_net_inflow | DECIMAL(20,2) | 主力净流入（万元） | DEFAULT 0 |
| super_large_inflow | DECIMAL(20,2) | 超大单净流入（万元） | DEFAULT 0 |
| large_inflow | DECIMAL(20,2) | 大单净流入（万元） | DEFAULT 0 |
| medium_inflow | DECIMAL(20,2) | 中单净流入（万元） | DEFAULT 0 |
| small_inflow | DECIMAL(20,2) | 小单净流入（万元） | DEFAULT 0 |
| change_pct | DECIMAL(10,4) | 板块指数加权涨跌幅 | NULL |
| avg_turnover | DECIMAL(10,4) | 平均换手率 | NULL |
| limit_up_count | INT | 涨停家数 | DEFAULT 0 |
| top_weight_stocks | JSON | 前5大权重股代码 | NULL |
| sector_rps_20 | DECIMAL(5,2) | 20日相对强度 | NULL |
| sector_rps_50 | DECIMAL(5,2) | 50日相对强度 | NULL |
| ma_status | TINYINT | 均线状态(1=多头,0=震荡,-1=空头) | DEFAULT 0 |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_sector_date` (`sector_name`, `trade_date`)
- INDEX: `idx_trade_date`, `idx_sector_date`, `idx_main_inflow`, `idx_sector_rps_20`, `idx_sector_rps_50`, `idx_limit_up_count`

**数据保留**: 3个月（90天）

---

### 16. market_index_daily - 全市场指数数据表

**用途**: 存储市场指数日K数据，用于RSRS牛熊市判断和市场状态识别。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| index_code | VARCHAR(20) | 指数代码（CSI1000=中证1000，CSI300=沪深300等） | NOT NULL, DEFAULT 'CSI1000' |
| index_name | VARCHAR(50) | 指数名称 | NOT NULL, DEFAULT '中证1000' |
| trade_date | DATE | 交易日期 | NOT NULL |
| open_price | DECIMAL(10,2) | 开盘价 | NULL |
| close_price | DECIMAL(10,2) | 收盘价 | NULL |
| high_price | DECIMAL(10,2) | 最高价 | NULL |
| low_price | DECIMAL(10,2) | 最低价 | NULL |
| volume | BIGINT UNSIGNED | 成交量 | DEFAULT 0 |
| amount | DECIMAL(20,2) | 成交额 | DEFAULT 0 |
| change_pct | DECIMAL(7,2) | 涨跌幅（%） | NULL |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_index_date` (`index_code`, `trade_date`)
- INDEX: `idx_trade_date`, `idx_index_code_date`

**数据保留**: 3年（用于RSRS计算）

---

### 17. sector_signal_snapshot - 板块信号快照表

**用途**: 记录系统发出的每一次机会或风险提示，用于信号追踪和回测分析。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| trade_date | DATE | 交易日期 | NOT NULL |
| sector_name | VARCHAR(100) | 板块名称 | NOT NULL |
| signal_type | VARCHAR(20) | 信号类型(OPPORTUNITY/RISK) | NOT NULL |
| strategy_code | VARCHAR(50) | 策略代码 | NOT NULL |
| technical_context | JSON | 技术背景快照 | NULL |
| confidence_score | DECIMAL(5,2) | 信号置信度 | NULL |
| created_at | TIMESTAMP | 创建时间 | AUTO |

**索引**:
- PRIMARY KEY: `id`
- INDEX: `idx_trade_date`, `idx_sector_name`, `idx_signal_type`, `idx_strategy_code`

**数据保留**: 永久保留（用于信号追踪）

---

### 18. next_day_prediction_cache - 下个交易日预测缓存表

**用途**: 缓存下个交易日的预测结果，每半小时更新，避免重复计算。

**字段说明**:

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | BIGINT UNSIGNED | 自增ID | PRIMARY KEY, AUTO_INCREMENT |
| target_date | DATE | 预测目标日期（下个交易日） | NOT NULL, UNIQUE |
| prediction_data | JSON | 预测结果JSON数据 | NOT NULL |
| created_at | TIMESTAMP | 创建时间 | AUTO |
| updated_at | TIMESTAMP | 更新时间 | AUTO UPDATE |

**索引**:
- PRIMARY KEY: `id`
- UNIQUE KEY: `uk_target_date` (`target_date`)
- INDEX: `idx_created_at`

**数据保留**: 7天

---

## 数据保留策略

| 表名 | 保留期限 | 说明 |
|------|----------|------|
| sheep_basic | 永久 | 基础信息表 |
| sheep_daily | 3年 | 用于模型回测 |
| sheep_money_flow | 3年 | 用于资金流分析 |
| sheep_financials | 5年 | 财务数据 |
| sector_money_flow | 3年 | 板块资金流数据 |
| market_hot_rank | 30天 | 热度榜数据 |
| market_index_daily | 3年 | 用于RSRS计算 |
| next_day_prediction_cache | 7天 | 预测缓存 |
| 其他表 | 永久 | 系统功能表 |

---

## 表关系图

```
sheep_basic (1) ──< (N) sheep_daily
sheep_basic (1) ──< (N) sheep_money_flow
sheep_basic (1) ──< (N) sheep_financials
sheep_basic (N) >──< (N) concept_theme [sheep_concept_mapping]

sector_money_flow (N) ──< (1) sector_signal_snapshot
```

---

## 注意事项

1. **数据精度**: 价格字段使用1位小数精度，金额字段使用2位小数精度
2. **时区**: 所有时间字段使用服务器时区（CST）
3. **字符集**: 统一使用utf8mb4，支持emoji和特殊字符，支持中文
4. **索引优化**: 根据查询频率和字段选择性创建索引
5. **数据清理**: 定时任务会自动清理过期数据，无需手动操作

---
