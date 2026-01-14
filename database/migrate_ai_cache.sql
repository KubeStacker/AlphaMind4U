-- AI分析结果缓存表
CREATE TABLE IF NOT EXISTS `ai_analysis_cache` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `cache_key` VARCHAR(100) NOT NULL COMMENT '缓存键（stock_code或recommend）',
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
