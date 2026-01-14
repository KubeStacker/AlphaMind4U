-- 定时任务执行记录表
CREATE TABLE IF NOT EXISTS `task_execution_log` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `task_name` VARCHAR(100) NOT NULL COMMENT '任务名称',
    `task_date` DATE NOT NULL COMMENT '任务执行日期',
    `execution_time` DATETIME NOT NULL COMMENT '实际执行时间',
    `status` VARCHAR(20) NOT NULL DEFAULT 'success' COMMENT '执行状态（success/failed）',
    `message` TEXT DEFAULT NULL COMMENT '执行消息或错误信息',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_task_date` (`task_name`, `task_date`),
    INDEX `idx_task_name` (`task_name`),
    INDEX `idx_task_date` (`task_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='定时任务执行记录表';
