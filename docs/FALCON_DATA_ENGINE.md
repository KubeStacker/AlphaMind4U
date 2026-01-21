# Falcon Data Engine (猎鹰数据引擎)

## 概述

Falcon Data Engine 是一个支持多周期调度、动态配置、字段级增量修补的统一数据采集平台。

### 核心原则

1. **自动化**：从冷启动到每日增量，再到分时监控，全自动运行
2. **健壮性**：断点续传，智能补全，数据校验
3. **静默高效**：严格控制日志量，避免在处理全市场 5000+ 标的时产生 GB 级的垃圾日志

## 配置文件

配置文件位于 `backend/config.yaml`，支持以下配置：

### 系统配置

```yaml
system:
  log_level: "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
  concurrency: 10    # 并发数
  cold_start_days: 365  # 冷启动回溯天数
```

### 调度任务配置

```yaml
scheduler:
  tasks:
    realtime_kline:
      cron: "*/1 9-15 * * 1-5"  # 实时K线（1分钟）
      enabled: true
    daily_settlement:
      cron: "0 16 * * 1-5"  # 每日盘后结算（16:00）
      enabled: true
```

### 数据校验配置

```yaml
validation:
  enable_atomic_check: true  # 原子性校验
  enable_money_flow_check: true  # 资金流校验
  enable_trading_calendar_check: true  # 交易日历检查
```

## 日志分级策略

### 日志级别说明

- **DEBUG**：用于记录具体的 HTTP 请求、单只股票的抓取状态（默认关闭）
- **INFO**：仅记录"任务级"里程碑（如 "Daily Task Started", "Task Finished: 5000 records inserted"）
- **ERROR**：记录采集失败的具体堆栈和参数

### 静默模式

生产环境标准输出（Stdout）必须干净。在 `config.yaml` 中设置：

```yaml
system:
  log_level: "INFO"  # 平时只看关键信息，排错时改为 DEBUG

logging:
  quiet_mode: true  # 静默模式
  progress_interval: 100  # 每处理100条记录输出一次进度
```

## 任务说明

### 1. 每日盘后结算任务（16:00）

**功能**：
- 增量采集：调用 AKShare 的"当日榜单"接口，一次请求获取全市场数据
- 智能断点续传：查询数据库 `max(trade_date)`，自动补齐缺失日期的数据

**日志要求**：
- 补全过程仅在开始和结束时输出 INFO 日志
- 中间每处理 100 个交易日输出一次进度

### 2. 盘中实时采集任务

#### 实时K线（1分钟）
- **目标表**：`sheep_min_kline` (code, time, open, high, low, close, vol)
- **逻辑**：仅采集当前时刻的数据，使用 `INSERT ON DUPLICATE KEY UPDATE`
- **日志要求**：极度静默，除非发生网络连接错误，否则不在控制台输出任何内容

#### 市场热度雷达（30分钟）
- **目标表**：`market_hot_rank_snapshot`
- **逻辑**：抓取热榜并计算连板高度

## CLI 命令行工具

### 模式A: 行级冷启动

场景：数据库为空，根据配置回溯 N 天

```bash
python cli.py --mode=cold_start --days=365
```

### 模式B: 字段级修补

场景：表中已有数据，但新增了字段（如 `sector_rps`）

```bash
# 修补sector_rps字段
python cli.py --mode=patch --target=sector_rps --days=365

# 修补rps_250字段
python cli.py --mode=patch --target=rps_250 --days=365

# 修补vcp_factor和vol_ma_5字段
python cli.py --mode=patch --target=vcp_factor --days=365
```

**支持的字段**：
- `sector_rps`：板块RPS指标
- `rps_250`：250日相对强度
- `vcp_factor`：VCP因子
- `vol_ma_5`：5日均量

**进度显示**：使用 `tqdm` 进度条显示进度，替代刷屏的日志流

## 数据校验（三道防线）

### 1. 原子性校验

校验规则：
- `high >= low`
- `volume >= 0`
- `amount >= 0`

### 2. 资金流校验

校验规则：
- `abs(主力净流入) <= 成交额 * 1.1`（允许10%误差）
- 如果校验失败，标记为异常（`is_valid=0`）并记录 ERROR 日志

### 3. 交易日历检查

任务执行前校验 `ak.tool_trade_date_hist_sina()`，非交易日直接静默退出（Return），不报错也不打日志。

## 使用示例

### 1. 修改日志级别

编辑 `backend/config.yaml`：

```yaml
system:
  log_level: "DEBUG"  # 改为DEBUG以查看详细日志
```

### 2. 禁用某个任务

编辑 `backend/config.yaml`：

```yaml
scheduler:
  tasks:
    realtime_kline:
      enabled: false  # 禁用实时K线采集
```

### 3. 修改任务调度时间

编辑 `backend/config.yaml`：

```yaml
scheduler:
  tasks:
    daily_settlement:
      cron: "0 17 * * 1-5"  # 改为17:00执行
```

## 注意事项

1. **日志量控制**：生产环境务必使用 `log_level: INFO` 和 `quiet_mode: true`
2. **交易日历**：非交易日任务会自动静默退出，不会产生错误日志
3. **断点续传**：每日盘后结算任务会自动检测数据缺失并补齐
4. **字段修补**：使用CLI工具进行字段级修补，避免重采整行数据

## 故障排查

### 查看详细日志

1. 修改 `config.yaml` 中的 `log_level` 为 `DEBUG`
2. 重启服务
3. 查看日志输出

### 手动触发任务

可以通过API手动触发数据采集任务（见 `api/main.py` 中的相关接口）

### 数据完整性检查

使用CLI工具检查数据缺失：

```bash
python cli.py --mode=cold_start --days=30  # 检查最近30天的数据
```

## 架构设计

```
Falcon Data Engine
├── config.yaml          # 配置文件
├── config.py            # 配置加载器
├── utils/
│   ├── logger.py        # 日志工具（静默模式）
│   └── validation.py    # 数据校验（三道防线）
├── services/
│   ├── falcon_scheduler.py  # 调度器（配置化）
│   └── falcon_tasks.py      # 任务函数
└── cli.py              # CLI命令行工具
```

## 版本历史

- **v1.0.0** (2024-01-XX)
  - 初始版本
  - 支持配置化任务调度
  - 实现日志分级策略
  - 实现数据校验（三道防线）
  - 实现字段级修补功能
