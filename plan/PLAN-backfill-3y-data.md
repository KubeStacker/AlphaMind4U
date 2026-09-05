# PLAN-backfill-3y-data — 10000积分2天有效期下3年与主力数据补采

> 2026-09-12；针对当前 10000 积分短凭据（2天有效期），充分发挥高权限优势，定向补采个股资金流向（含特大单/大单主力）、融资融券（主力杠杆）、沪深港通（北向主力）及全量财报与分红。

## 目标与范围

1. **个股资金流向 (`stock_moneyflow`)**：补齐最近 3 年历史（2023-09-11 ~ 2025-01-22，共 331 交易日，约 160 万条），覆盖大单特大单主力净流入。
2. **融资融券明细 (`stock_margin`)**：补齐最近 3 年历史（2023-09-11 ~ 2025-11-27，共 537 交易日，约 100 万条），覆盖杠杆主力动向。
3. **沪深港通资金流 (`moneyflow_hsgt`)**：全量补齐 2021-01-01 至今（约 1338 条），覆盖外资北向主力。
4. **财报三张表批量补齐**：解除自定义 URL 下短 token 误判为长 token 导致的单日 500 条截断，按季度 100 只分批补齐资产负债表与现金流量表。
5. **分红送股 (`stock_dividend`)**：全市场 5900 只股票分红数据回补。

## 实施步骤

1. **代码调优（quant-backend）**：
   - `internal/provider/provider.go`：当 `tokenType == "short"` 时保留 `isShort = true`，使批量财报接口生效。
   - `internal/engine/task_flow.go`：`SyncCapitalFlow` 支持优先按 `startDate` / `endDate` 指定范围，并增加进度日志。
   - `internal/engine/task_market.go`：`SyncMarginTradingData` 扩展支持 `years` 及起止日期参数，并增加进度日志。
   - `internal/engine/run.go`：透传 `StartDate` / `EndDate` 给 `moneyflow` 与 `margin`。
2. **构建与服务重启**：
   - 执行 `go vet ./...` 与 `go build ./...`。
   - 重启服务 `./jarvis.sh restart quant` 并验证健康接口。
3. **分步执行回补与状态跟踪**：
   - 第一步：执行 `moneyflow_hsgt`（秒级，1338 条全量入库）。
   - 第二步：执行 `moneyflow` 补 3 年（331 天，约 12 分钟，断点跳过已存在日期）。
   - 第三步：执行 `margin` 补 3 年（537 天，约 20 分钟，跳过已存在日期）。
   - 第四步：执行 `balancesheet` / `cashflow` 补齐财报。
4. **验证与交付核算**：
   - 检查库内行数、最小最大日期、覆盖率。
   - 记录验收报告与结果。
