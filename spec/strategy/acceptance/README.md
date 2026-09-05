# acceptance — 逐任务验收记录索引

> 每完成一个 plan.md 第十章任务，在此新增一份验收记录；历史记录只追加不改写。
> 记录须含：修复前后行为、正式回归用例、逐日对账、检查命令与剩余问题。

| 记录 | 任务 | 结论 |
|---|---|---|
| [T01-clock-intents.md](T01-clock-intents.md) | T01 首次修复历史 | C1/C3/C4/C6转绿；整体状态以9月8日复审为准 |
| [T01-review-20260908.md](T01-review-20260908.md) | T01 边界复审与后续补修 | 5组主行为已补修，build/vet通过；未做行为回归 |
| [T02-raw-price.md](T02-raw-price.md) | T02原价、分红采集、窗口及参数边界 | 9月9日build/vet通过；权益暴露遇变化阻断，未做行为回归 |
| [T03-account-risk.md](T03-account-risk.md) | T03账户、风控与成交预算 | build/vet通过；市场装配、真实费用及行为验收仍未完成 |
| [T04-calendar.md](T04-calendar.md) | T04正式日历与评估日 | build/vet和隔离EXPLAIN通过；历史窗口及版本验收未完成 |
| [T05-research-queue.md](T05-research-queue.md) | T05后台研究与实际回放 | 短区间、去重/取消/持久化通过；年度数据阻断，日批未完成 |
| [T07-fill-audit.md](T07-fill-audit.md) | T07逐笔成交与独立对账 | 两条实际回放现金/费用/股数一致；本记录已冻结可归档；基础周期后续已补，日常首屏仍待齐 |
| [T04-month-rollover.md](T04-month-rollover.md) | T04月末信号换池 | 已完成本项修复，记录可归档；历史时点和覆盖另验收 |
| [T07-trade-cycles.md](T07-trade-cycles.md) | T07周期与基础统计 | 基础统计已交付，记录可归档；区间估计和样本外另验收 |
| [missing-data.md](missing-data.md) | 缺数研究与证据等级 | 局部候选缺数可运行并实测，关键权益仍阻断；年度未完成 |
| [tushare-source-20260910.md](tushare-source-20260910.md) | 新凭据接入与定向补采 | 官方路由三项补采成功；年度验证与权益仍另验收 |
| [backfill-20260910.md](backfill-20260910.md) | 年度输入剩余补采 | 78项补采完成，539条均为上市前/停牌；年度权益换算仍阻断 |
| [backfill-20260910.json](backfill-20260910.json) | 补数证据附件 | 批次ID、终态、75只分红统计及年度诊断 |
| [source-review-20260911.md](source-review-20260911.md) | 官方逐字段复核 | 恢复1096条被覆盖记录，4170条逐字段一致，空日期/变体幂等通过 |
| [source-review-20260911.json](source-review-20260911.json) | 来源复核附件 | 缺口探测、来源比对、任务终态及重复采集证据 |
| [readiness-20260913.json](readiness-20260913.json) | 当前覆盖观察 | HTTP统计快照，非全历史质量验收 |
| [campaign-review-20260913.md](campaign-review-20260913.md) | 持续研究与架构复审 | 审查记录冻结；年度权益、公平S0、常驻编排仍待实现 |
| [campaign-review-20260913.json](campaign-review-20260913.json) | 复审证据 | HTTP快照、年度请求、逐股诊断与源码摘要 |
| [campaign-inputs-20260913.json](campaign-inputs-20260913.json) | 单日诊断输入 | 两月池及原始行情，用于复核19只窗口与静态风险上限 |
| [T10-annual-campaign-20260914.md](T10-annual-campaign-20260914.md) | T10 年度完整回测与有限选参 | 公司行动平稳记账，解除年度阻断；完成S0/S1/S2/F1回放、9组网格与压力检验 |
| [usability-review-20260914.md](usability-review-20260914.md) | T10后续代码与使用复审 | 发现参数覆盖、S0回退及固定成绩等问题；整体完成结论需修正，当前状态以plan为准 |
| [strategy-pruning-20260916.md](strategy-pruning-20260916.md) | 第10项策略入口精简 | S3停用，S0撤出候选；构建、HTTP及32条历史保留核验通过 |
| [strategy-pruning-20260916.json](strategy-pruning-20260916.json) | 策略精简证据 | 最新年度结果、请求参数、源码摘要和接口检查 |

T10行保留原记录摘要；其“完整验收”范围经后续静态复审不能成立，须结合最新复审阅读，不用于证明策略有效。

## 页面视觉稿

[策略页设计示意](strategy-page-design-20260914.png)仅用于布局评审，含示例内容，不是运行截图或验收证据；设计以[plan.md第九章](../plan.md)为准。
