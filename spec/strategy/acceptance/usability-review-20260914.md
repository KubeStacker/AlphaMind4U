# 2026-09-14：策略页面与运行流程复审

> 范围：当前源代码、方案及T10记录的静态复审；本记录不代表运行验收。
> 未修改业务代码、未新增或运行单元测试、未启动服务、未直接访问量化数据库。
> 当前方案与编号待办统一见[plan.md](../plan.md)，本记录用于说明状态修正理由。

## 一、主要结论

已有回测计算、队列、账户配置、流水与导出，继续复用；问题集中在参数入口、账本细节和展示真实性。
先修正这些问题并重跑四版本，再判断收益瓶颈；近期页面收敛为每日计划和回测两个页签。
T10记录声称第3～8项完整验收，但下列代码证据不足以支持这一范围，故当前待办恢复逐项复验。
这不否认旧回放可能完成，也不将旧记录的收益当作本次独立验证结果。

## 二、参数与基准存在可定位的缺陷

1. **前端与后台参数不一致。** [StrategyLabPage.vue](../../../frontend/src/pages/strategy-lab-page/StrategyLabPage.vue)
   第44～50、379～380行只提交突破周期、退出倍数等五项参数。
   [strategy_worker.go](../../../services/quant-backend/internal/api/strategy_worker.go)第66～78行
   直接执行`in.Params = *req.Params`，覆盖准备阶段已有默认值，其余周期和费用成为Go零值。
   [strategy.go](../../../services/quant-backend/internal/api/strategy.go)定义了`paramsConfig`，但请求和消费者均未调用。
   [s1s2.go](../../../services/quant-backend/internal/strategy/s1s2.go)的`daySnapshot`拒绝非正周期，
   因此前端默认请求可出现指标全部不可用；命令行使用完整默认参数的结果不能证明页面请求等价。
   修复要求：只允许登记过的字段，补齐默认值后校验并冻结，再用同一参数准备输入与计算；明确缺省与合法零值的区别。

2. **S0仍会退回固定8%。** [s0.go](../../../services/quant-backend/internal/strategy/s0.go)第365行构造
   `S12Input{Bars: in.Bars, Dates: in.Dates}`而未传`Params`，`daySnapshot`因零周期返回失败；
   第102行的8%回退因此仍生效，`atrVal`保持0，也无法正常使用跟踪ATR。
   该趋势快照还要求指数相对强度，S0调用没有指数输入；仅补参数仍不足以修复。
   修复要求：复用S0真正需要的MA/ATR计算，必需指标不明时报告不可用，不静默切换风控。

## 三、页面有内容，但不能直接用于判断效果

1. [StrategyLabPage.vue](../../../frontend/src/pages/strategy-lab-page/StrategyLabPage.vue)共984行，
   表单、目标配置、诊断长文、成交、权益、股票池、逐日表与网格集中在一个页面；净值图位于第752行之后。
   页面挂载仅加载任务列表，不自动打开报告；没有每日计划查询入口。此处评价基于模板和逻辑，未做视觉实测。
2. 第865～946行直接写入九组参数和压力成绩，没有绑定当前任务、本金、日期或输入版本。
   原文称“实测”也不能改变它们是固定展示的事实；应由关联任务读取，没有数据就显示未运行。
3. `simulationResult`将收益与回撤乘以风险倍数；账户限额、交易单位、成交拒单和回撤停用均会改变路径，
   这种计算不能作为新风险情景的回测或预期收益。应删除，改动风险后提交实际计算。
4. 诊断固定写“交易品质极高”“瓶颈并非胜率不足”，甚至把`trades`称为突破次数；
   `trades`在买入成交后增加。`profit_factor`为总盈利/总亏损，也不是平均盈利/平均亏损。
5. 导出文件名和复制报告读取可编辑的`from/to`，可能与所选历史任务不一致；须读取冻结请求。
   自定义第一目标用`>=`，而约定30%目标为严格`>`；零胜率也不能因真假值判断显示成缺失。
6. 当前各筛选计数分母不同，不能当成严格逐级减少的漏斗；缺数据、无信号和成交失败必须分开解释。

## 四、日常运行与权益仍有差距

1. [corporate_action.go](../../../services/quant-backend/internal/strategy/corporate_action.go)的
   `ApplyCorporateAction`在除权事件处理时直接增加现金和股份，未使用已加载的`RecordDate/PayDate`。
   尚未形成应收权益、到账、股份可售及税款状态；仅对账到同一套简化流水不能证明时间与经济含义正确。
2. [strategy_worker.go](../../../services/quant-backend/internal/api/strategy_worker.go)的`checkpoint`每10日只保存进度；
   [etl.go](../../../services/quant-backend/internal/api/etl.go)启动将RUNNING置回PENDING，随后从冻结输入重新运行。
   这属于可重放任务，不能称为完整账户断点接续；优先级也只在整项任务之间判断。
3. [sched.go](../../../services/quant-backend/internal/sched/sched.go)仅登记采集，
   [server.go](../../../services/quant-backend/internal/api/server.go)策略接口仅有回测和月池。
   当前可见代码中未接通每日计划发布、读取及长期研究编排；市场指标有表与读取函数，持续生成链路仍需核查。
4. [annual_replay/main.go](../../../services/quant-backend/cmd/annual_replay/main.go)另开生产库运行并将报告写入仓库外固定路径；
   它不能替代正式队列验收，也不得与量化服务并行直连该库。本次未执行此命令。
5. `./jarvis.sh status`本次报告quant、mcp、frontend均未运行；未启动或做浏览器访问，未测响应时间。

## 五、验收状态如何调整

保留[T10原记录](T10-annual-campaign-20260914.md)及其报告数字，但不再将其概括为“全部实现已验收”。
公司行动、公平S0、前后端一致性需修正复验；日批、完整恢复、自动研究和时间检验缺少相应完成证据。
页面和导出已有实现，转为简化与准确性修复；队列已有能力，补优先级与有效参数即可，不重新建一套。
下一次验收必须附正式任务ID、冻结请求、实际参数、输入版本、完整结果与对账记录；页面还须附实际交互检查。
历史收益最高为F1记录的4.14%，完整周期18次，不能据此证明满足30%/100%目标或策略已经稳定。
