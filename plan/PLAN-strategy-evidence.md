# PLAN-strategy-evidence — 区分计算完成与研究证据

> 2026-09-09；不降低任何当前缺数门槛，不做单元测试。

- 依据missing_data_policy，给新保存结果附evidence_level=engineering_replay及已知未验收范围。
- 后台完成状态不变；字段随结果保存，GET及前端只读，不对旧记录回填结论。
- 前端显示证据等级/限制，历史缺字段显示未保存；不把COMPLETED当作策略通过验收。
- 契约先行，模型版本升级；go build/vet、前端build验证；无新增SQL或迁移。
- 受限研究执行模式及逐字段依赖审计仍待后续实施，本项不跳过缺失数据。

状态：证据标记子任务已交付，构建和实际HTTP结果验证通过，可归档该计划；[验收](../spec/strategy/acceptance/missing-data.md)。
受限研究另由PLAN-strategy-missing-data跟踪，不把完整研究验证归入本计划完成范围。
