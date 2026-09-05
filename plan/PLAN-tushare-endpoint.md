# PLAN-tushare-endpoint — 修正已验证凭据的接入地址

> 2026-09-10；不新增、修改或运行单元测试。

- 已证实新SHORT_TUSHARE_TOKEN官方HTTPS日线成功、旧short专线失败；保留凭据变量，独立配置地址。
- 增加TUSHARE_API_URL并传至provider，可选参数兼容旧调用；显式地址走通用限速和逐证券财务模式。
- 部署使用https://api.tushare.pro，原始量化数据仍只由quant进程写入。
- build/vet后重启；重提两个日期的price和一个证券dividend，核对原NULL因子和任务结果。
- 不由少量接口成功声称10000积分覆盖全部数据或完整年度已通过。

## 交付状态

已完成本项接入修复及实际验收，可归档为历史子计划。
三项补采成功；年度复跑越过旧缺数阻断，转为权益换算未实现阻断。
证据：[补采验收](../spec/strategy/acceptance/tushare-source-20260910.md)。
全历史数据审计及完整权益账本继续由strategy活动计划跟踪。
