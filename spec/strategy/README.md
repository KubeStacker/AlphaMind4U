# strategy — 策略研究

> 归属：quant-backend量化域，不新增服务或端口。更新：2026-09-16。

## 一、当前方案

[策略研究与实施方案](plan.md)是唯一方案正文，第九章描述“今日建议在上、策略表现在下”的单页，第十章管理编号待办。
已有回测、队列、流水和导出；当前先修参数、基准及错误展示，再复验收益并接通每日计划。
本次代码复审发现T10的整体完成结论与实现不一致，详见[复审记录](acceptance/usability-review-20260914.md)。
已[精简策略入口](acceptance/strategy-pruning-20260916.md)：S3停用，S0只作研究基准，候选保留S1/S2/F1；历史记录不删除。

## 二、文件索引

| 文件 | 用途 |
|---|---|
| [plan.md](plan.md) | 当前唯一方案正文及待办 |
| [history.md](history.md) | 原分篇方案与审查资料，仅供追溯 |
| [acceptance/README.md](acceptance/README.md) | 原始验收记录及证据 |
