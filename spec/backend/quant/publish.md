# spec/backend/quant/publish — 数据发布日历

> 每类数据“何时可取”的唯一依据。质量快照的“是否延迟”按此表解读，
> 不要拿交易日历直接当数据就绪时间。

| 数据 | 源发布时间 | 本地任务 | 说明 |
|---|---|---|---|
| 日线行情/复权 | T 日 16:45 后 | price（16:45/18:30） | 收盘后发布，18:30 兜底 |
| 个股资金流 | T 日 16:45 后 | moneyflow（随 daily_update） | 与行情同批 |
| 日频指标 | T 日 16:45 后 | daily_basic（随 daily_update） | 估值换手同批 |
| 指数日线 | T 日 16:45 后 | index（随 daily_update） | 5 条核心指数 |
| 停复牌 | T 日盘中～收盘 | suspend（随 daily_update） | 无事件日为空属正常 |
| 业绩快报/预告 | 公告日持续 | express/forecast（随 daily_update，120 天窗） | 按公告日期追 |
| 指数权重 | 月末附近 | index_weight（随 daily_update，90 天窗） | 月频，非每日 |
| 沪深港通 | T 日 16:45 后 | hsgt（随 daily_update） | 日频 1 条 |
| 两融明细 | T+1 日晚 | margin（19:07，前 3 天） | 交易所次日披露，当日无 |
| 财报三张表 | 季报披露期持续 | income/balancesheet/cashflow（周日 02:00 + 按季度缺口） | 以 period 为准 |
| 财务指标/分红 | 随财报 | fina_indicator/dividend（手动或按需） | 暂无定时 |
| 概念/行业成员 | 当前快照 | concepts/index_members（手动） | 历史版本规划中 |
| 交易日历 | 年度预置 | calendar（手动，2020～2030） | 先有日历才有调度 |

规则：收盘类数据在 `as_of == 最新交易日` 前不判延迟；两融 T 日缺数属正常，
T+1 日晚仍缺才算缺口。
