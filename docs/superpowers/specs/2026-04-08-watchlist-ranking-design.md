# Watchlist 自动排序与突破预测设计文档

> 日期: 2026-04-08
> 状态: 待实施

## 1. 背景与目标

当前 Watchlist 已具备：

- `compact` 高频盯盘接口，列表首屏可直出动作信号、量比、换手和关键位
- 自适应结构点位引擎，能够从 MA、区间高低点、摆动高低点和成交密集区中筛选支撑/压力
- 深度分析接口，可在详情弹窗中给出更完整的结构、资金和风险说明

但当前观察列表仍存在三个核心短板：

1. 观察列表排序仍依赖 `watchlist.sort_order` 和前端拖拽，本质是人工维护，不具备自动推荐能力
2. `compact` 分析虽然已有轻量评分和动作文案，但没有显式输出“突破概率”“突破强度”“位置性价比”“综合推荐分”
3. 关键位已较过去明显改善，但还缺少“确认突破价”“失效价”“点位强度”“预期延展空间”等可直接交易的结构语义

本次重构的目标是：

- 取消观察列表人工拖拽排序，完全改为系统自动排序
- 在现有 Watchlist 链路内补齐多因子推荐引擎，不新增平行系统
- 进一步优化点位，使首屏展示的支撑/压力真正能用于交易决策
- 增加突破概率和突破强度评估，区分“快突破”“能突破”“突破后能否延续”
- 让前端首屏直接展示“推荐优先级 + 排序原因 + 关键操作位”

本次改动只针对观察列表自动排序和相关分析增强。持仓区仍保留现有“风险优先 + 盈亏优先”的独立排序逻辑。

## 2. 现状 Review

### 2.1 观察列表数据链路

当前高频观察列表主链路如下：

```
watchlist 表
  -> /admin/watchlist/realtime?analysis_depth=compact
  -> backend/api/routes/stocks.py::_build_compact_watch_analysis()
  -> frontend/src/views/Watchlist.vue
```

其中：

- [backend/api/routes/stocks.py](/root/jarvis/backend/api/routes/stocks.py) 的 `/watchlist/realtime` 在 `compact` 模式下会批量拉取自选、合并实时快照、拼接轻量分析结果
- [backend/api/routes/stocks.py](/root/jarvis/backend/api/routes/stocks.py) 的 `_build_compact_watch_analysis()` 会基于均线、量比、换手、主力资金和关键位生成 `suggestion`、`action_signal`、`signal_reasons`、`key_levels`
- [backend/etl/utils/kline_patterns.py](/root/jarvis/backend/etl/utils/kline_patterns.py) 的 `build_structural_price_levels()` 已提供自适应支撑/压力位
- [frontend/src/views/Watchlist.vue](/root/jarvis/frontend/src/views/Watchlist.vue) 当前观察区只按照后端返回顺序渲染；后端顺序本质由 `watchlist.sort_order` 决定

### 2.2 当前排序问题

当前排序行为：

- 后端 `watchlist` 查询按 `sort_order, created_at DESC` 返回
- 前端观察列表支持拖拽，拖拽后调用 `/admin/watchlist/reorder`
- `/admin/watchlist/realtime` 返回结果最后按原始代码顺序重排，不参与推荐排序

这意味着观察列表当前不存在真正的“推荐前置”。排在前面的股票不一定是最值得盯、最接近交易、也不一定是结构最优的标的。

### 2.3 当前 `compact` 分析问题

当前 `_build_compact_watch_analysis()` 已有一套轻量分数，但主要用于生成动作建议，存在以下限制：

- 分数是单一线性打分，缺少分项解释
- 没有显式区分“突破概率”和“突破后的延续强度”
- 没有把“当前位置盈亏比”抽象成独立维度
- 没有统一风险惩罚层，假突破、上方套牢和空心放量只能通过文案隐式表达
- 点位输出仍偏静态，缺少突破确认位和失效位语义

### 2.4 点位引擎现状

[backend/etl/utils/kline_patterns.py](/root/jarvis/backend/etl/utils/kline_patterns.py) 已经具备以下优势：

- 统一评估 MA5/10/20/60、近 5/10/20/60 日高低点、摆动高低点、近 60 日成交密集区
- 根据主板 / 创业板 / 科创板波动画像做自适应间距校准
- 用来源共振、触碰次数、量能和最近性打分
- 已有 `/admin/watchlist/levels/backtest` 可回看 adaptive vs legacy 表现差异

不足主要在于：

- 返回的 `key_levels` 更偏“结构说明”，还不够“交易语言”
- 没有输出点位自身可信度与可执行属性
- 没有把点位能力和排序能力直接打通

## 3. 核心设计总览

本次设计采用“后端双轨分数融合 + 前端状态分层展示”的方案。

核心原则：

1. 不再依赖人工拖拽排序，观察列表统一由后端推荐排序决定
2. 不用单一黑盒分数，而是把“突破机会”和“位置性价比”拆成两条可解释主线
3. 排序不只追突破，必须同时考虑风险、结构质量、资金承接和市场环境
4. 保持现有 `/admin/watchlist/realtime?analysis_depth=compact` 为主链路，不新增平行观察接口

重构后的结构：

```
历史行情 + 实时快照 + 因子层 + 资金流 + 主线信息
  -> 结构点位增强引擎
  -> 突破评估引擎
  -> 位置性价比引擎
  -> 风险惩罚引擎
  -> recommendation_score / state_bucket
  -> /admin/watchlist/realtime compact 返回排序结果
  -> Watchlist 观察列表首屏展示
```

## 4. 多因子评分与自动排序设计

### 4.1 输出的核心评分维度

每只观察股在 `compact` 分析中新增以下五个核心维度：

- `breakout_score`
  含义：未来 1-3 个交易日内对关键压力位形成有效突破的概率倾向，范围 0-100

- `breakout_strength`
  含义：如果突破成立，后续延续和走趋势的质量有多强，范围 0-100

- `entry_quality_score`
  含义：当前位置做观察或试错的盈亏比质量，范围 0-100

- `risk_penalty_score`
  含义：假突破、长上影、上方套牢、资金背离、量能空心等风险惩罚，范围 0-100，越高越危险

- `recommendation_score`
  含义：最终观察列表排序分，范围 0-100

### 4.2 推荐分的设计逻辑

`recommendation_score` 不是简单平均，而是融合两条主线：

- 突破主线：`breakout_score + breakout_strength`
- 位置主线：`entry_quality_score`

再结合市场环境、主线热度和风险惩罚做动态加权。

推荐公式按以下思路实现：

```
base =
  breakout_score * breakout_weight
  + breakout_strength * breakout_strength_weight
  + entry_quality_score * entry_weight
  + theme_alignment_score * theme_weight

recommendation_score =
  clamp(base - risk_penalty_score * risk_weight + market_regime_adjustment, 0, 100)
```

其中动态权重遵循：

- 市场偏强、主线清晰时：突破主线权重更高
- 市场震荡、轮动加快时：位置主线权重更高
- 市场偏弱时：整体推荐分上限压缩，风险惩罚加重

### 4.3 状态分层

为了让前端首屏更像交易界面，而不是纯分数表，本设计对观察股引入 `state_bucket` 分层。

状态层级如下：

- `A_BREAKOUT_READY`
  逼近突破确认位，量价资金共振明显，具备短期突破条件

- `B_PULLBACK_READY`
  靠近强支撑或回踩确认区，盈亏比清晰，适合观察低吸或试错

- `C_TREND_CONTINUE`
  趋势尚可，但位置一般，适合继续盯，不适合作为最优首选

- `D_NEUTRAL_WAIT`
  当前缺少结构优势，普通观察

- `E_RISK_AVOID`
  假突破风险高、结构走弱或资金承接差，应自动排到后部

### 4.4 最终排序规则

观察列表排序统一由后端完成，排序优先级如下：

1. `state_bucket`
2. `recommendation_score` 逆序
3. `breakout_score` 逆序
4. `entry_quality_score` 逆序
5. `risk_penalty_score` 正序
6. `amount` / `pct` / `volume_ratio` 作为打平因子

排序意图：

- 不让高风险的“快突破票”因为单一分高而压过更优的结构机会
- 不让位置很好的低吸票因为短线不在突破边缘就被系统埋没
- 保证前端首屏顶部总是“最值得盯”的票，而不是“最先添加”的票

## 5. 点位引擎升级设计

### 5.1 设计目标

点位优化不是返回更多价位，而是要把当前已有结构位升级为“可交易结构位”。

观察列表首屏需要的不只是：

- 支撑1
- 支撑2
- 压力1
- 压力2

还需要：

- 哪个压力位才是“有效突破确认位”
- 哪个支撑位才是“真正失效位”
- 这个点位是否足够强，值不值得围绕它做交易计划
- 如果突破成功，理论上还有多少短线延展空间

### 5.2 点位输出增强

对现有 `support_levels` / `resistance_levels`，每个点位新增以下字段：

- `level_role`
  `support` / `resistance`

- `level_strength_score`
  点位自身可信度，综合来源共振、触碰质量、最近性、成交支撑程度

- `distance_pct`
  距现价的百分比距离

- `source_resonance`
  当前点位由多少类来源共振，例如 MA20 + LOW_10 + 成交密集区

- `touch_quality`
  历史触碰后的反应质量，例如是否止跌、是否形成承接、是否一碰即破

- `freshness_score`
  结构新鲜度，避免过旧高低点对当前交易产生误导

- `break_confirm_price`
  对压力位而言，是“放量有效站上才算确认”的价格

- `fail_threshold_price`
  对支撑位而言，是“收盘有效跌破则当前逻辑失效”的防守位

- `expected_move_pct`
  若确认突破后，短线合理延展空间估计

### 5.3 支撑位优化思路

支撑位选择不再只看离现价最近，而更重视“可防守性”。

优先级：

1. 趋势回踩中的 `MA10/MA20 + 成交密集区` 共振位
2. 近 10/20 日出现过明显承接反应的 swing low
3. 放量起涨平台上沿附近的回踩确认位
4. 主板 / 创业板 / 科创板按不同波动画像保留不同容忍区间

弱化或降权：

- `OPEN`
- `PRE_CLOSE`
- 缺少历史反应的孤立低点
- 与更强支撑过于接近、信息重复的点位

### 5.4 压力位优化思路

压力位选择更重视“会不会真放行”，而不是单纯“最近前高”。

优先级：

1. 近 5/10/20 日可见平台前高
2. 最近一次冲高回落的主压位
3. 大成交密集区上沿
4. `MA60`、平台上沿、前波高点共振位

降权：

- 过远但没有现实交易价值的历史高点
- 与主压力过近、只会造成噪音的次级压力
- 没有量价反应支撑的孤立高点

### 5.5 统一的交易语义

`compact` 首屏只展示 4 个主点位：

- `支撑1`
- `支撑2`
- `压力1`
- `压力2`

但在点位字段中额外明示：

- `break_confirm_price`
- `fail_threshold_price`

这样前端可以直接展示“突破确认价”和“失效价”，避免用户自行从文案中猜测。

## 6. 突破概率与突破强度设计

### 6.1 设计原则

突破评估当前不采用黑盒模型，而采用结构化可解释评分。

原因：

- 当前代码已有清晰的技术与资金字段，适合用规则化多因子先做第一版
- 需要让前端和用户能理解“为什么系统认为它快突破”
- 后续若引入更复杂的统计模型，也应建立在这一层结构化特征输出之上

### 6.2 `breakout_score` 的构成

`breakout_score` 用于衡量“未来 1-3 日内突破关键压力位的概率倾向”，由以下子因子构成：

- `distance_to_breakout`
  现价距 `break_confirm_price` 多近，越近越高，但不能无限加分

- `price_structure`
  是否站上 `MA20/MA60`，是否出现多头排列，是否处于平台末端压缩

- `volume_preheat`
  量比、近 3 日量能抬升、是否提前放量预热

- `capital_support`
  主力净流入、连续净流入天数、大单占比

- `trend_quality`
  趋势分、综合因子分、RPS 或现有趋势代理字段

- `theme_alignment_score`
  是否属于当前主线方向，板块是否有共振

- `overhead_supply_penalty`
  上方套牢区是否过重，成交密集区抛压是否明显

- `false_breakout_penalty`
  长上影、放量不涨、冲高回落、压力附近承接不足

### 6.3 `breakout_strength` 的构成

`breakout_strength` 用于衡量“若突破成立，后续能否延续为趋势行情”，由以下子因子构成：

- 主线热度与题材命中度
- 近 5-10 日趋势领先性
- 成交额与换手是否足够支撑趋势延续
- 资金承接是否连续，而非单日脉冲
- 结构是否干净，是否属于平台突破而非高位强弩之末
- 板块内是否有协同上涨个股

### 6.4 突破成功与失败的定义

为后续验证和回测统一口径，需要给出明确标准。

建议定义：

- “突破成功”
  未来 `N` 日内，价格放量站上 `break_confirm_price`，且未在短时间内跌回确认位下方

- “假突破”
  盘中或收盘越过压力，但在 `M` 个交易日内重新跌回，且量能或资金未能延续

- “延续性良好”
  突破成功后未来 `K` 日 `MFE` 达到阈值，同时 `MAE` 控制在合理范围内

### 6.5 突破相关输出

在 `compact` 分析中新增：

- `detail.breakout.score`
- `detail.breakout.strength`
- `detail.breakout.status`
- `detail.breakout.confirm_price`
- `detail.breakout.fail_price`
- `detail.breakout.expected_move_pct`

其中 `status` 建议只保留简短状态词：

- `待突破`
- `逼近确认`
- `已确认`
- `疑似假突破`
- `突破失败`

## 7. 位置性价比与风险惩罚设计

### 7.1 `entry_quality_score`

`entry_quality_score` 解决的问题不是“会不会涨”，而是“当前位置值不值得围绕它观察或试错”。

主要因子：

- 现价与 `支撑1` 的距离是否足够近
- `支撑1 -> 失效价` 的防守空间是否清晰
- `现价 -> 压力1 / 突破确认价` 的潜在空间是否足够
- 当前是否位于区间中部；若在中部则大幅扣分
- 支撑位本身是否有共振和历史承接质量

辅助输出：

- `reward_risk_ratio`
- `support_distance_pct`
- `resistance_distance_pct`

### 7.2 `risk_penalty_score`

风险惩罚统一收口，不再分散在文案里。

惩罚项包括：

- 压力附近长上影
- 放量不涨
- 主力净流出且量比偏大
- 上方成交密集区过厚
- 短期累计涨幅过大导致追高风险上升
- 趋势分和资金分出现明显背离
- 已有冲高回落历史，存在重复假突破概率

`risk_penalty_score` 不直接决定动作文案，但会强烈影响最终排序和状态分层。

## 8. 后端落点设计

### 8.1 主要修改文件

- [backend/api/routes/stocks.py](/root/jarvis/backend/api/routes/stocks.py)
  负责 `compact` 分析增强、观察列表自动排序和返回字段扩展

- [backend/etl/utils/kline_patterns.py](/root/jarvis/backend/etl/utils/kline_patterns.py)
  负责结构点位字段增强与突破确认价、失效价计算

- [docs/README.md](/root/jarvis/docs/README.md)
  更新 Watchlist 接口和观察列表自动排序说明

- [AGENTS.md](/root/jarvis/AGENTS.md)
  更新 Watchlist 当前行为说明

### 8.2 `compact` 分析增强

在 `_build_compact_watch_analysis()` 中增加一个新的内部评分步骤：

1. 生成增强后的 `level_bundle`
2. 计算 `breakout_score`
3. 计算 `breakout_strength`
4. 计算 `entry_quality_score`
5. 计算 `risk_penalty_score`
6. 计算 `recommendation_score`
7. 推导 `state_bucket`
8. 生成更精炼的 `ranking.rank_reason`

保持兼容的原则：

- 现有 `suggestion`
- 现有 `action_signal`
- 现有 `signal_reasons`
- 现有 `key_levels`

继续保留，避免前端在灰度阶段整体失效。

### 8.3 `/admin/watchlist/realtime` 排序改造

当前 `/admin/watchlist/realtime` 在拼完数据后按用户原始自选顺序重排。

重构后行为：

- 持仓股仍按现有前端逻辑单独计算显示优先级
- 观察股在后端完成自动排序
- 当 `analysis_depth=compact` 时，接口直接返回“已推荐排序”的观察股顺序

为了兼容旧逻辑，可增加一个可选参数：

- `sort_mode=auto|manual`

默认值为 `auto`。前端 Watchlist 页面固定走 `auto`。如果未来需要保留某些诊断场景，可临时切回 `manual`，但不暴露到主界面。

### 8.4 返回字段

在 `detail` 中新增：

- `decision.state_bucket`
- `decision.recommendation_score`
- `breakout.score`
- `breakout.strength`
- `breakout.status`
- `breakout.confirm_price`
- `breakout.fail_price`
- `breakout.expected_move_pct`
- `entry_quality.score`
- `entry_quality.reward_risk_ratio`
- `ranking.rank_reason`
- `ranking.sort_key`

## 9. 前端展示设计

### 9.1 总体原则

前端不做复杂排序判断，也不在前端硬编码推荐逻辑。前端的职责是：

- 按后端返回顺序展示观察列表
- 把新加的排序语义做成更清晰的首屏信息
- 保持现有克制风格，不新增弱信息概览层

### 9.2 观察列表的直接改动

在 [frontend/src/views/Watchlist.vue](/root/jarvis/frontend/src/views/Watchlist.vue) 中：

- 移除观察列表拖拽交互
- 移除“拖拽可调整顺序”提示
- 标注观察列表为“自动排序”
- 不再调用 `/admin/watchlist/reorder`

### 9.3 首屏新增信息

每张观察卡片首屏只增加少量高价值信息：

- `推荐`：`recommendation_score`
- `状态`：`突破候选 / 回踩候选 / 趋势跟踪 / 普通观察 / 风险回避`
- `突破`：`breakout_score + breakout_strength`
- `位置`：`entry_quality_score`
- `关键位`：`支撑1 / 压力1 / 突破确认价 / 失效价`

排序理由只保留一句：

- `放量预热 + 靠近突破位`
- `支撑共振 + 盈亏比优`
- `趋势延续但位置一般`
- `冲高失败风险偏高`

禁止把长文案重新堆回列表。

### 9.4 详情弹窗增强

详情弹窗中可以补充更完整信息：

- 突破确认价与失效价
- 预计延展空间
- 点位强度说明
- 当前排序原因拆解

但这些内容只进入详情，不回流到首屏，避免页面再次膨胀。

## 10. 存储与兼容策略

### 10.1 `watchlist.sort_order` 处理

第一阶段不删除 `watchlist.sort_order` 字段，原因：

- 降低迁移风险
- 兼容旧接口和可能存在的其他管理页面
- 避免一次性做数据库破坏性改动

但主界面观察列表不再使用它作为实际排序依据。

### 10.2 `/watchlist/reorder` 处理

第一阶段策略：

- 主界面移除拖拽，不再调用该接口
- 后端接口保留，但标记为兼容/废弃路径

第二阶段若确认无人使用，再考虑移除。

## 11. 验证与回测设计

### 11.1 点位验证

继续使用现有 `/admin/watchlist/levels/backtest`，重点观察：

- 支撑/压力反应率
- 主点位距现价的合理性
- 二级间距异常是否继续改善

### 11.2 突破验证

新增突破评估逻辑：

1. 对历史样本每日生成 `breakout_score`
2. 记录未来 `N` 日是否突破 `confirm_price`
3. 判断是否形成假突破
4. 统计不同分数桶的成功率、失败率、延展空间

关键指标：

- 高分桶突破命中率
- 高分桶假突破率
- 成功突破后平均 `MFE`
- 失败突破后平均 `MAE`

### 11.3 排序验证

新增观察列表排序回放：

- 每个交易日对样本池生成 `recommendation_score`
- 取 TopN 标的，比较旧顺序和新顺序的后续表现

核心指标：

- Top10 命中率
- Top10 平均 `MFE`
- Top10 平均 `MAE`
- 假突破占比
- 支撑回踩后止跌成功率
- `A/B/C/D/E` 状态桶的后续收益差异

### 11.4 上线后的实际验证

按照项目约束，功能验证优先通过 API 实测，不跑额外构建流程。

重点验证：

- `curl /admin/watchlist/realtime?analysis_depth=compact`
- `curl /admin/watchlist/{ts_code}/analysis`
- 首屏排序是否稳定反映推荐优先级
- 实时快照与收盘快照是否都能返回新字段

## 12. 错误处理与降级

### 12.1 评分降级

若某些实时字段缺失：

- 缺少实时快照时，回退最近收盘日
- 缺少部分资金字段时，只降低相关分项权重，不让整票分析失败
- 若增强点位失败，则回退现有 `build_structural_price_levels()` 基础结果

### 12.2 排序降级

若新评分字段部分计算失败：

- 保底仍返回现有 `suggestion + action_signal + key_levels`
- 观察列表排序退化为 `suggestion 优先级 + 旧分数 + 流动性`
- 保证接口不因个别票失败而整体 500

## 13. 分阶段实施建议

### Phase 1：后端评分与点位增强

- 在 `kline_patterns.py` 中扩展点位字段
- 在 `stocks.py` 中补齐 `breakout`、`entry_quality`、`ranking` 结构
- 在 `compact` 模式下完成自动排序

### Phase 2：前端观察列表切换到自动排序

- 移除拖拽
- 新增状态、推荐分和排序理由显示
- 保持首屏信息密度可控

### Phase 3：验证与回测

- 补突破回测
- 补推荐排序回放
- 观察不同状态桶的表现差异并调参

## 14. 成功标准

本次设计落地后，认为达标需满足以下条件：

- 观察列表不再依赖人工拖拽排序
- 首屏前几名股票能稳定体现“最值得盯”的优先级
- 列表可明确区分“突破候选”和“回踩候选”
- 突破预测不再只靠文案，而有显式分值和状态字段
- 点位首屏可直接给出突破确认价和失效价
- 前端保持克制，不新增弱信息堆叠
- 回测能证明高推荐分桶相较旧顺序具有更高观察价值
