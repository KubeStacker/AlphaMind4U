# Watchlist 专注模式点位精简设计文档

> 日期: 2026-04-09
> 状态: 待实施

## 1. 背景与目标

当前 Watchlist 页面已经把常规视图拆成“持仓跟踪 + 观察列表”，并在首屏卡片中直出动作信号、触发/失效和关键点位。但专注模式仍然过于简化，只显示：

- 股票代码
- 最新价格
- 当日涨跌幅

这导致专注模式虽然干净，却失去了交易界面应有的可执行信息。用户在切到专注模式后，仍然需要退出再回到普通卡片查看支撑、压力、触发和失效价，无法形成真正的“盯盘席位”视图。

本次改动目标：

- 在专注模式中补上关键点位信息
- 只保留动作词和关键点位，不显示长句指导
- 继续复用现有 `compact` 数据链路，不引入新接口或重分析请求
- 保持专注模式的低干扰、高可扫读特性

本次改动只涉及 Watchlist 前端专注模式展示，不调整普通卡片、不改变后端评分逻辑，也不新增后端字段。

## 2. 现状 Review

### 2.1 现有专注模式结构

[frontend/src/views/Watchlist.vue](/root/jarvis/frontend/src/views/Watchlist.vue) 当前专注模式入口由顶部“专注”按钮触发，使用全屏遮罩渲染，仅遍历 `holdingRows`。每个持仓块只展示：

- `ts_code`
- `price`
- `pct`

专注模式当前不展示：

- `suggestion`
- `action_signal`
- `key_levels`
- `trade_plan.entry`
- `trade_plan.invalid`

### 2.2 当前可复用数据

当前 Watchlist 页面已经统一通过：

`/admin/watchlist/realtime?analysis_depth=compact&sort_mode=auto`

获取列表数据。该链路在 `compact` 模式下已经稳定返回以下首屏可用字段：

- `analyze.suggestion`
- `analyze.detail.action_signal.trigger`
- `analyze.detail.action_signal.fallback`
- `analyze.detail.trade_plan.entry`
- `analyze.detail.trade_plan.invalid`
- `analyze.detail.key_levels`

前端也已经有现成的轻量方法：

- `getSignalText(item)`
- `getTriggerText(item)`
- `getInvalidText(item)`
- `getKeyLevels(item)`

因此专注模式补点位不需要新增接口，不需要切换为 `full` 分析，也不需要为单票单独补拉详情。

### 2.3 当前问题

专注模式的问题不是“信息太少”，而是“缺少最关键的交易语义”：

1. 只能看到价格波动，看不到动作词，用户无法第一眼区分“试错 / 观望 / 减仓”
2. 看不到最近支撑和压力，无法快速判断当前位置距离结构位还有多远
3. 看不到触发和失效价，专注模式缺乏最基本的执行边界
4. 普通卡片已有的有效信息在专注模式里被完全抹掉，导致专注模式更像行情看板，不像交易界面

## 3. 设计原则

本次专注模式重构遵循以下原则：

1. 价格仍是第一视觉层级，不能被点位文字压过
2. 动作词只保留一个短词，不显示完整句子
3. 点位按交易顺序排布，优先级高于摘要文案
4. 缺失字段直接省略，不造默认解释性文案
5. 不增加新的交互层，不新增按钮、说明区或概览区

## 4. 核心设计

### 4.1 视觉 Thesis

专注模式应呈现为“黑底盯盘带”，像交易席位上的极简报价墙：价格最响，动作第二，点位第三，其他信息全部退场。

### 4.2 内容结构

每个持仓块改为四层信息，固定由上到下排列：

1. 代码
2. 价格与涨跌幅
3. 动作词
4. 关键点位

不再展示：

- 长句结论
- 量比
- 换手
- AI 分析入口
- 详情按钮
- 普通模式中的原因 chips

### 4.3 动作词规则

动作层只展示 `getSignalText(item)` 的结果，允许的可见词包括：

- `主动进攻`
- `试错`
- `关注`
- `观望`
- `减仓`
- `回避`
- `持币`

动作词沿用现有颜色语义：

- 偏买入/进攻使用红色系
- 偏卖出/风控使用绿色系
- 中性观察使用白灰色

专注模式不再拼接 `headline`、`summary`、`trade_plan.current` 等完整语句。

### 4.4 点位显示规则

关键点位层只显示四类信息，固定顺序如下：

1. `支`：最近有效支撑位
2. `压`：最近有效压力位
3. `触`：触发价或触发条件摘要
4. `失`：失效价或失效条件摘要

其中：

- `支` 与 `压` 来自 `getKeyLevels(item)`，只取最靠近当前交易的主支撑与主压力
- `触` 优先取 `action_signal.trigger`，没有则回退 `trade_plan.entry`
- `失` 优先取 `action_signal.fallback`，没有则回退 `trade_plan.invalid`

字段缺失时直接跳过，不渲染占位文案。

### 4.5 点位格式

为避免专注模式重新长成普通卡片，点位采用短标签 + 紧凑文本：

- `支 24.80`
- `压 26.10`
- `触 26.20放量站稳`
- `失 24.60跌破离场`

其中：

- 价格型字段优先显示两位小数
- 文本型 `触` / `失` 延续已有截断策略，保证单行可读
- 移动端允许自动换到第二行，但不超过两行总高度

## 5. 前端实现方案

### 5.1 复用现有数据方法

专注模式直接复用当前页面内已有的四组方法，不新增状态：

- `getSignalText(item)` 负责动作词
- `getKeyLevels(item)` 负责结构位
- `getTriggerText(item)` 负责触发信息
- `getInvalidText(item)` 负责失效信息

### 5.2 新增轻量提取方法

为了让专注模式不重复在模板里做过滤，前端新增一个纯函数 helper：

- `buildZenFocusTokens({ levels, triggerText, invalidText })`
  从现有 `key_levels` 中抽取主支撑和主压力，并按 `支 / 压 / 触 / 失` 顺序拼成专注模式 token 列表

该 helper 放在 [frontend/src/composables/watchlistZen.js](/root/jarvis/frontend/src/composables/watchlistZen.js)，只服务于专注模式，不改变普通模式既有点位展示逻辑。

### 5.3 专注模式布局

专注模式现有容器保留全屏和居中逻辑，但单个持仓块从“纯数字列”改为“窄列信息块”：

```text
代码
现价
涨跌幅
动作词
支 / 压 / 触 / 失
```

布局要求：

- 桌面端多列横向排布
- 移动端仍以多列为主，但允许宽度更紧凑
- 持仓块之间保留明显留白，不加厚重卡片边框
- 点位区视觉对比弱于价格区，避免喧宾夺主

## 6. 数据流与边界

### 6.1 数据流

改动后的专注模式数据流保持不变：

```
/admin/watchlist/realtime?analysis_depth=compact
  -> rows
  -> holdingRows
  -> 专注模式模板
```

不会新增：

- 单票详情补拉
- 新的后端接口
- 新的前端 store

### 6.2 边界行为

边界约束如下：

1. 若 `holdingRows` 为空，仍显示“暂无持仓股”
2. 若股票没有 `key_levels`，专注模式只显示动作词，不补“暂无点位”
3. 若股票只有 `触` 或只有 `失`，则只显示存在的那一项
4. 若 `suggestion` 缺失，则回退到 `观望`
5. 若专注模式刷新过程中数据尚未返回，沿用现有快照，不显示额外 loading 容器

## 7. 风险与规避

### 7.1 风险

1. 专注模式信息密度上升后，移动端可能出现横向拥挤
2. `触` / `失` 字段有时是短句，不一定总能压缩成纯价格
3. 普通模式与专注模式可能出现点位排序不一致，造成理解偏差

### 7.2 规避

1. 专注模式只取单个主支撑和主压力，不展示完整 `key_levels` 列表
2. `触` / `失` 继续复用现有截断函数，控制长度
3. 支撑和压力统一基于 `getKeyLevels(item)` 结果取首个主位，避免出现两套筛选逻辑

## 8. 验证方案

实施后按以下方式验证：

1. 进入 Watchlist，切换到专注模式，确认持仓股显示动作词和点位
2. 检查无点位股票是否只显示动作词且无多余占位文本
3. 检查有 `触` / `失` 文本时是否在移动端不超过两行
4. 通过浏览器访问 Watchlist 页面确认专注模式桌面端和移动端布局可读
5. 如需确认数据来源，通过 `/admin/watchlist/realtime?analysis_depth=compact&sort_mode=auto` 抽样核对返回的 `key_levels`、`trigger`、`fallback`

## 9. 涉及文件

- [frontend/src/views/Watchlist.vue](/root/jarvis/frontend/src/views/Watchlist.vue)
  负责专注模式模板与轻量 helper 增补

- [frontend/src/composables/watchlistZen.js](/root/jarvis/frontend/src/composables/watchlistZen.js)
  负责专注模式主支撑/主压力/触发/失效位的轻量提取与排序

- [AGENTS.md](/root/jarvis/AGENTS.md)
  需要补充 Watchlist 专注模式已支持动作词与关键点位直出的说明

- [docs/README.md](/root/jarvis/docs/README.md)
  需要同步更新 Watchlist 页面行为描述

- [docs/published/trading-system/README.md](/root/jarvis/docs/published/trading-system/README.md)
  需要同步 published 文档，保证对外说明一致
