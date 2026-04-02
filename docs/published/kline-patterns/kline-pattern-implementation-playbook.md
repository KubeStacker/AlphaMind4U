# 经典 K 线识别实施与测试路线

本文档是给后续模型或工程师继续接手时使用的实施手册。

目标不是解释理念，而是明确：

- 该能力如何在当前项目里继续设计和扩展
- 服务如何在容器中启动和自动加载代码
- 测试应如何分层推进
- 什么标准下可以进入 Watchlist 和 Falcon 插件

建议与 [经典 K 线识别引擎设计说明](./kline-pattern-recognition.md) 配合阅读。

## 1. 当前运行环境约束

### 1.1 服务启动方式

当前项目默认通过 Docker Compose 启动。

后端服务配置见：

- `docker-compose.yml`
- `backend/Dockerfile`
- `backend/main.py`

开发态后端启动命令：

```bash
docker-compose up backend
```

对应实际命令：

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload --reload-exclude 'data/*' --reload-exclude '__pycache__/*'
```

关键点：

- 宿主机 `./backend` 挂载到容器 `/app`
- Python 源码改动会被 `uvicorn --reload` 自动检测并重启进程
- `./data` 挂载到 `/app/data`，DuckDB 文件在容器重启后仍可保留

### 1.2 应用启动生命周期

FastAPI 在 `backend/main.py` 的 `lifespan()` 中完成：

1. 数据库初始化
2. 共享 DuckDB 连接预热
3. 后台任务消费者启动
4. 调度器启动

因此任何新增的 K 线插件、自动训练调度、校准文件加载逻辑，都应优先放在“按需加载”或“轻量初始化”路径，避免在启动阶段做重型全市场计算。

### 1.3 自动加载机制

当前系统里与 K 线识别相关的自动加载分两类。

#### Python 代码自动加载

开发态：

- 只要修改 `backend/` 下 Python 文件，`uvicorn --reload` 会自动重启进程
- 新增识别逻辑、插件逻辑、API 路由，会在容器内自动生效

生产态：

- `backend/Dockerfile` 默认使用无 `--reload` 的 `uvicorn`
- 生产环境更新代码后需要重新构建或重启容器

#### 校准文件自动加载

当前校准文件默认路径：

- `backend/etl/utils/kline_pattern_calibration.json`

当前实现已经支持：

- 首次请求时读取校准文件
- 文件 `mtime` 变化后，在下一次请求自动重新加载

这意味着：

- 训练脚本写入新的校准 JSON 后，开发态和生产态都不必手工修改代码
- 只要文件被覆盖，后续请求会自动拿到新校准结果

注意：

- 这是“按请求懒加载刷新”
- 不是后台定时主动刷新
- 如果未来要做多版本灰度，需要把“当前生效版本”从单文件升级为配置项或版本路由

## 2. 后续设计拆分路线

建议后续工作按 7 个阶段推进，而不是一次性把所有逻辑塞进插件。

## 3. 阶段一：数据协议定型

目标：先固定输入输出协议，防止后续多模型并行设计时字段漂移。

### 3.1 日线输入协议

至少要求以下字段：

- `trade_date`
- `ts_code`
- `open`
- `high`
- `low`
- `close`
- `vol` 或 `volume`
- `amount`

建议可选字段：

- `pct_chg`
- `turnover_rate`
- `factors`
- `rzye / rzmre / rzche`

### 3.2 识别结果输出协议

建议统一输出结构：

```json
{
  "pattern": "启明星",
  "code": "MORNING_STAR",
  "direction": "bullish",
  "raw_confidence": 0.82,
  "confidence": 0.79,
  "historical_hit_rate": 0.61,
  "historical_avg_ret": 0.028
}
```

### 3.3 插件层标准化协议

未来 Falcon 插件不要直接消费中文点评，而应消费标准化因子：

- `pattern_code`
- `pattern_name`
- `direction`
- `raw_confidence`
- `calibrated_confidence`
- `historical_hit_rate_5d`
- `historical_avg_ret_5d`
- `pattern_context_tags`

## 4. 阶段二：识别逻辑增强

目标：继续把规则层从“经典形态库”提升为“场景识别器”。

### 4.1 优先扩展方向

建议优先做这几类增强：

- `底部反转族`
  - 锤子线
  - 曙光初现
  - 看涨吞没
  - 启明星
- `顶部卖点族`
  - 射击之星
  - 乌云盖顶
  - 看跌吞没
  - 黄昏星
- `趋势延续族`
  - 红三兵
  - 上升三法
  - 老鸭头
- `风险预警族`
  - 量价背离
  - 高位长上影
  - 缩量滞涨

### 4.2 规则增强优先级

每个形态扩展时按下面顺序做，不要直接堆阈值：

1. 结构完整性
2. 趋势位置适配
3. 量能确认
4. 失败形态识别
5. 与其他风险信号冲突处理

### 4.3 失败形态处理

后续必须补一个很重要的能力：`failed pattern`。

例子：

- 看涨吞没出现，但次日放量跌回吞没区间内部
- 红三兵出现，但第三日长上影且量能衰竭
- 启明星出现，但未站回 5 日线

这些“识别了但失效”的形态，在卖点分析里往往比成功形态更重要。

## 5. 阶段三：训练与校准系统

目标：形成稳定的历史训练闭环。

### 5.1 当前训练脚本

现有脚本：

```bash
python3 scripts/train_kline_patterns.py \
  --start-date 2023-01-01 \
  --end-date 2026-03-07 \
  --horizons 3,5,10 \
  --min-confidence 0.55
```

### 5.2 推荐训练窗口

建议固定三套训练窗口：

1. 长周期：最近 3 年
2. 中周期：最近 18 个月
3. 短周期：最近 6 个月

目的：

- 长周期保证统计稳定性
- 中周期反映当前市场结构
- 短周期发现风格迁移

### 5.3 推荐训练产物

每次训练至少输出：

1. `kline_pattern_calibration.json`
2. `kline_pattern_training_summary.csv`
3. `kline_pattern_training_report.md`

其中报告中至少包括：

- 各形态样本数
- 各形态 3/5/10 日命中率
- 高置信度桶与低置信度桶对比
- 看涨/看跌分组基准表现
- 与上一版本校准的差异

### 5.4 训练任务后续落地方式

后续建议增加一个后台任务而不是只靠手动脚本：

- 手动触发接口
- 定时训练任务
- 训练完成后的自动版本记录

建议新增任务名：

- `kline_train`
- `kline_promote`
- `kline_validate`

## 6. 阶段四：Falcon 插件接入路线

目标：把 K 线识别从“点评层”提升到“选股因子层”。

### 6.1 插件定位

新插件建议命名：

- `classic_kline_recommender`

或中文展示名：

- `经典 K 线识别推荐`

### 6.2 插件输入

插件不应只拿单日截面数据，建议至少拿：

- 当日截面
- 每只股票最近 60~120 日历史
- 训练校准文件
- 市场环境过滤结果
- 主线板块强度

建议在插件实现里明确区分两类输入：

1. `snapshot_universe_df`
2. `history_panel_df`

其中：

- `snapshot_universe_df` 用于做当日截面评分与过滤
- `history_panel_df` 用于做逐股 K 线形态识别

推荐 `history_panel_df` 字段协议：

- `trade_date`
- `ts_code`
- `open`
- `high`
- `low`
- `close`
- `vol`
- `amount`
- `pct_chg`
- `turnover_rate` 可选
- `factors` 可选

推荐 `snapshot_universe_df` 字段协议：

- `ts_code`
- `name`
- `close`
- `pct_chg`
- `amount`
- `ret_5`
- `ret_10`
- `ret_20`
- `ma20`
- `ma50`
- `benchmark_ok`
- `benchmark_code`
- `mf_today`
- `mf_5`
- `sector_strength` 后续建议补充
- `mainline_score` 后续建议补充

### 6.3 插件输出

建议输出：

- `strategy_score`
- `confidence`
- `signal_label`
- `score_breakdown`

建议与现有 `FalconPlugin.run()` 输出协议保持一致：

```text
ts_code,name,strategy_score,confidence,signal_label,score_breakdown
```

其中 `score_breakdown` 至少包含：

- 触发形态列表
- 每个形态原始置信度
- 每个形态校准后置信度
- 历史命中率
- 风险冲突项
- 市场环境过滤结果

推荐 `score_breakdown` 结构：

```json
{
  "entry_logic": "classic_kline + trend + flow + sector - risk",
  "kline_signals": [
    {
      "pattern_code": "MORNING_STAR",
      "pattern_name": "启明星",
      "direction": "bullish",
      "raw_confidence": 0.82,
      "calibrated_confidence": 0.79,
      "historical_hit_rate_5d": 0.61,
      "historical_avg_ret_5d": 0.028
    }
  ],
  "risk_signals": [
    {
      "pattern_code": "DIVERGENCE",
      "pattern_name": "量价背离",
      "confidence": 0.71
    }
  ],
  "factor_scores": {
    "pattern_score": 72.0,
    "trend_score": 66.0,
    "flow_score": 58.0,
    "sector_score": 63.0,
    "market_regime_score": 70.0,
    "risk_penalty": 18.0
  },
  "market_env": {
    "benchmark_ok": true,
    "top_mainline": "AI",
    "risk_mode": "normal"
  }
}
```

### 6.4 插件融合逻辑

建议不要做简单“命中一个形态就推荐”，而是做如下结构：

```text
pattern_score
+ trend_score
+ flow_score
+ sector_score
- risk_penalty
= final_strategy_score
```

建议第一版采用显式权重，避免一开始就做黑盒模型：

```text
final_strategy_score =
  0.30 * pattern_score
  + 0.22 * trend_score
  + 0.16 * flow_score
  + 0.16 * sector_score
  + 0.10 * market_regime_score
  - 0.18 * risk_penalty
```

其中：

- `pattern_score` 来自看涨形态加分与看跌形态扣分
- `trend_score` 来自均线结构、位置、20/50 日趋势
- `flow_score` 来自资金流、量比、换手
- `sector_score` 来自主线与板块热度
- `market_regime_score` 来自大盘环境过滤
- `risk_penalty` 来自高位、背离、长上影、过热涨幅等

推荐 `pattern_score` 的第一版计算方式：

```text
pattern_score =
  sum(top bullish calibrated_confidence * bullish_weight)
  - sum(top bearish calibrated_confidence * bearish_weight)
```

约束建议：

- 单边最多取前 3 个形态
- 重复语义形态不能重复累计过多权重
- 强看跌信号优先级高于弱看涨信号

### 6.5 卖点插件建议

除了买点插件，建议单独设计一个卖点评分器：

- 高位看跌反转形态
- 量价背离
- 高换手冲高回落
- 跌破关键均线
- 连续长上影

这部分未来更适合直接喂给 Watchlist。

### 6.6 插件实现建议

建议新增插件文件：

- `backend/strategy/falcon/plugins/classic_kline_recommender.py`

建议实现步骤：

1. 继承 `FalconPlugin`
2. 在 `default_params()` 中声明默认权重、阈值、样本门槛
3. 在 `run()` 中对股票池逐股拉取历史面板并识别
4. 计算多空形态得分和风险扣分
5. 输出标准 Falcon 结果集
6. 在 `backend/strategy/falcon/registry.py` 注册插件

建议默认参数：

- `top_n`
- `min_pattern_confidence`
- `min_historical_hit_rate`
- `min_sample_count`
- `w_pattern`
- `w_trend`
- `w_flow`
- `w_sector`
- `w_market`
- `w_risk`
- `max_bearish_override`

### 6.7 插件回测口径

接入 Falcon 后，统一沿用当前框架的未来收益评估：

- `ret_5d`
- `ret_10d`
- `hit_5d`
- `hit_10d`

建议额外增加一个卖点相关评估口径：

- `max_drawdown_10d`
- `peak_to_close_drawdown`
- `event_exit_ret`

原因：

- K 线形态尤其是卖点形态，不一定提高收益，但可能显著降低回撤
- 只看 5/10 日收益会低估卖点插件的价值

## 7. 阶段五：Watchlist 点评接入路线

目标：把形态转成“可执行建议”，而不是技术术语堆砌。

### 7.1 前端展示优先级

建议优先展示：

1. 看跌卖点形态
2. 高历史命中率反转形态
3. 与风险因子共振的信号
4. 一般性中性形态

### 7.2 点评模板建议

建议统一模板：

```text
【形态】乌云盖顶
【历史】近 3 年同类形态 5 日转弱概率 63%
【结论】若次日失守 5 日线，偏向减仓；若放量反包，则本次卖点信号失效
```

### 7.3 Watchlist 的关键目标

Watchlist 的目标不是做选股，而是做：

- 持仓风险解释
- 卖点提醒
- 继续持有还是减仓的辅助判断

所以 Watchlist 中看跌形态的重要性通常高于看涨形态。

## 8. 详细测试路线

当前仓库还没有正式单元测试体系，因此这里需要明确后续测试分层。

## 9. 第一层：纯函数单元测试

目标：验证每个识别函数的结构正确性。

建议为以下函数补测试：

- `detect_hammer_hanging`
- `detect_shooting_star`
- `detect_engulfing`
- `detect_piercing_dark_cloud`
- `detect_morning_evening_star`
- `detect_three_white_soldiers`
- `detect_three_black_crows`
- `detect_rising_three_methods`
- `detect_falling_three_methods`
- `detect_volume_price_patterns`
- `train_pattern_calibration`
- `get_latest_signals`

### 9.1 测试方式

使用人工合成 OHLCV 数据构造极小样本：

- 标准形态样本
- 边界样本
- 伪形态样本
- 明显错误样本

### 9.2 核心断言

例如锤子线测试必须覆盖：

- 标准锤子线能触发
- 高位锤子线不应直接当作看涨锤子线
- 长下影但收盘太弱时置信度下降
- 小波动噪声不应误触发

## 10. 第二层：历史回放测试

目标：确认规则在真实 A 股历史样本里不是大面积误报。

### 10.1 测试数据范围

建议至少覆盖：

- 大盘蓝筹
- 中小盘题材
- 高波动妖股
- 长期下跌股
- 长期横盘股

### 10.2 回放目标

关注：

- 单个形态触发频率是否合理
- 是否出现明显泛滥误判
- 高置信度是否优于低置信度
- 看涨 / 看跌方向是否与未来统计一致

## 11. 第三层：校准结果测试

目标：验证训练输出本身可信。

### 11.1 关键检查项

- 每个形态样本数是否足够
- 高置信度桶是否显著优于低置信度桶
- 看涨形态的 `directional_edge` 是否大多为正
- 看跌形态的 `directional_edge` 是否大多为正
- 是否存在样本极少却排名极高的异常项

### 11.2 建议阈值

可先采用保守门槛：

- 样本数 < 30 的形态不直接用于正式推荐
- 样本数 < 15 的分桶不参与置信度提升
- 高置信度桶必须显著优于低置信度桶才允许启用

## 12. 第四层：API 集成测试

目标：确认专业点评链路可用。

建议测试：

- `GET /`
- `GET /system/status`
- Watchlist 页面数据所依赖的分析接口
- `PatternRecognizer` 的现有调用路径

### 12.1 集成检查项

- 新增形态能否出现在 `patterns`
- `suggestion` 是否符合多空方向
- `detail.patterns` 是否能正常展示
- 校准文件更新后是否能自动反映到下一次请求

## 13. 第五层：容器测试

目标：确认在你的实际部署方式下可工作。

### 13.1 开发态容器测试

步骤：

1. `docker-compose up backend`
2. 修改 `backend/etl/utils/kline_patterns.py`
3. 观察 `jarvis-backend` 日志，确认自动 reload
4. 再调用接口，确认新逻辑生效

### 13.2 校准文件自动刷新测试

步骤：

1. 启动后端容器
2. 先请求一次相关接口，让服务加载旧校准
3. 运行训练脚本覆盖 `kline_pattern_calibration.json`
4. 再请求一次接口
5. 确认响应中的 `confidence / historical_hit_rate` 发生变化

### 13.3 生产态容器测试

步骤：

1. 以 `backend/Dockerfile` 默认命令启动
2. 替换校准文件
3. 验证下一次请求是否自动刷新校准
4. 修改 Python 代码后，确认必须重启容器才生效

## 14. 第六层：策略验证测试

目标：验证“加入 K 线形态因子后，策略整体是否变好”。

不要只看形态本身命中率，要看插件接入后的总体效果：

- 选股数量是否过多
- 命中率是否改善
- 收益是否改善
- 回撤是否变大
- 卖点信号是否能减少高位回撤

建议比较三组：

1. 纯趋势因子
2. 趋势 + 原始形态因子
3. 趋势 + 校准后形态因子

### 14.1 插件专项回测

后续接入 Falcon 插件后，建议固定比较四组：

1. `falcon_momentum`
2. `classic_kline_recommender`
3. `falcon_momentum + classic_kline factor`
4. `falcon_momentum + classic_kline calibrated factor`

对比指标建议：

- `pick_count`
- `avg_score`
- `hit_5d`
- `hit_10d`
- `ret_5d`
- `ret_10d`
- `max_drawdown`
- `win_loss_ratio`

### 14.2 晋升门槛

建议新插件或新校准版本只有满足以下条件时才允许正式晋升：

- 过去 120 个交易日的 `hit_5d` 不低于当前版本
- `ret_10d` 明显改善，或相同收益下回撤明显下降
- 高置信度信号区间稳定，不依赖极少数样本
- 无明显样本外崩塌

建议可采用保守规则：

- 提升收益超过 `+1.5%` 或
- 回撤降低超过 `10%`
- 且信号样本数不少于旧版的 `70%`

## 15. 验收标准

后续模型或工程师完成设计后，至少要满足下面标准才能进入主链路。

### 15.1 规则层验收

- 没有未来函数
- 主要形态具备趋势/位置/量能确认
- 关键函数有合成样本测试

### 15.2 训练层验收

- 可稳定输出校准文件
- 训练报告能解释高低置信度差异
- 小样本形态不会被异常放大

### 15.3 系统层验收

- 容器开发态代码改动自动热加载
- 校准文件更新后请求级自动刷新
- Watchlist 点评链路不报错
- 无校准文件时能自动回退到原始规则模式

### 15.4 策略层验收

- 插件回测优于无形态版本，或至少风险收益比提升
- 卖点建议能识别明显高位风险样本

## 16. 自动发布与回滚 SOP

目标：让后续模型或工程师可以把训练、校准、上线串成一条稳定流程。

### 16.1 推荐 SOP

1. 更新日线数据
2. 运行训练脚本生成新校准文件
3. 生成训练摘要与报告
4. 与当前线上版本做历史比较
5. 若通过门槛，则发布为新版本
6. 发布后观察 Watchlist 与插件结果
7. 若异常，则回滚到上一版本校准文件

### 16.2 文件版本管理建议

建议保留：

- `kline_pattern_calibration.current.json`
- `kline_pattern_calibration.prev.json`
- `kline_pattern_calibration.candidate.json`

或使用：

- `kline_pattern_calibration.v20260307.json`
- `kline_pattern_calibration.v20260314.json`

### 16.3 发布方式

开发态容器：

- 直接覆盖校准文件
- 下一次请求自动加载

生产态容器：

- 推荐先写入 `candidate`
- 验证通过后再原子替换 `current`
- 替换完成后无需重启 Python 进程，下一次请求会自动刷新校准

### 16.4 回滚方式

若新版本出现以下问题，立即回滚：

- Watchlist 点评明显异常
- 看跌/看涨方向错乱
- 插件选股数量异常失控
- 历史命中率字段为空或大面积失真

回滚动作：

1. 将 `prev` 校准文件覆盖 `current`
2. 再触发一次相关接口请求确认加载
3. 记录回滚原因

## 17. 给后续模型的明确边界

后续模型继续设计时，应遵循以下边界：

- 不要推翻当前 `detect_all_patterns` / `PatternRecognizer` 对外接口
- 可以扩充形态，但不要直接删掉现有字段
- 训练和校准逻辑必须保持“离线使用未来数据，在线禁止未来数据”
- Watchlist 点评优先解释卖点和风险，而不是泛泛而谈看多
- Falcon 插件层必须把 K 线当作因子，不得直接把单一形态当成最终推荐依据

## 18. 推荐下一步实施顺序

建议后续继续按这个顺序推进：

1. 补 `pytest` 测试骨架和合成 OHLCV 样本库
2. 用真实 `daily_price` 训练首版校准文件
3. 产出训练报告，筛掉低效或低样本形态
4. 接入 Watchlist 卖点点评
5. 设计 Falcon 新插件
6. 接入自动训练与版本晋升
