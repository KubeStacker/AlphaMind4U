# spec/backend/quant/database — 选型、范式、效率检查、迁移

## 1. 选型决定：quant 用 SQLite（pure-Go），不引 PostgreSQL

- 2026-09 重构时由 DuckDB 一次性迁移：CGO 版 go-duckdb 在 2C/3.8G 上编译易 OOM
  且每次构建慢；SQLite 单写者模型与旧 RLock 语义同构，点查 SQL 全可移植，
  构建秒级、无 CGO，符合“轻量”。PG 常驻 ~300MB + 运维重，不引。
- 并发根治靠“单属主进程 + 单连接串行”（`SetMaxOpenConns(1)` + Mutex），不靠换库。
- 库文件 `data/quant.db`，`journal_mode=WAL` + `synchronous=NORMAL` + 独占打开。
- 未来后端各带各库文件，互不共享，不跨库 join。

## 2. 现役与删除（2026-09 已执行）

- 现役 24 表：22 白名单数据表（含 `strategy_pool_month/entry` 月度 Top20 快照，0002 建）
  + `trade_calendar` + `etl_tasks`，行数与旧库 1:1（快照表为新表，无旧行）。
- 已删 25 表（迁移时不迁，后期按本节重建）：`ai_analysis_cache`、`ai_trends`、
  `doc_notes`、`doc_reading_progress`、`doc_tag_mapping`、`doc_user_tags`、
  `fx_daily`、`margin_detail`、`mainline_scores`、`market_overview_snapshots`、
  `market_sentiment`、`stock_concepts_old`、`stock_factor_daily`、
  `stock_financials`、`strategy_backtest_runs`、`strategy_daily_summaries`、
  `strategy_definitions`、`strategy_observations`、`strategy_recommendations`、
  `user_ai_config`、`user_ai_provider_configs`、`user_holdings`、
  `user_prompt_templates`、`users`、`watchlist`；另删 `data/jarvis.db` 旧库、
  `etl_tasks` 90 天前 `FAILED` 行（4 行）。

## 3. 范式与类型规范

- 快照表（0002）：`strategy_pool_month` 以 `effect_month` 为主键存月份边界与数据版本；
  `strategy_pool_entry` 以 `(effect_month, rank)` 为主键存名次、代码、累计金额（元），
  不递补、不存第 21 名；重建幂等（BulkUpsert 同键覆盖），修订另存版本不倒改已用快照。
- 维度表（basic/concepts/成分）：3NF + 主键（`ts_code`、`code`、复合 PK）。
- 事实表（日线/资金/daily_basic/margin/suspend/weight/hsgt）：不建主键保写入，
  靠 `(ts_code,trade_date)` 逻辑去重（Dedupe + BulkUpsert 先 UPDATE 后 INSERT）。
- 类型：`TEXT` 定长语义、日期 `TEXT 'YYYY-MM-DD'`、金额 `REAL`、状态码短串；
  新增列必须可重放（`ALTER TABLE … ADD COLUMN`，SQLite 无 IF NOT EXISTS 时先查 `PRAGMA`）。

## 4. 效率检查（每次建表/改查询必过）

1. 索引：维度表主键自带；无主键事实表必须建键列复合索引
  （`daily_price/moneyflow/daily_basic/market_index/margin`，SQLite 行存必需，
   否则 11M 行全扫描卡死——2026-09 已实测）。
2. 查询强制列白名单 + `LIMIT≤1000` + `ORDER BY` 白名单。
3. 新 SQL 附 `EXPLAIN QUERY PLAN` 说明；事实表禁止 `SELECT *`。
   - 月度 Top20 聚合（`GROUP BY ts_code` 全月扫描，EXPLAIN 为 `SCAN daily_price` +
     双 TEMP B-TREE，11M 表实测约 12s）为离线月批，结果落快照表后推荐/回放只读快照
     （毫秒级主键点查），不每次重扫。
4. ETL 批量写单事务，单批次 ≤5000 行（默认 2000）。

## 5. 迁移规范

- 内嵌执行器：`internal/db/migrations/NNNN_*.sql` 按名排序执行，
  `schema_version` 表记账，启动时自动补齐（已应用跳过）。
- 历史时点起点：迁移 0001 给 7 张财务/事件表加 `first_seen_at`，
  INSERT 分支自动打标，UPDATE 永不覆盖；修订识别靠 `ann_date`/`first_seen_at`
  对比，行业有效区间与完整版本链为规划中。
- 禁手改线上库；备份：停写后 `cp quant.db quant.YYYYMMDD.db`（SQLite 在线拷需先停写）。

## 6. 策略版本追加存储（0003）

新增strategy_pool_snapshot：id递增主键、effect_month、内容version（同内容可在不同发布记录出现）、payload_json、created_at。
版本按规范化完整月池内容的SHA256生成，整份JSON单次INSERT原子写入，相邻同内容幂等，修订及恢复旧内容均追加。
按(effect_month,id)索引查最新快照，不更新已存版本；不加入通用数据集白名单。
旧0002两表保留为历史记录，新的策略快照路径不再写入或读作已验收版本，不虚构旧来源时间。
新增表需要服务启动迁移后才生效；内容hash不等于全市场原始数据版本或历史公开时间证明。

## 7. 研究任务（0004）

strategy_runs保存run_id、请求键/JSON、状态/进度、冻结输入JSON/hash、模型版本、结果JSON和错误。
活动同请求键唯一，终态保留后可新建实验；按(status,created_at,run_id)取队首，最近列表使用(created_at,run_id)索引，单消费者先ETL后研究。
RUNNING重启恢复PENDING，已有输入不再从源重建；取消或失败不发布部分绩效。
原始输入以完整JSON保存，结果与完成状态单次UPDATE发布；不加入通用数据集白名单。

## 8. 分红阶段保留（0005）

stock_dividend主键改为(ts_code,end_date,ann_date,div_proc)，迁移重建同名表并逐列保留既有记录与first_seen_at。
官方接口存在同公告日不同实施阶段；旧三列键会覆盖阶段，迁移后必须按证券重采恢复来源记录。
不改变字段/API；阶段内修订仍更新该阶段，不声称形成完整历史版本链；历史first_seen_at不倒填为公告时间。
分红公告日允许NULL，分红BulkUpsert键使用SQLite IS匹配NULL，避免重复采集反复插入；不伪造公告日期。

## 9. 分红来源内容保留（0006，取代0005的唯一性假设）

官方同公告日同阶段仍有基准股本/现金额等不同版本，四列只能用作查询索引，不能作唯一约束。
0006重建同名表保留全部旧字段、记录及first_seen_at；取消四列PK，建立四列普通索引。
分红逻辑去重键采用全部16个来源业务字段，NULL安全比较；内容不同追加，完全相同幂等保留首次采集时间。
不增加API字段；来源内容快照不能证明历史可知时间，事件选择须独立解决来源修订与重复实施辨识。
