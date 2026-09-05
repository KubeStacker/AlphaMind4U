# spec/frontend/routing — URI 路由、目录、数据接入

## 1. URI→后端映射（端口见 `backend/ports.md`）

| 前缀 | 目标 | 说明 |
|---|---|---|
| `/admin/*` | quant :8000 | 数据集 + ETL + 质量接口 |
| `/mcp` | mcp :8081 | streamable-http（仅此前缀代理） |
| `/doc/*` | docreader :8002 | 预留，占位 502 页 |
| `/settings` | 本地 | 系统设置页 |

页面路径与 API 前缀严格区分：MCP 检查页为 `/lab-mcp`、文档页为 `/lab-doc`、
质量页为 `/quality`，策略研究页为 `/lab-strategy`（调真接口
`POST /admin/strategy/backtests` 与 `GET /admin/strategy/pools`），
不占用 `/mcp`、`/doc`（否则 dev 反代拦截导致 401/500）。

反代由 `vite.config.ts server.proxy` 承担（dev 模式生效，`jarvis.sh` 即用 dev 对外，
替代已删 nginx.conf）。

## 2. 目录

```text
frontend/src/
  pages/{data-dashboard,mcp-inspector,doc-home,settings,strategy-lab}-*/
  widgets/  api/{quant,mcp,doc,strategy}.ts  stores/  styles/tokens.css  shared/
```

## 3. 数据接入

- `api/` 按域分 client（baseURL + `Authorization: Bearer` 统一注入）。
- 登录走真接口 `POST /admin/auth/login`：用户名、密码、共享令牌服务端一次验明，
  通过后存返回的共用令牌；用户鉴权即该单用户模型，不另设会话。
- 删 localStorage 假数据（`TABLES_DATA`、`INITIAL_USERS`），改调真接口；
  本地仅保留登录态缓存（`jarvis_token`/`jarvis_current_user`）。

策略研究页提交后台任务后轮询只读状态，支持取消和读取最近任务，不在刷新时重算策略。
2026-09-14页面简化目标与验收统一见[strategy/plan.md第九、十章](../strategy/plan.md)：沿用原路径，单页展示今日建议与策略表现，回测设置由侧栏打开；尚未实施。
账户控件只提交已实现字段；月份按起止日期自动生成，结果展示冻结账户及该run月池，不读取最新池替换。
任务离开页面仍在后端保留，通过最近任务继续查看；浏览器不另存行情、回测结果或账户真源。

策略研究结果新增逐笔成交和期末现金/累计费用/持股表，仅读取保存字段；旧run未保存时明确提示。

策略研究周期表读取已保存cycles/trade_summary；零完成样本胜率不可估，旧任务不补算，点估计旁显示样本量。

策略结果显示保存的证据等级及限制，COMPLETED不是研究通过标志；旧结果缺字段显示未保存证据等级。

研究提交可选择strict或available_candidates；显示冻结策略和缺口数量/有界样本，关键权益缺失仍阻断。
