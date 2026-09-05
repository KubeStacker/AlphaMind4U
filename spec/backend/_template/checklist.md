# spec/backend/_template/checklist — 新后端合入清单

- [ ] 端口已在 `backend/ports.md` 登记，无冲突。
- [ ] `GET /health` 返回 `{status:"ok",service:"<name>"}`。
- [ ] 契约 `openapi-<name>.yaml` 已补，查询类接口有 `LIMIT` 上限。
- [ ] 独立库文件，不直连他人库；无跨服务 import。
- [ ] 本服务 `AGENTS.md` 已补；`jarvis.sh` 可启停；`go vet` 通过。
- [ ] 前端页面 + api client 已接（或明确占位 502）。
