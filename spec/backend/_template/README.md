# spec/backend/_template/README — 新后端脚手架用法

> 接后端 3 起：复制本模板，不手搭空架。

## 1. 复制改名

```sh
cp -r services/_template-backend services/<name>-backend
```

服务名 `kebab-case`；Go 包小写短名；文件 `snake_case`；导出 `CamelCase`。

## 2. 落齐四件套

1. `backend/ports.md` 领端口并登记。
2. `shared/contracts/openapi-<name>.yaml` 契约先行。
3. 本服务 `AGENTS.md`（可读文件 + 禁区 + 本地命令）。
4. 前端 `src/pages/<域>-*` + `src/api/<域>.ts`（见 `frontend/routing.md`）。

## 3. 合入

过 `checklist.md` 后合入；独立库文件放 `data/<name>.db`，禁碰他人库。
