# services/_template-backend/AGENTS.md — 新后端脚手架（AI 必读）

> 接新后端时复制本目录为 `services/<name>-backend/`，按 `spec/backend/_template/` 落齐。

## 复制改名

```sh
cp -r services/_template-backend services/<name>-backend
```

服务名 `kebab-case`；Go 包小写短名；文件 `snake_case`；导出 `CamelCase`。

## 落齐四件套

1. `spec/backend/ports.md` 领端口并登记。
2. `shared/contracts/openapi-<name>.yaml` 契约先行。
3. 本服务 `AGENTS.md`（复制本文件并改写）。
4. 前端 `src/pages/<域>-*` + `src/api/<域>.ts`。

## 禁区

独立库文件放 `data/<name>.db`；禁碰他人库；禁跨服务 import；改接口先改契约。
