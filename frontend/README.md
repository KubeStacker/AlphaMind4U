# frontend — JARVIS 新版控制台（Vite + Vue3 + TS）

> 入口 `index.html` → `src/main.ts` → `src/App.vue`。

## 命令

```sh
npm i        # 安装依赖（源可用 registry.npmmirror.com）
npm run dev -- --port 8080 --host  # 对外服务（含反代，jarvis.sh 即此方式）
npm run build    # 类型检查 + 构建，产物在 frontend/dist（不提交）
```

## 说明

- 反代（仅 dev 生效）：`vite.config.ts server.proxy` 实现
  `/admin/*→127.0.0.1:8000`、`/mcp→127.0.0.1:8081`、`/doc→127.0.0.1:8002`。
- 登录为本地门禁 + Bearer 令牌持有，等量化后端补齐 `/admin/auth` 后替换。
- 构建内存不足时：`NODE_OPTIONS=--max-old-space-size=1024 npm run build`。
