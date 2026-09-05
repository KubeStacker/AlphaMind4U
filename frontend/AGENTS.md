# frontend/AGENTS.md — 新版控制台说明

- 技术栈：Vite + Vue3 + TS + Vue Router + Pinia；UI 自建组件，不引 Element/AntD；图表暂不引 ECharts（占位在 `src/widgets/ChartSlot.vue`）。
- 入口：`index.html` → `src/main.ts` → `src/App.vue`。
- 目录：`src/pages/{login,data-dashboard,quality,mcp-inspector,doc-home,settings}-*/` 编排页面；
  `src/widgets/` 放指标卡与图表占位；`src/api/{quant,mcp,doc}.ts` 按域分 client（Bearer 统一注入）；
  `src/stores/` 放 auth/toast/confirm；`src/styles/tokens.css` 放冻结风格 token；
  `src/shared/` 放 Topbar/AuthGuard/DataTable/ConfirmModal/Toast/api-error。
- 路由（懒加载，页面路径避开 API 前缀 `/admin`、`/mcp`、`/doc`）：`/data`（数据集真数据）、
  `/quality`（覆盖看板）、`/lab-mcp`（三只读工具）、`/lab-doc`（502 占位）、
  `/settings`（用户管理本地占位，标注待后端接口）。图标按需导入（见 `shared/icons.ts`）。
- 约束：每个导出符号一句中文注释；日志/报错中文；失败只展示错误不造假数据；不打全量数据到 console；
  `node_modules/`、`dist/` 不提交。
