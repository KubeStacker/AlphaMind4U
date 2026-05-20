# 仪表盘 (Dashboard) — 已下线

## 状态

该模块已于 2026-05-18 从当前代码中移除，不再作为前端路由或独立页面存在。

## 已移除范围

- 前端路由 `/dashboard`
- `frontend/src/views/Dashboard.vue`
- `frontend/src/components/dashboard/*`
- 后端接口 `/admin/market_sentiment`
- 后端接口 `/admin/mainline_history`

## 说明

- 市场方向建议仍可通过 `/admin/market/suggestion` 获取（用于其他功能链路）。
- 本文档仅用于历史兼容提示，不代表当前有效功能模块。
