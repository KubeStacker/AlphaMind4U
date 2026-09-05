# spec/frontend/style — 方案、风格 token、复用

## 1. 方案：Vite + Vue3 + TS + Router + Pinia（业界 Admin 主流）

- 选 Vue3 不选 React：SFC 心智接近现单文件、构建快、配套少、中文生态全。
- 不引 Element/AntD；图表按需 ECharts；其余自建组件，保轻量。

## 2. 风格 token（沿用现 index.html，冻结）

- 背景 `#0C1017`；面 `#161B22`；边 `#30363D`；次面 `#21262D`。
- 主色蓝 `#388BFD`；成功绿、警告 amber、危险 rose 沿用现值。
- 正文 `Plus Jakarta Sans`，数字/代码 `JetBrains Mono`；图标 `lucide`。
- 滚动条 5px 细条；圆角 `rounded-lg/xl`；中文文案简洁，数字 mono。

## 3. 复用（各 page 只编排）

- `shared/`：`Topbar/AuthGuard/DataTable/ConfirmModal/Toast/api-error`。
- 组件用法只写本文件，不在 `frontend/README.md` 重写。

## 4. 策略页视觉调整（待实施）

策略页改为浅色单页，布局、字号与配色统一见[strategy/plan.md第九章](../strategy/plan.md)。
该页采用局部主题覆盖；上述深色token仍适用于其他页面，不做全站主题重构。
