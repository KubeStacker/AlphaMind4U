import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

// 前端路由表（页面路径与 API 前缀严格区分：API 用 /admin 与 /mcp，页面不用二者）。
// 路由懒加载，首屏只下发当前页分包。
const routes: RouteRecordRaw[] = [
  { path: '/', redirect: '/data' },
  { path: '/login', component: () => import('@/pages/login-page/LoginPage.vue') },
  { path: '/data', component: () => import('@/pages/data-dashboard-page/DataDashboardPage.vue'), meta: { requiresAuth: true } },
  { path: '/lab-mcp', component: () => import('@/pages/mcp-inspector-page/McpInspectorPage.vue'), meta: { requiresAuth: true } },
  { path: '/lab-doc', component: () => import('@/pages/doc-home-page/DocHomePage.vue'), meta: { requiresAuth: true } },
  { path: '/quality', component: () => import('@/pages/quality-page/QualityPage.vue'), meta: { requiresAuth: true } },
  { path: '/lab-strategy', component: () => import('@/pages/strategy-lab-page/StrategyLabPage.vue'), meta: { requiresAuth: true } },
  { path: '/settings', component: () => import('@/pages/settings-page/SettingsPage.vue'), meta: { requiresAuth: true, requiresAdmin: true } },
  { path: '/:pathMatch(.*)*', redirect: '/data' },
]

// 应用路由实例（history 模式，preview 自带单页回退）。
const router = createRouter({ history: createWebHistory(), routes })

// 全局门禁：未登录跳登录页，非管理员禁入设置页。
router.beforeEach((to) => {
  const auth = useAuthStore()
  if (to.meta.requiresAuth && !auth.isAuthed) return '/login'
  if (to.meta.requiresAdmin && !auth.isAdmin) return '/data'
  return true
})

export default router
