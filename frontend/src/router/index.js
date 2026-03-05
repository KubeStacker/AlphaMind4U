import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

// 懒加载导入所有视图
const Login = () => import('@/views/Login.vue')
const Dashboard = () => import('@/views/Dashboard.vue')
const Watchlist = () => import('@/views/Watchlist.vue')
const Settings = () => import('@/views/Settings.vue')
const Falcon = () => import('@/views/Falcon.vue')
const AppLayout = () => import('@/components/AppLayout.vue')


const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/login',
      name: 'login',
      component: Login
    },
    {
      path: '/',
      component: AppLayout, // 使用一个包含导航栏的布局组件
      // 所有需要登录的页面都作为 AppLayout 的子路由
      children: [
        {
            path: '',
            name: 'watchlist',
            component: Watchlist,
            meta: { requiresAuth: true }
        },
        {
            path: 'dashboard',
            name: 'dashboard',
            component: Dashboard,
            meta: { requiresAuth: true }
        },
        {
            path: 'settings',
            name: 'settings',
            component: Settings,
            meta: { requiresAuth: true }
        },
        {
            path: 'falcon',
            name: 'falcon',
            component: Falcon,
            meta: { requiresAuth: true }
        }
      ]
    }
  ]
})

// 全局路由守卫
router.beforeEach((to, from, next) => {
    const auth = useAuthStore();
    // 检查路由是否需要认证
    if (to.meta.requiresAuth && !auth.isAuthenticated) {
        // 如果用户未认证，则重定向到登录页
        next({ name: 'login' });
    } else {
        // 否则，正常导航
        next();
    }
});


export default router
