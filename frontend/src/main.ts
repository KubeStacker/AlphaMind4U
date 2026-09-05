import { createApp } from 'vue'
import { createPinia } from 'pinia'
import App from './App.vue'
import router from './router'
import { refreshIcons } from './shared/icons'
import './styles/tokens.css'

// 应用启动入口：挂载 Pinia / Router 后渲染。
const app = createApp(App)
app.use(createPinia())
app.use(router)
app.mount('#app')

// 路由切换后刷新 lucide 图标。
router.afterEach(() => {
  requestAnimationFrame(() => refreshIcons())
})
refreshIcons()
