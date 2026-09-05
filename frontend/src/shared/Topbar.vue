<script setup lang="ts">
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

// 顶部导航条（品牌 + 路由切换 + 用户 + 退出）。
const auth = useAuthStore()
const router = useRouter()

// 当前是否为管理员。
const isAdmin = computed(() => auth.isAdmin)

// 退出登录并回到登录页。
function handleLogout() {
  auth.logout()
  router.push('/login')
}
</script>

<template>
  <header class="topbar">
    <div class="brand">
      <div class="logo">J</div>
      <span class="name">JARVIS</span>
      <span class="sep">/</span>
      <span v-if="isAdmin" class="badge-admin">管理员</span>
      <span v-else class="badge-user">普通用户</span>
    </div>
    <nav class="nav">
      <router-link to="/data" class="nav-link"><i data-lucide="database"></i><span>数据看板</span></router-link>
      <router-link to="/quality" class="nav-link"><i data-lucide="activity"></i><span>质量看板</span></router-link>
      <router-link to="/lab-strategy" class="nav-link"><i data-lucide="trending-up"></i><span>策略研究</span></router-link>
      <router-link to="/lab-mcp" class="nav-link"><i data-lucide="plug"></i><span>MCP 检查</span></router-link>
      <router-link to="/lab-doc" class="nav-link"><i data-lucide="book-open"></i><span>文档</span></router-link>
      <router-link v-if="isAdmin" to="/settings" class="nav-link"><i data-lucide="settings"></i><span>设置</span></router-link>
    </nav>
    <div class="user">
      <i data-lucide="user"></i>
      <span class="mono username">{{ auth.username }}</span>
      <button class="icon-btn" title="退出登录" @click="handleLogout"><i data-lucide="log-out"></i></button>
    </div>
  </header>
</template>

<style scoped>
.topbar {
  height: 52px; background: var(--surface); border-bottom: 1px solid var(--border);
  padding: 0 16px; display: flex; align-items: center; justify-content: space-between;
  position: sticky; top: 0; z-index: 40; gap: 12px;
}
.brand { display: flex; align-items: center; gap: 8px; }
.logo {
  width: 24px; height: 24px; border-radius: 6px; background: var(--surface-subtle);
  border: 1px solid var(--border); color: var(--accent-blue);
  display: flex; align-items: center; justify-content: center;
  font-family: var(--font-mono); font-weight: 700; font-size: 12px;
}
.name { font-size: 12px; font-weight: 600; color: var(--white); letter-spacing: 0.05em; }
.sep { color: var(--text-dim); }
.badge-admin {
  font-size: 10px; font-family: var(--font-mono); padding: 2px 8px; border-radius: 4px;
  background: rgba(56, 139, 253, 0.12); color: var(--accent-blue); border: 1px solid rgba(56, 139, 253, 0.4);
}
.badge-user {
  font-size: 10px; font-family: var(--font-mono); padding: 2px 8px; border-radius: 4px;
  background: var(--surface-subtle); color: var(--text-dim); border: 1px solid var(--border);
}
.nav { display: flex; align-items: center; gap: 4px; }
.nav-link {
  display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text);
  text-decoration: none; padding: 6px 10px; border-radius: 6px; border: 1px solid transparent;
}
.nav-link:hover { background: var(--surface-subtle); color: var(--white); }
.nav-link.router-link-active { background: var(--surface-subtle); border-color: var(--border); color: var(--white); }
.user { display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--text-dim); }
.username { color: var(--text); }
.icon-btn {
  background: transparent; border: none; color: var(--text-dim); cursor: pointer;
  padding: 4px; border-radius: 4px; display: flex;
}
.icon-btn:hover { color: var(--accent-rose); background: var(--surface-subtle); }
@media (max-width: 640px) { .nav-link span { display: none; } }
</style>
