<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore, LOCAL_ADMIN } from '@/stores/auth'
import { useToastStore } from '@/stores/toast'

// 登录页（真接口：用户名、密码、共享令牌服务端一次验明）。
const auth = useAuthStore()
const toast = useToastStore()
const router = useRouter()
const username = ref(LOCAL_ADMIN)
const password = ref('')
const token = ref('')

// 快捷填充管理员账号。
function quickFillAdmin() {
  username.value = LOCAL_ADMIN
}

// 提交登录表单。
async function handleLogin() {
  try {
    await auth.login(username.value, password.value, token.value)
    toast.push(`欢迎，${auth.username}`, 'success')
    router.push('/data')
  } catch (e) {
    toast.push(e instanceof Error ? e.message : '登录失败', 'error')
  }
}
</script>

<template>
  <div class="login-wrap">
    <div class="login-card">
      <div class="brand">
        <div class="logo">J</div>
        <div>
          <h1>JARVIS</h1>
          <p class="mono">Telemetry Data Platform</p>
        </div>
        <span class="ver mono">v1.0</span>
      </div>
      <form @submit.prevent="handleLogin" class="form">
        <div>
          <label class="field-label">用户名 / 操作员账号</label>
          <input v-model="username" class="input" placeholder="admin" autocomplete="username">
        </div>
        <div>
          <label class="field-label">密码</label>
          <input v-model="password" type="password" class="input" placeholder="请输入管理员密码" autocomplete="current-password">
        </div>
        <div>
          <label class="field-label">访问令牌（与后端 Bearer 共用）</label>
          <input v-model="token" type="password" class="input" placeholder="请输入共享令牌" autocomplete="off">
        </div>
        <div class="quick">
          <span>快捷填充：</span>
          <button type="button" class="link" @click="quickFillAdmin">管理员</button>
        </div>
        <button type="submit" class="btn btn-primary submit"><i data-lucide="log-in"></i><span>登录控制台</span></button>
      </form>
      <p class="note mono">单用户控制台：用户名、密码、共享令牌由量化后端一次验明，通过后共用该令牌调用接口</p>
    </div>
  </div>
</template>

<style scoped>
.login-wrap { flex: 1; display: flex; align-items: center; justify-content: center; padding: 24px 16px; }
.login-card { width: 100%; max-width: 380px; background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-lg); padding: 28px 26px; }
.brand { display: flex; align-items: center; gap: 10px; padding-bottom: 16px; border-bottom: 1px solid var(--surface-subtle); margin-bottom: 18px; }
.logo { width: 32px; height: 32px; border-radius: 8px; background: var(--surface-subtle); border: 1px solid var(--border); color: var(--accent-blue); display: flex; align-items: center; justify-content: center; font-family: var(--font-mono); font-weight: 700; }
.brand h1 { margin: 0; font-size: 14px; color: var(--white); letter-spacing: 0.05em; }
.brand p { margin: 2px 0 0; font-size: 11px; color: var(--text-dim); }
.ver { margin-left: auto; font-size: 10px; padding: 2px 8px; border-radius: 4px; background: var(--surface-subtle); color: var(--text-dim); border: 1px solid var(--border); }
.form { display: flex; flex-direction: column; gap: 14px; }
.quick { display: flex; align-items: center; gap: 6px; font-size: 11px; color: var(--text-dim); }
.link { background: none; border: none; color: var(--accent-blue); cursor: pointer; font-size: 11px; padding: 0; }
.link:hover { text-decoration: underline; }
.submit { justify-content: center; padding: 10px; }
.note { margin: 16px 0 0; padding-top: 12px; border-top: 1px solid var(--surface-subtle); font-size: 10px; color: var(--text-dim); line-height: 1.7; }
</style>
