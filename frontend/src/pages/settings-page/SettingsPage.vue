<script setup lang="ts">
import { ref } from 'vue'
import { useAuthStore } from '@/stores/auth'
import { useToastStore } from '@/stores/toast'
import { useConfirmStore } from '@/stores/confirm'
import DataTable from '@/shared/DataTable.vue'
import { refreshIcons } from '@/shared/icons'

// 本地用户记录（占位：待后端用户接口）。
interface LocalUser {
  username: string
  role: 'admin' | 'user'
  createdAt: string
}

// 设置页：用户管理为本地占位，明确标注待后端接口。
const USER_KEY = 'jarvis_local_users'
const auth = useAuthStore()
const toast = useToastStore()
const confirm = useConfirmStore()

// 读取本地占位用户。
function readUsers(): LocalUser[] {
  try {
    const raw = localStorage.getItem(USER_KEY)
    if (raw) return JSON.parse(raw) as LocalUser[]
  } catch { /* 本地缓存损坏则回退默认 */ }
  return [{ username: 'admin', role: 'admin', createdAt: '内置' }]
}

const users = ref<LocalUser[]>(readUsers())
const newUsername = ref('')
const newRole = ref<'admin' | 'user'>('user')

// 持久化本地占位用户。
function saveUsers() {
  localStorage.setItem(USER_KEY, JSON.stringify(users.value))
  refreshIcons()
}

// 添加本地占位用户。
function addUser() {
  const name = newUsername.value.trim()
  if (!name) {
    toast.push('请输入用户名', 'error')
    return
  }
  if (users.value.some((u) => u.username === name)) {
    toast.push('该用户名已存在', 'error')
    return
  }
  users.value.push({ username: name, role: newRole.value, createdAt: new Date().toLocaleString('zh-CN') })
  newUsername.value = ''
  saveUsers()
  toast.push(`已添加本地占位用户 ${name}`, 'success')
}

// 删除本地占位用户（二次确认）。
async function removeUser(name: string) {
  if (name === 'admin') {
    toast.push('内置管理员不可删除', 'error')
    return
  }
  const ok = await confirm.ask({ title: '删除本地占位用户', message: `确定删除本地用户 ${name} 吗？`, okText: '确认删除' })
  if (!ok) return
  users.value = users.value.filter((u) => u.username !== name)
  saveUsers()
  toast.push(`已删除本地用户 ${name}`, 'success')
}

// 当前登录令牌后四位（避免完整展示敏感信息）。
function tokenTail(): string {
  if (!auth.token) return '未设置'
  return `…${auth.token.slice(-4)}`
}
</script>

<template>
  <div class="page">
    <section class="card">
      <div class="sec-title">
        <i data-lucide="users"></i><h3>用户管理</h3>
        <span class="pending mono">本地占位 · 待后端接口</span>
      </div>
      <p class="dim">说明：用户增删改仅保存在浏览器本地，真接口待量化后端提供用户管理 API 后接入。</p>
      <div class="add-row">
        <input v-model="newUsername" class="input" placeholder="新用户名，如 trader_01">
        <select v-model="newRole" class="select">
          <option value="user">普通用户</option>
          <option value="admin">管理员</option>
        </select>
        <button class="btn btn-primary" @click="addUser"><i data-lucide="user-plus"></i><span>添加</span></button>
      </div>
      <DataTable
        :columns="[{ key: 'username', title: '账号' }, { key: 'role', title: '角色' }, { key: 'createdAt', title: '创建时间', align: 'right' }]"
        :rows="users"
      >
        <template #body="{ rows: list }">
          <tr v-for="u in (list as unknown as LocalUser[])" :key="u.username">
            <td class="mono name">{{ u.username }}</td>
            <td>{{ u.role === 'admin' ? '管理员' : '普通用户' }}</td>
            <td class="r dim">{{ u.createdAt }}</td>
            <td class="r">
              <button v-if="u.username !== 'admin'" class="btn btn-danger btn-sm" @click="removeUser(u.username)">删除</button>
              <span v-else class="dim">内置</span>
            </td>
          </tr>
        </template>
        <template #head-extra><th class="r">操作</th></template>
      </DataTable>
    </section>

    <section class="card">
      <div class="sec-title"><i data-lucide="key"></i><h3>访问令牌</h3></div>
      <p class="dim mono">当前登录：{{ auth.username || '—' }} ｜ 令牌尾号：{{ tokenTail() }}（调用 /admin 与 /mcp 时以 Bearer 注入）</p>
    </section>
  </div>
</template>

<style scoped>
.page { display: flex; flex-direction: column; gap: 16px; }
.sec-title { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
.sec-title h3 { margin: 0; font-size: 13px; color: var(--white); }
.pending { font-size: 10px; padding: 2px 8px; border-radius: 4px; background: rgba(210, 153, 34, 0.12); color: var(--accent-amber); border: 1px solid rgba(210, 153, 34, 0.4); }
.dim { color: var(--text-dim); font-size: 12px; line-height: 1.7; }
.add-row { display: flex; gap: 8px; margin: 12px 0; flex-wrap: wrap; }
.add-row .input { flex: 2; min-width: 160px; }
.add-row .select { flex: 1; min-width: 120px; }
.mono.name { color: var(--white); }
.r { text-align: right; }
.btn-sm { padding: 4px 10px; font-size: 11px; }
</style>
