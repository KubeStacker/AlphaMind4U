import { defineStore } from 'pinia'
import { login as apiLogin } from '@/api/auth'

// 本地登录态存储键。
const TOKEN_KEY = 'jarvis_token'
const USER_KEY = 'jarvis_current_user'

// 登录门禁说明：单用户控制台，用户名、密码、共享令牌由量化后端一次验明；
// 通过后存服务端返回的共用令牌，后续 /admin 与 /mcp 调用统一注入。
export const LOCAL_ADMIN = 'admin'

// 认证状态管理（真接口登录 + Bearer 令牌持有）。
export const useAuthStore = defineStore('auth', {
  state: () => ({
    username: localStorage.getItem(USER_KEY) || '',
    token: localStorage.getItem(TOKEN_KEY) || '',
  }),
  getters: {
    // 是否已登录。
    isAuthed: (s) => s.username !== '' && s.token !== '',
    // 是否为管理员（本地规则：仅 admin）。
    isAdmin: (s) => s.username === LOCAL_ADMIN,
  },
  actions: {
    // 真登录：三者服务端验明，失败抛中文错误。
    async login(username: string, password: string, token: string) {
      const name = username.trim()
      if (!name) throw new Error('请输入用户名')
      if (!password) throw new Error('请输入密码')
      if (!token.trim()) throw new Error('请输入访问令牌')
      const res = await apiLogin({ username: name, password, token: token.trim() })
      if (!res.username || !res.token) throw new Error('登录响应非法')
      this.username = res.username
      this.token = res.token
      localStorage.setItem(USER_KEY, this.username)
      localStorage.setItem(TOKEN_KEY, this.token)
    },
    // 退出登录并清理本地态。
    logout() {
      this.username = ''
      this.token = ''
      localStorage.removeItem(USER_KEY)
      localStorage.removeItem(TOKEN_KEY)
    },
  },
})
