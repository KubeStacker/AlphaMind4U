import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import axios from 'axios'

export const useAuthStore = defineStore('auth', () => {
  // 从 localStorage 初始化，实现持久化登录
  const token = ref(localStorage.getItem('jarvis-token') || null)
  const user = ref(JSON.parse(localStorage.getItem('jarvis-user') || 'null'))

  // isAuthenticated 是一个计算属性，当 token 存在时为 true
  const isAuthenticated = computed(() => !!token.value)
  const isAdmin = computed(() => user.value?.role === 'admin')

  /**
   * 登录方法
   * @param {string} username - 用户名
   * @param {string} password - 密码
   */
  async function login(username, password) {
    const params = new URLSearchParams();
    params.append('username', username);
    params.append('password', password);

    const response = await axios.post('/api/auth/token', params, {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    });

    if (response.data.access_token) {
      token.value = response.data.access_token
      user.value = {
        username: response.data.username,
        role: response.data.role
      }
      
      localStorage.setItem('jarvis-token', token.value)
      localStorage.setItem('jarvis-user', JSON.stringify(user.value))
      
      axios.defaults.headers.common['Authorization'] = `Bearer ${token.value}`;
    }
  }

  /**
   * 登出方法
   */
  function logout() {
    token.value = null
    user.value = null
    localStorage.removeItem('jarvis-token')
    localStorage.removeItem('jarvis-user')
    delete axios.defaults.headers.common['Authorization'];
  }

  return { token, user, isAuthenticated, isAdmin, login, logout }
})
