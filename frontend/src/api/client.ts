import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api'

const client = axios.create({
  baseURL: API_BASE_URL,
  timeout: 120000, // 120秒，AI分析可能需要较长时间
  headers: {
    'Content-Type': 'application/json; charset=utf-8',
  },
  responseType: 'json',
  responseEncoding: 'utf8',
})

// 请求拦截器：添加token
client.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token')
    if (token) {
      config.headers.Authorization = `Bearer ${token}`
    }
    return config
  },
  (error) => {
    return Promise.reject(error)
  }
)

// 响应拦截器：处理401错误
client.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      const token = localStorage.getItem('token')
      // 只有在有token的情况下才自动跳转（表示token过期）
      // 如果没有token，说明是登录请求失败，不自动跳转，让调用者处理错误
      if (token && window.location.pathname !== '/login') {
        localStorage.removeItem('token')
        localStorage.removeItem('username')
        window.location.href = '/login'
      }
    }
    return Promise.reject(error)
  }
)

export default client
export const apiClient = client
