import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api'

const client = axios.create({
  baseURL: API_BASE_URL,
  timeout: 300000, // 300秒（5分钟），T7模型计算可能需要较长时间
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
    // 开发环境下记录请求日志
    if (import.meta.env.DEV) {
      console.log(`[API Request] ${config.method?.toUpperCase()} ${config.baseURL}${config.url}`, {
        params: config.params,
        data: config.data
      })
    }
    return config
  },
  (error) => {
    console.error('[API Request Error]', error)
    return Promise.reject(error)
  }
)

// 响应拦截器：处理401错误
client.interceptors.response.use(
  (response) => {
    // 开发环境下记录响应日志
    if (import.meta.env.DEV) {
      console.log(`[API Response] ${response.config.method?.toUpperCase()} ${response.config.url}`, {
        status: response.status,
        data: response.data
      })
    }
    return response
  },
  (error) => {
    // 记录错误详情
    if (import.meta.env.DEV) {
      console.error(`[API Error] ${error.config?.method?.toUpperCase()} ${error.config?.url}`, {
        message: error.message,
        code: error.code,
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
        request: error.request ? 'Request sent but no response' : null
      })
    }
    
    // 处理502错误（Bad Gateway）
    if (error.response?.status === 502) {
      console.error('[502 Error] 后端服务不可用，可能的原因：')
      console.error('  1. 后端容器未运行或已崩溃')
      console.error('  2. 后端服务启动失败')
      console.error('  3. 网络连接问题')
      console.error('  请检查：docker ps | grep backend 或 docker logs stock-backend')
    }
    
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
