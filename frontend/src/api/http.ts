import { ApiHttpError } from '@/shared/api-error'

// 本地登录态 token 存取键（与 stores/auth 保持一致）。
const TOKEN_KEY = 'jarvis_token'

// 读取当前 Bearer 令牌。
export function getToken(): string {
  return localStorage.getItem(TOKEN_KEY) || ''
}

// 统一 fetch 封装：自动注入 Authorization Bearer，错误只抛摘要。
export async function apiFetch(base: string, path: string, init: RequestInit = {}): Promise<unknown> {
  const headers: Record<string, string> = { Accept: 'application/json' }
  const token = getToken()
  if (token) headers['Authorization'] = `Bearer ${token}`
  if (init.body !== undefined) headers['Content-Type'] = 'application/json'
  Object.assign(headers, init.headers || {})
  let resp: Response
  try {
    resp = await fetch(`${base}${path}`, { ...init, headers })
  } catch {
    throw new TypeError('网络连接失败')
  }
  const text = await resp.text()
  if (!resp.ok) {
    throw new ApiHttpError(resp.status, path, text.slice(0, 500))
  }
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch {
    throw new Error('响应不是合法 JSON')
  }
}
