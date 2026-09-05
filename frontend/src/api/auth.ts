import { apiFetch } from './http'

// 登录请求（用户名、密码、共享令牌三者须同时匹配）。
export interface LoginParams {
  username: string
  password: string
  token: string
}

// 登录响应（返回后续共用的 Bearer 令牌）。
export interface LoginResult {
  username: string
  token: string
}

// 控制台登录（真接口 POST /admin/auth/login，免鉴权）。
export async function login(params: LoginParams): Promise<LoginResult> {
  return (await apiFetch('/admin', '/auth/login', {
    method: 'POST',
    body: JSON.stringify(params),
  })) as LoginResult
}
