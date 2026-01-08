import client from './client'

export interface LoginRequest {
  username: string
  password: string
}

export interface LoginResponse {
  access_token: string
  token_type: string
  username: string
}

export interface UserInfo {
  id: number
  username: string
}

export const authApi = {
  login: async (data: LoginRequest): Promise<LoginResponse> => {
    const response = await client.post('/auth/login', data)
    return response.data
  },

  logout: async (): Promise<void> => {
    const token = localStorage.getItem('token')
    if (token) {
      try {
        await client.post('/auth/logout', {}, {
          headers: { Authorization: `Bearer ${token}` }
        })
      } catch (error) {
        // 即使失败也清除本地token
        console.error('登出请求失败:', error)
      }
    }
    localStorage.removeItem('token')
    localStorage.removeItem('username')
  },

  getCurrentUser: async (): Promise<UserInfo> => {
    const response = await client.get('/auth/me')
    return response.data
  },
}
