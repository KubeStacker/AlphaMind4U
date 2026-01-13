import { apiClient } from './client'

export interface User {
  id: number
  username: string
  is_active: boolean
  can_use_ai_recommend: boolean
  failed_login_attempts: number
  locked_until?: string
  last_login?: string
  created_at?: string
  updated_at?: string
  is_admin: boolean
}

export interface UserCreate {
  username: string
  password: string
  can_use_ai_recommend: boolean
}

export interface UserUpdate {
  password?: string
  is_active?: boolean
  can_use_ai_recommend?: boolean
}

export interface UsersResponse {
  users: User[]
}

export const userApi = {
  // 获取用户列表
  getUsers: async (): Promise<UsersResponse> => {
    const response = await apiClient.get('/users')
    return response.data
  },

  // 创建用户
  createUser: async (user: UserCreate): Promise<void> => {
    await apiClient.post('/users', user)
  },

  // 更新用户
  updateUser: async (userId: number, user: UserUpdate): Promise<void> => {
    await apiClient.put(`/users/${userId}`, user)
  },

  // 删除用户
  deleteUser: async (userId: number): Promise<void> => {
    await apiClient.delete(`/users/${userId}`)
  },
}
