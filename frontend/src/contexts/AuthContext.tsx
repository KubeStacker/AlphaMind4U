import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import { authApi, UserInfo } from '../api/auth'

interface AuthContextType {
  user: UserInfo | null
  isAuthenticated: boolean
  isLoading: boolean
  login: (username: string, password: string) => Promise<void>
  logout: () => Promise<void>
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

export const useAuth = () => {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider')
  }
  return context
}

interface AuthProviderProps {
  children: ReactNode
}

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const [user, setUser] = useState<UserInfo | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  // 检查是否已登录
  useEffect(() => {
    const checkAuth = async () => {
      const token = localStorage.getItem('token')
      if (token) {
        try {
          const userInfo = await authApi.getCurrentUser()
          setUser(userInfo)
        } catch (error) {
          // token无效，清除
          localStorage.removeItem('token')
          localStorage.removeItem('username')
          setUser(null)
        }
      }
      setIsLoading(false)
    }
    checkAuth()
  }, [])

  const login = async (username: string, password: string) => {
    try {
      const response = await authApi.login({ username, password })
      localStorage.setItem('token', response.access_token)
      localStorage.setItem('username', response.username)
      try {
        const userInfo = await authApi.getCurrentUser()
        setUser(userInfo)
      } catch (error) {
        // 如果获取用户信息失败，但登录已成功，仍然设置用户信息
        console.error('获取用户信息失败:', error)
        setUser({ id: 0, username: response.username })
      }
    } catch (error: any) {
      // 登录失败，重新抛出错误让调用者处理
      console.error('登录API调用失败:', error)
      // 记录更详细的错误信息
      if (error?.response) {
        console.error('响应状态:', error.response.status)
        console.error('响应数据:', error.response.data)
        
        // 502错误的特殊处理
        if (error.response.status === 502) {
          console.error('⚠️ 后端服务不可用 (502 Bad Gateway)')
          console.error('诊断步骤：')
          console.error('  1. 检查后端容器状态: docker ps | grep backend')
          console.error('  2. 查看后端日志: docker logs stock-backend')
          console.error('  3. 检查网络连接: docker network ls')
          console.error('  4. 重启后端服务: docker restart stock-backend')
        }
      } else if (error?.request) {
        console.error('请求已发送但无响应:', error.request)
        console.error('可能是网络连接问题或后端服务未启动')
      } else {
        console.error('错误详情:', error?.message || error)
      }
      throw error
    }
  }

  const logout = async () => {
    await authApi.logout()
    setUser(null)
  }

  return (
    <AuthContext.Provider
      value={{
        user,
        isAuthenticated: !!user,
        isLoading,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}
