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
