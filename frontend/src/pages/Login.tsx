import React, { useState, useEffect } from 'react'
import { Form, Input, Button, Card, message, Typography } from 'antd'
import { UserOutlined, LockOutlined } from '@ant-design/icons'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

const { Title } = Typography

const Login: React.FC = () => {
  const [loading, setLoading] = useState(false)
  const [isMobile, setIsMobile] = useState(false)
  const { login } = useAuth()
  const navigate = useNavigate()

  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768)
    }
    checkMobile()
    window.addEventListener('resize', checkMobile)
    return () => window.removeEventListener('resize', checkMobile)
  }, [])

  const onFinish = async (values: { username: string; password: string }) => {
    setLoading(true)
    try {
      await login(values.username, values.password)
      message.success('登录成功')
      navigate('/sheep-analysis')
    } catch (error: any) {
      console.error('登录失败:', error)
      // 处理不同格式的错误信息
      let errorMessage = '登录失败，请检查用户名和密码'
      
      // 网络错误处理
      if (error?.code === 'ECONNABORTED' || error?.message?.includes('timeout')) {
        errorMessage = '请求超时，请检查网络连接或稍后重试'
      } else if (error?.code === 'ERR_NETWORK' || error?.message === 'Network Error') {
        errorMessage = '网络连接失败，请检查网络连接或确认后端服务是否正常运行'
      } else if (error?.response?.status === 502) {
        errorMessage = '后端服务不可用 (502)，请检查：1) 后端服务是否运行 2) 容器是否正常启动 3) 网络连接是否正常'
      } else if (error?.response?.status === 503) {
        errorMessage = '后端服务暂时不可用 (503)，请稍后重试'
      } else if (error?.response?.status === 504) {
        errorMessage = '网关超时 (504)，后端服务响应时间过长'
      } else if (error?.response) {
        // 有响应但状态码不是2xx
        if (error.response.data?.detail) {
          errorMessage = error.response.data.detail
        } else if (error.response.data?.message) {
          errorMessage = error.response.data.message
        } else if (error.response.status === 401) {
          errorMessage = '用户名或密码错误'
        } else if (error.response.status === 403) {
          errorMessage = error.response.data?.detail || '账户已被锁定，请稍后再试'
        } else if (error.response.status >= 500) {
          errorMessage = '服务器错误，请稍后重试'
        } else {
          errorMessage = `登录失败 (${error.response.status})`
        }
      } else if (error?.message) {
        errorMessage = error.message
      }
      
      console.error('最终错误消息:', errorMessage)
      message.error(errorMessage)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div
      style={{
        minHeight: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        padding: '16px',
      }}
    >
      <Card
        style={{
          width: '100%',
          maxWidth: 400,
          boxShadow: '0 8px 24px rgba(0,0,0,0.2)',
        }}
        bodyStyle={{
          padding: isMobile ? '24px 16px' : '32px',
        }}
      >
        <div style={{ textAlign: 'center', marginBottom: isMobile ? 24 : 32 }}>
          <Title level={isMobile ? 3 : 2} style={{ marginBottom: 8 }}>
            数据分析系统
          </Title>
          <p style={{ color: '#666', marginBottom: 0, fontSize: isMobile ? '14px' : '16px' }}>请登录以访问系统</p>
        </div>

        <Form
          name="login"
          onFinish={onFinish}
          autoComplete="off"
          size="large"
        >
          <Form.Item
            name="username"
            rules={[{ required: true, message: '请输入用户名' }]}
          >
            <Input
              prefix={<UserOutlined />}
              placeholder="用户名"
            />
          </Form.Item>

          <Form.Item
            name="password"
            rules={[{ required: true, message: '请输入密码' }]}
          >
            <Input.Password
              prefix={<LockOutlined />}
              placeholder="密码"
            />
          </Form.Item>

          <Form.Item>
            <Button
              type="primary"
              htmlType="submit"
              loading={loading}
              block
              style={{
                background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                border: 'none',
                height: 40,
              }}
            >
              登录
            </Button>
          </Form.Item>
        </Form>
      </Card>
    </div>
  )
}

export default Login
