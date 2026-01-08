import React, { useState, useEffect } from 'react'
import { Layout as AntLayout, Menu, theme, Button, Dropdown, message } from 'antd'
import { useNavigate, useLocation } from 'react-router-dom'
import { BarChartOutlined, FireOutlined, StarOutlined, LogoutOutlined, UserOutlined } from '@ant-design/icons'
import { useAuth } from '../contexts/AuthContext'

const { Header, Content } = AntLayout

interface LayoutProps {
  children: React.ReactNode
}

const Layout: React.FC<LayoutProps> = ({ children }) => {
  const [isMobile, setIsMobile] = useState(false)
  const navigate = useNavigate()
  const location = useLocation()
  const { user, logout } = useAuth()
  const {
    token: { colorBgContainer },
  } = theme.useToken()

  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768)
    }
    checkMobile()
    window.addEventListener('resize', checkMobile)
    return () => window.removeEventListener('resize', checkMobile)
  }, [])

  const handleLogout = async () => {
    try {
      await logout()
      message.success('已退出登录')
      navigate('/login')
    } catch (error) {
      message.error('退出登录失败')
    }
  }

  const userMenuItems = [
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: '退出登录',
      onClick: handleLogout,
    },
  ]

  const menuItems = [
    {
      key: '/recommendations',
      icon: <StarOutlined />,
      label: '智能推荐',
    },
    {
      key: '/stock-analysis',
      icon: <BarChartOutlined />,
      label: '标的分析',
    },
    {
      key: '/hot-stocks',
      icon: <FireOutlined />,
      label: '热度榜单',
    },
  ]

  return (
    <AntLayout style={{ minHeight: '100vh' }}>
      <Header
        style={{
          display: 'flex',
          alignItems: 'center',
          background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
          padding: isMobile ? '0 16px' : '0 50px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
          flexWrap: isMobile ? 'wrap' : 'nowrap',
          height: isMobile ? 'auto' : '64px',
          minHeight: isMobile ? '64px' : '64px',
        }}
      >
        <div
          style={{
            color: '#fff',
            fontSize: isMobile ? '16px' : '20px',
            fontWeight: 'bold',
            marginRight: isMobile ? '8px' : '50px',
            flexShrink: 0,
          }}
        >
          数据分析系统
        </div>
        <Menu
          theme="dark"
          mode={isMobile ? 'vertical' : 'horizontal'}
          selectedKeys={[location.pathname]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{
            flex: isMobile ? 'none' : 1,
            background: 'transparent',
            borderBottom: 'none',
            width: isMobile ? '100%' : 'auto',
            marginTop: isMobile ? '8px' : 0,
          }}
        />
        <Dropdown menu={{ items: userMenuItems }} placement="bottomRight">
          <Button
            type="text"
            icon={<UserOutlined />}
            style={{
              color: '#fff',
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              flexShrink: 0,
            }}
          >
            {!isMobile && (user?.username || '用户')}
          </Button>
        </Dropdown>
      </Header>
      <Content
        style={{
          padding: isMobile ? '16px' : '24px 50px',
          background: '#f5f7fa',
          minHeight: 'calc(100vh - 64px)',
        }}
      >
        <div
          style={{
            background: colorBgContainer,
            padding: isMobile ? '16px' : '24px',
            borderRadius: '8px',
            boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
            minHeight: 'calc(100vh - 112px)',
          }}
        >
          {children}
        </div>
      </Content>
    </AntLayout>
  )
}

export default Layout
