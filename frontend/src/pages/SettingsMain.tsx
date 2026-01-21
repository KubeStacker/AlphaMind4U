import React, { useState, useEffect } from 'react'
import { Menu, Layout } from 'antd'
import { SettingOutlined, UserOutlined, RobotOutlined, DatabaseOutlined } from '@ant-design/icons'
import { useNavigate, useLocation } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'
import AIManagement from './AIManagement'
import UserManagement from './UserManagement'
import DataManagement from './DataManagement'

const { Sider, Content } = Layout

const SettingsMain: React.FC = () => {
  const navigate = useNavigate()
  const location = useLocation()
  const { user } = useAuth()
  const [collapsed, setCollapsed] = useState(false)
  const isAdmin = user?.is_admin || user?.username === 'admin'

  const menuItems = [
    // 已移除：概念管理（已使用Jaccard聚类算法自动处理）
    // {
    //   key: '/settings/sector-mapping',
    //   icon: <AppstoreOutlined />,
    //   label: '概念管理',
    // },
    {
      key: '/settings/ai',
      icon: <RobotOutlined />,
      label: 'AI管理',
    },
    ...(isAdmin ? [
      {
        key: '/settings/data-management',
        icon: <DatabaseOutlined />,
        label: '数据管理',
      },
      {
        key: '/settings/users',
        icon: <UserOutlined />,
        label: '用户管理',
      }
    ] : []),
  ]

  const handleMenuClick = ({ key }: { key: string }) => {
    navigate(key)
  }

  // 根据当前路径确定选中的菜单项
  const getSelectedKey = () => {
    const path = location.pathname
    // 已移除：概念管理
    // if (path.includes('/sector-mapping') || path === '/settings' || path === '/settings/') {
    //   return '/settings/sector-mapping'
    // }
    if (path.includes('/ai')) {
      return '/settings/ai'
    }
    if (path.includes('/data-management')) {
      return '/settings/data-management'
    }
    if (path.includes('/users')) {
      return '/settings/users'
    }
    // 默认跳转到AI管理
    return '/settings/ai'
  }
  
  const selectedKey = getSelectedKey()

  // 默认跳转到AI管理（已移除概念管理）
  useEffect(() => {
    if (location.pathname === '/settings' || location.pathname === '/settings/') {
      navigate('/settings/ai', { replace: true })
    }
  }, [location.pathname, navigate])

  // 渲染内容
  const renderContent = () => {
    const path = location.pathname
    if (path.includes('/ai')) {
      return <AIManagement />
    }
    if (path.includes('/data-management')) {
      return <DataManagement />
    }
    if (path.includes('/users')) {
      return <UserManagement />
    }
    // 已移除：概念管理（已使用Jaccard聚类算法自动处理）
    // if (path.includes('/sector-mapping')) {
    //   return <SectorMappingManagement />
    // }
    // 默认显示AI管理
    return <AIManagement />
  }

  return (
    <Layout style={{ minHeight: 'calc(100vh - 112px)', background: 'transparent' }}>
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        width={200}
        style={{
          background: '#fff',
          marginRight: 16,
        }}
      >
        <div style={{ padding: '16px', textAlign: 'center', borderBottom: '1px solid #f0f0f0' }}>
          <SettingOutlined style={{ fontSize: 24, color: '#1890ff' }} />
          {!collapsed && <div style={{ marginTop: 8, fontWeight: 'bold' }}>系统设置</div>}
        </div>
        <Menu
          mode="inline"
          selectedKeys={[selectedKey]}
          items={menuItems}
          onClick={handleMenuClick}
          style={{ borderRight: 0 }}
        />
      </Sider>
      <Content style={{ background: 'transparent' }}>
        {renderContent()}
      </Content>
    </Layout>
  )
}

export default SettingsMain
