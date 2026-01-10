import React, { useState, useEffect } from 'react'
import { Menu, Layout } from 'antd'
import { SettingOutlined, UserOutlined, AppstoreOutlined } from '@ant-design/icons'
import { useNavigate, useLocation } from 'react-router-dom'
import SectorMappingManagement from './Settings'

const { Sider, Content } = Layout

const SettingsMain: React.FC = () => {
  const navigate = useNavigate()
  const location = useLocation()
  const [collapsed, setCollapsed] = useState(false)

  const menuItems = [
    {
      key: '/settings/sector-mapping',
      icon: <AppstoreOutlined />,
      label: '概念管理',
    },
    {
      key: '/settings/users',
      icon: <UserOutlined />,
      label: '用户管理',
      disabled: true, // 暂时禁用，后续可以开发
    },
  ]

  const handleMenuClick = ({ key }: { key: string }) => {
    navigate(key)
  }

  // 根据当前路径确定选中的菜单项
  const getSelectedKey = () => {
    const path = location.pathname
    if (path.includes('/sector-mapping') || path === '/settings' || path === '/settings/') {
      return '/settings/sector-mapping'
    }
    if (path.includes('/users')) {
      return '/settings/users'
    }
    return '/settings/sector-mapping'
  }
  
  const selectedKey = getSelectedKey()

  // 默认跳转到概念管理
  useEffect(() => {
    if (location.pathname === '/settings' || location.pathname === '/settings/') {
      navigate('/settings/sector-mapping', { replace: true })
    }
  }, [location.pathname, navigate])

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
        <SectorMappingManagement />
      </Content>
    </Layout>
  )
}

export default SettingsMain
