import React, { useState, useEffect } from 'react'
import { Card, Table, Button, Modal, Form, Input, Switch, message, Space, Popconfirm, Tag } from 'antd'
import { UserOutlined, PlusOutlined, EditOutlined, DeleteOutlined } from '@ant-design/icons'
import { userApi, User, UserCreate, UserUpdate } from '../api/user'

const UserManagement: React.FC = () => {
  const [users, setUsers] = useState<User[]>([])
  const [loading, setLoading] = useState(false)
  const [modalVisible, setModalVisible] = useState(false)
  const [editingUser, setEditingUser] = useState<User | null>(null)
  const [form] = Form.useForm()

  useEffect(() => {
    loadUsers()
  }, [])

  const loadUsers = async () => {
    setLoading(true)
    try {
      const data = await userApi.getUsers()
      setUsers(data.users)
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || error.message || '加载用户列表失败'
      message.error(errorMsg)
      console.error('加载用户列表失败:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleCreate = () => {
    setEditingUser(null)
    form.resetFields()
    setModalVisible(true)
  }

  const handleEdit = (record: User) => {
    setEditingUser(record)
    form.setFieldsValue({
      is_active: record.is_active,
    })
    setModalVisible(true)
  }

  const handleDelete = async (userId: number) => {
    try {
      await userApi.deleteUser(userId)
      message.success('删除成功')
      loadUsers()
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || error.message || '删除失败'
      message.error(errorMsg)
    }
  }

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields()
      if (editingUser) {
        // 更新用户
        const update: UserUpdate = {
          is_active: values.is_active,
        }
        // 如果提供了新密码，则更新密码
        if (values.password && values.password.trim()) {
          update.password = values.password
        }
        await userApi.updateUser(editingUser.id, update)
        message.success('更新成功')
      } else {
        // 创建用户
        const create: UserCreate = {
          username: values.username,
          password: values.password,
        }
        await userApi.createUser(create)
        message.success('创建成功')
      }
      setModalVisible(false)
      loadUsers()
    } catch (error: any) {
      if (error.errorFields) {
        return // 表单验证错误
      }
      const errorMsg = error.response?.data?.detail || error.message || '操作失败'
      message.error(errorMsg)
    }
  }

  const columns = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 80,
    },
    {
      title: '用户名',
      dataIndex: 'username',
      key: 'username',
      width: 150,
      render: (username: string, record: User) => (
        <Space>
          <span>{username}</span>
          {record.is_admin && (
            <Tag color="red">管理员</Tag>
          )}
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'is_active',
      key: 'is_active',
      width: 100,
      render: (active: boolean) => (
        <Tag color={active ? 'green' : 'red'}>
          {active ? '启用' : '禁用'}
        </Tag>
      ),
    },
    {
      title: '登录失败次数',
      dataIndex: 'failed_login_attempts',
      key: 'failed_login_attempts',
      width: 120,
      render: (count: number) => count > 0 ? <Tag color="orange">{count}</Tag> : '-',
    },
    {
      title: '最后登录',
      dataIndex: 'last_login',
      key: 'last_login',
      width: 180,
      render: (lastLogin: string) => lastLogin ? new Date(lastLogin).toLocaleString('zh-CN') : '-',
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      render: (createdAt: string) => createdAt ? new Date(createdAt).toLocaleString('zh-CN') : '-',
    },
    {
      title: '操作',
      key: 'action',
      width: 200,
      render: (_: any, record: User) => (
        <Space>
          {!record.is_admin && (
            <>
              <Button
                type="link"
                size="small"
                icon={<EditOutlined />}
                onClick={() => handleEdit(record)}
              >
                编辑
              </Button>
              <Popconfirm
                title="确定要删除这个用户吗？"
                onConfirm={() => handleDelete(record.id)}
                okText="确定"
                cancelText="取消"
              >
                <Button type="link" size="small" danger icon={<DeleteOutlined />}>
                  删除
                </Button>
              </Popconfirm>
            </>
          )}
          {record.is_admin && (
            <Tag color="default">系统管理员</Tag>
          )}
        </Space>
      ),
    },
  ]

  return (
    <div>
      <Card
        title={
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span>
              <UserOutlined style={{ marginRight: 8 }} />
              用户管理
            </span>
            <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
              添加用户
            </Button>
          </div>
        }
      >
        <Table
          dataSource={users}
          columns={columns}
          rowKey="id"
          loading={loading}
          pagination={{ pageSize: 20 }}
        />
      </Card>

      <Modal
        title={editingUser ? '编辑用户' : '添加用户'}
        open={modalVisible}
        onOk={handleSubmit}
        onCancel={() => setModalVisible(false)}
        width={600}
      >
        <Form form={form} layout="vertical">
          {!editingUser && (
            <>
              <Form.Item
                name="username"
                label="用户名"
                rules={[
                  { required: true, message: '请输入用户名' },
                  { min: 3, message: '用户名至少3个字符' },
                  { max: 50, message: '用户名最多50个字符' },
                  { pattern: /^[a-zA-Z0-9_]+$/, message: '用户名只能包含字母、数字和下划线' }
                ]}
              >
                <Input
                  placeholder="请输入用户名（3-50个字符，只能包含字母、数字和下划线）"
                  maxLength={50}
                />
              </Form.Item>
              <Form.Item
                name="password"
                label="密码"
                rules={[
                  { required: true, message: '请输入密码' },
                  { min: 6, message: '密码至少6个字符' }
                ]}
              >
                <Input.Password
                  placeholder="请输入密码（至少6个字符）"
                />
              </Form.Item>
            </>
          )}
          {editingUser && (
            <>
              <Form.Item label="用户名">
                <Input value={editingUser.username} disabled />
              </Form.Item>
              <Form.Item
                name="password"
                label="新密码（留空则不修改）"
                rules={[
                  { min: 6, message: '密码至少6个字符' }
                ]}
              >
                <Input.Password
                  placeholder="留空则不修改密码"
                />
              </Form.Item>
            </>
          )}
          {editingUser && (
            <Form.Item
              name="is_active"
              label="账户状态"
              valuePropName="checked"
              extra={editingUser.is_admin ? "管理员账户不能被禁用" : ""}
            >
              <Switch
                checkedChildren="启用"
                unCheckedChildren="禁用"
                disabled={editingUser.is_admin}
              />
            </Form.Item>
          )}
        </Form>
      </Modal>
    </div>
  )
}

export default UserManagement
