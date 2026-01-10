import React, { useState, useEffect } from 'react'
import { Card, Tabs, Table, Button, Modal, Form, Input, Switch, message, Space, Popconfirm, Tag } from 'antd'
import { AppstoreOutlined, PlusOutlined, EditOutlined, DeleteOutlined, ReloadOutlined } from '@ant-design/icons'
import { sectorApi, SectorMapping, SectorMappingCreate, SectorMappingUpdate } from '../api/sector'

const Settings: React.FC = () => {
  const [mappings, setMappings] = useState<SectorMapping[]>([])
  const [loading, setLoading] = useState(false)
  const [modalVisible, setModalVisible] = useState(false)
  const [editingMapping, setEditingMapping] = useState<SectorMapping | null>(null)
  const [form] = Form.useForm()

  useEffect(() => {
    loadMappings()
  }, [])

  const loadMappings = async () => {
    setLoading(true)
    try {
      const data = await sectorApi.getMappings()
      setMappings(data)
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || error.message || '加载板块映射失败'
      message.error(errorMsg)
      console.error('加载板块映射失败:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleCreate = () => {
    setEditingMapping(null)
    form.resetFields()
    setModalVisible(true)
  }

  const handleEdit = (record: SectorMapping) => {
    setEditingMapping(record)
    form.setFieldsValue({
      source_sector: record.source_sector,
      target_sector: record.target_sector,
      description: record.description,
      is_active: record.is_active,
    })
    setModalVisible(true)
  }

  const handleDelete = async (id: number) => {
    try {
      await sectorApi.deleteMapping(id)
      message.success('删除成功')
      loadMappings()
    } catch (error) {
      message.error('删除失败')
    }
  }

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields()
      if (editingMapping) {
        const update: SectorMappingUpdate = {
          target_sector: values.target_sector,
          description: values.description,
          is_active: values.is_active,
        }
        await sectorApi.updateMapping(editingMapping.id, update)
        message.success('更新成功')
      } else {
        const create: SectorMappingCreate = {
          source_sector: values.source_sector,
          target_sector: values.target_sector,
          description: values.description,
        }
        await sectorApi.createMapping(create)
        message.success('创建成功')
      }
      setModalVisible(false)
      loadMappings()
      // 刷新缓存
      try {
        await sectorApi.refreshCache()
      } catch (error) {
        // 忽略缓存刷新错误
      }
    } catch (error) {
      console.error('提交失败:', error)
    }
  }

  const handleRefreshCache = async () => {
    try {
      const result = await sectorApi.refreshCache()
      message.success(`缓存刷新成功，共 ${result.count} 条映射规则`)
    } catch (error) {
      message.error('刷新缓存失败')
    }
  }

  // 按目标板块分组
  const groupedMappings = mappings.reduce((acc, mapping) => {
    const target = mapping.target_sector
    if (!acc[target]) {
      acc[target] = []
    }
    acc[target].push(mapping)
    return acc
  }, {} as Record<string, SectorMapping[]>)

  const columns = [
    {
      title: '源板块（细分概念）',
      dataIndex: 'source_sector',
      key: 'source_sector',
      width: 200,
    },
    {
      title: '目标板块（主板块）',
      dataIndex: 'target_sector',
      key: 'target_sector',
      width: 200,
      render: (text: string) => <Tag color="blue">{text}</Tag>,
    },
    {
      title: '说明',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: '状态',
      dataIndex: 'is_active',
      key: 'is_active',
      width: 80,
      render: (active: boolean) => (
        <Tag color={active ? 'green' : 'red'}>{active ? '启用' : '禁用'}</Tag>
      ),
    },
    {
      title: '操作',
      key: 'action',
      width: 150,
      render: (_: any, record: SectorMapping) => (
        <Space>
          <Button
            type="link"
            size="small"
            icon={<EditOutlined />}
            onClick={() => handleEdit(record)}
          >
            编辑
          </Button>
          <Popconfirm
            title="确定要删除这个映射规则吗？"
            onConfirm={() => handleDelete(record.id)}
            okText="确定"
            cancelText="取消"
          >
            <Button type="link" size="small" danger icon={<DeleteOutlined />}>
              删除
            </Button>
          </Popconfirm>
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
              <AppstoreOutlined style={{ marginRight: 8 }} />
              概念管理
            </span>
            <Space>
              <Button
                icon={<ReloadOutlined />}
                onClick={handleRefreshCache}
              >
                刷新缓存
              </Button>
              <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
                新增映射
              </Button>
            </Space>
          </div>
        }
      >
        <Tabs
          defaultActiveKey="list"
          items={[
            {
              key: 'list',
              label: '列表视图',
              children: (
                <Table
                  dataSource={mappings}
                  columns={columns}
                  rowKey="id"
                  loading={loading}
                  pagination={{ pageSize: 20 }}
                />
              ),
            },
            {
              key: 'grouped',
              label: '分组视图',
              children: (
                <div>
                  {Object.entries(groupedMappings).map(([target, items]) => (
                    <Card
                      key={target}
                      title={
                        <Tag color="blue" style={{ fontSize: 16, padding: '4px 12px' }}>
                          {target}
                        </Tag>
                      }
                      style={{ marginBottom: 16 }}
                      size="small"
                    >
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                        {items.map((item) => (
                          <Tag
                            key={item.id}
                            color={item.is_active ? 'green' : 'default'}
                            style={{ fontSize: 14, padding: '4px 8px' }}
                          >
                            {item.source_sector}
                            {item.description && (
                              <span style={{ marginLeft: 4, opacity: 0.7 }}>
                                ({item.description})
                              </span>
                            )}
                          </Tag>
                        ))}
                      </div>
                    </Card>
                  ))}
                </div>
              ),
            },
          ]}
        />
      </Card>

      <Modal
        title={editingMapping ? '编辑板块映射' : '新增板块映射'}
        open={modalVisible}
        onOk={handleSubmit}
        onCancel={() => setModalVisible(false)}
        width={600}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="source_sector"
            label="源板块（细分概念）"
            rules={[{ required: true, message: '请输入源板块名称' }]}
          >
            <Input
              placeholder="例如：Sora概念、Kimi概念"
              disabled={!!editingMapping}
              maxLength={100}
            />
          </Form.Item>
          <Form.Item
            name="target_sector"
            label="目标板块（主板块）"
            rules={[{ required: true, message: '请输入目标板块名称' }]}
          >
            <Input 
              placeholder="例如：AI应用、商业航天、脑机接口" 
              maxLength={100}
            />
          </Form.Item>
          <Form.Item name="description" label="说明">
            <Input.TextArea
              rows={3}
              placeholder="可选：添加映射说明，帮助理解该映射的用途"
              maxLength={255}
            />
          </Form.Item>
          {editingMapping && (
            <Form.Item name="is_active" label="状态" valuePropName="checked">
              <Switch checkedChildren="启用" unCheckedChildren="禁用" />
            </Form.Item>
          )}
        </Form>
      </Modal>
    </div>
  )
}

export default Settings
