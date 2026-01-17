import React, { useState, useEffect } from 'react'
import { Card, Table, Button, message, Space, Typography, Alert, Tag, Popconfirm } from 'antd'
import { DatabaseOutlined, PlayCircleOutlined, ReloadOutlined } from '@ant-design/icons'
import { dataCollectionApi, DataCollectionType, DataCollectionResult, CollectAllResult } from '../api/dataCollection'
import { useAuth } from '../contexts/AuthContext'

const { Title, Text } = Typography

const DataManagement: React.FC = () => {
  const { user } = useAuth()
  const isAdmin = user?.is_admin || user?.username === 'admin'
  const [loading, setLoading] = useState(false)
  const [collecting, setCollecting] = useState<string | null>(null)
  const [types, setTypes] = useState<DataCollectionType[]>([])
  const [collectAllLoading, setCollectAllLoading] = useState(false)

  useEffect(() => {
    if (isAdmin) {
      loadTypes()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isAdmin])

  const loadTypes = async () => {
    setLoading(true)
    try {
      const data = await dataCollectionApi.getTypes()
      setTypes(data)
    } catch (error: any) {
      message.error(`加载数据类型失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    } finally {
      setLoading(false)
    }
  }

  const handleCollectAll = async (forceTradingDay: boolean = false) => {
    setCollectAllLoading(true)
    try {
      const result: CollectAllResult = await dataCollectionApi.collectAll(forceTradingDay)
      const successCount = result.success_count
      const totalCount = result.total_count
      const totalTime = result.total_time

      if (result.success) {
        message.success(`批量采集完成！成功: ${successCount}/${totalCount}，总耗时: ${totalTime}秒`)
      } else {
        message.warning(`批量采集完成，但部分失败。成功: ${successCount}/${totalCount}，总耗时: ${totalTime}秒`)
      }
    } catch (error: any) {
      message.error(`批量采集失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    } finally {
      setCollectAllLoading(false)
    }
  }

  const handleCollectSpecific = async (dataType: string) => {
    setCollecting(dataType)
    try {
      const result: DataCollectionResult = await dataCollectionApi.collectSpecific(dataType, { force: false })
      if (result.success) {
        message.success(`${result.message}${result.elapsed_time ? `，耗时: ${result.elapsed_time}秒` : ''}`)
      } else {
        message.error(result.message || '采集失败')
      }
    } catch (error: any) {
      message.error(`采集失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    } finally {
      setCollecting(null)
    }
  }

  const columns = [
    {
      title: '数据类型',
      dataIndex: 'label',
      key: 'label',
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
    },
    {
      title: '需要交易日',
      dataIndex: 'requires_trading_day',
      key: 'requires_trading_day',
      render: (requires: boolean) => (
        <Tag color={requires ? 'orange' : 'green'}>
          {requires ? '是' : '否'}
        </Tag>
      ),
    },
    {
      title: '操作',
      key: 'action',
      render: (_: any, record: DataCollectionType) => (
        <Button
          type="primary"
          icon={<PlayCircleOutlined />}
          loading={collecting === record.value}
          onClick={() => handleCollectSpecific(record.value)}
        >
          采集
        </Button>
      ),
    },
  ]

  if (!isAdmin) {
    return (
      <Card>
        <Alert
          message="权限不足"
          description="您没有权限访问数据管理功能，请联系管理员。"
          type="warning"
          showIcon
        />
      </Card>
    )
  }

  return (
    <div>
      <Card
        title={
          <Space>
            <DatabaseOutlined />
            <span>数据管理</span>
          </Space>
        }
        loading={loading}
      >
        <Alert
          message="数据采集说明"
          description="数据采集功能用于手动触发各类数据的采集任务。部分数据类型需要在交易日才能采集。批量采集会依次执行所有数据类型的采集任务。"
          type="info"
          showIcon
          style={{ marginBottom: 24 }}
        />

        <div style={{ marginBottom: 16 }}>
          <Space>
            <Popconfirm
              title="确定要批量采集所有数据吗？"
              description="此操作将依次执行所有数据类型的采集任务，可能需要较长时间。"
              onConfirm={() => handleCollectAll(false)}
              okText="确定"
              cancelText="取消"
            >
              <Button
                type="primary"
                icon={<ReloadOutlined />}
                loading={collectAllLoading}
                size="large"
              >
                批量采集所有数据
              </Button>
            </Popconfirm>
            <Popconfirm
              title="确定要强制批量采集所有数据吗？"
              description="此操作将强制在非交易日也执行采集任务，可能会采集到空数据。"
              onConfirm={() => handleCollectAll(true)}
              okText="确定"
              cancelText="取消"
            >
              <Button
                icon={<ReloadOutlined />}
                loading={collectAllLoading}
                size="large"
              >
                强制批量采集（非交易日）
              </Button>
            </Popconfirm>
          </Space>
        </div>

        <Title level={4}>数据类型列表</Title>
        <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
          点击"采集"按钮可以单独采集指定类型的数据。
        </Text>
        <Table
          dataSource={types}
          columns={columns}
          rowKey="value"
          pagination={false}
        />
      </Card>
    </div>
  )
}

export default DataManagement
