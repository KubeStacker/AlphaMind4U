import React, { useState, useEffect } from 'react'
import { Card, Table, Button, message, Space, Typography, Alert, Tag, Tooltip } from 'antd'
import { DatabaseOutlined, PlayCircleOutlined } from '@ant-design/icons'
import { dataCollectionApi, DataCollectionType, DataCollectionResult } from '../api/dataCollection'
import { useAuth } from '../contexts/AuthContext'

const { Title, Text } = Typography

const DataManagement: React.FC = () => {
  const { user } = useAuth()
  const isAdmin = user?.is_admin || user?.username === 'admin'
  const [loading, setLoading] = useState(false)
  const [collecting, setCollecting] = useState<string | null>(null)
  const [types, setTypes] = useState<DataCollectionType[]>()


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

  const handleCollectSpecific = async (dataType: string) => {
    setCollecting(dataType)
    try {
      const result: DataCollectionResult = await dataCollectionApi.collectSpecific(dataType)
      if (result.success !== false) {
        message.success(result.message || '任务已启动')
        // 采集成功后刷新数据统计
        await loadTypes()
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
      width: 150,
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      width: 250,
    },
    {
      title: '需要交易日',
      dataIndex: 'requires_trading_day',
      key: 'requires_trading_day',
      width: 100,
      render: (requires: boolean) => (
        <Tag color={requires ? 'orange' : 'green'}>
          {requires ? '是' : '否'}
        </Tag>
      ),
    },
    {
      title: '数据表名称',
      key: 'table_name',
      width: 180,
      render: (_: any, record: DataCollectionType) => record.stats?.table_name || '-',
    },
    {
      title: '定时采集时间',
      key: 'schedule_time',
      width: 180,
      render: (_: any, record: DataCollectionType) => record.stats?.schedule_time || '-',
    },
    {
      title: '数据轮转周期',
      key: 'retention_days',
      width: 120,
      render: (_: any, record: DataCollectionType) => {
        const days = record.stats?.retention_days
        if (days === null || days === undefined) return '永久保留'
        return `${days}天`
      },
    },
    {
      title: '最近N天交易日数',
      key: 'trading_days_in_period',
      width: 150,
      render: (_: any, record: DataCollectionType) => {
        const days = record.stats?.trading_days_in_period
        const retentionDays = record.stats?.retention_days
        if (days === null || days === undefined) {
          return retentionDays ? '-' : '不适用'
        }
        return `${days}天`
      },
    },
    {
      title: '实际存储数据天数',
      key: 'actual_data_days',
      width: 150,
      render: (_: any, record: DataCollectionType) => {
        const days = record.stats?.actual_data_days
        if (days === null || days === undefined) return '-'
        const retentionDays = record.stats?.retention_days
        const tradingDays = record.stats?.trading_days_in_period
        
        // 如果有保留天数和交易日数，计算完整性
        if (retentionDays && tradingDays !== null && tradingDays !== undefined) {
          const completeness = days / tradingDays
          const color = completeness >= 0.95 ? 'green' : completeness >= 0.8 ? 'orange' : 'red'
          return <Tag color={color}>{days}天 ({Math.round(completeness * 100)}%)</Tag>
        }
        return `${days}天`
      },
    },
    {
      title: '操作',
      key: 'action',
      width: 180,
      fixed: 'right' as const,
      render: (_: any, record: DataCollectionType) => (
        <Space size="small">
          <Tooltip title="计算当日数据">
            <Button
              type="primary"
              icon={<PlayCircleOutlined />}
              loading={collecting === record.value}
              onClick={() => handleCollectSpecific(record.value)}
              size="small"
            >
              采集
            </Button>
          </Tooltip>

        </Space>
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
          description="数据采集功能用于手动触发各类数据的采集任务。需要交易日数据的数据类型，如果不是交易日，将自动采集最近交易日的数据。"
          type="info"
          showIcon
          style={{ marginBottom: 24 }}
        />

        <Title level={4}>数据类型列表</Title>
        <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
          点击"采集"按钮可以采集指定类型的数据。
        </Text>
        <Table
          dataSource={types}
          columns={columns}
          rowKey="value"
          pagination={false}
          scroll={{ x: 1400 }}
        />
      </Card>


    </div>
  )
}

export default DataManagement
