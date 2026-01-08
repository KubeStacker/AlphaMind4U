import React, { useState, useEffect } from 'react'
import { Card, Row, Col, Tag, Table, Statistic, Spin, message } from 'antd'
import { StarOutlined, RiseOutlined, DollarOutlined, FireOutlined } from '@ant-design/icons'
import { recommendationsApi, Recommendations } from '../api/recommendations'

const Tab3: React.FC = () => {
  const [data, setData] = useState<Recommendations | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    loadRecommendations()
  }, [])

  const loadRecommendations = async () => {
    setLoading(true)
    try {
      const recommendations = await recommendationsApi.getRecommendations()
      setData(recommendations)
    } catch (error) {
      message.error('加载推荐数据失败')
    } finally {
      setLoading(false)
    }
  }

  const hotStocksColumns = [
    {
      title: '标的代码',
      dataIndex: 'stock_code',
      key: 'stock_code',
      width: 120,
    },
    {
      title: '标的名称',
      dataIndex: 'stock_name',
      key: 'stock_name',
      width: 150,
    },
    {
      title: '当前价格',
      dataIndex: 'current_price',
      key: 'current_price',
      width: 100,
      render: (price: number) => price ? (
        <span style={{ fontWeight: 'bold', color: '#1890ff' }}>
          ¥{price.toFixed(2)}
        </span>
      ) : '-',
    },
    {
      title: '当日涨幅',
      dataIndex: 'change_pct',
      key: 'change_pct',
      width: 100,
      render: (pct: number) => pct !== undefined && pct !== null ? (
        <Tag color={pct >= 0 ? 'red' : 'green'}>
          {pct >= 0 ? '+' : ''}{pct.toFixed(2)}%
        </Tag>
      ) : '-',
    },
    {
      title: '7天涨幅',
      dataIndex: 'avg_change_7d',
      key: 'avg_change_7d',
      width: 100,
      render: (pct: number) => pct !== undefined && pct !== null ? (
        <Tag color={pct >= 0 ? 'orange' : 'cyan'}>
          {pct >= 0 ? '+' : ''}{pct.toFixed(2)}%
        </Tag>
      ) : '-',
    },
    {
      title: '连板情况',
      dataIndex: 'consecutive_boards',
      key: 'consecutive_boards',
      width: 100,
      render: (days: number) => days ? (
        <Tag color="purple">{days} 板</Tag>
      ) : '-',
    },
    {
      title: '所属板块',
      dataIndex: 'sector',
      key: 'sector',
      width: 120,
      render: (sector: string) => sector ? <Tag>{sector}</Tag> : '-',
    },
    {
      title: '热度排名',
      dataIndex: 'rank',
      key: 'rank',
      width: 100,
      render: (rank: number) => rank ? <Tag color="orange">#{rank}</Tag> : '-',
    },
  ]

  const capitalFlowColumns = [
    {
      title: '标的代码',
      dataIndex: 'stock_code',
      key: 'stock_code',
    },
    {
      title: '标的名称',
      dataIndex: 'stock_name',
      key: 'stock_name',
    },
    {
      title: '平均净流入（万元）',
      dataIndex: 'avg_inflow',
      key: 'avg_inflow',
      render: (inflow: number) => (
        <Tag color="green">{inflow.toFixed(2)}</Tag>
      ),
    },
    {
      title: '持续流入天数',
      dataIndex: 'positive_days',
      key: 'positive_days',
      render: (days: number) => <Tag color="blue">{days} 天</Tag>,
    },
  ]

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: 50 }}>
        <Spin size="large" />
      </div>
    )
  }

  if (!data) {
    return <div>暂无数据</div>
  }

  return (
    <div>
      <Row gutter={16} style={{ marginBottom: 24 }}>
        <Col xs={12} sm={12} md={6}>
          <Card>
            <Statistic
              title="热门板块数"
              value={data.hot_sectors.length}
              prefix={<FireOutlined />}
              valueStyle={{ color: '#667eea' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={12} md={6}>
          <Card>
            <Statistic
              title="热门标的数"
              value={data.hot_stocks.length}
              prefix={<StarOutlined />}
              valueStyle={{ color: '#f5222d' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={12} md={6}>
          <Card>
            <Statistic
              title="资金流入标的数"
              value={data.capital_flow_stocks.length}
              prefix={<DollarOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={12} md={6}>
          <Card>
            <Statistic
              title="推荐板块热度"
              value={data.hot_sectors.reduce((sum, s) => sum + s.hot_count, 0)}
              prefix={<RiseOutlined />}
              valueStyle={{ color: '#fa8c16' }}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col xs={24} sm={24} md={12}>
          <Card
            title={
              <span>
                <FireOutlined style={{ marginRight: 8 }} />
                最热门板块推荐
              </span>
            }
            style={{ marginBottom: 24 }}
          >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              {data.hot_sectors.map((sector, index) => (
                <Card
                  key={sector.sector_name}
                  size="small"
                  hoverable
                  style={{
                    background: index === 0
                      ? 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
                      : '#f5f7fa',
                    color: index === 0 ? '#fff' : '#333',
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: sector.hot_stocks && sector.hot_stocks.length > 0 ? 12 : 0 }}>
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 16, fontWeight: 'bold', marginBottom: 4 }}>
                        {index + 1}. {sector.sector_name}
                      </div>
                    </div>
                    <Tag color={index === 0 ? 'gold' : 'blue'} style={{ fontSize: 14 }}>
                      {sector.hot_count} 只热门股
                    </Tag>
                  </div>
                  {sector.hot_stocks && sector.hot_stocks.length > 0 && (
                    <div style={{ 
                      marginTop: 12, 
                      paddingTop: 12, 
                      borderTop: index === 0 ? '1px solid rgba(255,255,255,0.3)' : '1px solid #e8e8e8' 
                    }}>
                      <div style={{ 
                        fontSize: 12, 
                        marginBottom: 8, 
                        opacity: index === 0 ? 0.9 : 0.7 
                      }}>
                        热门标的：
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                        {sector.hot_stocks.slice(0, 5).map((stock, idx) => (
                          <div 
                            key={stock.stock_code} 
                            style={{ 
                              fontSize: 12, 
                              opacity: index === 0 ? 0.9 : 0.8,
                              display: 'flex',
                              alignItems: 'center',
                              gap: 8
                            }}
                          >
                            <span>{idx + 1}. {stock.stock_name} ({stock.stock_code})</span>
                            {stock.rank && (
                              <Tag 
                                color={index === 0 ? 'gold' : 'orange'} 
                                style={{ fontSize: 10, margin: 0 }}
                              >
                                #{stock.rank}
                              </Tag>
                            )}
                          </div>
                        ))}
                        {sector.hot_stocks.length > 5 && (
                          <div style={{ 
                            fontSize: 11, 
                            opacity: index === 0 ? 0.7 : 0.6, 
                            marginTop: 4 
                          }}>
                            还有 {sector.hot_stocks.length - 5} 只...
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </Card>
              ))}
            </div>
          </Card>
        </Col>

        <Col xs={24} sm={24} md={12}>
          <Card
            title={
              <span>
                <StarOutlined style={{ marginRight: 8 }} />
                最热门标的推荐
              </span>
            }
            style={{ marginBottom: 24 }}
          >
            <Table
              dataSource={data.hot_stocks}
              columns={hotStocksColumns}
              rowKey="stock_code"
              pagination={{ pageSize: 10 }}
              size="small"
            />
          </Card>
        </Col>
      </Row>

      <Card
        title={
          <span>
            <DollarOutlined style={{ marginRight: 8 }} />
            资金持续流入标的推荐（近5天持续正流入）
          </span>
        }
      >
        <Table
          dataSource={data.capital_flow_stocks}
          columns={capitalFlowColumns}
          rowKey="stock_code"
          pagination={{ pageSize: 10 }}
          size="small"
        />
      </Card>
    </div>
  )
}

export default Tab3
