import React, { useState, useEffect } from 'react'
import { Card, Button, Table, Tag, Collapse, message, Popover, Spin } from 'antd'
import { ReloadOutlined, FireOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import { hotApi, HotStock, SectorInfo, SectorStock } from '../api/hot'

const { Panel } = Collapse

const Tab2: React.FC = () => {
  const [hotStocks, setHotStocks] = useState<HotStock[]>([])
  const [hotSectors, setHotSectors] = useState<SectorInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [sectorChartData, setSectorChartData] = useState<Record<string, any>>({})
  const [sectorStocks, setSectorStocks] = useState<Record<string, SectorStock[]>>({})
  const [defaultStock, setDefaultStock] = useState<{ code: string; name: string; volume: number } | null>(null)
  const [isMobile, setIsMobile] = useState(false)

  useEffect(() => {
    loadData()
    loadDefaultStock()
  }, [])

  // 检测移动端
  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768)
    }
    checkMobile()
    window.addEventListener('resize', checkMobile)
    return () => window.removeEventListener('resize', checkMobile)
  }, [])

  const loadDefaultStock = async () => {
    try {
      const { stockApi } = await import('../api/stock')
      const dailyData = await stockApi.getStockDaily('688022')
      if (dailyData && dailyData.length > 0) {
        const latest = dailyData[dailyData.length - 1]
        // 获取股票名称
        const stocks = await stockApi.searchStocks('688022')
        const stockName = stocks.length > 0 ? stocks[0].name : '688022'
        setDefaultStock({
          code: '688022',
          name: stockName,
          volume: latest.volume || 0
        })
      }
    } catch (error) {
      console.error('加载默认股票信息失败:', error)
    }
  }

  const loadData = async () => {
    setLoading(true)
    try {
      const [stocks, sectors] = await Promise.all([
        hotApi.getHotStocks(),
        hotApi.getHotSectors(),
      ])
      setHotStocks(stocks)
      setHotSectors(sectors)
    } catch (error) {
      message.error('加载数据失败')
    } finally {
      setLoading(false)
    }
  }

  const handleRefresh = async () => {
    setRefreshing(true)
    try {
      await hotApi.refreshHotStocks()
      await loadData()
      message.success('热度榜数据已刷新')
    } catch (error) {
      message.error('刷新失败')
    } finally {
      setRefreshing(false)
    }
  }

  const handleSectorClick = async (sectorName: string) => {
    if (sectorChartData[sectorName]) return // 已加载

    try {
      const [dailyData, stocks] = await Promise.all([
        hotApi.getSectorDaily(sectorName),
        hotApi.getSectorStocks(sectorName),
      ])
      
      setSectorChartData({ ...sectorChartData, [sectorName]: dailyData })
      setSectorStocks({ ...sectorStocks, [sectorName]: stocks })
    } catch (error) {
      message.error('加载板块数据失败')
    }
  }

  const getSectorChartOption = (sectorName: string) => {
    const data = sectorChartData[sectorName] || []
    if (data.length === 0) return null

    const dates = data.map((d: any) => d.trade_date)
    const kData = data.map((d: any) => [d.open_price, d.close_price, d.low_price, d.high_price])

    return {
      title: {
        text: `${sectorName} - 板块K线`,
        left: 'center',
        textStyle: { fontSize: 14 },
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
      },
      xAxis: {
        type: 'category',
        data: dates,
        boundaryGap: false,
      },
      yAxis: {
        type: 'value',
        scale: true,
      },
      series: [
        {
          name: 'K线',
          type: 'candlestick',
          data: kData,
          itemStyle: {
            color: '#ef5350',
            color0: '#26a69a',
            borderColor: '#ef5350',
            borderColor0: '#26a69a',
          },
        },
      ],
    }
  }

  const hotStocksColumns = [
    {
      title: '排名',
      dataIndex: 'rank',
      key: 'rank',
      width: 80,
    },
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
      title: '所属板块',
      dataIndex: 'sectors',
      key: 'sectors',
      width: 200,
      render: (sectors: string[] | undefined) => {
        if (!sectors || sectors.length === 0) return '-'
        return (
          <div>
            {sectors.slice(0, 3).map((sector, idx) => (
              <Tag key={idx} color="cyan" style={{ marginBottom: 4 }}>
                {sector}
              </Tag>
            ))}
          </div>
        )
      },
    },
    {
      title: '数据来源',
      dataIndex: 'source',
      key: 'source',
      width: 120,
      render: (source: string) => (
        <Tag color={source === 'xueqiu' ? 'blue' : 'purple'}>
          {source === 'xueqiu' ? '雪球' : '东财'}
        </Tag>
      ),
    },
    {
      title: '连续上榜天数',
      dataIndex: 'consecutive_days',
      key: 'consecutive_days',
      width: 120,
      render: (days: number) => <Tag color="orange">{days} 天</Tag>,
    },
    {
      title: '成交量',
      dataIndex: 'volume',
      key: 'volume',
      width: 120,
      render: (volume: number) => {
        if (!volume || volume === 0) return '-'
        if (volume >= 10000) return (volume / 10000).toFixed(2) + '万'
        return volume.toLocaleString()
      },
    },
  ]

  const sectorStocksColumns = [
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
      title: '热度排名',
      dataIndex: 'rank',
      key: 'rank',
      render: (rank: number) => rank ? <Tag color="red">#{rank}</Tag> : '-',
    },
    {
      title: '连续上榜',
      dataIndex: 'consecutive_days',
      key: 'consecutive_days',
      render: (days: number) => <Tag>{days} 天</Tag>,
    },
  ]

  const xueqiuStocks = hotStocks.filter(s => s.source === 'xueqiu').slice(0, 100)
  const dongcaiStocks = hotStocks.filter(s => s.source === 'dongcai').slice(0, 100)

  return (
    <div>
      <Card
        style={{ marginBottom: 24 }}
        title={
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span>
              <FireOutlined style={{ marginRight: 8 }} />
              热门板块推荐
            </span>
            <Button
              type="primary"
              icon={<ReloadOutlined />}
              onClick={handleRefresh}
              loading={refreshing}
            >
              刷新数据
            </Button>
          </div>
        }
      >
        {loading ? (
          <div style={{ textAlign: 'center', padding: 20 }}>
            <Spin />
          </div>
        ) : (
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
            {hotSectors.map((sector) => {
              const chartOption = getSectorChartOption(sector.sector_name)
              const stocks = sectorStocks[sector.sector_name] || []

              return (
                <Popover
                  key={sector.sector_name}
                  title={sector.sector_name}
                  content={
                    <div style={{ width: isMobile ? '90vw' : 600, maxWidth: '100%' }}>
                      {chartOption ? (
                        <>
                          <ReactECharts
                            option={chartOption}
                            style={{ height: isMobile ? '250px' : '300px', width: '100%' }}
                          />
                          {stocks.length > 0 && (
                            <Table
                              dataSource={stocks}
                              columns={sectorStocksColumns}
                              pagination={false}
                              size="small"
                              style={{ marginTop: 16 }}
                            />
                          )}
                        </>
                      ) : (
                        <div style={{ textAlign: 'center', padding: 20 }}>
                          <Button onClick={() => handleSectorClick(sector.sector_name)}>
                            加载板块数据
                          </Button>
                        </div>
                      )}
                    </div>
                  }
                  trigger="click"
                  placement="bottom"
                >
                  <Card
                    hoverable
                    style={{
                      width: isMobile ? '100%' : 280,
                      cursor: 'pointer',
                      background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                      color: '#fff',
                    }}
                    bodyStyle={{ padding: isMobile ? 12 : 16 }}
                  >
                    <div style={{ fontSize: 16, fontWeight: 'bold', marginBottom: 8 }}>
                      {sector.sector_name}
                    </div>
                    <div style={{ fontSize: 24, fontWeight: 'bold', marginBottom: 12 }}>
                      {sector.hot_count} 只热门股
                    </div>
                    {sector.hot_stocks && sector.hot_stocks.length > 0 && (
                      <div style={{ marginTop: 12, borderTop: '1px solid rgba(255,255,255,0.3)', paddingTop: 12 }}>
                        <div style={{ fontSize: 12, marginBottom: 8, opacity: 0.9 }}>热门标的：</div>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                          {sector.hot_stocks.slice(0, 5).map((stock, idx) => (
                            <div key={stock.stock_code} style={{ fontSize: 12, opacity: 0.9 }}>
                              {idx + 1}. {stock.stock_name} ({stock.stock_code})
                              {stock.rank && (
                                <Tag color="gold" style={{ marginLeft: 4, fontSize: 10 }}>
                                  #{stock.rank}
                                </Tag>
                              )}
                            </div>
                          ))}
                          {sector.hot_stocks.length > 5 && (
                            <div style={{ fontSize: 11, opacity: 0.7, marginTop: 4 }}>
                              还有 {sector.hot_stocks.length - 5} 只...
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </Card>
                </Popover>
              )
            })}
          </div>
        )}
      </Card>

      {defaultStock && (
        <Card style={{ marginBottom: 24 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            <span style={{ fontSize: 16, fontWeight: 'bold' }}>默认标的：</span>
            <Tag color="blue" style={{ fontSize: 14, padding: '4px 12px' }}>
              {defaultStock.code} {defaultStock.name}
            </Tag>
            <span style={{ color: '#666' }}>成交量：</span>
            <Tag color="green" style={{ fontSize: 14, padding: '4px 12px' }}>
              {defaultStock.volume >= 10000 
                ? (defaultStock.volume / 10000).toFixed(2) + '万' 
                : defaultStock.volume.toLocaleString()}
            </Tag>
          </div>
        </Card>
      )}

      <Collapse defaultActiveKey={['xueqiu']} style={{ marginBottom: 24 }}>
        <Panel
          header={
            <span>
              <FireOutlined style={{ marginRight: 8 }} />
              雪球热度榜 (前100)
            </span>
          }
          key="xueqiu"
        >
          <Table
            dataSource={xueqiuStocks}
            columns={hotStocksColumns}
            rowKey={(record) => `${record.source}-${record.stock_code}-${record.rank}`}
            pagination={{ pageSize: 20 }}
            size="small"
          />
        </Panel>
        <Panel
          header={
            <span>
              <FireOutlined style={{ marginRight: 8 }} />
              东财热度榜 (前100)
            </span>
          }
          key="dongcai"
        >
          <Table
            dataSource={dongcaiStocks}
            columns={hotStocksColumns}
            rowKey={(record) => `${record.source}-${record.stock_code}-${record.rank}`}
            pagination={{ pageSize: 20 }}
            size="small"
          />
        </Panel>
      </Collapse>
    </div>
  )
}

export default Tab2
