import React, { useState, useEffect } from 'react'
import { Card, Button, Table, Tag, Collapse, message, Popover, Spin, Modal, Space, Select } from 'antd'
import { ReloadOutlined, FireOutlined, RobotOutlined, BulbOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { hotApi, HotStock, SectorInfo, SectorStock } from '../api/hot'
import { stockApi, StockDailyData } from '../api/stock'
import { aiApi } from '../api/ai'

const { Panel } = Collapse

const Tab2: React.FC = () => {
  const [selectedModel, setSelectedModel] = useState<string>('')
  const [availableModels, setAvailableModels] = useState<Array<{ id: number; model_name: string; model_display_name: string }>>([])
  const [hotStocks, setHotStocks] = useState<HotStock[]>([])
  const [hotSectors, setHotSectors] = useState<SectorInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [sectorChartData, setSectorChartData] = useState<Record<string, any>>({})
  const [sectorStocks, setSectorStocks] = useState<Record<string, SectorStock[]>>({})
  const [isMobile, setIsMobile] = useState(false)
  const [klineModalVisible, setKlineModalVisible] = useState(false)
  const [selectedStockForKline, setSelectedStockForKline] = useState<{ code: string; name: string } | null>(null)
  const [klineData, setKlineData] = useState<StockDailyData[]>([])
  const [klineLoading, setKlineLoading] = useState(false)
  const [sectorStocksModalVisible, setSectorStocksModalVisible] = useState(false)
  const [selectedSectorForStocks, setSelectedSectorForStocks] = useState<{ name: string; stocks: SectorStock[] } | null>(null)
  const [aiRecommendModalVisible, setAiRecommendModalVisible] = useState(false)
  const [aiRecommendLoading, setAiRecommendLoading] = useState(false)
  const [aiRecommendResult, setAiRecommendResult] = useState<string>('')
  const [aiAnalyzeModalVisible, setAiAnalyzeModalVisible] = useState(false)
  const [aiAnalyzeLoading, setAiAnalyzeLoading] = useState(false)
  const [aiAnalyzeResult, setAiAnalyzeResult] = useState<string>('')
  const [selectedStockForAnalyze, setSelectedStockForAnalyze] = useState<{ code: string; name: string } | null>(null)

  const loadData = async () => {
    setLoading(true)
    try {
      const [stocks, sectors] = await Promise.all([
        hotApi.getHotStocks(),
        hotApi.getHotSectors(),
      ])
      setHotStocks(stocks || [])
      setHotSectors(sectors || [])
      
      // 调试信息：检查数据格式
      if (import.meta.env.DEV) {
        console.log('热门板块数据:', sectors)
        console.log('热门股票数据:', stocks)
      }
    } catch (error: any) {
      console.error('加载数据失败:', error)
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      message.error(`加载数据失败: ${errorMsg}`)
      setHotStocks([])
      setHotSectors([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadData()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // 加载可用模型
  useEffect(() => {
    const loadModels = async () => {
      try {
        const response = await aiApi.getActiveAIModels()
        setAvailableModels(response.models)
        if (response.models.length > 0 && !selectedModel) {
          setSelectedModel(response.models[0].model_name)
        }
      } catch (error) {
        console.error('加载模型列表失败:', error)
      }
    }
    
    loadModels()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // 页面可见性变化时刷新数据
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        // 页面重新可见时刷新数据
        loadData()
      }
    }
    
    document.addEventListener('visibilitychange', handleVisibilityChange)
    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
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

  const handleRefresh = async () => {
    setRefreshing(true)
    try {
      await hotApi.refreshHotStocks()
      await loadData()
      message.success('热度榜数据已刷新')
    } catch (error: any) {
      console.error('刷新数据失败:', error)
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      message.error(`刷新失败: ${errorMsg}`)
    } finally {
      setRefreshing(false)
    }
  }

  const handleAIRecommend = async () => {
    setAiRecommendModalVisible(true)
    setAiRecommendLoading(true)
    setAiRecommendResult('')
    try {
      const result = await aiApi.recommendStocks(selectedModel || undefined)
      setAiRecommendResult(result.recommendation)
    } catch (error: any) {
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      if (errorMsg.includes('API Key未配置')) {
        message.warning('API Key未配置，请前往AI管理设置中配置模型API Key')
      } else {
        message.error(`AI推荐失败: ${errorMsg}`)
      }
      setAiRecommendResult(`错误: ${errorMsg}`)
    } finally {
      setAiRecommendLoading(false)
    }
  }

  const handleAIAnalyze = async (stockCode: string, stockName: string) => {
    setSelectedStockForAnalyze({ code: stockCode, name: stockName })
    setAiAnalyzeModalVisible(true)
    setAiAnalyzeLoading(true)
    setAiAnalyzeResult('')
    try {
      const result = await aiApi.analyzeStock(stockCode, selectedModel || undefined)
      setAiAnalyzeResult(result.analysis)
    } catch (error: any) {
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      if (errorMsg.includes('API Key未配置')) {
        message.warning('API Key未配置，请前往AI管理设置中配置模型API Key')
      } else {
        message.error(`AI分析失败: ${errorMsg}`)
      }
      setAiAnalyzeResult(`错误: ${errorMsg}`)
    } finally {
      setAiAnalyzeLoading(false)
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

  const handleSectorStocksClick = async (sectorName: string) => {
    try {
      let stocks = sectorStocks[sectorName]
      if (!stocks || stocks.length === 0) {
        stocks = await hotApi.getSectorStocks(sectorName)
        setSectorStocks({ ...sectorStocks, [sectorName]: stocks })
      }
      setSelectedSectorForStocks({ name: sectorName, stocks })
      setSectorStocksModalVisible(true)
    } catch (error) {
      message.error('加载板块股票列表失败')
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
      render: (name: string, record: HotStock) => (
        <Space>
          <span style={{ cursor: 'pointer', color: '#1890ff' }} 
                onClick={() => handleStockClick(record.stock_code, name)}>
            {name || '-'}
          </span>
          <Button
            type="link"
            size="small"
            icon={<BulbOutlined />}
            onClick={(e) => {
              e.stopPropagation()
              handleAIAnalyze(record.stock_code, name || record.stock_code)
            }}
            title="AI分析"
          />
        </Space>
      ),
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
      render: (days: number) => days ? <Tag color="orange">{days} 天</Tag> : '-',
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
      render: (name: string, record: SectorStock) => (
        <span style={{ cursor: 'pointer', color: '#1890ff' }}>
          {name || record.stock_code || '-'}
        </span>
      ),
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

  const handleStockClick = async (stockCode: string, stockName: string) => {
    setSelectedStockForKline({ code: stockCode, name: stockName })
    setKlineModalVisible(true)
    setKlineLoading(true)
    try {
      const data = await stockApi.getStockDaily(stockCode)
      setKlineData(data)
    } catch (error) {
      message.error('加载K线数据失败')
      setKlineData([])
    } finally {
      setKlineLoading(false)
    }
  }

  const getKLineOption = () => {
    if (!klineData || klineData.length === 0) {
      return null
    }
    
    const dates = klineData.map(d => d.trade_date)
    const kData = klineData.map(d => [d.open_price, d.close_price, d.low_price, d.high_price])
    const volumes = klineData.map(d => d.volume || 0)

    return {
      title: {
        text: `${selectedStockForKline?.name || selectedStockForKline?.code} - K线图`,
        left: 'center',
        textStyle: { fontSize: 18, fontWeight: 'bold' },
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
      },
      legend: {
        data: ['K线', 'MA5', 'MA10', 'MA20', 'MA30', 'MA60', '成交量'],
        top: 30,
      },
      grid: [
        { left: '10%', right: '8%', top: '15%', height: '50%' },
        { left: '10%', right: '8%', top: '70%', height: '15%' },
      ],
      xAxis: [
        {
          type: 'category',
          data: dates,
          scale: true,
          boundaryGap: false,
          axisLine: { onZero: false },
          splitLine: { show: false },
          min: 'dataMin',
          max: 'dataMax',
        },
        {
          type: 'category',
          gridIndex: 1,
          data: dates,
          scale: true,
          boundaryGap: false,
          axisLine: { onZero: false },
          splitLine: { show: false },
          min: 'dataMin',
          max: 'dataMax',
        },
      ],
      yAxis: [
        {
          scale: true,
          splitArea: { show: true },
        },
        {
          scale: true,
          gridIndex: 1,
          splitNumber: 2,
          axisLabel: { show: false },
          splitLine: { show: false },
        },
      ],
      dataZoom: [
        {
          type: 'inside',
          xAxisIndex: [0, 1],
          start: 70,
          end: 100,
        },
        {
          show: true,
          xAxisIndex: [0, 1],
          type: 'slider',
          top: '90%',
          start: 70,
          end: 100,
        },
      ],
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
        {
          name: 'MA5',
          type: 'line',
          data: klineData.map(d => d.ma5),
          smooth: true,
          lineStyle: { width: 1 },
          showSymbol: false,
        },
        {
          name: 'MA10',
          type: 'line',
          data: klineData.map(d => d.ma10),
          smooth: true,
          lineStyle: { width: 1 },
          showSymbol: false,
        },
        {
          name: 'MA20',
          type: 'line',
          data: klineData.map(d => d.ma20),
          smooth: true,
          lineStyle: { width: 1 },
          showSymbol: false,
        },
        {
          name: 'MA30',
          type: 'line',
          data: klineData.map(d => d.ma30),
          smooth: true,
          lineStyle: { width: 1 },
          showSymbol: false,
        },
        {
          name: 'MA60',
          type: 'line',
          data: klineData.map(d => d.ma60),
          smooth: true,
          lineStyle: { width: 1 },
          showSymbol: false,
        },
        {
          name: '成交量',
          type: 'bar',
          xAxisIndex: 1,
          yAxisIndex: 1,
          data: volumes,
          itemStyle: {
            color: (params: any) => {
              const idx = params.dataIndex
              if (idx > 0 && idx < klineData.length && klineData[idx] && klineData[idx - 1]) {
                if (klineData[idx].close_price > klineData[idx - 1].close_price) {
                  return '#ef5350'
                }
              }
              return '#26a69a'
            },
          },
        },
      ],
    }
  }

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
            <Space>
              {availableModels.length > 0 && (
                <Select
                  value={selectedModel}
                  onChange={setSelectedModel}
                  style={{ width: 150 }}
                  placeholder="选择模型"
                  size="small"
                >
                  {availableModels.map(model => (
                    <Select.Option key={model.id} value={model.model_name}>
                      {model.model_display_name}
                    </Select.Option>
                  ))}
                </Select>
              )}
              <Button
                type="default"
                icon={<RobotOutlined />}
                onClick={handleAIRecommend}
                title="AI推荐"
              >
                AI推荐
              </Button>
              <Button
                type="primary"
                icon={<ReloadOutlined />}
                onClick={handleRefresh}
                loading={refreshing}
              >
                刷新数据
              </Button>
            </Space>
          </div>
        }
      >
        {loading ? (
          <div style={{ textAlign: 'center', padding: 20 }}>
            <Spin />
          </div>
        ) : hotSectors.length === 0 ? (
          <div style={{ textAlign: 'center', padding: 40, color: '#999' }}>
            <div style={{ fontSize: 16, marginBottom: 8 }}>暂无热门板块数据</div>
            <div style={{ fontSize: 14 }}>请确保已采集热门股票数据，或点击刷新按钮更新数据</div>
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
                              onRow={(record) => ({
                                onClick: () => handleStockClick(record.stock_code, record.stock_name || record.stock_code),
                                style: { cursor: 'pointer' }
                              })}
                            />
                          )}
                        </>
                      ) : (
                        <div style={{ textAlign: 'center', padding: 20 }}>
                          <Spin />
                          <div style={{ marginTop: 10, color: '#999' }}>正在加载板块数据...</div>
                        </div>
                      )}
                    </div>
                  }
                  trigger="click"
                  placement="bottom"
                  onOpenChange={(open) => {
                    if (open && !chartOption) {
                      handleSectorClick(sector.sector_name)
                    }
                  }}
                >
                  <Card
                    hoverable
                    onClick={() => handleSectorClick(sector.sector_name)}
                    style={{
                      width: isMobile ? '100%' : 280,
                      cursor: 'pointer',
                      background: sector.color === 'red' 
                        ? 'linear-gradient(135deg, #ff6b6b 0%, #ee5a6f 100%)'
                        : sector.color === 'orange'
                        ? 'linear-gradient(135deg, #ffa726 0%, #fb8c00 100%)'
                        : 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                      color: '#fff',
                    }}
                    bodyStyle={{ padding: isMobile ? 12 : 16 }}
                  >
                    <div style={{ fontSize: 16, fontWeight: 'bold', marginBottom: 8 }}>
                      {sector.sector_name}
                    </div>
                    <div style={{ fontSize: 24, fontWeight: 'bold', marginBottom: 12 }}>
                      {sector.hot_count || sector.hot_score || 0} 只热门股
                    </div>
                    {sector.hot_stocks && sector.hot_stocks.length > 0 && (
                      <div style={{ marginTop: 12, borderTop: '1px solid rgba(255,255,255,0.3)', paddingTop: 12 }}>
                        <div style={{ fontSize: 12, marginBottom: 8, opacity: 0.9 }}>热门标的：</div>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                          {sector.hot_stocks.slice(0, 5).map((stock, idx) => (
                            <div 
                              key={stock.stock_code} 
                              style={{ fontSize: 12, opacity: 0.9, cursor: 'pointer' }}
                              onClick={(e) => {
                                e.stopPropagation()
                                handleStockClick(stock.stock_code, stock.stock_name || stock.stock_code)
                              }}
                            >
                              {idx + 1}. {stock.stock_name || stock.stock_code} ({stock.stock_code})
                              {stock.rank && (
                                <Tag color="gold" style={{ marginLeft: 4, fontSize: 10 }}>
                                  #{stock.rank}
                                </Tag>
                              )}
                            </div>
                          ))}
                          {sector.hot_stocks.length > 5 && (
                            <div 
                              style={{ fontSize: 11, opacity: 0.9, marginTop: 4, cursor: 'pointer', textDecoration: 'underline' }}
                              onClick={(e) => {
                                e.stopPropagation()
                                handleSectorStocksClick(sector.sector_name)
                              }}
                            >
                              查看全部 {sector.hot_stocks.length} 只股票...
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
            onRow={(record) => ({
              onClick: () => handleStockClick(record.stock_code, record.stock_name || record.stock_code),
              style: { cursor: 'pointer' }
            })}
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
            onRow={(record) => ({
              onClick: () => handleStockClick(record.stock_code, record.stock_name || record.stock_code),
              style: { cursor: 'pointer' }
            })}
          />
        </Panel>
      </Collapse>

      <Modal
        title={
          <Space>
            <span>{selectedStockForKline?.name || selectedStockForKline?.code} - K线图</span>
            {selectedStockForKline && (
              <Button
                type="link"
                icon={<BulbOutlined />}
                onClick={() => {
                  if (selectedStockForKline) {
                    handleAIAnalyze(selectedStockForKline.code, selectedStockForKline.name)
                  }
                }}
                title="AI分析"
              >
                AI分析
              </Button>
            )}
          </Space>
        }
        open={klineModalVisible}
        onCancel={() => setKlineModalVisible(false)}
        footer={null}
        width={isMobile ? '95%' : 1200}
        style={{ top: 20 }}
      >
        {klineLoading ? (
          <div style={{ textAlign: 'center', padding: 50 }}>
            <Spin size="large" />
          </div>
        ) : klineData.length > 0 ? (
          <ReactECharts
            option={getKLineOption()}
            style={{ 
              height: isMobile ? '400px' : '600px', 
              width: '100%' 
            }}
          />
        ) : (
          <div style={{ textAlign: 'center', padding: 50, color: '#999' }}>
            暂无K线数据
          </div>
        )}
      </Modal>

      <Modal
        title={`${selectedSectorForStocks?.name} - 全部股票列表`}
        open={sectorStocksModalVisible}
        onCancel={() => setSectorStocksModalVisible(false)}
        footer={null}
        width={isMobile ? '95%' : 1000}
        style={{ top: 20 }}
      >
        {selectedSectorForStocks && selectedSectorForStocks.stocks.length > 0 ? (
          <Table
            dataSource={selectedSectorForStocks.stocks}
            columns={sectorStocksColumns}
            rowKey={(record) => record.stock_code}
            pagination={{ pageSize: 20 }}
            size="small"
            onRow={(record) => ({
              onClick: () => {
                setSectorStocksModalVisible(false)
                handleStockClick(record.stock_code, record.stock_name || record.stock_code)
              },
              style: { cursor: 'pointer' }
            })}
          />
        ) : (
          <div style={{ textAlign: 'center', padding: 50, color: '#999' }}>
            暂无股票数据
          </div>
        )}
      </Modal>

      {/* AI推荐Modal */}
      <Modal
        title={
          <Space>
            <RobotOutlined />
            <span>AI股票推荐</span>
          </Space>
        }
        open={aiRecommendModalVisible}
        onCancel={() => setAiRecommendModalVisible(false)}
        footer={null}
        width={isMobile ? '95%' : 800}
        style={{ top: 20 }}
      >
        {aiRecommendLoading ? (
          <div style={{ textAlign: 'center', padding: 50 }}>
            <Spin size="large" />
            <div style={{ marginTop: 16, color: '#999' }}>AI正在分析中，请稍候...</div>
          </div>
        ) : aiRecommendResult ? (
          <div
            style={{
              maxHeight: '70vh',
              overflow: 'auto',
              padding: '16px',
              lineHeight: '1.8',
              fontSize: '14px'
            }}
          >
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                h1: ({...props}: any) => <h1 style={{ fontSize: '20px', marginTop: '16px', marginBottom: '12px' }} {...props} />,
                h2: ({...props}: any) => <h2 style={{ fontSize: '18px', marginTop: '14px', marginBottom: '10px' }} {...props} />,
                h3: ({...props}: any) => <h3 style={{ fontSize: '16px', marginTop: '12px', marginBottom: '8px' }} {...props} />,
                p: ({...props}: any) => <p style={{ marginBottom: '8px' }} {...props} />,
                ul: ({...props}: any) => <ul style={{ marginBottom: '8px', paddingLeft: '24px' }} {...props} />,
                ol: ({...props}: any) => <ol style={{ marginBottom: '8px', paddingLeft: '24px' }} {...props} />,
                li: ({...props}: any) => <li style={{ marginBottom: '4px' }} {...props} />,
                code: ({inline, ...props}: any) => 
                  inline ? (
                    <code style={{ background: '#f5f5f5', padding: '2px 6px', borderRadius: '3px', fontFamily: 'monospace' }} {...props} />
                  ) : (
                    <code style={{ display: 'block', background: '#f5f5f5', padding: '12px', borderRadius: '4px', overflow: 'auto', fontFamily: 'monospace' }} {...props} />
                  ),
                blockquote: ({...props}: any) => <blockquote style={{ borderLeft: '4px solid #1890ff', paddingLeft: '12px', marginLeft: 0, color: '#666' }} {...props} />,
              }}
            >
              {aiRecommendResult}
            </ReactMarkdown>
          </div>
        ) : (
          <div style={{ textAlign: 'center', padding: 50, color: '#999' }}>
            暂无推荐结果
          </div>
        )}
      </Modal>

      {/* AI分析Modal */}
      <Modal
        title={
          <Space>
            <BulbOutlined />
            <span>AI股票分析 - {selectedStockForAnalyze?.name || selectedStockForAnalyze?.code}</span>
          </Space>
        }
        open={aiAnalyzeModalVisible}
        onCancel={() => setAiAnalyzeModalVisible(false)}
        footer={null}
        width={isMobile ? '95%' : 800}
        style={{ top: 20 }}
      >
        {aiAnalyzeLoading ? (
          <div style={{ textAlign: 'center', padding: 50 }}>
            <Spin size="large" />
            <div style={{ marginTop: 16, color: '#999' }}>AI正在分析中，请稍候...</div>
          </div>
        ) : aiAnalyzeResult ? (
          <div
            style={{
              maxHeight: '70vh',
              overflow: 'auto',
              padding: '16px',
              lineHeight: '1.8',
              fontSize: '14px'
            }}
          >
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                h1: ({...props}: any) => <h1 style={{ fontSize: '20px', marginTop: '16px', marginBottom: '12px' }} {...props} />,
                h2: ({...props}: any) => <h2 style={{ fontSize: '18px', marginTop: '14px', marginBottom: '10px' }} {...props} />,
                h3: ({...props}: any) => <h3 style={{ fontSize: '16px', marginTop: '12px', marginBottom: '8px' }} {...props} />,
                p: ({...props}: any) => <p style={{ marginBottom: '8px' }} {...props} />,
                ul: ({...props}: any) => <ul style={{ marginBottom: '8px', paddingLeft: '24px' }} {...props} />,
                ol: ({...props}: any) => <ol style={{ marginBottom: '8px', paddingLeft: '24px' }} {...props} />,
                li: ({...props}: any) => <li style={{ marginBottom: '4px' }} {...props} />,
                code: ({inline, ...props}: any) => 
                  inline ? (
                    <code style={{ background: '#f5f5f5', padding: '2px 6px', borderRadius: '3px', fontFamily: 'monospace' }} {...props} />
                  ) : (
                    <code style={{ display: 'block', background: '#f5f5f5', padding: '12px', borderRadius: '4px', overflow: 'auto', fontFamily: 'monospace' }} {...props} />
                  ),
                blockquote: ({...props}: any) => <blockquote style={{ borderLeft: '4px solid #1890ff', paddingLeft: '12px', marginLeft: 0, color: '#666' }} {...props} />,
              }}
            >
              {aiAnalyzeResult}
            </ReactMarkdown>
          </div>
        ) : (
          <div style={{ textAlign: 'center', padding: 50, color: '#999' }}>
            暂无分析结果
          </div>
        )}
      </Modal>
    </div>
  )
}

export default Tab2
