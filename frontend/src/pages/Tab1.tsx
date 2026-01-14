import React, { useState, useEffect, useRef } from 'react'
import { Input, Card, Spin, message, AutoComplete, Tag, Button, Modal, Space, Select } from 'antd'
import { SearchOutlined, BulbOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { stockApi, StockDailyData, CapitalFlowData } from '../api/stock'
import { aiApi } from '../api/ai'

const Tab1: React.FC = () => {
  const [selectedModel, setSelectedModel] = useState<string>('')
  const [availableModels, setAvailableModels] = useState<Array<{ id: number; model_name: string; model_display_name: string }>>([])
  
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
  
  const [selectedStock, setSelectedStock] = useState<string>('')
  const [selectedStockName, setSelectedStockName] = useState<string>('')
  const [dailyData, setDailyData] = useState<StockDailyData[]>([])
  const [capitalFlowData, setCapitalFlowData] = useState<CapitalFlowData[]>([])
  const [loading, setLoading] = useState(false)
  const [isMobile, setIsMobile] = useState(false)
  const [searchOptions, setSearchOptions] = useState<Array<{ value: string; label: React.ReactNode; code: string; name: string }>>([])
  const [searchValue, setSearchValue] = useState<string>('')
  const [searchLoading, setSearchLoading] = useState(false)
  const [aiAnalyzeModalVisible, setAiAnalyzeModalVisible] = useState(false)
  const [aiAnalyzeLoading, setAiAnalyzeLoading] = useState(false)
  const [aiAnalyzeResult, setAiAnalyzeResult] = useState<string>('')

  // 检测移动端
  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768)
    }
    checkMobile()
    window.addEventListener('resize', checkMobile)
    return () => window.removeEventListener('resize', checkMobile)
  }, [])

  // 当selectedStock变化时加载数据（仅在selectedStock不为空时）
  // 使用useRef来跟踪是否是首次渲染，避免初始化时加载数据
  const isFirstRender = useRef(true)
  
  useEffect(() => {
    // 首次渲染时，确保数据为空
    if (isFirstRender.current) {
      isFirstRender.current = false
      setDailyData([])
      setCapitalFlowData([])
      setSelectedStockName('')
      return
    }
    
    // 后续变化时才加载数据
    if (selectedStock && selectedStock.trim() !== '') {
      loadStockData(selectedStock, true) // 自动刷新最新数据
    } else {
      // 清空数据
      setDailyData([])
      setCapitalFlowData([])
      setSelectedStockName('')
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedStock])

  const normalizeStockCode = (code: string): string => {
    // 移除SZ/SH前缀，只保留数字部分
    code = code.trim().toUpperCase()
    if (code.startsWith('SZ') || code.startsWith('SH')) {
      code = code.substring(2)
    }
    return code
  }

  const loadStockName = async (stockCode: string) => {
    try {
      const normalizedCode = normalizeStockCode(stockCode)
      const stocks = await stockApi.searchStocks(normalizedCode)
      // 确保stocks是数组
      if (Array.isArray(stocks) && stocks.length > 0) {
        const matchedStock = stocks.find(s => s.code === normalizedCode) || stocks[0]
        setSelectedStockName(matchedStock?.name || '')
      }
    } catch (error) {
      console.error('获取股票名称失败:', error)
    }
  }

  // 判断是否为交易时段
  const isTradingHours = (): boolean => {
    const now = new Date()
    const hour = now.getHours()
    const minute = now.getMinutes()
    const timeMinutes = hour * 60 + minute
    
    // 上午交易时段：9:30-11:30
    const morningStart = 9 * 60 + 30  // 9:30
    const morningEnd = 11 * 60 + 30   // 11:30
    
    // 下午交易时段：13:00-15:00
    const afternoonStart = 13 * 60   // 13:00
    const afternoonEnd = 15 * 60      // 15:00
    
    // 判断是否在交易时段内
    const isMorning = timeMinutes >= morningStart && timeMinutes <= morningEnd
    const isAfternoon = timeMinutes >= afternoonStart && timeMinutes <= afternoonEnd
    
    // 判断是否为工作日（周一到周五）
    const dayOfWeek = now.getDay()
    const isWeekday = dayOfWeek >= 1 && dayOfWeek <= 5
    
    return isWeekday && (isMorning || isAfternoon)
  }

  const loadStockData = async (stockCode: string, autoRefresh: boolean = false) => {
    setLoading(true)
    try {
      // 标准化股票代码
      const normalizedCode = normalizeStockCode(stockCode)
      
      // 如果是交易时段且需要自动刷新，先刷新数据
      if (autoRefresh && isTradingHours()) {
        try {
          await stockApi.refreshStockData(normalizedCode)
          message.success('已自动刷新最新市场数据', 2)
        } catch (error: any) {
          // 刷新失败不影响数据加载，只记录警告
          const errorMsg = error?.response?.data?.detail || error?.message
          if (errorMsg && !errorMsg.includes('不是交易时段')) {
            console.warn('自动刷新数据失败:', errorMsg)
          }
        }
      }
      
      const [daily, capitalFlow] = await Promise.all([
        stockApi.getStockDaily(normalizedCode),
        stockApi.getCapitalFlow(normalizedCode),
      ])
      // 确保返回的是数组，避免undefined错误
      setDailyData(Array.isArray(daily) ? daily : [])
      setCapitalFlowData(Array.isArray(capitalFlow) ? capitalFlow : [])
      
      // 加载股票名称
      await loadStockName(normalizedCode)
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || '加载标的数据失败'
      message.error(errorMsg)
      // 出错时清空数据
      setDailyData([])
      setCapitalFlowData([])
    } finally {
      setLoading(false)
    }
  }

  // 搜索股票（支持中文和首字母）
  const handleSearch = async (value: string) => {
    if (!value || value.trim().length === 0) {
      setSearchOptions([])
      return
    }

    setSearchLoading(true)
    try {
      const stocks = await stockApi.searchStocks(value.trim())
      // 确保stocks是数组
      if (Array.isArray(stocks) && stocks.length > 0) {
        const options = stocks.map(stock => ({
          value: `${stock.code} ${stock.name}`,
          label: (
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span>
                <Tag color="blue" style={{ marginRight: 8 }}>{stock.code}</Tag>
                <span style={{ fontWeight: 500 }}>{stock.name}</span>
              </span>
              {stock.sector && <Tag color="cyan" style={{ fontSize: 11 }}>{stock.sector}</Tag>}
            </div>
          ),
          code: stock.code,
          name: stock.name
        }))
        setSearchOptions(options)
      } else {
        setSearchOptions([])
      }
    } catch (error) {
      console.error('搜索失败:', error)
      setSearchOptions([])
    } finally {
      setSearchLoading(false)
    }
  }

  // 选择股票
  const handleStockSelect = async (value: string, option: any) => {
    const stockCode = option.code || value.split(' ')[0]
    const stockName = option.name || value.split(' ')[1] || stockCode
    setSearchValue('')
    setSearchOptions([])
    setSelectedStock(stockCode)
    setSelectedStockName(stockName)
    // loadStockData会在useEffect中自动调用，这里不需要手动调用
    // 但如果是交易时段，会自动刷新最新数据
  }

  // AI分析股票
  const handleAIAnalyze = async () => {
    if (!selectedStock) {
      message.warning('请先选择一只股票')
      return
    }
    
    setAiAnalyzeModalVisible(true)
    setAiAnalyzeLoading(true)
    setAiAnalyzeResult('')
    try {
      const result = await aiApi.analyzeStock(selectedStock, selectedModel || undefined)
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

  const getKLineOption = () => {
    if (!dailyData || dailyData.length === 0) {
      return null
    }
    
    const dates = dailyData.map(d => d.trade_date)
    const kData = dailyData.map(d => [d.open_price, d.close_price, d.low_price, d.high_price])
    const volumes = dailyData.map(d => d.volume || 0)  // 确保volume不为undefined

    return {
      title: {
        text: 'K线图',
        left: 'center',
        textStyle: { fontSize: 16, fontWeight: 'bold' },
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
        formatter: (params: any) => {
          if (!params || params.length === 0) return ''
          
          const date = params[0].axisValue
          let result = `<div style="margin-bottom: 4px;"><strong>${date}</strong></div>`
          
          params.forEach((param: any) => {
            if (param.seriesName === 'K线') {
              const data = param.data as number[]
              if (data && data.length === 4) {
                const [open, close, low, high] = data
                const dataIndex = param.dataIndex
                const stockData = dailyData[dataIndex]
                
                // 计算涨跌幅（如果有change_pct就用，否则计算）
                let changePct = stockData?.change_pct
                if (changePct === undefined || changePct === null) {
                  // 从前一天计算涨跌幅
                  if (dataIndex > 0 && dailyData[dataIndex - 1]) {
                    const prevClose = dailyData[dataIndex - 1].close_price
                    if (prevClose && prevClose > 0) {
                      changePct = ((close - prevClose) / prevClose) * 100
                    }
                  }
                }
                
                const changeText = changePct !== undefined && changePct !== null
                  ? `<span style="color: ${changePct >= 0 ? '#ef5350' : '#26a69a'}; font-weight: bold;">
                      ${changePct >= 0 ? '+' : ''}${changePct.toFixed(2)}%
                    </span>`
                  : ''
                
                result += `
                  <div style="margin: 4px 0;">
                    <span style="color: #666;">开盘：</span><span style="color: #333; font-weight: bold;">${open.toFixed(2)}</span><br/>
                    <span style="color: #666;">收盘：</span><span style="color: #333; font-weight: bold;">${close.toFixed(2)}</span> ${changeText}<br/>
                    <span style="color: #666;">最高：</span><span style="color: #333; font-weight: bold;">${high.toFixed(2)}</span><br/>
                    <span style="color: #666;">最低：</span><span style="color: #333; font-weight: bold;">${low.toFixed(2)}</span>
                  </div>
                `
              }
            } else if (param.seriesName === '成交量') {
              const volume = param.value
              if (volume) {
                const volumeText = volume >= 10000 
                  ? `${(volume / 10000).toFixed(2)}万`
                  : volume.toLocaleString()
                result += `<div style="margin: 4px 0;"><span style="color: #666;">成交量：</span><span style="color: #333; font-weight: bold;">${volumeText}</span></div>`
              }
            } else {
              // MA线
              const value = param.value
              if (value !== null && value !== undefined) {
                result += `<div style="margin: 2px 0;"><span style="color: #666;">${param.seriesName}：</span><span style="color: #333;">${value.toFixed(2)}</span></div>`
              }
            }
          })
          
          return result
        },
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
          data: dailyData.map(d => d.ma5),
          smooth: true,
          lineStyle: { width: 1 },
          showSymbol: false,
        },
        {
          name: 'MA10',
          type: 'line',
          data: dailyData.map(d => d.ma10),
          smooth: true,
          lineStyle: { width: 1 },
          showSymbol: false,
        },
        {
          name: 'MA20',
          type: 'line',
          data: dailyData.map(d => d.ma20),
          smooth: true,
          lineStyle: { width: 1 },
          showSymbol: false,
        },
        {
          name: 'MA30',
          type: 'line',
          data: dailyData.map(d => d.ma30),
          smooth: true,
          lineStyle: { width: 1 },
          showSymbol: false,
        },
        {
          name: 'MA60',
          type: 'line',
          data: dailyData.map(d => d.ma60),
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
              if (idx > 0 && idx < dailyData.length && dailyData[idx] && dailyData[idx - 1]) {
                if (dailyData[idx].close_price > dailyData[idx - 1].close_price) {
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

  const getCapitalFlowOption = () => {
    // 确保capitalFlowData是数组且不为空
    if (!capitalFlowData || !Array.isArray(capitalFlowData) || capitalFlowData.length === 0) {
      return null
    }
    
    const dates = capitalFlowData.map(d => d.trade_date)
    const mainInflow = capitalFlowData.map(d => d.main_net_inflow)

    return {
      title: {
        text: '主力资金流入情况',
        left: 'center',
        textStyle: { fontSize: 16, fontWeight: 'bold' },
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
      },
      xAxis: {
        type: 'category',
        data: dates,
      },
      yAxis: {
        type: 'value',
        name: '净流入（万元）',
      },
      series: [
        {
          name: '主力净流入',
          type: 'bar',
          data: mainInflow,
          itemStyle: {
            color: (params: any) => {
              return params.value >= 0 ? '#ef5350' : '#26a69a'
            },
          },
        },
      ],
    }
  }

  // 计算资金流向图表配置
  const capitalFlowOption = capitalFlowData.length > 0 ? getCapitalFlowOption() : null

  return (
    <div>
      <Card style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', gap: 16, alignItems: 'center', flexWrap: 'wrap' }}>
          <span style={{ fontSize: 16, fontWeight: 'bold' }}>标的搜索：</span>
          <AutoComplete
            style={{ flex: 1, minWidth: 300, maxWidth: 500 }}
            options={searchOptions}
            onSearch={handleSearch}
            onSelect={handleStockSelect}
            value={searchValue}
            onChange={setSearchValue}
            placeholder="输入股票代码、中文名称或首字母拼音（如：688072、平安、PA）"
            allowClear
            notFoundContent={searchLoading ? <Spin size="small" /> : '未找到匹配的股票'}
          >
            <Input
              prefix={searchLoading ? <Spin size="small" /> : <SearchOutlined />}
              size="large"
              allowClear
              onPressEnter={async (e) => {
                const value = (e.target as HTMLInputElement).value.trim()
                if (value) {
                  // 如果是纯数字代码，直接设置selectedStock，useEffect会自动加载
                  const normalized = normalizeStockCode(value)
                  if (normalized.match(/^\d{6}$/)) {
                    setSelectedStock(normalized)
                  } else {
                    // 否则触发搜索
                    await handleSearch(value)
                  }
                }
              }}
            />
          </AutoComplete>
          {selectedStockName && (
            <div style={{ fontSize: 14, color: '#666' }}>
              当前标的：<span style={{ fontWeight: 'bold', color: '#1890ff' }}>{selectedStockName} ({selectedStock})</span>
            </div>
          )}
        </div>
      </Card>

      {loading ? (
        <div style={{ textAlign: 'center', padding: 50 }}>
          <Spin size="large" />
        </div>
      ) : selectedStock && dailyData.length > 0 ? (
        <>
          <Card 
            style={{ marginBottom: 24 }}
            title={
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
                <span>
                  {selectedStockName ? `${selectedStockName} (${selectedStock})` : selectedStock} - K线图
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
                    type="primary"
                    icon={<BulbOutlined />}
                    onClick={handleAIAnalyze}
                  >
                    AI分析
                  </Button>
                </Space>
              </div>
            }
          >
            <ReactECharts
              option={getKLineOption()}
              style={{ 
                height: isMobile ? '400px' : '600px', 
                width: '100%' 
              }}
            />
          </Card>
          {capitalFlowOption && (
            <Card>
              <ReactECharts
                option={capitalFlowOption}
                style={{ 
                  height: isMobile ? '300px' : '400px', 
                  width: '100%' 
                }}
              />
            </Card>
          )}
        </>
      ) : (
        <Card
          title={
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
              <span>标的分析</span>
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
                  type="primary"
                  icon={<BulbOutlined />}
                  onClick={handleAIAnalyze}
                  disabled={!selectedStock}
                  title={!selectedStock ? '请先选择一只股票' : ''}
                >
                  AI分析
                </Button>
              </Space>
            </div>
          }
        >
          <div style={{ textAlign: 'center', padding: 50, color: '#999' }}>
            请搜索并选择标的查看K线图和资金流入情况
          </div>
        </Card>
      )}

      {/* AI分析Modal */}
      <Modal
        title={
          <Space>
            <BulbOutlined />
            <span>AI股票分析 - {selectedStockName || selectedStock}</span>
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

export default Tab1
