import React, { useState, useEffect } from 'react'
import { Input, Card, Spin, message, AutoComplete, Tag } from 'antd'
import { SearchOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import { stockApi, StockDailyData, CapitalFlowData } from '../api/stock'

const Tab1: React.FC = () => {
  const [selectedStock, setSelectedStock] = useState<string>('688072')
  const [selectedStockName, setSelectedStockName] = useState<string>('')
  const [dailyData, setDailyData] = useState<StockDailyData[]>([])
  const [capitalFlowData, setCapitalFlowData] = useState<CapitalFlowData[]>([])
  const [loading, setLoading] = useState(false)
  const [isMobile, setIsMobile] = useState(false)
  const [searchOptions, setSearchOptions] = useState<Array<{ value: string; label: React.ReactNode; code: string; name: string }>>([])
  const [searchValue, setSearchValue] = useState<string>('')
  const [searchLoading, setSearchLoading] = useState(false)

  // 组件加载时自动加载默认标的数据
  useEffect(() => {
    loadStockData('688072')
    // 加载默认股票的名称
    loadStockName('688072')
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

  // 当selectedStock变化时加载数据
  useEffect(() => {
    if (selectedStock) {
      loadStockData(selectedStock)
    }
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
      if (stocks && stocks.length > 0) {
        const matchedStock = stocks.find(s => s.code === normalizedCode) || stocks[0]
        setSelectedStockName(matchedStock.name || '')
      }
    } catch (error) {
      console.error('获取股票名称失败:', error)
    }
  }

  const loadStockData = async (stockCode: string) => {
    setLoading(true)
    try {
      // 标准化股票代码
      const normalizedCode = normalizeStockCode(stockCode)
      
      const [daily, capitalFlow] = await Promise.all([
        stockApi.getStockDaily(normalizedCode),
        stockApi.getCapitalFlow(normalizedCode),
      ])
      setDailyData(daily)
      setCapitalFlowData(capitalFlow)
      
      // 加载股票名称
      await loadStockName(normalizedCode)
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || '加载标的数据失败'
      message.error(errorMsg)
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
    await loadStockData(stockCode)
  }

  const getKLineOption = () => {
    if (!dailyData || dailyData.length === 0) {
      return null
    }
    
    const dates = dailyData.map(d => d.trade_date)
    const kData = dailyData.map(d => [d.open_price, d.close_price, d.low_price, d.high_price])
    const volumes = dailyData.map(d => d.volume || 0)  // 确保volume不为undefined

    const titleText = selectedStockName 
      ? `${selectedStockName} (${selectedStock}) - K线图`
      : `${selectedStock} - K线图`

    return {
      title: {
        text: titleText,
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
                  // 如果是纯数字代码，直接加载
                  const normalized = normalizeStockCode(value)
                  if (normalized.match(/^\d{6}$/)) {
                    setSelectedStock(normalized)
                    await loadStockData(normalized)
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
          <Card style={{ marginBottom: 24 }}>
            <ReactECharts
              option={getKLineOption()}
              style={{ 
                height: isMobile ? '400px' : '600px', 
                width: '100%' 
              }}
            />
          </Card>
          {capitalFlowData.length > 0 && (
            <Card>
              <ReactECharts
                option={getCapitalFlowOption()}
                style={{ 
                  height: isMobile ? '300px' : '400px', 
                  width: '100%' 
                }}
              />
            </Card>
          )}
        </>
      ) : (
        <Card>
          <div style={{ textAlign: 'center', padding: 50, color: '#999' }}>
            请搜索并选择标的查看K线图和资金流入情况
          </div>
        </Card>
      )}
    </div>
  )
}

export default Tab1
