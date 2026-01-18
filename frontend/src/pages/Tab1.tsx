import React, { useState, useEffect, useRef } from 'react'
import { Input, Card, Spin, message, AutoComplete, Tag, Button, Modal, Space } from 'antd'
const { TextArea } = Input
import { SearchOutlined, BulbOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { sheepApi, SheepDailyData, CapitalFlowData } from '../api/sheep'
import { hotApi } from '../api/hot'
import { aiApi } from '../api/ai'
import DeepAnalysis from '../components/DeepAnalysis'

const Tab1: React.FC = () => {
  const [modelSelectModalVisible, setModelSelectModalVisible] = useState(false)
  const [promptEditModalVisible, setPromptEditModalVisible] = useState(false)
  const [pendingAction, setPendingAction] = useState<'analyze' | null>(null)
  const [selectedModelName, setSelectedModelName] = useState<string>('')
  const [promptText, setPromptText] = useState<string>('')
  const [renderedPromptText, setRenderedPromptText] = useState<string>('')
  
  const [selectedSheep, setSelectedSheep] = useState<string>('')
  const [selectedSheepName, setSelectedSheepName] = useState<string>('')
  const [dailyData, setDailyData] = useState<SheepDailyData[]>([])
  const [capitalFlowData, setCapitalFlowData] = useState<CapitalFlowData[]>([])
  const [loading, setLoading] = useState(false)
  const [isMobile, setIsMobile] = useState(false)
  const [searchOptions, setSearchOptions] = useState<Array<{ value: string; label: React.ReactNode; code: string; name: string }>>([])
  const [searchValue, setSearchValue] = useState<string>('')
  const [searchLoading, setSearchLoading] = useState(false)
  const [aiAnalyzeModalVisible, setAiAnalyzeModalVisible] = useState(false)
  const [aiAnalyzeLoading, setAiAnalyzeLoading] = useState(false)
  const [aiAnalyzeResult, setAiAnalyzeResult] = useState<string>('')
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  // 检测移动端
  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768)
    }
    checkMobile()
    window.addEventListener('resize', checkMobile)
    return () => window.removeEventListener('resize', checkMobile)
  }, [])

  // 当selectedSheep变化时加载数据
  const isFirstRender = useRef(true)
  
  useEffect(() => {
    // 首次渲染时，确保数据为空
    if (isFirstRender.current) {
      isFirstRender.current = false
      setDailyData([])
      setCapitalFlowData([])
      setSelectedSheepName('')
      return
    }
    
    // 后续变化时才加载数据
    if (selectedSheep && selectedSheep.trim() !== '') {
      loadSheepData(selectedSheep, true) // 自动刷新最新数据
    } else {
      // 清空数据
      setDailyData([])
      setCapitalFlowData([])
      setSelectedSheepName('')
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedSheep])

  // 页面可见性变化时刷新数据
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible' && selectedSheep) {
        loadSheepData(selectedSheep)
      }
    }
    
    document.addEventListener('visibilitychange', handleVisibilityChange)
    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange)
    }
  }, [selectedSheep])

  // 交易时段每分钟自动刷新
  useEffect(() => {
    let interval: ReturnType<typeof setInterval> | null = null
    
    if (selectedSheep && isTradingHours()) {
      interval = setInterval(() => {
        loadSheepData(selectedSheep)
      }, 60000) // 1分钟
    }
    
    return () => {
      if (interval) clearInterval(interval)
    }
  }, [selectedSheep])

  const normalizeSheepCode = (code: string): string => {
    // 移除SZ/SH前缀，只保留数字部分
    code = code.trim().toUpperCase()
    if (code.startsWith('SZ') || code.startsWith('SH')) {
      code = code.substring(2)
    }
    return code
  }

  const loadSheepName = async (stockCode: string) => {
    try {
      const normalizedCode = normalizeSheepCode(stockCode)
      const sheep = await sheepApi.searchSheeps(normalizedCode)
      // 确保sheep是数组
      if (Array.isArray(sheep) && sheep.length > 0) {
        const matchedSheep = sheep.find(s => s.code === normalizedCode) || sheep[0]
        setSelectedSheepName(matchedSheep?.name || '')
      }
    } catch (error) {
      console.error('获取肥羊名称失败:', error)
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

  const loadSheepData = async (stockCode: string, autoRefresh: boolean = false) => {
    setLoading(true)
    try {
      // 标准化肥羊代码
      const normalizedCode = normalizeSheepCode(stockCode)
      
      // 如果是交易时段且需要自动刷新，先刷新数据
      if (autoRefresh && isTradingHours()) {
        try {
          await sheepApi.refreshSheepData(normalizedCode)
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
        sheepApi.getSheepDaily(normalizedCode),
        sheepApi.getCapitalFlow(normalizedCode, 60).catch((error) => {
          // 资金流数据获取失败时，记录但不影响主流程
          console.warn('获取资金流数据失败:', error)
          return []
        }),
      ])
      // 确保返回的是数组，避免undefined错误
      setDailyData(Array.isArray(daily) ? daily : [])
      setCapitalFlowData(Array.isArray(capitalFlow) ? capitalFlow : [])
      
      // 加载肥羊名称
      await loadSheepName(normalizedCode)
      setLastUpdated(new Date())
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

  // 搜索肥羊（支持中文和首字母）
  const handleSearch = async (value: string) => {
    const trimmedValue = value?.trim() || ''
    
    // 如果输入为空，清空选项
    if (trimmedValue.length === 0) {
      setSearchOptions([])
      return
    }

    // 至少输入1个字符才搜索
    if (trimmedValue.length < 1) {
      return
    }

    setSearchLoading(true)
    try {
      const sheep = await sheepApi.searchSheeps(trimmedValue)
      
      // 确保sheep是数组
      if (Array.isArray(sheep) && sheep.length > 0) {
        const options = sheep.map((stock: any) => {
          // 确保名称和代码存在
          const code = stock.code || stock.sheep_code || ''
          const name = stock.name || stock.sheep_name || code
          
          return {
            value: `${code} ${name}`,
            label: (
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span>
                  <Tag color="blue" style={{ marginRight: 8 }}>{code}</Tag>
                  <span style={{ fontWeight: 500 }}>{name}</span>
                </span>
                {(stock.sector || stock.industry) && (
                  <Tag color="cyan" style={{ fontSize: 11 }}>{stock.sector || stock.industry}</Tag>
                )}
              </div>
            ),
            code: code,
            name: name
          }
        })
        setSearchOptions(options)
      } else {
        setSearchOptions([])
      }
    } catch (error: any) {
      console.error('搜索失败:', error)
      setSearchOptions([])
      // 只在非取消请求的情况下显示错误
      if (error?.name !== 'CanceledError') {
        message.error('搜索失败，请稍后重试')
      }
    } finally {
      setSearchLoading(false)
    }
  }

  // 选择肥羊
  const handleSheepSelect = async (value: string, option: any) => {
    const stockCode = option.code || value.split(' ')[0]
    const stockName = option.name || value.split(' ')[1] || stockCode
    setSearchValue('')
    setSearchOptions([])
    setSelectedSheep(stockCode)
    setSelectedSheepName(stockName)
    // loadSheepData会在useEffect中自动调用，这里不需要手动调用
    // 但如果是交易时段，会自动刷新最新数据
  }

  // 渲染分析提示词
  const renderAnalyzePrompt = async (template: string, sheepName: string, sheepCode: string, klineData: SheepDailyData[], capitalFlowData: CapitalFlowData[]): Promise<string> => {
    try {
      // 获取当前日期
      const date = new Date().toISOString().split('T')[0]
      
      // 获取板块信息（从热门肥羊API获取）
      let sectors = 'N/A'
      try {
        const hotSheeps = await hotApi.getHotSheeps()
        if (hotSheeps && hotSheeps.length > 0) {
          const sheepInfo = hotSheeps.find(s => s.sheep_code === sheepCode)
          if (sheepInfo && sheepInfo.sectors && sheepInfo.sectors.length > 0) {
            sectors = sheepInfo.sectors.join(', ')
          }
        }
      } catch (error) {
        console.warn('获取板块信息失败:', error)
      }
      
      // 格式化K线数据（最近10天）
      const klineSummary = klineData.slice(-10).map(item => ({
        日期: item.trade_date,
        收盘价: item.close_price,
        涨跌: item.close_price && item.open_price ? (item.close_price - item.open_price).toFixed(2) : 'N/A',
        成交量: item.volume || 'N/A',
        MA5: item.ma5 || 'N/A',
        MA20: item.ma20 || 'N/A',
      }))
      
      // 格式化资金流向数据（最近10天汇总）
      const recentFlows = capitalFlowData.slice(-10)
      const moneyFlowSummary = recentFlows.length > 0 ? {
        最近10天主力净流入: recentFlows.reduce((sum, item) => sum + (item.main_net_inflow || 0), 0),
        最近10天超大单流入: recentFlows.reduce((sum, item) => sum + (item.super_large_inflow || 0), 0),
        最近10天大单流入: recentFlows.reduce((sum, item) => sum + (item.large_inflow || 0), 0),
      } : {}
      
      const currentPrice = klineData.length > 0 ? klineData[klineData.length - 1].close_price : 'N/A'
      const changePct = klineData.length > 0 && klineData[klineData.length - 1].change_pct !== null 
        ? klineData[klineData.length - 1].change_pct 
        : 'N/A'
      const volume = klineData.length > 0 ? klineData[klineData.length - 1].volume : 'N/A'
      
      const dataStr = `
当前价格：${currentPrice}
涨跌幅：${changePct}%
成交量：${volume}
K线数据（最近10天）：${JSON.stringify(klineSummary, null, 2)}
资金流向（最近10天汇总）：${JSON.stringify(moneyFlowSummary, null, 2)}
`
      
      // 替换变量
      return template
        .replace(/{date}/g, date)
        .replace(/{sheep_name}/g, sheepName || sheepCode || 'N/A')
        .replace(/{sectors}/g, sectors)
        .replace(/{data}/g, dataStr)
    } catch (error) {
      console.error('渲染提示词失败:', error)
      return template // 如果渲染失败，返回原始模板
    }
  }

  // AI分析肥羊
  const handleAIAnalyze = async (modelName?: string, customPrompt?: string) => {
    if (!selectedSheep) {
      message.warning('请先选择一只肥羊')
      return
    }
    
    // 如果没有传入模型名称，需要先检查可用的模型
    if (!modelName) {
      try {
        const response = await aiApi.getActiveAIModels()
        // 过滤出有API Key的模型
        const modelsWithApiKey = response.models.filter(model => model.api_key && model.api_key.trim() !== '')
        
        if (modelsWithApiKey.length === 0) {
          message.error('未配置API Key，请前往设置-AI应用设置中配置模型API Key和提示词')
          return
        } else if (modelsWithApiKey.length === 1) {
          // 只有一个模型，获取提示词并显示编辑对话框
          modelName = modelsWithApiKey[0].model_name
          setSelectedModelName(modelName)
          try {
            const response = await aiApi.getPrompts()
            const prompt = response.prompts.analyze || ''
            // 渲染提示词（替换变量）
            // 简化：不加载K线数据用于AI分析提示词（优化性能）
            const rendered = await renderAnalyzePrompt(prompt, selectedSheepName, selectedSheep, dailyData, capitalFlowData)
            setPromptText(prompt)
            setRenderedPromptText(rendered)
            setPromptEditModalVisible(true)
            setPendingAction('analyze')
            return
          } catch (error) {
            console.error('获取提示词失败:', error)
            message.error('获取提示词失败')
            return
          }
        } else {
          // 多个模型，弹出选择框
          setPendingAction('analyze')
          setModelSelectModalVisible(true)
          return
        }
      } catch (error) {
        console.error('加载模型列表失败:', error)
        message.error('加载模型列表失败')
        return
      }
    }
    
    // 执行AI分析
    setAiAnalyzeModalVisible(true)
    setAiAnalyzeLoading(true)
    setAiAnalyzeResult('')
    try {
      const result = await aiApi.analyzeSheep(selectedSheep, modelName, customPrompt)
      setAiAnalyzeResult(result.analysis)
    } catch (error: any) {
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      if (errorMsg.includes('API Key未配置')) {
        message.warning('API Key未配置，请前往设置-AI应用设置中配置模型API Key和提示词')
      } else {
        message.error(`AI分析失败: ${errorMsg}`)
      }
      setAiAnalyzeResult(`错误: ${errorMsg}`)
    } finally {
      setAiAnalyzeLoading(false)
    }
  }
  
  // 处理模型选择
  const handleModelSelect = async (modelName: string) => {
    setModelSelectModalVisible(false)
    setSelectedModelName(modelName)
    
    if (pendingAction === 'analyze') {
      // 获取提示词模板
      try {
        const response = await aiApi.getPrompts()
        const prompt = response.prompts.analyze || ''
        // 渲染提示词（替换变量）
        // 简化：不加载K线数据用于AI分析提示词（优化性能）
        const rendered = await renderAnalyzePrompt(prompt, selectedSheepName, selectedSheep, [], [])
        setPromptText(prompt)
        setRenderedPromptText(rendered)
        setPromptEditModalVisible(true)
      } catch (error) {
        console.error('获取提示词失败:', error)
        message.error('获取提示词失败')
      }
    }
  }
  
  // 处理提示词确认
  const handlePromptConfirm = async () => {
    setPromptEditModalVisible(false)
    if (pendingAction === 'analyze') {
      setPendingAction(null)
      await handleAIAnalyze(selectedModelName, promptText)
    }
  }

  const getKLineOption = () => {
    if (!dailyData || dailyData.length === 0) {
      return null
    }
    
    const dates = dailyData.map(d => d.trade_date)
    const kData = dailyData.map(d => [d.open_price, d.close_price, d.low_price, d.high_price])
    const volumes = dailyData.map(d => d.volume || 0)
    
    // 合并资金流数据：按日期匹配
    const mainFlowMap = new Map<string, number>()
    if (capitalFlowData && capitalFlowData.length > 0) {
      capitalFlowData.forEach((cf: CapitalFlowData) => {
        if (cf.trade_date) {
          mainFlowMap.set(cf.trade_date, (cf.main_net_inflow || 0) / 10000) // 转换为亿元
        }
      })
    }
    const mainFlowData = dates.map(date => mainFlowMap.get(date) || 0)
    const hasCapitalFlow = capitalFlowData && capitalFlowData.length > 0

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
          const dataIndex = params[0].dataIndex
          let result = `<div style="margin-bottom: 4px;"><strong>${date}</strong></div>`
          
          params.forEach((param: any) => {
            if (param.seriesName === 'K线') {
              const data = param.data as number[]
              if (data && data.length === 4) {
                const [open, close, low, high] = data
                const stockData = dailyData[dataIndex]
                
                // 计算涨跌幅
                let changePct = stockData?.change_pct
                if (changePct === undefined || changePct === null) {
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
            } else if (param.seriesName === '主力净流入') {
              const value = param.value
              if (value !== undefined && value !== null) {
                const color = value >= 0 ? '#ef5350' : '#26a69a'
                result += `<div style="margin: 4px 0;"><span style="color: #666;">主力净流入：</span><span style="color: ${color}; font-weight: bold;">${value >= 0 ? '+' : ''}${value.toFixed(2)}亿元</span></div>`
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
        data: hasCapitalFlow 
          ? ['K线', 'MA5', 'MA10', 'MA20', 'MA30', 'MA60', '成交量', '主力净流入']
          : ['K线', 'MA5', 'MA10', 'MA20', 'MA30', 'MA60', '成交量'],
        top: 30,
      },
      grid: [
        { left: '10%', right: '8%', top: '10%', height: '45%' },
        { left: '10%', right: '8%', top: '57%', height: '12%' },
        ...(hasCapitalFlow ? [{ left: '10%', right: '8%', top: '72%', height: '15%' }] : []),
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
        ...(hasCapitalFlow ? [{
          type: 'category',
          gridIndex: 2,
          data: dates,
          scale: true,
          boundaryGap: false,
          axisLine: { onZero: false },
          splitLine: { show: false },
          min: 'dataMin',
          max: 'dataMax',
        }] : []),
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
        ...(hasCapitalFlow ? [{
          scale: true,
          gridIndex: 2,
          splitNumber: 2,
          axisLabel: { 
            formatter: (value: number) => `${value.toFixed(1)}亿`,
            fontSize: 10
          },
          splitLine: { show: false },
        }] : []),
      ],
      dataZoom: [
        {
          type: 'inside',
          xAxisIndex: hasCapitalFlow ? [0, 1, 2] : [0, 1],
          start: dailyData.length > 30 ? ((dailyData.length - 30) / dailyData.length * 100) : 0,
          end: 100,
        },
        {
          show: true,
          xAxisIndex: hasCapitalFlow ? [0, 1, 2] : [0, 1],
          type: 'slider',
          top: hasCapitalFlow ? '92%' : '90%',
          start: dailyData.length > 30 ? ((dailyData.length - 30) / dailyData.length * 100) : 0,
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
        ...(hasCapitalFlow ? [{
          name: '主力净流入',
          type: 'bar',
          xAxisIndex: 2,
          yAxisIndex: 2,
          data: mainFlowData.map(v => ({
            value: v,
            itemStyle: { color: v >= 0 ? '#ef5350' : '#26a69a' }
          })),
          barWidth: '60%',
        }] : []),
      ],
    }
  }

  // getCapitalFlowOption已移除，资金流向已整合到K线图中

  return (
    <div>
      <Card style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', gap: 16, alignItems: 'center', flexWrap: 'wrap' }}>
          <span style={{ fontSize: 16, fontWeight: 'bold' }}>标的搜索：</span>
          <AutoComplete
            style={{ flex: isMobile ? 1 : 'none', minWidth: isMobile ? 'auto' : 300, maxWidth: 500 }}
            options={searchOptions}
            onSearch={handleSearch}
            onSelect={handleSheepSelect}
            value={searchValue}
            onChange={setSearchValue}
            placeholder="输入肥羊代码、中文名称或首字母拼音（如：688072、平安、PA）"
            allowClear
            notFoundContent={searchLoading ? <Spin size="small" /> : (searchValue ? '未找到匹配的肥羊' : null)}
            filterOption={false}
            defaultActiveFirstOption={false}
          >
            <Input
              prefix={searchLoading ? <Spin size="small" /> : <SearchOutlined />}
              size="large"
              allowClear
              onPressEnter={async (e) => {
                const value = (e.target as HTMLInputElement).value.trim()
                if (value) {
                  // 如果是纯数字代码，直接设置selectedSheep，useEffect会自动加载
                  const normalized = normalizeSheepCode(value)
                  if (normalized.match(/^\d{6}$/)) {
                    setSelectedSheep(normalized)
                  } else {
                    // 否则触发搜索
                    await handleSearch(value)
                  }
                }
              }}
            />
          </AutoComplete>
        {selectedSheepName && (
          <div style={{ fontSize: 14, color: '#666', display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap' }}>
            <div style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
              当前标的：<span style={{ fontWeight: 'bold', color: '#1890ff' }}>{selectedSheepName} ({selectedSheep})</span>
            </div>
            {lastUpdated && (
              <div style={{ fontSize: 12, color: '#999', whiteSpace: 'nowrap' }}>
                最后更新：{lastUpdated.toLocaleTimeString()}
              </div>
            )}
          </div>
        )}
        </div>
      </Card>

      {loading ? (
        <div style={{ textAlign: 'center', padding: 50 }}>
          <Spin size="large" />
        </div>
      ) : selectedSheep && dailyData.length > 0 ? (
        <>
          <Card 
            style={{ marginBottom: 24 }}
            title={
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
                <span>
                  {selectedSheepName ? `${selectedSheepName} (${selectedSheep})` : selectedSheep} - K线图
                </span>
                <Space>
                  <Button
                    type="primary"
                    icon={<BulbOutlined />}
                    onClick={() => handleAIAnalyze()}
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
                height: isMobile ? '300px' : '600px', 
                width: '100%' 
              }}
            />
          </Card>
          {/* 深度分析：走势预判 + 形态识别 + 止盈止损 */}
          <DeepAnalysis 
            sheepCode={selectedSheep} 
            sheepName={selectedSheepName}
          />
        </>
      ) : (
        <Card
          title={
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
              <span>标的分析</span>
              <Space>
                <Button
                  type="primary"
                  icon={<BulbOutlined />}
                  onClick={() => handleAIAnalyze()}
                  disabled={!selectedSheep}
                  title={!selectedSheep ? '请先选择一只肥羊' : ''}
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
            <span>AI肥羊分析 - {selectedSheepName || selectedSheep}</span>
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

      {/* 模型选择Modal */}
      <Modal
        title="选择AI模型"
        open={modelSelectModalVisible}
        onCancel={() => {
          setModelSelectModalVisible(false)
          setPendingAction(null)
        }}
        footer={null}
      >
        <ModelSelectModal
          onSelect={handleModelSelect}
          onCancel={() => {
            setModelSelectModalVisible(false)
            setPendingAction(null)
          }}
        />
      </Modal>

      {/* 提示词编辑Modal */}
      <Modal
        title="编辑提示词"
        open={promptEditModalVisible}
        onOk={handlePromptConfirm}
        onCancel={() => {
          setPromptEditModalVisible(false)
          setPendingAction(null)
          setPromptText('')
        }}
        okText="确认并分析"
        cancelText="取消"
        width={800}
      >
        <div>
          <p style={{ marginBottom: 8, color: '#666' }}>以下是渲染后的提示词（变量已替换），您可以编辑：</p>
          <TextArea
            value={renderedPromptText || promptText}
            onChange={(e) => {
              setRenderedPromptText(e.target.value)
              setPromptText(e.target.value) // 同时更新原始提示词，以便提交时使用
            }}
            rows={15}
            placeholder="请输入提示词..."
            style={{ fontFamily: 'monospace', fontSize: '12px' }}
          />
        </div>
      </Modal>
    </div>
  )
}

// 模型选择组件
const ModelSelectModal: React.FC<{
  onSelect: (modelName: string) => void
  onCancel: () => void
}> = ({ onSelect, onCancel }) => {
  const [models, setModels] = useState<Array<{ id: number; model_name: string; model_display_name: string; api_key: string }>>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const loadModels = async () => {
      try {
        const response = await aiApi.getActiveAIModels()
        // 过滤出有API Key的模型
        const modelsWithApiKey = response.models.filter(model => model.api_key && model.api_key.trim() !== '')
        setModels(modelsWithApiKey)
      } catch (error) {
        console.error('加载模型列表失败:', error)
        message.error('加载模型列表失败')
      } finally {
        setLoading(false)
      }
    }
    loadModels()
  }, [])

  if (loading) {
    return <Spin />
  }

  if (models.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: 20 }}>
        <p>未配置API Key，请前往设置-AI应用设置中配置模型API Key和提示词</p>
        <Button type="primary" onClick={onCancel}>确定</Button>
      </div>
    )
  }

  return (
    <div>
      <p style={{ marginBottom: 16 }}>请选择要使用的AI模型：</p>
      <Space direction="vertical" style={{ width: '100%' }}>
        {models.map(model => (
          <Button
            key={model.id}
            block
            onClick={() => onSelect(model.model_name)}
            style={{ textAlign: 'left', height: 'auto', padding: '12px' }}
          >
            <div>
              <div style={{ fontWeight: 'bold' }}>{model.model_display_name}</div>
              <div style={{ fontSize: '12px', color: '#999', marginTop: 4 }}>{model.model_name}</div>
            </div>
          </Button>
        ))}
      </Space>
      <div style={{ marginTop: 16, textAlign: 'right' }}>
        <Button onClick={onCancel}>取消</Button>
      </div>
    </div>
  )
}

export default Tab1
