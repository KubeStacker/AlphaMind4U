import React, { useState, useEffect } from 'react'
import { Card, Button, Table, Tag, Collapse, message, Spin, Modal, Space, Radio, Input } from 'antd'
const { TextArea } = Input
import { FireOutlined, RobotOutlined, BulbOutlined, DollarOutlined, DownOutlined, RightOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { hotApi, HotSheep, SectorStockByChange } from '../api/hot'
import { capitalInflowApi, CapitalInflowStock, sectorMoneyFlowApi, SectorMoneyFlowInfo, SectorMoneyFlowDailyData } from '../api/hot'
import { falconRadarApi, marketSentimentApi, smartMoneyMatrixApi, HottestSector, FalconRecommendations, MarketSentiment, SmartMoneyMatrix } from '../api/hot'
import { sheepApi, SheepDailyData, CapitalFlowData } from '../api/sheep'
import { aiApi } from '../api/ai'

const { Panel } = Collapse

// 辅助函数：标准化代码（移除SH/SZ前缀，只保留6位数字）
const normalizeCode = (code: string): string => {
  if (!code) return ''
  const normalized = code.trim().toUpperCase()
  // 移除SH/SZ前缀
  if (normalized.startsWith('SH') || normalized.startsWith('SZ')) {
    return normalized.substring(2)
  }
  return normalized
}

// 辅助函数：获取显示名称（如果名称是代码或为空，使用代码作为后备）
const getDisplayName = (name: string | undefined, code: string): string => {
  if (!name || !name.trim()) {
    return normalizeCode(code)
  }
  
  const nameTrimmed = name.trim()
  
  // 如果是6位纯数字，认为是代码
  if (/^\d{6}$/.test(nameTrimmed)) {
    return normalizeCode(code)
  }
  
  // 如果以SH或SZ开头后跟6位数字，也认为是代码
  if ((nameTrimmed.startsWith('SH') || nameTrimmed.startsWith('SZ')) && /^[A-Z]{2}\d{6}$/.test(nameTrimmed)) {
    return normalizeCode(code)
  }
  
  // 如果名称等于代码，也认为是无效名称
  if (normalizeCode(nameTrimmed) === normalizeCode(code)) {
    return normalizeCode(code)
  }
  
  // 否则返回名称
  return nameTrimmed
}

// 判断是否为交易时段（需要在组件外部定义，以便在组件初始化时使用）
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

const Tab2: React.FC = () => {
  const [modelSelectModalVisible, setModelSelectModalVisible] = useState(false)
  const [promptEditModalVisible, setPromptEditModalVisible] = useState(false)
  const [pendingAction, setPendingAction] = useState<'recommend' | 'analyze' | null>(null)
  const [selectedModelName, setSelectedModelName] = useState<string>('')
  const [promptText, setPromptText] = useState<string>('')
  const [renderedPromptText, setRenderedPromptText] = useState<string>('')
  const [promptType, setPromptType] = useState<'recommend' | 'analyze'>('analyze')
  const [hotSheeps, setHotSheeps] = useState<HotSheep[]>([])
  const [loading, setLoading] = useState(false)
  const [isMobile, setIsMobile] = useState(false)
  const [klineModalVisible, setKlineModalVisible] = useState(false)
  const [selectedSheepForKline, setSelectedSheepForKline] = useState<{ code: string; name: string } | null>(null)
  const [klineData, setKlineData] = useState<SheepDailyData[]>([])
  const [klineCapitalFlowData, setKlineCapitalFlowData] = useState<CapitalFlowData[]>([])
  const [klineLoading, setKlineLoading] = useState(false)
  const [aiRecommendModalVisible, setAiRecommendModalVisible] = useState(false)
  const [aiRecommendLoading, setAiRecommendLoading] = useState(false)
  const [aiRecommendResult, setAiRecommendResult] = useState<string>('')
  const [aiAnalyzeModalVisible, setAiAnalyzeModalVisible] = useState(false)
  const [aiAnalyzeLoading, setAiAnalyzeLoading] = useState(false)
  const [aiAnalyzeResult, setAiAnalyzeResult] = useState<string>('')
  const [selectedSheepForAnalyze, setSelectedSheepForAnalyze] = useState<{ code: string; name: string } | null>(null)
  const [capitalInflowDays, setCapitalInflowDays] = useState<number>(5)
  const [capitalInflowStocks, setCapitalInflowStocks] = useState<CapitalInflowStock[]>([])
  const [capitalInflowLoading, setCapitalInflowLoading] = useState(false)
  const [sectorInflowDays, setSectorInflowDays] = useState<number>(1)
  const [sectorInflowSectors, setSectorInflowSectors] = useState<SectorMoneyFlowInfo[]>([])
  const [sectorInflowLoading, setSectorInflowLoading] = useState(false)
  const [sectorInflowPage, setSectorInflowPage] = useState<number>(1)
  const [capitalInflowLoaded, setCapitalInflowLoaded] = useState<boolean>(false)  // 资金流入是否已加载数据
  const [sectorInflowLoaded, setSectorInflowLoaded] = useState<boolean>(false)  // 板块流入是否已加载数据
  const [sectorInflowMetadata, setSectorInflowMetadata] = useState<{ total_days_in_db: number, actual_days_used: number, requested_days: number, has_sufficient_data: boolean, warning?: string } | null>(null)
  const [sectorStocksModalVisible, setSectorStocksModalVisible] = useState<boolean>(false)
  const [selectedSectorForStocks, setSelectedSectorForStocks] = useState<{ name: string; stocks: SectorStockByChange[] } | null>(null)
  const [sectorStocksLoading, setSectorStocksLoading] = useState<boolean>(false)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  
  

  // 1/3/5天榜单相关状态
  const [rankingDays, setRankingDays] = useState<number>(1)
  const [topInflowStocks, setTopInflowStocks] = useState<CapitalInflowStock[]>([])
  const [topInflowStocksLoading, setTopInflowStocksLoading] = useState<boolean>(false)
  const [topInflowStocksPage, setTopInflowStocksPage] = useState<number>(1)
  const [topInflowSectors, setTopInflowSectors] = useState<SectorMoneyFlowInfo[]>([])
  const [topInflowSectorsLoading, setTopInflowSectorsLoading] = useState<boolean>(false)
  const [topInflowSectorsPage, setTopInflowSectorsPage] = useState<number>(1)
  
  // 板块K线相关状态
  const [selectedSectorForKline, setSelectedSectorForKline] = useState<{ name: string } | null>(null)
  const [sectorKlineData, setSectorKlineData] = useState<SectorMoneyFlowDailyData[]>([])
  const [sectorKlineLoading, setSectorKlineLoading] = useState<boolean>(false)
  
  // 猎鹰雷达新功能相关状态
  const [hottestSectors, setHottestSectors] = useState<HottestSector[]>([])
  const [hottestSectorsLoading, setHottestSectorsLoading] = useState<boolean>(false)
  const [falconRecommendations, setFalconRecommendations] = useState<FalconRecommendations | null>(null)
  const [falconRecommendationsLoading, setFalconRecommendationsLoading] = useState<boolean>(false)
  const [marketSentiment, setMarketSentiment] = useState<MarketSentiment | null>(null)
  const [marketSentimentLoading, setMarketSentimentLoading] = useState<boolean>(false)
  const [smartMoneyMatrix, setSmartMoneyMatrix] = useState<SmartMoneyMatrix | null>(null)
  const [smartMoneyMatrixLoading, setSmartMoneyMatrixLoading] = useState<boolean>(false)
  const [smartMoneyMatrixDays, setSmartMoneyMatrixDays] = useState<number>(1)
  const [eagleRadarExpanded, setEagleRadarExpanded] = useState<boolean>(false)
  
  // 雪球热度榜 - 板块聚合状态
  const [xueqiuExpandedSectors, setXueqiuExpandedSectors] = useState<Set<string>>(new Set())

  const loadData = async () => {
    setLoading(true)
    try {
      const sheep = await hotApi.getHotSheeps()
      setHotSheeps(sheep || [])
      setLastUpdated(new Date())
    } catch (error: any) {
      console.error('加载数据失败:', error)
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      message.error(`加载数据失败: ${errorMsg}`)
      setHotSheeps([])
    } finally {
      setLoading(false)
    }
  }


  const loadCapitalInflowData = async (days: number) => {
    setCapitalInflowLoading(true)
    try {
      const result = await capitalInflowApi.getRecommendations(days)
      setCapitalInflowStocks(result.stocks || [])
    } catch (error: any) {
      console.error('加载资金流入推荐失败:', error)
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      message.error(`加载资金流入推荐失败: ${errorMsg}`)
      setCapitalInflowStocks([])
    } finally {
      setCapitalInflowLoading(false)
    }
  }

  const loadSectorInflowData = async (days: number) => {
    setSectorInflowLoading(true)
    try {
      const result = await sectorMoneyFlowApi.getRecommendations(days, 30)
      setSectorInflowSectors(result.sectors || [])
      setSectorInflowMetadata(result.metadata || null)
      if (result.sectors && result.sectors.length === 0) {
        message.info('暂无板块资金流入数据，请稍后再试')
      } else if (result.metadata?.warning) {
        message.warning(result.metadata.warning)
      }
    } catch (error: any) {
      console.error('加载板块资金流入推荐失败:', error)
      // 处理不同类型的错误
      let errorMsg = '未知错误'
      if (error?.code === 'ECONNABORTED' || error?.message?.includes('timeout')) {
        errorMsg = '请求超时，请稍后重试'
      } else if (error?.code === 'ERR_NETWORK' || error?.message === 'Network Error') {
        errorMsg = '网络连接失败，请检查网络或稍后重试'
      } else if (error?.response?.data?.detail) {
        errorMsg = error.response.data.detail
      } else if (error?.message) {
        errorMsg = error.message
      }
      message.error(`加载板块资金流入推荐失败: ${errorMsg}`)
      setSectorInflowSectors([])
    } finally {
      setSectorInflowLoading(false)
    }
  }



  // 加载1/3/5天净流入Top100肥羊
  const loadTopInflowStocks = async (days: number) => {
    setTopInflowStocksLoading(true)
    try {
      const result = await capitalInflowApi.getTop(days, 100)
      setTopInflowStocks(result.stocks || [])
    } catch (error: any) {
      console.error('加载净流入Top肥羊失败:', error)
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      message.error(`加载净流入Top肥羊失败: ${errorMsg}`)
      setTopInflowStocks([])
    } finally {
      setTopInflowStocksLoading(false)
    }
  }

  // 加载1/3/5天净流入Top10板块（已聚类）
  const loadTopInflowSectors = async (days: number) => {
    setTopInflowSectorsLoading(true)
    try {
      const result = await sectorMoneyFlowApi.getRecommendations(days, 10)
      setTopInflowSectors(result.sectors || [])
    } catch (error: any) {
      console.error('加载净流入Top板块失败:', error)
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      message.error(`加载净流入Top板块失败: ${errorMsg}`)
      setTopInflowSectors([])
    } finally {
      setTopInflowSectorsLoading(false)
    }
  }


  // 加载板块K线数据（涨跌幅+主力流入）
  const handleSectorClick = async (sectorName: string) => {
    setSelectedSectorForKline({ name: sectorName })
    setKlineModalVisible(true)
    setSectorKlineLoading(true)
    try {
      const result = await sectorMoneyFlowApi.getMoneyFlowHistory(sectorName, 60)
      setSectorKlineData(result.data || [])
      // 板块K线使用不同的数据结构，需要特殊处理
      // 这里先设置数据，后续在getKLineOption中判断是否为板块
    } catch (error: any) {
      console.error('加载板块K线数据失败:', error)
      message.error('加载板块K线数据失败')
      setSectorKlineData([])
    } finally {
      setSectorKlineLoading(false)
    }
  }

  // 加载当日最热板块
  const loadHottestSectors = async () => {
    setHottestSectorsLoading(true)
    try {
      const result = await falconRadarApi.getHottestSectors(10)
      setHottestSectors(result.sectors || [])
    } catch (error: any) {
      console.error('加载当日最热板块失败:', error)
      message.error('加载当日最热板块失败')
      setHottestSectors([])
    } finally {
      setHottestSectorsLoading(false)
    }
  }

  // 加载猎鹰推荐
  const loadFalconRecommendations = async () => {
    setFalconRecommendationsLoading(true)
    try {
      const result = await falconRadarApi.getRecommendations()
      setFalconRecommendations(result)
      if (result.error) {
        message.warning(`猎鹰推荐加载失败: ${result.error}`)
      }
    } catch (error: any) {
      console.error('加载猎鹰推荐失败:', error)
      message.error('加载猎鹰推荐失败')
      setFalconRecommendations(null)
    } finally {
      setFalconRecommendationsLoading(false)
    }
  }

  // 加载市场情绪数据
  const loadMarketSentiment = async () => {
    setMarketSentimentLoading(true)
    try {
      const result = await marketSentimentApi.getMarketSentiment()
      setMarketSentiment(result)
    } catch (error: any) {
      console.error('加载市场情绪数据失败:', error)
      message.error('加载市场情绪数据失败')
      setMarketSentiment(null)
    } finally {
      setMarketSentimentLoading(false)
    }
  }

  // 加载智能资金矩阵
  const loadSmartMoneyMatrix = async (days: number = 1) => {
    setSmartMoneyMatrixLoading(true)
    try {
      const result = await smartMoneyMatrixApi.getSmartMoneyMatrix(days, 100)
      setSmartMoneyMatrix(result)
    } catch (error: any) {
      console.error('加载智能资金矩阵失败:', error)
      message.error('加载智能资金矩阵失败')
      setSmartMoneyMatrix(null)
    } finally {
      setSmartMoneyMatrixLoading(false)
    }
  }

  useEffect(() => {
    loadData()
    // 默认不加载，等用户展开折叠面板时再加载
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    if (capitalInflowLoaded) {
      loadCapitalInflowData(capitalInflowDays)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [capitalInflowDays])

  useEffect(() => {
    if (sectorInflowLoaded) {
      loadSectorInflowData(sectorInflowDays)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sectorInflowDays])


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

  // 交易时段每分钟自动刷新，非交易时段每小时刷新
  useEffect(() => {
    let interval: ReturnType<typeof setInterval> | null = null
    
    const refreshInterval = isTradingHours() ? 60000 : 3600000  // 交易时段1分钟，非交易时段1小时
    
    interval = setInterval(() => {
      loadData()
      // 同时刷新其他已展开的数据
      if (capitalInflowLoaded) loadCapitalInflowData(capitalInflowDays)
      if (sectorInflowLoaded) loadSectorInflowData(sectorInflowDays)
      loadTopInflowStocks(rankingDays)  // 刷新榜单数据
      loadTopInflowSectors(rankingDays)
    }, refreshInterval)
    
    return () => {
      if (interval) clearInterval(interval)
    }
  }, [capitalInflowLoaded, capitalInflowDays, sectorInflowLoaded, sectorInflowDays, rankingDays])

  // 当rankingDays变化时，重新加载榜单数据
  useEffect(() => {
    loadTopInflowStocks(rankingDays)
    loadTopInflowSectors(rankingDays)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rankingDays])


  // 检测移动端
  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768)
    }
    checkMobile()
    window.addEventListener('resize', checkMobile)
    return () => window.removeEventListener('resize', checkMobile)
  }, [])


  const handleAIRecommend = async (modelName?: string, customPrompt?: string) => {
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
            const prompt = response.prompts.recommend || ''
            // 渲染提示词（替换变量）
            const rendered = await renderRecommendPrompt(prompt, hotSheeps)
            setPromptText(prompt)
            setRenderedPromptText(rendered)
            setPromptType('recommend')
            setPromptEditModalVisible(true)
            setPendingAction('recommend')
            return
          } catch (error) {
            console.error('获取提示词失败:', error)
            message.error('获取提示词失败')
            return
          }
        } else {
          // 多个模型，弹出选择框
          setPendingAction('recommend')
          setModelSelectModalVisible(true)
          return
        }
      } catch (error) {
        console.error('加载模型列表失败:', error)
        message.error('加载模型列表失败')
        return
      }
    }
    
    // 执行AI推荐
    setAiRecommendModalVisible(true)
    setAiRecommendLoading(true)
    setAiRecommendResult('')
    try {
      const result = await aiApi.recommendSheeps(modelName, customPrompt)
      setAiRecommendResult(result.recommendation)
    } catch (error: any) {
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      if (errorMsg.includes('API Key未配置')) {
        message.warning('API Key未配置，请前往设置-AI应用设置中配置模型API Key和提示词')
      } else {
        message.error(`AI推荐失败: ${errorMsg}`)
      }
      setAiRecommendResult(`错误: ${errorMsg}`)
    } finally {
      setAiRecommendLoading(false)
    }
  }

  const handleAIAnalyze = async (stockCode: string, stockName: string, modelName?: string, customPrompt?: string) => {
    setSelectedSheepForAnalyze({ code: stockCode, name: stockName })
    
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
            // 渲染提示词（替换变量）- 需要获取肥羊数据
            const rendered = await renderAnalyzePromptForTab2(prompt, stockCode, stockName)
            setPromptText(prompt)
            setRenderedPromptText(rendered)
            setPromptType('analyze')
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
      const result = await aiApi.analyzeSheep(stockCode, modelName, customPrompt)
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
    
    // 获取提示词模板
    try {
        const response = await aiApi.getPrompts()
        if (pendingAction === 'recommend') {
          const prompt = response.prompts.recommend || ''
          // 渲染提示词（替换变量）
          const rendered = await renderRecommendPrompt(prompt, hotSheeps)
          setPromptText(prompt)
          setRenderedPromptText(rendered)
          setPromptType('recommend')
          setPromptEditModalVisible(true)
        } else if (pendingAction === 'analyze' && selectedSheepForAnalyze) {
          const prompt = response.prompts.analyze || ''
          // 渲染提示词（替换变量）
          const rendered = await renderAnalyzePromptForTab2(prompt, selectedSheepForAnalyze.code, selectedSheepForAnalyze.name)
          setPromptText(prompt)
          setRenderedPromptText(rendered)
          setPromptType('analyze')
          setPromptEditModalVisible(true)
        }
    } catch (error) {
      console.error('获取提示词失败:', error)
      message.error('获取提示词失败')
    }
  }
  
  // 渲染推荐提示词
  const renderRecommendPrompt = async (template: string, sheeps: HotSheep[]): Promise<string> => {
    try {
      // 获取当前日期
      const date = new Date().toISOString().split('T')[0]
      
      // 格式化热门肥羊数据
      const sheepData = sheeps.slice(0, 20).map(sheep => ({
        代码: sheep.sheep_code,
        名称: sheep.sheep_name,
        排名: sheep.rank,
        涨幅: sheep.change_pct,
        成交量: sheep.volume,
        来源: sheep.source
      }))
      
      const dataStr = `
热门肥羊列表：
${JSON.stringify(sheepData, null, 2)}
`
      
      // 替换变量
      return template
        .replace(/{date}/g, date)
        .replace(/{hot_sectors}/g, '暂无热门板块')
        .replace(/{data}/g, dataStr)
    } catch (error) {
      console.error('渲染推荐提示词失败:', error)
      return template // 如果渲染失败，返回原始模板
    }
  }
  
  // 渲染分析提示词（Tab2版本）
  const renderAnalyzePromptForTab2 = async (template: string, stockCode: string, stockName: string): Promise<string> => {
    try {
      // 获取当前日期
      const date = new Date().toISOString().split('T')[0]
      
      // 获取肥羊数据
      const [dailyData, capitalFlowData] = await Promise.all([
        sheepApi.getSheepDaily(stockCode).catch(() => []),
        sheepApi.getCapitalFlow(stockCode, 30).catch(() => [])
      ])
      
      // 获取板块信息（从热门肥羊中查找）
      const sheepInfo = hotSheeps.find(s => s.sheep_code === stockCode)
      const sectors = sheepInfo?.sectors || []
      const sectorsStr = sectors.length > 0 ? sectors.join(', ') : 'N/A'
      
      // 格式化K线数据（最近10天）
      const klineSummary = (dailyData as SheepDailyData[]).slice(-10).map(item => ({
        日期: item.trade_date,
        收盘价: item.close_price,
        涨跌: item.close_price && item.open_price ? (item.close_price - item.open_price).toFixed(2) : 'N/A',
        成交量: item.volume || 'N/A',
        MA5: item.ma5 || 'N/A',
        MA20: item.ma20 || 'N/A',
      }))
      
      // 格式化资金流向数据（最近10天汇总）
      const recentFlows = (capitalFlowData as CapitalFlowData[]).slice(-10)
      const moneyFlowSummary = recentFlows.length > 0 ? {
        最近10天主力净流入: recentFlows.reduce((sum: number, item: any) => sum + (item.main_net_inflow || 0), 0),
        最近10天超大单流入: recentFlows.reduce((sum: number, item: any) => sum + (item.super_large_inflow || 0), 0),
        最近10天大单流入: recentFlows.reduce((sum: number, item: any) => sum + (item.large_inflow || 0), 0),
      } : {}
      
      const currentPrice = (dailyData as SheepDailyData[]).length > 0 
        ? (dailyData as SheepDailyData[])[(dailyData as SheepDailyData[]).length - 1].close_price 
        : 'N/A'
      const changePct = (dailyData as SheepDailyData[]).length > 0 
        ? (dailyData as SheepDailyData[])[(dailyData as SheepDailyData[]).length - 1].change_pct 
        : 'N/A'
      const volume = (dailyData as SheepDailyData[]).length > 0 
        ? (dailyData as SheepDailyData[])[(dailyData as SheepDailyData[]).length - 1].volume 
        : 'N/A'
      
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
        .replace(/{sheep_name}/g, stockName || stockCode)
        .replace(/{sectors}/g, sectorsStr)
        .replace(/{data}/g, dataStr)
    } catch (error) {
      console.error('渲染分析提示词失败:', error)
      return template // 如果渲染失败，返回原始模板
    }
  }
  
  // 处理提示词确认
  const handlePromptConfirm = async () => {
    setPromptEditModalVisible(false)
    if (pendingAction === 'recommend') {
      setPendingAction(null)
      await handleAIRecommend(selectedModelName, promptText)
    } else if (pendingAction === 'analyze' && selectedSheepForAnalyze) {
      setPendingAction(null)
      await handleAIAnalyze(selectedSheepForAnalyze.code, selectedSheepForAnalyze.name, selectedModelName, promptText)
    }
  }

  const handleSectorStocksClick = async (sectorName: string) => {
    setSectorStocksModalVisible(true)
    setSectorStocksLoading(true)
    setSelectedSectorForStocks(null)
    try {
      const stocks = await hotApi.getSectorStocksByChange(sectorName, 10)
      setSelectedSectorForStocks({ name: sectorName, stocks })
    } catch (error: any) {
      console.error('加载板块涨幅前10概念股失败:', error)
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      message.error(`加载板块涨幅前10概念股失败: ${errorMsg}`)
      setSelectedSectorForStocks({ name: sectorName, stocks: [] })
    } finally {
      setSectorStocksLoading(false)
    }
  }


  const hotSheepsColumns = [
    {
      title: '排名',
      dataIndex: 'rank',
      key: 'rank',
      width: 80,
    },
    {
      title: '标的代码',
      dataIndex: 'sheep_code',
      key: 'sheep_code',
      width: 120,
    },
    {
      title: '标的名称',
      dataIndex: 'sheep_name',
      key: 'sheep_name',
      width: 150,
      render: (name: string, record: HotSheep) => {
        const displayName = getDisplayName(name, record.sheep_code)
        return (
          <Space>
            <span style={{ cursor: 'pointer', color: '#1890ff' }} 
                  onClick={() => handleSheepClick(record.sheep_code, displayName)}>
              {displayName}
            </span>
            <Button
              type="link"
              size="small"
              icon={<BulbOutlined />}
              onClick={(e) => {
                e.stopPropagation()
                handleAIAnalyze(record.sheep_code, displayName)
              }}
              title="AI分析"
            />
          </Space>
        )
      },
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

  // 雪球热度榜专用列定义（去掉数据来源、连续上榜天数和成交量列）
  const xueqiuSheepsColumns = hotSheepsColumns.filter(
    col => col.key !== 'source' && col.key !== 'volume' && col.key !== 'consecutive_days'
  )

  const capitalInflowColumns = [
    {
      title: '标的代码',
      dataIndex: 'sheep_code',
      key: 'sheep_code',
      width: 100,
    },
    {
      title: '标的名称',
      dataIndex: 'sheep_name',
      key: 'sheep_name',
      width: 120,
      render: (name: string, record: CapitalInflowStock) => {
        const displayName = getDisplayName(name, record.sheep_code)
        return (
          <span style={{ cursor: 'pointer', color: '#1890ff' }} 
                onClick={() => handleSheepClick(record.sheep_code, displayName)}>
            {displayName}
          </span>
        )
      },
    },
    {
      title: '连续流入天数',
      dataIndex: 'continuous_days',
      key: 'continuous_days',
      width: 120,
      render: (days: number) => <Tag color="green">{days} 天</Tag>,
    },
    {
      title: '总流入（亿元）',
      dataIndex: 'total_inflow',
      key: 'total_inflow',
      width: 130,
      render: (inflow: number) => (
        <span style={{ fontWeight: 'bold', color: '#ef5350' }}>
          +{inflow.toFixed(2)}
        </span>
      ),
    },
    {
      title: '单日最大流入（亿元）',
      dataIndex: 'max_single_day_inflow',
      key: 'max_single_day_inflow',
      width: 150,
      render: (inflow: number) => (
        <span style={{ color: '#1890ff' }}>
          {inflow.toFixed(2)}
        </span>
      ),
    },
    {
      title: '日均流入（亿元）',
      dataIndex: 'avg_daily_inflow',
      key: 'avg_daily_inflow',
      width: 130,
      render: (inflow: number) => (
        <span style={{ color: '#666' }}>
          {inflow.toFixed(2)}
        </span>
      ),
    },
  ]

  const handleSheepClick = async (stockCode: string, stockName: string) => {
    setSelectedSheepForKline({ code: stockCode, name: stockName })
    setSelectedSectorForKline(null)  // 清除板块选择
    setKlineModalVisible(true)
    setKlineLoading(true)
    setSectorKlineLoading(false)
    try {
      const normalizedCode = normalizeCode(stockCode)
      const [data, capitalFlow] = await Promise.all([
        sheepApi.getSheepDaily(normalizedCode),
        sheepApi.getCapitalFlow(normalizedCode, 60).catch(() => [])
      ])
      setKlineData(data || [])
      setKlineCapitalFlowData(Array.isArray(capitalFlow) ? capitalFlow : [])
      setSectorKlineData([])  // 清除板块数据
    } catch (error) {
      console.error('加载K线数据失败:', error)
      message.error('加载K线数据失败')
      setKlineData([])
      setKlineCapitalFlowData([])
    } finally {
      setKlineLoading(false)
    }
  }

  // K线图配置（参考Tab1的实现）
  const getKLineOption = () => {
    if (!klineData || klineData.length === 0) {
      return null
    }
    
    const dates = klineData.map(d => d.trade_date)
    const kData = klineData.map(d => [d.open_price, d.close_price, d.low_price, d.high_price])
    const volumes = klineData.map(d => d.volume || 0)
    
    // 合并资金流数据：按日期匹配
    const mainFlowMap = new Map<string, number>()
    if (klineCapitalFlowData && klineCapitalFlowData.length > 0) {
      klineCapitalFlowData.forEach((cf: CapitalFlowData) => {
        if (cf.trade_date) {
          mainFlowMap.set(cf.trade_date, (cf.main_net_inflow || 0) / 10000) // 转换为亿元
        }
      })
    }
    const mainFlowData = dates.map(date => mainFlowMap.get(date) || 0)
    const hasCapitalFlow = klineCapitalFlowData && klineCapitalFlowData.length > 0

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
                const stockData = klineData[dataIndex]
                
                let changePct = stockData?.change_pct
                if (changePct === undefined || changePct === null) {
                  if (dataIndex > 0 && klineData[dataIndex - 1]) {
                    const prevClose = klineData[dataIndex - 1].close_price
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
          start: klineData.length > 30 ? ((klineData.length - 30) / klineData.length * 100) : 0,
          end: 100,
        },
        {
          show: true,
          xAxisIndex: hasCapitalFlow ? [0, 1, 2] : [0, 1],
          type: 'slider',
          top: hasCapitalFlow ? '92%' : '90%',
          start: klineData.length > 30 ? ((klineData.length - 30) / klineData.length * 100) : 0,
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

  const xueqiuSheeps = hotSheeps.filter(s => s.source === 'xueqiu').slice(0, 100)

  // 雪球热度榜 - 聚合最热门板块（最多5个）
  const aggregatedHotSectors = React.useMemo(() => {
    const sectorMap = new Map<string, { stocks: HotSheep[], totalRank: number }>()
    
    // 统计每个板块的热门股
    xueqiuSheeps.forEach(sheep => {
      if (sheep.sectors && sheep.sectors.length > 0) {
        sheep.sectors.forEach(sector => {
          if (!sectorMap.has(sector)) {
            sectorMap.set(sector, { stocks: [], totalRank: 0 })
          }
          const sectorData = sectorMap.get(sector)!
          sectorData.stocks.push(sheep)
          sectorData.totalRank += sheep.rank // 使用排名总和作为热度指标（越小越热）
        })
      }
    })
    
    // 转换为数组并排序（按股票数量降序，同样数量按总排名升序）
    const sectors = Array.from(sectorMap.entries()).map(([name, data]) => ({
      name,
      stocks: data.stocks.sort((a, b) => a.rank - b.rank), // 按排名升序
      count: data.stocks.length,
      avgRank: data.totalRank / data.stocks.length
    }))
    
    // 排序：优先按股票数量降序，同样数量按平均排名升序
    sectors.sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count
      return a.avgRank - b.avgRank
    })
    
    return sectors.slice(0, 5) // 最多5个板块
  }, [xueqiuSheeps])

  // 获取板块颜色
  const getSectorColor = (index: number): string => {
    if (index === 0) return '#ff4d4f' // 红色 - 最热
    if (index === 1) return '#fa8c16' // 橙色 - 第二
    return '#1890ff' // 浅蓝色 - 其他
  }

  // 切换板块展开状态
  const toggleSectorExpanded = (sectorName: string) => {
    setXueqiuExpandedSectors(prev => {
      const newSet = new Set(prev)
      if (newSet.has(sectorName)) {
        newSet.delete(sectorName)
      } else {
        newSet.add(sectorName)
      }
      return newSet
    })
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Space>
          <Button type="primary" onClick={loadData} loading={loading}>
            手动刷新
          </Button>
          {lastUpdated && (
            <span style={{ fontSize: 12, color: '#999' }}>
              最后更新：{lastUpdated.toLocaleTimeString()} {isTradingHours() && '(交易时段自动刷新中)'}
            </span>
          )}
        </Space>
        <Space>
          <Button 
            type="primary" 
            danger
            icon={<DollarOutlined />} 
            onClick={() => handleAIRecommend()}
            loading={aiRecommendLoading}
          >
            AI大盘推荐
          </Button>
        </Space>
      </div>


      {/* 猎鹰雷达核心看板 */}
      <Collapse 
        defaultActiveKey={eagleRadarExpanded ? ['falconRadar'] : []}
        style={{ marginBottom: 24 }}
        onChange={(keys: string | string[]) => {
          const activeKeys = Array.isArray(keys) ? keys : [keys]
          const isExpanded = activeKeys.includes('falconRadar')
          setEagleRadarExpanded(isExpanded)
          if (isExpanded) {
            loadHottestSectors()
            loadFalconRecommendations()
            loadMarketSentiment()
            loadSmartMoneyMatrix(smartMoneyMatrixDays)
          }
        }}
      >
        <Panel
          header={
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
              <span>
                <FireOutlined style={{ marginRight: 8, color: '#ff4d4f' }} />
                猎鹰雷达（Falcon Radar）
              </span>
              <Tag color={eagleRadarExpanded ? 'green' : 'default'}>
                {eagleRadarExpanded ? '已展开' : '点击展开'}
              </Tag>
            </div>
          }
          key="falconRadar"
        >
          <Spin spinning={hottestSectorsLoading || falconRecommendationsLoading || marketSentimentLoading || smartMoneyMatrixLoading}>
            {/* Module 1: 当日最热 + 猎鹰推荐 */}
            <Card 
              title="Module 1: 猎鹰雷达核心看板" 
              style={{ marginBottom: 16 }}
              extra={
                <Space>
                  <Button size="small" onClick={() => { loadHottestSectors(); loadFalconRecommendations(); }}>
                    刷新
                  </Button>
                </Space>
              }
            >
              <div style={{ display: 'grid', gridTemplateColumns: isMobile ? '1fr' : '1fr 1fr', gap: 16 }}>
                {/* 当日最热 */}
                <Card size="small" title="当日最热（市场焦点）">
                  {hottestSectors.length === 0 ? (
                    <div style={{ textAlign: 'center', padding: 20, color: '#999' }}>暂无数据</div>
                  ) : (
                    <Table
                      size="small"
                      dataSource={hottestSectors}
                      columns={[
                        { 
                          title: '板块', 
                          dataIndex: 'sector_name', 
                          key: 'sector_name',
                          render: (name: string, record: HottestSector) => (
                            <span 
                              style={{ cursor: 'pointer', color: '#1890ff' }} 
                              onClick={() => handleSectorClick(name)}
                              title="点击查看K线图"
                            >
                              {record.display_name || name}
                              {record.aggregated_count && record.aggregated_count > 0 ? (
                                <span style={{ color: '#999', fontSize: 10, marginLeft: 4 }}>
                                  (+{record.aggregated_count})
                                </span>
                              ) : null}
                            </span>
                          )
                        },
                        { 
                          title: '净流入(万)', 
                          dataIndex: 'main_net_inflow', 
                          key: 'main_net_inflow',
                          render: (val: number) => (val / 10000).toFixed(2),
                          sorter: (a, b) => a.main_net_inflow - b.main_net_inflow
                        },
                        { 
                          title: '涨幅%', 
                          dataIndex: 'change_pct', 
                          key: 'change_pct',
                          render: (val: number) => <span style={{ color: val >= 0 ? '#ef5350' : '#26a69a' }}>{val.toFixed(2)}</span>
                        },
                        { title: '涨停数', dataIndex: 'limit_up_count', key: 'limit_up_count' },
                        { title: 'RPS20', dataIndex: 'sector_rps_20', key: 'sector_rps_20', render: (val: number) => val.toFixed(1) }
                      ]}
                      rowKey="sector_name"
                      pagination={false}
                      scroll={{ y: 300 }}
                    />
                  )}
                </Card>
                
                {/* 猎鹰推荐 */}
                <Card size="small" title="猎鹰推荐（量化挖掘）">
                  {falconRecommendations ? (
                    <div>
                      <div style={{ marginBottom: 12 }}>
                        <Tag color="red">主线首阴</Tag>
                        {falconRecommendations.leader_pullback.length > 0 ? (
                          <Table
                            size="small"
                            dataSource={falconRecommendations.leader_pullback}
                            columns={[
                              { 
                                title: '名称', 
                                dataIndex: 'sheep_name', 
                                key: 'sheep_name',
                                render: (name: string, record: any) => (
                                  <span 
                                    style={{ cursor: 'pointer', color: '#1890ff' }} 
                                    onClick={() => handleSheepClick(record.sheep_code, name)}
                                    title="点击查看K线图"
                                  >
                                    {name}
                                  </span>
                                )
                              },
                              { 
                                title: '跌幅%', 
                                dataIndex: 'change_pct', 
                                key: 'change_pct',
                                render: (val: number) => <span style={{ color: '#26a69a' }}>{val.toFixed(2)}</span>
                              },
                              { title: '量比', dataIndex: 'volume_ratio', key: 'volume_ratio', render: (val: number) => val.toFixed(2) },
                              { title: '推荐理由', dataIndex: 'reason', key: 'reason', ellipsis: true }
                            ]}
                            rowKey="sheep_code"
                            pagination={false}
                            scroll={{ y: 200 }}
                          />
                        ) : (
                          <div style={{ padding: 10, color: '#999' }}>暂无推荐</div>
                        )}
                      </div>
                      <div style={{ marginBottom: 12 }}>
                        <Tag color="orange">资金背离</Tag>
                        {falconRecommendations.money_divergence.length > 0 ? (
                          <Table
                            size="small"
                            dataSource={falconRecommendations.money_divergence}
                            columns={[
                              { 
                                title: '名称', 
                                dataIndex: 'sheep_name', 
                                key: 'sheep_name',
                                render: (name: string, record: any) => (
                                  <span 
                                    style={{ cursor: 'pointer', color: '#1890ff' }} 
                                    onClick={() => handleSheepClick(record.sheep_code, name)}
                                    title="点击查看K线图"
                                  >
                                    {name}
                                  </span>
                                )
                              },
                              { title: '净流入(亿)', dataIndex: 'total_inflow', key: 'total_inflow', render: (val: number) => val.toFixed(2) },
                              { title: '振幅%', dataIndex: 'amplitude', key: 'amplitude', render: (val: number) => val.toFixed(2) },
                              { title: '推荐理由', dataIndex: 'reason', key: 'reason', ellipsis: true }
                            ]}
                            rowKey="sheep_code"
                            pagination={false}
                            scroll={{ y: 200 }}
                          />
                        ) : (
                          <div style={{ padding: 10, color: '#999' }}>暂无推荐</div>
                        )}
                      </div>
                      <div>
                        <Tag color="blue">平台突破</Tag>
                        {falconRecommendations.box_breakout.length > 0 ? (
                          <Table
                            size="small"
                            dataSource={falconRecommendations.box_breakout}
                            columns={[
                              { 
                                title: '名称', 
                                dataIndex: 'sheep_name', 
                                key: 'sheep_name',
                                render: (name: string, record: any) => (
                                  <span 
                                    style={{ cursor: 'pointer', color: '#1890ff' }} 
                                    onClick={() => handleSheepClick(record.sheep_code, name)}
                                    title="点击查看K线图"
                                  >
                                    {name}
                                  </span>
                                )
                              },
                              { title: '量比', dataIndex: 'volume_ratio', key: 'volume_ratio', render: (val: number) => val.toFixed(2) },
                              { title: '波动率%', dataIndex: 'volatility', key: 'volatility', render: (val: number) => val.toFixed(2) },
                              { title: '推荐理由', dataIndex: 'reason', key: 'reason', ellipsis: true }
                            ]}
                            rowKey="sheep_code"
                            pagination={false}
                            scroll={{ y: 200 }}
                          />
                        ) : (
                          <div style={{ padding: 10, color: '#999' }}>暂无推荐</div>
                        )}
                      </div>
                    </div>
                  ) : (
                    <div style={{ textAlign: 'center', padding: 20, color: '#999' }}>加载中...</div>
                  )}
                </Card>
              </div>
            </Card>

            {/* Module 2: 市场情绪仪表盘 */}
            <Card 
              title="Module 2: 市场情绪仪表盘" 
              style={{ marginBottom: 16 }}
              extra={
                <Button size="small" onClick={loadMarketSentiment}>刷新</Button>
              }
            >
              {marketSentiment ? (
                <div style={{ display: 'grid', gridTemplateColumns: isMobile ? '1fr' : '1fr 1fr 1fr', gap: 16 }}>
                  <Card size="small" title="赚钱效应">
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: 24, fontWeight: 'bold', color: marketSentiment.profit_effect.value >= 0 ? '#ef5350' : '#26a69a' }}>
                        {(marketSentiment.profit_effect.value * 100).toFixed(2)}%
                      </div>
                      <div style={{ marginTop: 8 }}>
                        <Tag color={marketSentiment.profit_effect.level === 'extreme_hot' ? 'red' : marketSentiment.profit_effect.level === 'extreme_cold' ? 'blue' : 'default'}>
                          {marketSentiment.profit_effect.message}
                        </Tag>
                      </div>
                      <div style={{ marginTop: 8, fontSize: 12, color: '#999' }}>
                        上涨: {marketSentiment.profit_effect.up_count} | 下跌: {marketSentiment.profit_effect.down_count}
                      </div>
                    </div>
                  </Card>
                  <Card size="small" title="连板高度">
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: 24, fontWeight: 'bold', color: '#ff9800' }}>
                        {marketSentiment.consecutive_limit_height}连板
                      </div>
                      <div style={{ marginTop: 8, fontSize: 12, color: '#999' }}>
                        市场最高连板高度
                      </div>
                    </div>
                  </Card>
                  <Card size="small" title="炸板率">
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: 24, fontWeight: 'bold', color: marketSentiment.limit_up_failure_rate.value > 0.4 ? '#ef5350' : '#26a69a' }}>
                        {(marketSentiment.limit_up_failure_rate.value * 100).toFixed(2)}%
                      </div>
                      <div style={{ marginTop: 8 }}>
                        <Tag color={marketSentiment.limit_up_failure_rate.level === 'high_risk' ? 'red' : marketSentiment.limit_up_failure_rate.level === 'medium_risk' ? 'orange' : 'green'}>
                          {marketSentiment.limit_up_failure_rate.message}
                        </Tag>
                      </div>
                      <div style={{ marginTop: 8, fontSize: 12, color: '#999' }}>
                        涨停: {marketSentiment.limit_up_failure_rate.limit_up_count} | 炸板: {marketSentiment.limit_up_failure_rate.failure_count}
                      </div>
                    </div>
                  </Card>
                </div>
              ) : (
                <div style={{ textAlign: 'center', padding: 20, color: '#999' }}>加载中...</div>
              )}
            </Card>

            {/* Module 3: 智能资金矩阵 */}
            <Card 
              title="Module 3: 智能资金矩阵" 
              extra={
                <Space>
                  <Radio.Group 
                    value={smartMoneyMatrixDays} 
                    onChange={(e) => {
                      setSmartMoneyMatrixDays(e.target.value)
                      loadSmartMoneyMatrix(e.target.value)
                    }}
                    buttonStyle="solid"
                    size="small"
                  >
                    <Radio.Button value={1}>当日</Radio.Button>
                    <Radio.Button value={3}>3日</Radio.Button>
                    <Radio.Button value={5}>5日</Radio.Button>
                  </Radio.Group>
                  <Button size="small" onClick={() => loadSmartMoneyMatrix(smartMoneyMatrixDays)}>刷新</Button>
                </Space>
              }
            >
              {smartMoneyMatrix ? (
                (() => {
                  // 计算全局最大值（统一坐标系）
                  const globalMaxInflow = smartMoneyMatrixDays > 1 
                    ? Math.max(...smartMoneyMatrix.stocks.flatMap((s: any) => 
                        (s.daily_data || []).map((d: any) => Math.abs(d.main_net_inflow))
                      ).filter((v: number) => !isNaN(v)), 0.01)
                    : 1;
                  
                  return (
                    <Table
                      size="small"
                      dataSource={smartMoneyMatrix.stocks}
                      columns={[
                        { 
                          title: '名称', 
                          dataIndex: 'sheep_name', 
                          key: 'sheep_name',
                          render: (text: string, record: any) => (
                            <span 
                              style={{ 
                                cursor: 'pointer', 
                                color: record.is_high_potential ? '#ff4d4f' : '#1890ff', 
                                fontWeight: record.is_high_potential ? 'bold' : 'normal' 
                              }}
                              onClick={() => handleSheepClick(record.sheep_code, text)}
                              title="点击查看K线图"
                            >
                              {getDisplayName(text, record.sheep_code)}
                              {record.current_price > 0 && (
                                <span style={{ color: '#999', fontSize: 11, marginLeft: 4 }}>
                                  ¥{record.current_price.toFixed(2)}
                                </span>
                              )}
                            </span>
                          )
                        },
                        { 
                          title: '净流入(亿)', 
                          dataIndex: 'total_inflow', 
                          key: 'total_inflow',
                          render: (val: number) => val.toFixed(2),
                          sorter: (a: any, b: any) => a.total_inflow - b.total_inflow
                        },
                        ...(smartMoneyMatrixDays > 1 ? [{
                          title: '趋势',
                          key: 'trend',
                          width: 80,
                          render: (_: any, record: any) => {
                            const dailyData = record.daily_data || []
                            if (dailyData.length === 0) return '-'
                            return (
                              <div style={{ display: 'flex', alignItems: 'center', gap: 1, height: 20 }}>
                                {dailyData.map((d: any, idx: number) => {
                                  const height = globalMaxInflow > 0 ? Math.max(4, Math.abs(d.main_net_inflow) / globalMaxInflow * 16) : 4
                                  return (
                                    <div 
                                      key={idx}
                                      style={{
                                        width: 10,
                                        height: height,
                                        backgroundColor: d.main_net_inflow >= 0 ? '#ef5350' : '#26a69a',
                                        borderRadius: 1
                                      }}
                                      title={`${d.trade_date}: ${d.main_net_inflow >= 0 ? '+' : ''}${d.main_net_inflow.toFixed(2)}亿`}
                                    />
                                  )
                                })}
                              </div>
                            )
                          }
                        }] : []),
                        { 
                          title: '5日涨幅%', 
                          dataIndex: 'change_pct_5d', 
                          key: 'change_pct_5d',
                          render: (val: number) => <span style={{ color: val >= 0 ? '#ef5350' : '#26a69a' }}>{val.toFixed(2)}</span>
                        },
                        { title: '换手率%', dataIndex: 'turnover_rate', key: 'turnover_rate', render: (val: number) => val.toFixed(2) },
                        { title: '主力成本(MA5)', dataIndex: 'ma5_price', key: 'ma5_price', render: (val: number) => val.toFixed(2) },
                        { 
                          title: '潜力评分', 
                          dataIndex: 'potential_score', 
                          key: 'potential_score',
                          sorter: (a: any, b: any) => a.potential_score - b.potential_score
                        },
                        { 
                          title: '潜力原因', 
                          dataIndex: 'potential_reason', 
                          key: 'potential_reason',
                          ellipsis: true
                        }
                      ]}
                      rowClassName={(record) => record.is_high_potential ? 'high-potential-row' : ''}
                      pagination={{ pageSize: 20 }}
                      scroll={{ y: 400 }}
                    />
                  );
                })()
              ) : (
                <div style={{ textAlign: 'center', padding: 20, color: '#999' }}>加载中...</div>
              )}
            </Card>
          </Spin>
        </Panel>
      </Collapse>

      {/* 第二块：1/3/5天榜单 */}
      <Collapse 
        defaultActiveKey={['ranking']}
        style={{ marginBottom: 24 }}
      >
        <Panel
          header={
            <div 
              style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%', paddingRight: 16 }}
              onClick={(e) => e.stopPropagation()}
            >
              <span>
                <DollarOutlined style={{ marginRight: 8, color: '#1890ff' }} />
                净流入榜单
              </span>
              <Radio.Group 
                value={rankingDays} 
                onChange={(e) => setRankingDays(e.target.value)}
                buttonStyle="solid"
                size="small"
              >
                <Radio.Button value={1}>最近1天</Radio.Button>
                <Radio.Button value={3}>最近3天</Radio.Button>
                <Radio.Button value={5}>最近5天</Radio.Button>
              </Radio.Group>
            </div>
          }
          key="ranking"
        >
          <div style={{ display: 'grid', gridTemplateColumns: isMobile ? '1fr' : '1fr 1fr', gap: 16 }}>
            {/* 肥羊净流入Top100 */}
            <Card 
              title={
                <span>
                  <DollarOutlined style={{ marginRight: 8, color: '#52c41a' }} />
                  肥羊净流入Top100
                </span>
              }
              size="small"
            >
              {topInflowStocksLoading ? (
                <div style={{ textAlign: 'center', padding: 20 }}>
                  <Spin />
                </div>
              ) : topInflowStocks.length === 0 ? (
                <div style={{ textAlign: 'center', padding: 40, color: '#999' }}>
                  <div style={{ fontSize: 14 }}>暂无数据</div>
                </div>
              ) : (
                (() => {
                  // 计算全局最大值（统一坐标系）
                  const globalMaxInflow = rankingDays > 1 
                    ? Math.max(...topInflowStocks.flatMap((s: CapitalInflowStock) => 
                        (s.daily_data || []).map((d) => Math.abs(d.main_net_inflow))
                      ).filter((v: number) => !isNaN(v)), 0.01)
                    : 1;
                                
                  return (
                    <Table
                      dataSource={topInflowStocks.slice((topInflowStocksPage - 1) * 5, topInflowStocksPage * 5)}
                      columns={[
                        {
                          title: '排名',
                          key: 'index',
                          width: 50,
                          render: (_: any, __: any, index: number) => (topInflowStocksPage - 1) * 5 + index + 1,
                        },
                        {
                          title: '标的名称',
                          dataIndex: 'sheep_name',
                          key: 'sheep_name',
                          width: 100,
                          render: (name: string, record: CapitalInflowStock) => {
                            const displayName = getDisplayName(name, record.sheep_code)
                            return (
                              <span 
                                style={{ cursor: 'pointer', color: '#1890ff' }} 
                                onClick={() => handleSheepClick(record.sheep_code, displayName)}
                              >
                                {displayName}
                              </span>
                            )
                          },
                        },
                        {
                          title: '净流入（亿元）',
                          dataIndex: 'total_inflow',
                          key: 'total_inflow',
                          width: 100,
                          render: (inflow: number) => (
                            <span style={{ fontWeight: 'bold', color: '#ef5350' }}>
                              +{inflow.toFixed(2)}
                            </span>
                          ),
                        },
                        ...(rankingDays > 1 ? [{
                          title: '趋势',
                          key: 'trend',
                          width: 70,
                          render: (_: any, record: CapitalInflowStock) => {
                            const dailyData = record.daily_data || []
                            if (dailyData.length === 0) return '-'
                            return (
                              <div style={{ display: 'flex', alignItems: 'center', gap: 1, height: 20 }}>
                                {dailyData.map((d, idx: number) => {
                                  const height = globalMaxInflow > 0 ? Math.max(4, Math.abs(d.main_net_inflow) / globalMaxInflow * 16) : 4
                                  return (
                                    <div 
                                      key={idx}
                                      style={{
                                        width: 10,
                                        height: height,
                                        backgroundColor: d.main_net_inflow >= 0 ? '#ef5350' : '#26a69a',
                                        borderRadius: 1
                                      }}
                                      title={`${d.trade_date}: ${d.main_net_inflow >= 0 ? '+' : ''}${d.main_net_inflow.toFixed(2)}亿`}
                                    />
                                  )
                                })}
                              </div>
                            )
                          }
                        }] : []),
                        {
                          title: '日均流入（亿元）',
                          dataIndex: 'avg_daily_inflow',
                          key: 'avg_daily_inflow',
                          width: 100,
                          render: (inflow: number) => (
                            <span style={{ color: '#666' }}>
                              {inflow.toFixed(2)}
                            </span>
                          ),
                        },
                      ]}
                      rowKey="sheep_code"
                      pagination={{ 
                        current: topInflowStocksPage,
                        pageSize: 5,
                        total: topInflowStocks.length,
                        showTotal: (total) => `共 ${total} 只，每顥5只`,
                        onChange: (page) => setTopInflowStocksPage(page)
                      }}
                      size="small"
                    />
                  );
                })()
              )}
            </Card>

            {/* 板块净流入Top10（已聚类） */}
            <Card 
              title={
                <span>
                  <DollarOutlined style={{ marginRight: 8, color: '#1890ff' }} />
                  板块净流入Top10（已聚类）
                </span>
              }
              size="small"
            >
              {topInflowSectorsLoading ? (
                <div style={{ textAlign: 'center', padding: 20 }}>
                  <Spin />
                </div>
              ) : topInflowSectors.length === 0 ? (
                <div style={{ textAlign: 'center', padding: 40, color: '#999' }}>
                  <div style={{ fontSize: 14 }}>暂无数据</div>
                </div>
              ) : (
                (() => {
                  // 计算全局最大值（统一坐标系）
                  const globalMaxInflow = rankingDays > 1 
                    ? Math.max(...topInflowSectors.flatMap((s: SectorMoneyFlowInfo) => 
                        (s.daily_data || []).map((d) => Math.abs(d.main_net_inflow))
                      ).filter((v: number) => !isNaN(v)), 0.01)
                    : 1;
                  
                  return (
                    <Table
                      dataSource={topInflowSectors.slice((topInflowSectorsPage - 1) * 5, topInflowSectorsPage * 5)}
                      columns={[
                        {
                          title: '排名',
                          key: 'index',
                          width: 50,
                          render: (_: any, __: any, index: number) => (topInflowSectorsPage - 1) * 5 + index + 1,
                        },
                        {
                          title: '板块名称',
                          dataIndex: 'sector_name',
                          key: 'sector_name',
                          width: 120,
                          render: (name: string) => (
                            <span 
                              style={{ cursor: 'pointer', color: '#1890ff' }} 
                              onClick={() => handleSectorClick(name)}
                            >
                              {name}
                            </span>
                          )
                        },
                        {
                          title: rankingDays === 1 ? '当日净流入' : '累计净流入',
                          dataIndex: rankingDays === 1 ? 'main_net_inflow' : 'total_inflow',
                          key: 'inflow',
                          width: 100,
                          align: 'right' as const,
                          render: (value: number, record: SectorMoneyFlowInfo) => {
                            const inflow = rankingDays === 1 ? (value || 0) : ((record.total_inflow || 0) / 10000)
                            const displayValue = rankingDays === 1 ? (inflow / 10000).toFixed(2) : inflow.toFixed(2)
                            const color = inflow >= 0 ? '#ff4d4f' : '#52c41a'
                            return (
                              <span style={{ color, fontWeight: 500 }}>
                                {inflow >= 0 ? '+' : ''}{displayValue} 亿
                              </span>
                            )
                          },
                        },
                        ...(rankingDays > 1 ? [{
                          title: '趋势',
                          key: 'trend',
                          width: 70,
                          render: (_: any, record: SectorMoneyFlowInfo) => {
                            const dailyData = record.daily_data || []
                            if (dailyData.length === 0) return '-'
                            return (
                              <div style={{ display: 'flex', alignItems: 'center', gap: 1, height: 20 }}>
                                {dailyData.map((d, idx: number) => {
                                  // daily_data 的 main_net_inflow 是万元，转换为亿元
                                  const inflowValue = d.main_net_inflow / 10000
                                  const height = globalMaxInflow > 0 ? Math.max(4, Math.abs(inflowValue) / (globalMaxInflow / 10000) * 16) : 4
                                  return (
                                    <div 
                                      key={idx}
                                      style={{
                                        width: 10,
                                        height: height,
                                        backgroundColor: d.main_net_inflow >= 0 ? '#ef5350' : '#26a69a',
                                        borderRadius: 1
                                      }}
                                      title={`${d.trade_date}: ${inflowValue >= 0 ? '+' : ''}${inflowValue.toFixed(2)}亿`}
                                    />
                                  )
                                })}
                              </div>
                            )
                          }
                        }] : []),
                      ]}
                      rowKey="sector_name"
                      pagination={{ 
                        current: topInflowSectorsPage,
                        pageSize: 5,
                        total: topInflowSectors.length,
                        showTotal: (total) => `共 ${total} 个，每页5个`,
                        onChange: (page) => setTopInflowSectorsPage(page)
                      }}
                      size="small"
                    />
                  );
                })()
              )}
            </Card>
          </div>
        </Panel>
      </Collapse>

      {/* 保留原有的净流入肥羊推荐和净流入板块推荐（已隐藏，备用） */}
      {false && (
        <>
      {/* 净流入肥羊推荐 */}
      <Collapse 
        defaultActiveKey={[]} 
        style={{ marginBottom: 24 }}
        onChange={(keys: string | string[]) => {
          const activeKeys = Array.isArray(keys) ? keys : [keys]
          if (activeKeys.includes('capitalInflow') && !capitalInflowLoaded) {
            setCapitalInflowLoaded(true)
            loadCapitalInflowData(capitalInflowDays)
          }
        }}
      >
        <Panel
          header={
            <div 
              style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%', paddingRight: 16 }}
              onClick={(e) => e.stopPropagation()}
            >
              <span>
                <DollarOutlined style={{ marginRight: 8, color: '#52c41a' }} />
                净流入肥羊推荐
              </span>
              <Radio.Group 
                value={capitalInflowDays} 
                onChange={(e) => setCapitalInflowDays(e.target.value)}
                buttonStyle="solid"
                size="small"
              >
                <Radio.Button value={5}>最近5天</Radio.Button>
                <Radio.Button value={10}>最近10天</Radio.Button>
                <Radio.Button value={20}>最近20天</Radio.Button>
              </Radio.Group>
            </div>
          }
          key="capitalInflow"
        >
          {capitalInflowLoading ? (
            <div style={{ textAlign: 'center', padding: 20 }}>
              <Spin />
            </div>
          ) : capitalInflowStocks.length === 0 ? (
            <div style={{ textAlign: 'center', padding: 40, color: '#999' }}>
              <div style={{ fontSize: 16, marginBottom: 8 }}>暂无资金持续流入的标的</div>
              <div style={{ fontSize: 14 }}>最近{capitalInflowDays}天内没有找到持续流入的标的，请尝试调整天数或稍后重试</div>
            </div>
          ) : (
            <div>
              <div style={{ marginBottom: 16, color: '#666', fontSize: '14px' }}>
                找到 <strong style={{ color: '#52c41a' }}>{capitalInflowStocks.length}</strong> 只最近{capitalInflowDays}天资金持续流入的标的
                <span style={{ marginLeft: 16, fontSize: '12px', color: '#999' }}>
                  （单位：亿元，红色=流入，绿色=流出）
                </span>
              </div>
              <Table
                dataSource={capitalInflowStocks}
                columns={capitalInflowColumns}
                rowKey="sheep_code"
                pagination={{ pageSize: 10, showTotal: (total) => `共 ${total} 只标的` }}
                size="small"
              />
            </div>
          )}
        </Panel>
      </Collapse>

      {/* 净流入板块推荐 */}
      <Collapse 
        defaultActiveKey={[]} 
        style={{ marginBottom: 24 }}
        onChange={(keys: string | string[]) => {
          const activeKeys = Array.isArray(keys) ? keys : [keys]
          if (activeKeys.includes('sectorInflow') && !sectorInflowLoaded) {
            setSectorInflowLoaded(true)
            loadSectorInflowData(sectorInflowDays)
          }
        }}
      >
        <Panel
          header={
            <div 
              style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%', paddingRight: 16 }}
              onClick={(e) => e.stopPropagation()}
            >
              <span>
                <DollarOutlined style={{ marginRight: 8, color: '#1890ff' }} />
                净流入板块推荐
              </span>
              <Radio.Group 
                value={sectorInflowDays} 
                onChange={(e) => setSectorInflowDays(e.target.value)}
                buttonStyle="solid"
                size="small"
              >
                <Radio.Button value={1}>当日</Radio.Button>
                <Radio.Button value={3}>最近3天</Radio.Button>
                <Radio.Button value={5}>最近5天</Radio.Button>
              </Radio.Group>
            </div>
          }
          key="sectorInflow"
        >
          {sectorInflowLoading ? (
            <div style={{ textAlign: 'center', padding: 20 }}>
              <Spin />
            </div>
          ) : sectorInflowSectors.length === 0 ? (
            <div style={{ textAlign: 'center', padding: 40, color: '#999' }}>
              <div style={{ fontSize: 16, marginBottom: 8 }}>暂无板块资金流入数据</div>
              <div style={{ fontSize: 14 }}>请稍后重试或联系管理员</div>
            </div>
          ) : (
            <div>
              <div style={{ marginBottom: 16, color: '#666', fontSize: '14px' }}>
                找到 <strong style={{ color: '#1890ff' }}>{sectorInflowSectors.length}</strong> 个资金净流入板块
                <span style={{ marginLeft: 16, fontSize: '12px', color: '#999' }}>
                  （单位：亿元）
                  {sectorInflowDays === 1 && ' - 当日为最近交易日的资金流入'}
                  {sectorInflowDays === 3 && ' - 最近3天为最近3个交易日的资金总量'}
                  {sectorInflowDays === 5 && ' - 最近5天为最近5个交易日的资金总量'}
                </span>
                {sectorInflowMetadata && (
                  <div style={{ marginTop: 8, fontSize: '12px' }}>
                    <span style={{ color: sectorInflowMetadata?.has_sufficient_data ? '#52c41a' : '#ff9800' }}>
                      数据库中有 {sectorInflowMetadata?.total_days_in_db ?? 0} 天的数据，
                      实际使用 {sectorInflowMetadata?.actual_days_used ?? 0} 天
                      {!sectorInflowMetadata?.has_sufficient_data && sectorInflowMetadata?.warning && (
                        <span style={{ color: '#ff4d4f', marginLeft: 8 }}>⚠️ {sectorInflowMetadata?.warning}</span>
                      )}
                    </span>
                  </div>
                )}
              </div>
              <Table
                dataSource={sectorInflowSectors}
                columns={[
                  {
                    title: '板块名称',
                    dataIndex: 'sector_name',
                    key: 'sector_name',
                    width: 200,
                    render: (name: string) => (
                      <span 
                        style={{ fontWeight: 500, cursor: 'pointer', color: '#1890ff' }}
                        onClick={() => handleSectorStocksClick(name)}
                        title="点击查看涨幅前10关联概念股"
                      >
                        {name}
                      </span>
                    )
                  },
                  {
                    title: sectorInflowDays === 1 ? '当日净流入' : '累计净流入',
                    dataIndex: sectorInflowDays === 1 ? 'main_net_inflow' : 'total_inflow',
                    key: 'inflow',
                    width: 150,
                    align: 'right',
                    render: (value: number, record: SectorMoneyFlowInfo) => {
                      const inflow = sectorInflowDays === 1 ? value : (record.total_inflow || 0)
                      const color = inflow >= 0 ? '#ff4d4f' : '#52c41a'
                      const displayValue = (inflow / 10000).toFixed(2) // 转换为亿元
                      return (
                        <span style={{ color, fontWeight: 500 }}>
                          {inflow >= 0 ? '+' : ''}{displayValue} 亿元
                        </span>
                      )
                    },
                    sorter: (a: SectorMoneyFlowInfo, b: SectorMoneyFlowInfo) => {
                      const aVal = sectorInflowDays === 1 ? (a.main_net_inflow || 0) : (a.total_inflow || 0)
                      const bVal = sectorInflowDays === 1 ? (b.main_net_inflow || 0) : (b.total_inflow || 0)
                      // bVal - aVal 表示降序（大的在前）
                      return bVal - aVal
                    }
                  },
                  ...(sectorInflowDays > 1 ? [{
                    title: '每日净流入趋势',
                    key: 'daily_chart',
                    width: sectorInflowDays === 3 ? 120 : 180,
                    render: (_: any, record: SectorMoneyFlowInfo) => {
                      const dailyData = record.daily_data || []
                      if (dailyData.length === 0) return '-'
                      
                      // 后端已按日期正序返回（从旧到新），取最后N天（最近的N个交易日）
                      const recentData = dailyData.slice(-sectorInflowDays)
                      
                      // 计算最大值用于归一化
                      const maxInflow = Math.max(...recentData.map(d => Math.abs(d.main_net_inflow)), 1)
                      
                      // 小巧的正方形柱状图
                      const barSize = sectorInflowDays === 3 ? 20 : 24  // 3天用20px，5天用24px
                      const gap = 3  // 柱子之间的间距
                      
                      return (
                        <div style={{ 
                          display: 'flex', 
                          alignItems: 'flex-end', 
                          justifyContent: 'center',
                          gap: gap,
                          height: barSize + 20,
                          paddingTop: 2
                        }}>
                          {recentData.map((day, idx) => {
                            const height = Math.abs(day.main_net_inflow) / maxInflow * barSize
                            const color = day.main_net_inflow >= 0 ? '#ff4d4f' : '#52c41a'
                            return (
                              <div 
                                key={idx} 
                                style={{ 
                                  display: 'flex', 
                                  flexDirection: 'column', 
                                  alignItems: 'center',
                                  position: 'relative'
                                }}
                                title={`${day.trade_date}: ${(day.main_net_inflow / 10000).toFixed(2)} 亿元`}
                              >
                                <div
                                  style={{
                                    width: `${barSize}px`,
                                    height: `${Math.max(height, day.main_net_inflow === 0 ? 1 : 2)}px`,
                                    backgroundColor: color,
                                    borderRadius: '2px',
                                    minHeight: day.main_net_inflow === 0 ? 1 : 2,
                                  }}
                                />
                                <div style={{ 
                                  fontSize: 9, 
                                  color: '#999', 
                                  marginTop: 2,
                                  transform: 'scale(0.85)',
                                  whiteSpace: 'nowrap'
                                }}>
                                  {day.trade_date.split('-').slice(1).join('/')}
                                </div>
                              </div>
                            )
                          })}
                        </div>
                      )
                    }
                  }] : []),
                  {
                    title: '超大单',
                    dataIndex: sectorInflowDays === 1 ? 'super_large_inflow' : 'total_super_large',
                    key: 'super_large',
                    width: 120,
                    align: 'right',
                    render: (value: number, record: SectorMoneyFlowInfo) => {
                      const val = sectorInflowDays === 1 ? (value || 0) : (record.total_super_large || 0)
                      return (val / 10000).toFixed(2) + ' 亿元'
                    }
                  },
                  {
                    title: '大单',
                    dataIndex: sectorInflowDays === 1 ? 'large_inflow' : 'total_large',
                    key: 'large',
                    width: 120,
                    align: 'right',
                    render: (value: number, record: SectorMoneyFlowInfo) => {
                      const val = sectorInflowDays === 1 ? (value || 0) : (record.total_large || 0)
                      return (val / 10000).toFixed(2) + ' 亿元'
                    }
                  }
                ]}
                rowKey="sector_name"
                pagination={{ 
                  current: sectorInflowPage,
                  pageSize: 5,
                  showTotal: (total) => `共 ${total} 个板块`,
                  onChange: (page) => setSectorInflowPage(page)
                }}
                size="small"
              />
            </div>
          )}
        </Panel>
      </Collapse>
        </>
      )}

      
      <Collapse defaultActiveKey={[]} style={{ marginBottom: 24 }}>
        <Panel
          header={
            <span>
              <FireOutlined style={{ marginRight: 8 }} />
              雪球热度榜 (Top {xueqiuSheeps.length})
            </span>
          }
          key="xueqiu"
        >
          {/* 热门板块聚合视图 */}
          {aggregatedHotSectors.length > 0 && (
            <div style={{ marginBottom: 24 }}>
              <div style={{ marginBottom: 12, fontWeight: 'bold', color: '#333' }}>
                <FireOutlined style={{ marginRight: 8, color: '#ff4d4f' }} />
                热门板块
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
                {aggregatedHotSectors.map((sector, idx) => {
                  const color = getSectorColor(idx)
                  const isExpanded = xueqiuExpandedSectors.has(sector.name)
                  const displayStocks = isExpanded ? sector.stocks : sector.stocks.slice(0, 5)
                  
                  return (
                    <Card
                      key={sector.name}
                      size="small"
                      style={{ 
                        width: isMobile ? '100%' : 280,
                        borderTop: `3px solid ${color}`,
                        boxShadow: idx === 0 ? '0 2px 8px rgba(255, 77, 79, 0.3)' : undefined
                      }}
                      title={
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                          <span 
                            style={{ 
                              color, 
                              fontWeight: 'bold', 
                              cursor: 'pointer',
                              display: 'flex',
                              alignItems: 'center'
                            }}
                            onClick={() => handleSectorClick(sector.name)}
                            title="点击查看K线图"
                          >
                            {idx === 0 && <FireOutlined style={{ marginRight: 4 }} />}
                            {sector.name}
                          </span>
                          <Tag color={idx === 0 ? 'red' : idx === 1 ? 'orange' : 'blue'}>
                            {sector.count}只
                          </Tag>
                        </div>
                      }
                    >
                      <div style={{ maxHeight: isExpanded ? 300 : 150, overflowY: 'auto' }}>
                        {displayStocks.map((stock, stockIdx) => (
                          <div 
                            key={stock.sheep_code}
                            style={{ 
                              padding: '4px 0',
                              borderBottom: stockIdx < displayStocks.length - 1 ? '1px solid #f0f0f0' : 'none',
                              display: 'flex',
                              justifyContent: 'space-between',
                              alignItems: 'center'
                            }}
                          >
                            <span 
                              style={{ 
                                cursor: 'pointer', 
                                color: '#1890ff',
                                flex: 1,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap'
                              }}
                              onClick={() => handleSheepClick(stock.sheep_code, getDisplayName(stock.sheep_name, stock.sheep_code))}
                              title="点击查看K线图"
                            >
                              {getDisplayName(stock.sheep_name, stock.sheep_code)}
                            </span>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginLeft: 8 }}>
                              {stock.change_pct !== undefined && (
                                <span style={{ 
                                  color: stock.change_pct >= 0 ? '#ef5350' : '#26a69a',
                                  fontSize: 12,
                                  fontWeight: 'bold'
                                }}>
                                  {stock.change_pct >= 0 ? '+' : ''}{stock.change_pct.toFixed(2)}%
                                </span>
                              )}
                              <Tag color="geekblue" style={{ margin: 0, fontSize: 10 }}>#{stock.rank}</Tag>
                            </div>
                          </div>
                        ))}
                      </div>
                      {sector.stocks.length > 5 && (
                        <div 
                          style={{ 
                            marginTop: 8, 
                            textAlign: 'center', 
                            cursor: 'pointer',
                            color: '#1890ff',
                            fontSize: 12
                          }}
                          onClick={() => toggleSectorExpanded(sector.name)}
                        >
                          {isExpanded ? (
                            <><DownOutlined style={{ marginRight: 4 }} />收起</>
                          ) : (
                            <><RightOutlined style={{ marginRight: 4 }} />查看更多 ({sector.stocks.length - 5})</>
                          )}
                        </div>
                      )}
                    </Card>
                  )
                })}
              </div>
            </div>
          )}
          
          {/* 原始榜单表格 */}
          <div style={{ marginTop: aggregatedHotSectors.length > 0 ? 16 : 0 }}>
            <div style={{ marginBottom: 12, fontWeight: 'bold', color: '#333' }}>
              完整榜单
            </div>
            <Table
              dataSource={xueqiuSheeps}
              columns={xueqiuSheepsColumns}
              rowKey={(record) => `${record.source}-${record.sheep_code}-${record.rank}`}
              pagination={{ pageSize: 20 }}
              size="small"
              onRow={(record) => ({
                onClick: () => handleSheepClick(record.sheep_code, getDisplayName(record.sheep_name, record.sheep_code)),
                style: { cursor: 'pointer' }
              })}
            />
          </div>
        </Panel>
      </Collapse>

      {/* K线图弹窗（支持肥羊和板块） */}
      <Modal
        title={
          <Space>
            <span>
              {selectedSheepForKline ? `${selectedSheepForKline.name || selectedSheepForKline.code} - K线图` : 
               selectedSectorForKline ? `${selectedSectorForKline.name} - 资金流走势图` : 'K线图'}
            </span>
          </Space>
        }
        open={klineModalVisible}
        onCancel={() => {
          setKlineModalVisible(false)
          setSelectedSheepForKline(null)
          setSelectedSectorForKline(null)
        }}
        footer={null}
        width={isMobile ? '95%' : 1200}
        style={{ top: 20 }}
      >
        {(klineLoading || sectorKlineLoading) ? (
          <div style={{ textAlign: 'center', padding: 50 }}>
            <Spin size="large" />
          </div>
        ) : selectedSectorForKline && sectorKlineData.length > 0 ? (
          // 板块K线：主力流入
          <ReactECharts
            option={{
              title: { text: `${selectedSectorForKline.name} - 资金流走势图`, left: 'center' },
              tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
              legend: { data: ['主力净流入'], top: 30 },
              xAxis: { type: 'category', data: sectorKlineData.map(d => d.trade_date), boundaryGap: false },
              yAxis: [
                { type: 'value', name: '主力净流入(亿元)', position: 'left' }
              ],
              series: [
                {
                  name: '主力净流入',
                  type: 'bar',
                  data: sectorKlineData.map(d => ({
                    value: (d.main_net_inflow || 0) / 10000,
                    itemStyle: { color: (d.main_net_inflow || 0) >= 0 ? '#ef5350' : '#26a69a' }
                  }))
                }
              ]
            }}
            style={{ height: isMobile ? '400px' : '600px', width: '100%' }}
          />
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

      {/* AI推荐Modal */}
      <Modal
        title={
          <Space>
            <RobotOutlined />
            <span>AI肥羊推荐</span>
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
            <span>AI肥羊分析 - {selectedSheepForAnalyze?.name || selectedSheepForAnalyze?.code}</span>
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



      {/* 板块涨幅前10概念股Modal */}
      <Modal
        title={`${selectedSectorForStocks?.name || ''} - 涨幅前10关联概念股`}
        open={sectorStocksModalVisible}
        onCancel={() => setSectorStocksModalVisible(false)}
        footer={null}
        width={isMobile ? '95%' : 800}
        style={{ top: 20 }}
      >
        {sectorStocksLoading ? (
          <div style={{ textAlign: 'center', padding: 50 }}>
            <Spin size="large" />
            <div style={{ marginTop: 16, color: '#999' }}>正在加载数据...</div>
          </div>
        ) : selectedSectorForStocks && selectedSectorForStocks.stocks.length > 0 ? (
          <Table
            dataSource={selectedSectorForStocks.stocks}
            columns={[
              {
                title: '排名',
                key: 'index',
                width: 80,
                render: (_: any, __: any, index: number) => index + 1,
              },
              {
                title: '标的代码',
                dataIndex: 'sheep_code',
                key: 'sheep_code',
                width: 120,
              },
              {
                title: '标的名称',
                dataIndex: 'sheep_name',
                key: 'sheep_name',
                width: 150,
                render: (name: string, record: SectorStockByChange) => {
                  const displayName = getDisplayName(name, record.sheep_code)
                  return (
                    <span 
                      style={{ cursor: 'pointer', color: '#1890ff' }} 
                      onClick={() => handleSheepClick(record.sheep_code, displayName)}
                    >
                      {displayName}
                    </span>
                  )
                },
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
                title: '涨幅',
                dataIndex: 'change_pct',
                key: 'change_pct',
                width: 100,
                render: (pct: number) => pct !== undefined && pct !== null ? (
                  <Tag color={pct >= 0 ? 'red' : 'green'}>
                    {pct >= 0 ? '+' : ''}{pct.toFixed(2)}%
                  </Tag>
                ) : '-',
                sorter: (a: SectorStockByChange, b: SectorStockByChange) => (b.change_pct || 0) - (a.change_pct || 0),
                defaultSortOrder: 'descend',
              },
              {
                title: '热度排名',
                dataIndex: 'rank',
                key: 'rank',
                width: 100,
                render: (rank: number) => rank ? <Tag color="orange">#{rank}</Tag> : '-',
              },
            ]}
            rowKey="sheep_code"
            pagination={false}
            size="small"
            onRow={(record) => ({
              onClick: () => handleSheepClick(record.sheep_code, getDisplayName(record.sheep_name, record.sheep_code)),
              style: { cursor: 'pointer' }
            })}
          />
        ) : (
          <div style={{ textAlign: 'center', padding: 50, color: '#999' }}>
            暂无概念股数据
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
        okText={promptType === 'recommend' ? '确认并推荐' : '确认并分析'}
        cancelText="取消"
        width={800}
      >
        <div>
          <p style={{ marginBottom: 8, color: '#666' }}>
            以下是渲染后的提示词（变量已替换），您可以编辑：
          </p>
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

// 添加全局样式用于高亮潜力标的
if (typeof document !== 'undefined') {
  const style = document.createElement('style')
  style.textContent = `
    .high-potential-row {
      background-color: #fff1f0 !important;
    }
    .high-potential-row:hover {
      background-color: #ffe7e5 !important;
    }
  `
  document.head.appendChild(style)
}

export default Tab2
