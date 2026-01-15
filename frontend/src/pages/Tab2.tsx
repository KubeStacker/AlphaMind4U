import React, { useState, useEffect } from 'react'
import { Card, Button, Table, Tag, Collapse, message, Popover, Spin, Modal, Space, Select, Radio, Pagination } from 'antd'
import { ReloadOutlined, FireOutlined, RobotOutlined, BulbOutlined, DollarOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { hotApi, HotSheep, SectorInfo, SectorSheep } from '../api/hot'
import { capitalInflowApi, CapitalInflowStock, sectorMoneyFlowApi, SectorMoneyFlowInfo } from '../api/hot'
import { sheepApi, SheepDailyData } from '../api/sheep'
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

const Tab2: React.FC = () => {
  const [selectedModel, setSelectedModel] = useState<string>('')
  const [availableModels, setAvailableModels] = useState<Array<{ id: number; model_name: string; model_display_name: string }>>([])
  const [hotSheeps, setHotSheeps] = useState<HotSheep[]>([])
  const [hotSectors, setHotSectors] = useState<SectorInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [sectorChartData, setSectorChartData] = useState<Record<string, any>>({})
  const [sectorSheeps, setSectorSheeps] = useState<Record<string, SectorSheep[]>>({})
  const [isMobile, setIsMobile] = useState(false)
  const [klineModalVisible, setKlineModalVisible] = useState(false)
  const [selectedSheepForKline, setSelectedSheepForKline] = useState<{ code: string; name: string } | null>(null)
  const [klineData, setKlineData] = useState<SheepDailyData[]>([])
  const [klineLoading, setKlineLoading] = useState(false)
  const [sectorSheepsModalVisible, setSectorSheepsModalVisible] = useState(false)
  const [selectedSectorForSheeps, setSelectedSectorForSheeps] = useState<{ name: string; sheep: SectorSheep[] } | null>(null)
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
  const [sectorInflowLoaded, setSectorInflowLoaded] = useState<boolean>(false)  // 是否已加载数据
  const [capitalInflowLoaded, setCapitalInflowLoaded] = useState<boolean>(false)  // 是否已加载数据
  const [hotSectorsPage, setHotSectorsPage] = useState<number>(1)  // 热门板块分页

  const loadData = async () => {
    setLoading(true)
    try {
      const [sheep, sectors] = await Promise.all([
        hotApi.getHotSheeps(),
        hotApi.getHotSectors(),
      ])
      setHotSheeps(sheep || [])
      setHotSectors(sectors || [])
      
      // 调试信息：检查数据格式
      if (import.meta.env.DEV) {
      }
    } catch (error: any) {
      console.error('加载数据失败:', error)
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      message.error(`加载数据失败: ${errorMsg}`)
      setHotSheeps([])
      setHotSectors([])
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
      const result = await sectorMoneyFlowApi.getRecommendations(days, 20)
      setSectorInflowSectors(result.sectors || [])
    } catch (error: any) {
      console.error('加载板块资金流入推荐失败:', error)
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      message.error(`加载板块资金流入推荐失败: ${errorMsg}`)
      setSectorInflowSectors([])
    } finally {
      setSectorInflowLoading(false)
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
      await hotApi.refreshHotSheeps()
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
      const result = await aiApi.recommendSheeps(selectedModel || undefined)
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
    setSelectedSheepForAnalyze({ code: stockCode, name: stockName })
    setAiAnalyzeModalVisible(true)
    setAiAnalyzeLoading(true)
    setAiAnalyzeResult('')
    try {
      const result = await aiApi.analyzeSheep(stockCode, selectedModel || undefined)
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
      const [dailyData, sheep] = await Promise.all([
        hotApi.getSectorDaily(sectorName),
        hotApi.getSectorSheeps(sectorName),
      ])
      
      setSectorChartData({ ...sectorChartData, [sectorName]: dailyData })
      setSectorSheeps({ ...sectorSheeps, [sectorName]: sheep })
    } catch (error) {
      message.error('加载板块数据失败')
    }
  }

  const handleSectorSheepsClick = async (sectorName: string) => {
    try {
      let sheep = sectorSheeps[sectorName]
      if (!sheep || sheep.length === 0) {
        sheep = await hotApi.getSectorSheeps(sectorName)
        setSectorSheeps({ ...sectorSheeps, [sectorName]: sheep })
      }
      setSelectedSectorForSheeps({ name: sectorName, sheep })
      setSectorSheepsModalVisible(true)
    } catch (error) {
      message.error('加载板块肥羊列表失败')
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

  const sectorSheepsColumns = [
    {
      title: '标的代码',
      dataIndex: 'sheep_code',
      key: 'sheep_code',
    },
    {
      title: '标的名称',
      dataIndex: 'sheep_name',
      key: 'sheep_name',
      render: (name: string, record: SectorSheep) => {
        const displayName = getDisplayName(name, record.sheep_code)
        return (
          <span style={{ cursor: 'pointer', color: '#1890ff' }}>
            {displayName}
          </span>
        )
      },
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
    setKlineModalVisible(true)
    setKlineLoading(true)
    try {
      const data = await sheepApi.getSheepDaily(stockCode)
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
        text: `${selectedSheepForKline?.name || selectedSheepForKline?.code} - K线图`,
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

  const xueqiuSheeps = hotSheeps.filter(s => s.source === 'xueqiu').slice(0, 100)
  const dongcaiSheeps = hotSheeps.filter(s => s.source === 'dongcai').slice(0, 100)

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
            <div style={{ fontSize: 14 }}>请确保已采集热门肥羊数据，或点击刷新按钮更新数据</div>
          </div>
        ) : (
          <div>
            {/* 热门板块列表（倒序排序，分页显示） */}
            <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 16 }}>
              {hotSectors
                .sort((a, b) => (b.hot_count || b.hot_score || 0) - (a.hot_count || a.hot_score || 0))  // 倒序排序
                .slice((hotSectorsPage - 1) * 5, hotSectorsPage * 5)  // 分页，每页5个
                .map((sector) => {
              const chartOption = getSectorChartOption(sector.sector_name)
              const sheep = sectorSheeps[sector.sector_name] || []

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
                          {sheep.length > 0 && (
                            <Table
                              dataSource={sheep}
                              columns={sectorSheepsColumns}
                              pagination={false}
                              size="small"
                              style={{ marginTop: 16 }}
                              onRow={(record) => ({
                                onClick: () => handleSheepClick(record.sheep_code, getDisplayName(record.sheep_name, record.sheep_code)),
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
                    {sector.hot_sheep && sector.hot_sheep.length > 0 && (
                      <div style={{ marginTop: 12, borderTop: '1px solid rgba(255,255,255,0.3)', paddingTop: 12 }}>
                        <div style={{ fontSize: 12, marginBottom: 8, opacity: 0.9 }}>热门标的：</div>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                          {sector.hot_sheep.slice(0, 5).map((stock: any, idx: number) => {
                            const displayName = getDisplayName(stock.sheep_name, stock.sheep_code)
                            const normalizedCode = normalizeCode(stock.sheep_code)
                            return (
                            <div 
                              key={stock.sheep_code} 
                              style={{ fontSize: 12, opacity: 0.9, cursor: 'pointer' }}
                              onClick={(e) => {
                                e.stopPropagation()
                                handleSheepClick(normalizedCode, displayName)
                              }}
                            >
                              {idx + 1}. {displayName}（{normalizedCode}）{stock.rank && ` #${stock.rank}`}
                            </div>
                            )
                          })}
                          {sector.hot_sheep.length > 5 && (
                            <div 
                              style={{ fontSize: 11, opacity: 0.9, marginTop: 4, cursor: 'pointer', textDecoration: 'underline' }}
                              onClick={(e) => {
                                e.stopPropagation()
                                handleSectorSheepsClick(sector.sector_name)
                              }}
                            >
                              查看全部 {sector.hot_sheep?.length || 0} 只肥羊...
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
            {/* 分页器 */}
            <div style={{ display: 'flex', justifyContent: 'center', marginTop: 16 }}>
              <Pagination
                current={hotSectorsPage}
                total={hotSectors.length}
                pageSize={5}
                showTotal={(total) => `共 ${total} 个板块`}
                onChange={(page) => setHotSectorsPage(page)}
                showSizeChanger={false}
              />
            </div>
          </div>
        )}
      </Card>

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
                  （单位：亿元，按净流入倒序排序）
                </span>
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
                      <span style={{ fontWeight: 500 }}>{name}</span>
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
                      return bVal - aVal
                    },
                    defaultSortOrder: 'descend'
                  },
                  ...(sectorInflowDays > 1 ? [{
                    title: '每日净流入',
                    key: 'daily_chart',
                    width: 200,
                    render: (_: any, record: SectorMoneyFlowInfo) => {
                      const dailyData = record.daily_data || []
                      if (dailyData.length === 0) return '-'
                      
                      // 计算最大值用于归一化
                      const maxInflow = Math.max(...dailyData.map(d => Math.abs(d.main_net_inflow)), 1)
                      
                      return (
                        <div style={{ display: 'flex', alignItems: 'flex-end', gap: 4, height: 40 }}>
                          {dailyData.slice(0, sectorInflowDays).map((day, idx) => {
                            const height = Math.abs(day.main_net_inflow) / maxInflow * 30
                            const color = day.main_net_inflow >= 0 ? '#ff4d4f' : '#52c41a'
                            return (
                              <div key={idx} style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                                <div
                                  style={{
                                    width: '100%',
                                    height: `${height}px`,
                                    backgroundColor: color,
                                    minHeight: day.main_net_inflow === 0 ? 1 : undefined,
                                    borderRadius: '2px 2px 0 0',
                                  }}
                                  title={`${day.trade_date}: ${(day.main_net_inflow / 10000).toFixed(2)} 亿元`}
                                />
                                <div style={{ fontSize: 10, color: '#999', marginTop: 2 }}>
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

      <Collapse defaultActiveKey={[]} style={{ marginBottom: 24 }}>
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
            dataSource={xueqiuSheeps}
            columns={hotSheepsColumns}
            rowKey={(record) => `${record.source}-${record.sheep_code}-${record.rank}`}
            pagination={{ pageSize: 20 }}
            size="small"
            onRow={(record) => ({
              onClick: () => handleSheepClick(record.sheep_code, getDisplayName(record.sheep_name, record.sheep_code)),
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
            dataSource={dongcaiSheeps}
            columns={hotSheepsColumns}
            rowKey={(record) => `${record.source}-${record.sheep_code}-${record.rank}`}
            pagination={{ pageSize: 20 }}
            size="small"
            onRow={(record) => ({
              onClick: () => handleSheepClick(record.sheep_code, getDisplayName(record.sheep_name, record.sheep_code)),
              style: { cursor: 'pointer' }
            })}
          />
        </Panel>
      </Collapse>

      <Modal
        title={
          <Space>
            <span>{selectedSheepForKline?.name || selectedSheepForKline?.code} - K线图</span>
            {selectedSheepForKline && (
              <Button
                type="link"
                icon={<BulbOutlined />}
                onClick={() => {
                  if (selectedSheepForKline) {
                    handleAIAnalyze(selectedSheepForKline.code, selectedSheepForKline.name)
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
        title={`${selectedSectorForSheeps?.name} - 全部肥羊列表`}
        open={sectorSheepsModalVisible}
        onCancel={() => setSectorSheepsModalVisible(false)}
        footer={null}
        width={isMobile ? '95%' : 1000}
        style={{ top: 20 }}
      >
        {selectedSectorForSheeps && selectedSectorForSheeps.sheep.length > 0 ? (
          <Table
            dataSource={selectedSectorForSheeps.sheep}
            columns={sectorSheepsColumns}
            rowKey={(record) => record.sheep_code}
            pagination={{ pageSize: 20 }}
            size="small"
            onRow={(record) => ({
              onClick: () => {
                setSectorSheepsModalVisible(false)
                handleSheepClick(record.sheep_code, getDisplayName(record.sheep_name, record.sheep_code))
              },
              style: { cursor: 'pointer' }
            })}
          />
        ) : (
          <div style={{ textAlign: 'center', padding: 50, color: '#999' }}>
            暂无肥羊数据
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
    </div>
  )
}

export default Tab2
