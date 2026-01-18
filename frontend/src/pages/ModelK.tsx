import React, { useState, useEffect } from 'react'
import { Card, Button, Slider, Switch, Space, message, Tabs, Table, Tag, Statistic, Row, Col, Modal, DatePicker, Alert, Collapse, Divider, Tooltip, Popover, Spin } from 'antd'
import { ThunderboltOutlined, RocketOutlined, DeleteOutlined, ClearOutlined, QuestionCircleOutlined, SettingOutlined, LineChartOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import { modelKApi, BacktestParams, BacktestResult, Recommendation, RecommendationHistory } from '../api/modelK'
import dayjs, { Dayjs } from 'dayjs'
import type { TabsProps } from 'antd'
import { sheepApi, SheepDailyData, CapitalFlowData } from '../api/sheep'

const { RangePicker } = DatePicker
const { Panel } = Collapse

const ModelK: React.FC = () => {
  // 检测移动设备
  const [isMobile, setIsMobile] = useState(false)
  
  useEffect(() => {
    const checkIsMobile = () => {
      setIsMobile(window.innerWidth < 768)
    }
    
    checkIsMobile()
    window.addEventListener('resize', checkIsMobile)
    
    return () => {
      window.removeEventListener('resize', checkIsMobile)
    }
  }, [])
  
  // 前端参数状态（初始化为空，等待从后端加载）
  const [params, setParams] = useState<BacktestParams>({})
  const [paramsLoaded, setParamsLoaded] = useState(false)
  // modelVersion已移除，改用selectedModel
  
  // 从后端加载默认参数（自动同步）
  useEffect(() => {
    const loadDefaultParams = async () => {
      try {
        const response = await modelKApi.getDefaultParams()
        // 合并后端参数和前端额外参数
        setParams({
          // 前端默认与后端同步
          ...response.params,
          // 仅在后端没有时设置默认
          min_mv: response.params.min_mv || 10,
          max_mv: response.params.max_mv || 1000,
          change_pct_required: true,
        })
        // modelVersion已移除，改用selectedModel
        setParamsLoaded(true)
        console.log('已从后端同步默认参数:', response.params)
      } catch (error) {
        console.error('加载默认参数失败，使用前端默认值:', error)
        // 回退到前端默认参数
        setParams({
          min_mv: 10, max_mv: 1000, rps_threshold: 75, vol_threshold: 1.3,
          min_change_pct: 1.0, max_change_pct: 9.9, 
          min_main_inflow: -300, require_positive_inflow: false,
          min_turnover: 1.0, max_turnover: 35.0,
          breakout_validation: true, min_breakout_quality: 30,
          min_ai_score: 40, max_recommendations: 20, require_concept_resonance: true,
          prefer_20cm: true, change_pct_required: true,
          enable_sector_linkage: true,  // v6.0新增
        })
        setParamsLoaded(true)
      }
    }
    loadDefaultParams()
  }, [])
  const [backtestLoading, setBacktestLoading] = useState(false)
  const [backtestResult, setBacktestResult] = useState<BacktestResult | null>(() => {
    // 从 localStorage 恢复最近一次的回测结果
    try {
      const saved = localStorage.getItem('modelk_backtest_result')
      if (saved) {
        const parsed = JSON.parse(saved)
        console.log('从 localStorage 恢复回测结果')
        return parsed
      }
    } catch (e) {
      console.warn('恢复回测结果失败:', e)
    }
    return null
  })
  // 默认回测范围改为3个月（更容易成功）
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>(() => {
    // 尝试从 localStorage 恢复日期范围
    try {
      const saved = localStorage.getItem('modelk_backtest_daterange')
      if (saved) {
        const parsed = JSON.parse(saved)
        return [dayjs(parsed[0]), dayjs(parsed[1])]
      }
    } catch (e) {
      console.warn('恢复日期范围失败:', e)
    }
    // 默认3个月
    return [dayjs().subtract(3, 'month'), dayjs().subtract(1, 'day')]
  })
  const [backtestProgress, setBacktestProgress] = useState<string>('')  // 回测进度提示
  const [recommendLoading, setRecommendLoading] = useState(false)
  const [recommendations, setRecommendations] = useState<Recommendation[]>([])
  const [recommendDate, setRecommendDate] = useState<string>('')
  const [selectedRecommendDate, setSelectedRecommendDate] = useState<Dayjs | null>(null) // 用户选择的推荐日期
  const [diagnosticInfo, setDiagnosticInfo] = useState<string>('')
  const [historyLoading, setHistoryLoading] = useState(false)
  const [historyData, setHistoryData] = useState<RecommendationHistory[]>([])
  const [marketRegime, setMarketRegime] = useState<string>('')
  const [regimeScore, setRegimeScore] = useState<number>(0)
  const [funnelData, setFunnelData] = useState<{total: number, L0_pass: number, L1_pass: number, L2_pass: number, L3_pass: number, final: number} | null>(null)
  const [regimeDetails, setRegimeDetails] = useState<any>(null)
  const [breakoutStats, setBreakoutStats] = useState<{high_quality_count: number, medium_quality_count: number, trap_risk_count: number} | null>(null)
  
  // K线图弹窗状态
  const [klineVisible, setKlineVisible] = useState(false)
  const [selectedStock, setSelectedStock] = useState<{code: string, name: string}>({code: '', name: ''})
  const [klineData, setKlineData] = useState<SheepDailyData[]>([])
  const [klineCapitalFlowData, setKlineCapitalFlowData] = useState<CapitalFlowData[]>([])
  const [klineLoading, setKlineLoading] = useState(false)
  
  // 动态进度状态
  const [currentStep, setCurrentStep] = useState(0)
  const stepStartTimeRef = React.useRef<number>(0)
  
  // 执行步骤定义 - v6.0重构版
  const EXECUTION_STEPS = [
    { name: '市场状态识别', desc: 'RSRS/板块轮动/市场宽度', duration: 2000 },
    { name: 'Filter Layer', desc: 'SQL层筛选(ST/新股/市值/RPS)', duration: 2500 },
    { name: 'Feature Layer', desc: '因子提取(技术/资金/概念)', duration: 3000 },
    { name: 'Score Layer', desc: 'Z-Score标准化 + 动态权重', duration: 3500 },
    { name: 'Validate Layer', desc: '启动质量验证(扣分制)', duration: 3000 },
    { name: 'Final Filter', desc: '涌幅/概念共振/量比筛选', duration: 2000 },
  ]
  
  // 进度定时器 - 改进版：循环显示直到完成
  useEffect(() => {
    let timer: ReturnType<typeof setInterval> | undefined
    if (recommendLoading) {
      setCurrentStep(0)
      stepStartTimeRef.current = Date.now()
      
      // 计算总时长
      const totalDuration = EXECUTION_STEPS.reduce((sum, s) => sum + s.duration, 0)
      
      timer = setInterval(() => {
        const elapsed = Date.now() - stepStartTimeRef.current
        // 循环播放进度（每完成一轮重新开始）
        const cycleElapsed = elapsed % totalDuration
        
        let accumulatedTime = 0
        for (let i = 0; i < EXECUTION_STEPS.length; i++) {
          accumulatedTime += EXECUTION_STEPS[i].duration
          if (cycleElapsed < accumulatedTime) {
            setCurrentStep(i)
            return
          }
        }
        setCurrentStep(EXECUTION_STEPS.length - 1)
      }, 200)  // 更频繁更新，更流畅
    } else {
      setCurrentStep(0)
    }
    return () => {
      if (timer) clearInterval(timer)
    }
  }, [recommendLoading])

  const handleBacktest = async () => {
    if (!dateRange[0] || !dateRange[1]) { message.warning('请选择回测日期范围'); return }
    
    const startDate = dateRange[0]
    const endDate = dateRange[1]
    const daysDiff = endDate.diff(startDate, 'day')
    
    // 日期范围验证
    if (daysDiff < 7) {
      message.warning('回测日期范围至少需要7天')
      return
    }
    if (daysDiff > 180) {
      message.warning('回测日期范围不能超过180天（6个月），建议使用3个月以内的范围')
      return
    }
    
    // 保存日期范围到 localStorage
    try {
      localStorage.setItem('modelk_backtest_daterange', JSON.stringify([startDate.format('YYYY-MM-DD'), endDate.format('YYYY-MM-DD')]))
    } catch (e) {
      console.warn('保存日期范围失败:', e)
    }
    
    setBacktestLoading(true)
    setBacktestProgress('正在初始化回测引擎...')
    
    try {
      // 预估回测时间
      const estimatedTradingDays = Math.floor(daysDiff * 0.7)
      const estimatedMinutes = Math.ceil(estimatedTradingDays / 20)  // 约20天/分钟
      setBacktestProgress(`正在执行回测（约${estimatedTradingDays}个交易日，预计${estimatedMinutes}-${estimatedMinutes * 2}分钟）...`)
      
      const result = await modelKApi.runBacktest({
        start_date: startDate.format('YYYY-MM-DD'),
        end_date: endDate.format('YYYY-MM-DD'),
        params
      })
      
      if (result.success) {
        setBacktestResult(result)
        // 保存回测结果到 localStorage（持久化）
        try {
          const saveData = {
            ...result,
            _savedAt: new Date().toISOString(),
            _dateRange: [startDate.format('YYYY-MM-DD'), endDate.format('YYYY-MM-DD')]
          }
          localStorage.setItem('modelk_backtest_result', JSON.stringify(saveData))
          console.log('回测结果已保存到 localStorage')
        } catch (e) {
          console.warn('保存回测结果失败:', e)
        }
        message.success(`回测完成！共${result.trades?.length || 0}笔交易，胜率${result.metrics?.win_rate || 0}%`)
      } else {
        message.error(result.message || '回测失败，请尝试缩短日期范围或调整参数')
      }
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || error.message || '回测失败'
      message.error(errorMsg, 8)  // 显示8秒
      console.error('回测错误:', error)
    } finally {
      setBacktestLoading(false)
      setBacktestProgress('')
    }
  }

  const handleGetRecommendations = async () => {
    setRecommendLoading(true)
    try {
      // 如果用户选择了日期，使用用户选择的日期；否则使用null（后端会自动使用最近的交易日）
      const tradeDate = selectedRecommendDate ? selectedRecommendDate.format('YYYY-MM-DD') : undefined
      // 默认限制返回20只，避免超时和返回过多数据
      const result = await modelKApi.getRecommendations(params, tradeDate, 20)
      setRecommendations(result.recommendations || [])
      setRecommendDate(result.trade_date || '')
      setDiagnosticInfo(result.diagnostic_info || '')
      
      // 设置市场状态和漏斗数据
      if (result.metadata) {
        setMarketRegime(result.metadata.market_regime || '')
        setRegimeScore(result.metadata.regime_score || 0)
        setFunnelData(result.metadata.funnel_data || null)
        setRegimeDetails(result.metadata.regime_details || null)
        setBreakoutStats(result.metadata.breakout_stats || null)
        // 调试日志
        if (import.meta.env.DEV) {
          console.log('接收到的metadata:', result.metadata)
        }
      } else {
        setMarketRegime('')
        setRegimeScore(0)
        setFunnelData(null)
        setRegimeDetails(null)
        setBreakoutStats(null)
      }
      // 推荐完成后自动刷新历史记录（立即保存后可见）
      loadHistory()
      
      if (result.count > 0) {
        message.success(`获取到 ${result.count} 只推荐肥羊（${result.trade_date}）`)
      } else {
        const detailMsg = result.diagnostic_info 
          ? `未找到符合条件的推荐肥羊。诊断信息：${result.diagnostic_info}`
          : '未找到符合条件的推荐肥羊，请尝试调整参数或选择其他日期'
        message.warning(detailMsg, 8) // 显示8秒，让用户有时间阅读诊断信息
      }
    } catch (error: any) {
      console.error('获取推荐失败:', error)
      message.error(error.response?.data?.detail || '获取推荐失败')
      setRecommendations([])
      setRecommendDate('')
      setDiagnosticInfo('')
      setMarketRegime('')
      setRegimeScore(0)
      setFunnelData(null)
      setRegimeDetails(null)
      setBreakoutStats(null)
    }
    finally { setRecommendLoading(false) }
  }

  const loadHistory = async () => {
    setHistoryLoading(true)
    try {
      const result = await modelKApi.getHistory(undefined, 100, 0)
      setHistoryData(result.recommendations)
    } catch (error: any) { message.error('加载历史记录失败') }
    finally { setHistoryLoading(false) }
  }

  useEffect(() => { loadHistory() }, [])  // 页面加载时加载历史

  const handleClearHistory = (failedOnly: boolean = false) => {
    Modal.confirm({
      title: failedOnly ? '清空失败记录' : '清空所有历史',
      content: `确定要${failedOnly ? '清空所有失败记录' : '清空所有历史记录'}吗？`,
      onOk: async () => {
        try {
          const result = await modelKApi.clearHistory(failedOnly)
          message.success(result.message)
          loadHistory()
        } catch (error: any) { message.error('清空失败') }
      }
    })
  }

  const getEquityCurveOption = () => {
    if (!backtestResult?.equity_curve) return {}
    const dates = backtestResult.equity_curve.map(item => item.date)
    const returns = backtestResult.equity_curve.map(item => item.return_pct)
    return {
      title: { text: '策略资金曲线', left: 'center' },
      tooltip: { trigger: 'axis', formatter: (params: any) => `${params[0].axisValue}<br/>收益率: ${params[0].value.toFixed(2)}%` },
      xAxis: { type: 'category', data: dates, boundaryGap: false },
      yAxis: { type: 'value', name: '收益率 (%)', axisLabel: { formatter: '{value}%' } },
      series: [{
        name: '策略收益', type: 'line', data: returns, smooth: true,
        areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: 'rgba(102, 126, 234, 0.3)' }, { offset: 1, color: 'rgba(102, 126, 234, 0.1)' }] } },
        lineStyle: { color: '#667eea', width: 2 }
      }],
      grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true }
    }
  }

  // 点击肥羊查看K线图
  const handleStockClick = async (code: string, name: string) => {
    setSelectedStock({ code, name })
    setKlineVisible(true)
    setKlineLoading(true)
    try {
      const [data, capitalFlow] = await Promise.all([
        sheepApi.getSheepDaily(code),
        sheepApi.getCapitalFlow(code, 60).catch(() => [])
      ])
      setKlineData(data || [])
      setKlineCapitalFlowData(Array.isArray(capitalFlow) ? capitalFlow : [])
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

  // 今日推荐不显示涨幅列
  const getRecommendationColumns = () => {
    return [
      { 
        title: '肥羊代码', 
        dataIndex: 'sheep_code', 
        key: 'sheep_code', 
        width: 100, 
        fixed: 'left' as const,
        render: (code: string, record: Recommendation) => (
          <span 
            style={{ cursor: 'pointer', color: '#1890ff' }}
            onClick={() => handleStockClick(code, record.sheep_name)}
          >
            {code}
          </span>
        )
      },
      { 
        title: '肥羊名称', 
        dataIndex: 'sheep_name', 
        key: 'sheep_name', 
        width: 100,
        render: (name: string, record: Recommendation) => (
          <span 
            style={{ cursor: 'pointer', color: '#1890ff', fontWeight: 'bold' }}
            onClick={() => handleStockClick(record.sheep_code, name)}
          >
            <LineChartOutlined style={{ marginRight: 4 }} />
            {name}
          </span>
        )
      },
      { title: '现价', dataIndex: 'entry_price', key: 'entry_price', width: 80, render: (price: number) => `¥${price.toFixed(2)}` },
      { 
        title: '市值', 
        dataIndex: 'estimated_mv', 
        key: 'estimated_mv', 
        width: 80, 
        render: (mv: number | undefined) => {
          if (!mv || mv <= 0) return '-'
          return <span style={{ color: mv < 100 ? '#52c41a' : mv > 500 ? '#ff4d4f' : '#1890ff' }}>
            {mv.toFixed(0)}亿
          </span>
        }
      },
      { title: 'AI打分', dataIndex: 'ai_score', key: 'ai_score', width: 90, render: (score: number) => <Tag color={score > 50 ? 'green' : score > 30 ? 'orange' : 'red'}>{score.toFixed(1)}</Tag> },
      { 
        title: '启动质量', 
        dataIndex: 'breakout_quality', 
        key: 'breakout_quality', 
        width: 100, 
        render: (quality: number | undefined, record: Recommendation) => {
          const q = quality || record.win_probability || 50
          const warning = record.breakout_warning
          return (
            <Tooltip title={warning ? `风险: ${warning}` : '启动质量评估'}>
              <Tag color={q >= 70 ? 'green' : q >= 40 ? 'orange' : 'red'}>
                {q >= 70 ? '优质' : q >= 40 ? '一般' : '风险'} {q.toFixed(0)}
              </Tag>
            </Tooltip>
          )
        }
      },
      { 
        title: '市场状态', 
        dataIndex: 'market_regime', 
        key: 'market_regime', 
        width: 90, 
        render: (regime: string) => {
          if (!regime) return '-'
          const colorMap: Record<string, string> = {
            'Attack': 'red',
            'Defense': 'blue',
            'Balance': 'default'
          }
          const textMap: Record<string, string> = {
            'Attack': '进攻',
            'Defense': '防守',
            'Balance': '震荡'
          }
          return <Tag color={colorMap[regime] || 'default'}>{textMap[regime] || regime}</Tag>
        }
      },
      { 
        title: '驱动概念', 
        dataIndex: 'concept_trend', 
        key: 'concept_trend', 
        width: 120, 
        ellipsis: true,
        render: (concept: string, record: Recommendation) => {
          const conceptName = concept || record.sector_trend || '-'
          if (conceptName === '-') return '-'
          
          // 显示驱动概念徽章，hover时显示资金流信息
          const inflow = record.tag_total_inflow
          const avgPct = record.tag_avg_pct
          
          return (
            <Tooltip 
              title={
                <div>
                  <div>概念: {conceptName}</div>
                  {inflow !== undefined && <div>板块资金流入: {inflow > 0 ? '+' : ''}{(inflow / 10000).toFixed(2)}亿元</div>}
                  {avgPct !== undefined && <div>板块平均涨幅: {avgPct > 0 ? '+' : ''}{avgPct.toFixed(2)}%</div>}
                </div>
              }
            >
              <Tag color="blue">{conceptName}</Tag>
            </Tooltip>
          )
        }
      },
      {
        title: '弹性',
        key: 'elasticity',
        width: 80,
        render: (_: any, record: Recommendation) => {
          const is20cm = record.is_star_market || record.is_gem
          if (is20cm) {
            return <Tag color="purple">20cm</Tag>
          }
          return '-'
        }
      },
      { 
        title: '共振分', 
        dataIndex: 'resonance_score', 
        key: 'resonance_score', 
        width: 90, 
        render: (score: number | undefined) => {
          if (score === undefined || score === null) return '-'
          return <Tag color={score > 0 ? 'green' : score < 0 ? 'red' : 'default'}>{score > 0 ? '+' : ''}{score.toFixed(0)}</Tag>
        }
      },
      { title: '核心理由', dataIndex: 'reason_tags', key: 'reason_tags', width: 200, ellipsis: true },
      { title: '止损价', dataIndex: 'stop_loss_price', key: 'stop_loss_price', width: 90, render: (price: number) => `¥${price.toFixed(2)}` }
    ]
  }

  // 历史战绩表格列（紧凑布局）
  const historyColumns = [
    { title: '日期', dataIndex: 'run_date', key: 'run_date', width: 90 },
    { 
      title: '名称', 
      dataIndex: 'sheep_name', 
      key: 'sheep_name', 
      width: 120, 
      ellipsis: true,
      render: (name: string, record: RecommendationHistory) => (
        <span 
          style={{ cursor: 'pointer', color: '#1890ff' }}
          onClick={() => handleStockClick(record.sheep_code, name)}
        >
          {name}
        </span>
      )
    },
    { title: '参数', dataIndex: 'params_snapshot', key: 'params_snapshot', width: 70, 
      render: (p: BacktestParams) => <Tooltip title={`倍量:${p?.vol_threshold}x RPS:${p?.rps_threshold}`}><span style={{ fontSize: '11px' }}>{p?.vol_threshold}x/{p?.rps_threshold}</span></Tooltip> 
    },
    { 
      title: <Tooltip title="买入：推荐日收盘价 | 计算：后5个交易日内最高价涨幅"><span>最大涨幅 <QuestionCircleOutlined style={{ fontSize: '10px', color: '#999' }} /></span></Tooltip>, 
      dataIndex: 'max_return_5d', key: 'max_return_5d', width: 80, 
      render: (pct: number | undefined, r: RecommendationHistory) => {
        if (!r.is_verified) return <span style={{ color: '#999', fontSize: '11px' }}>未验证</span>
        const p = pct || 0
        return <span style={{ color: p > 0 ? '#ff4d4f' : p < 0 ? '#52c41a' : '#999' }}>{p > 0 ? '+' : ''}{p.toFixed(1)}%</span>
      }
    },
    { 
      title: <Tooltip title="买入：推荐日收盘价 | 计算：第5个交易日收盘价涨幅"><span>最终涨幅 <QuestionCircleOutlined style={{ fontSize: '10px', color: '#999' }} /></span></Tooltip>, 
      dataIndex: 'final_return_5d', key: 'final_return_5d', width: 80, 
      render: (pct: number | undefined, r: RecommendationHistory) => {
        if (!r.is_verified) return <span style={{ color: '#999', fontSize: '11px' }}>未验证</span>
        const p = pct || 0
        return <span style={{ color: p > 0 ? '#ff4d4f' : p < 0 ? '#52c41a' : '#999' }}>{p > 0 ? '+' : ''}{p.toFixed(1)}%</span>
      }
    },
    { 
      title: <Tooltip title="成功标准：5日最终涨幅>5%"><span>结果 <QuestionCircleOutlined style={{ fontSize: '10px', color: '#999' }} /></span></Tooltip>, 
      dataIndex: 'final_result', key: 'final_result', width: 55, 
      render: (result: string | undefined) => {
        if (!result) return <span style={{ color: '#999', fontSize: '11px' }}>-</span>
        return <span style={{ color: result === 'SUCCESS' ? '#52c41a' : '#ff4d4f', fontWeight: 600 }}>{result === 'SUCCESS' ? '✓' : '✗'}</span>
      }
    }
  ]

  // 筛选过程展示组件
  const renderFilterProcess = () => {
    // 如果没有数据，返回空数组而不是null，避免渲染错误
    if (!funnelData && !regimeDetails) {
      return <div style={{ display: 'none' }}></div>
    }
    
    return (
      <Card size="small" style={{ marginBottom: '16px', background: '#fafafa' }}>
        <Row gutter={[16, 8]}>
          {/* 筛选漏斗 - v6.0重构版 */}
          <Col xs={24} md={12}>
            <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>📊 筛选漏斗 <span style={{ fontSize: '11px', color: '#999', fontWeight: 'normal' }}>v6.0</span></div>
            {funnelData && (
              <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', alignItems: 'center', fontSize: '12px' }}>
                <Tooltip title="全市场活跃肥羊">
                  <Tag color="blue">全市场 {funnelData.total}</Tag>
                </Tooltip>
                <span>→</span>
                <Tooltip title="Filter Layer: SQL层筛选(ST/新股/市值/RPS)">
                  <Tag color="cyan">Filter {funnelData.L0_pass || funnelData.L1_pass}</Tag>
                </Tooltip>
                <span>→</span>
                <Tooltip title="Feature Layer: 因子提取(技术/资金/概念)">
                  <Tag color="geekblue">Feature {funnelData.L1_pass}</Tag>
                </Tooltip>
                <span>→</span>
                <Tooltip title="Score + Validate: 多因子评分+启动验证">
                  <Tag color="orange">Score {funnelData.L2_pass}</Tag>
                </Tooltip>
                <span>→</span>
                <Tooltip title="Final Filter: 涌幅/概念共振/量比筛选">
                  <Tag color="purple">Final {funnelData.L3_pass || funnelData.final}</Tag>
                </Tooltip>
                <span>→</span>
                <Tooltip title="符合AI评分+启动质量门槛">
                  <Tag color="green">优选 {funnelData.final}</Tag>
                </Tooltip>
              </div>
            )}
          </Col>
          
          {/* 市场状态详情 */}
          <Col xs={24} md={12}>
            <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>
              📈 市场状态: 
              <Tag 
                color={marketRegime === 'Attack' ? 'green' : marketRegime === 'Defense' ? 'red' : 'default'}
                style={{ marginLeft: '8px' }}
              >
                {marketRegime === 'Attack' ? '进攻' : marketRegime === 'Defense' ? '防守' : '震荡'}
              </Tag>
              <span style={{ fontSize: '12px', color: '#999', marginLeft: '8px' }}>
                综合评分: {(regimeScore * 100).toFixed(0)}%
              </span>
            </div>
            {regimeDetails && (
              <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap', fontSize: '12px' }}>
                {regimeDetails.sector_rotation_score !== undefined && (
                  <Tooltip title="v6.0新增：板块轮动强度（替代RSRS在结构性牛市中的判断）">
                    <Tag color="purple">轮动 {((regimeDetails.sector_rotation_score || 0) * 100).toFixed(0)}%</Tag>
                  </Tooltip>
                )}
                {regimeDetails.rsrs_score !== undefined && (
                  <Tooltip title="支撑阻力相对强度（v6.0权重降低）">
                    <Tag>RSRS {((regimeDetails.rsrs_score || 0) * 100).toFixed(0)}%</Tag>
                  </Tooltip>
                )}
                {(regimeDetails.up_count !== undefined || regimeDetails.down_count !== undefined) && (
                  <Tooltip title={`涨${regimeDetails.up_count || 0} 跌${regimeDetails.down_count || 0}`}>
                    <Tag>宽度 {((regimeDetails.market_breadth_score || 0) * 100).toFixed(0)}%</Tag>
                  </Tooltip>
                )}
                {regimeDetails.ma_score !== undefined && (
                  <Tooltip title="均线多空排列">
                    <Tag>均线 {((regimeDetails.ma_score || 0) * 100).toFixed(0)}%</Tag>
                  </Tooltip>
                )}
                {(regimeDetails.limit_up_count !== undefined || regimeDetails.limit_down_count !== undefined) && (
                  <Tooltip title={`涨停${regimeDetails.limit_up_count || 0} 跌停${regimeDetails.limit_down_count || 0}`}>
                    <Tag>情绪 {((regimeDetails.sentiment_score || 0) * 100).toFixed(0)}%</Tag>
                  </Tooltip>
                )}
              </div>
            )}
          </Col>
          
          {/* 启动质量统计 */}
          {breakoutStats && (
            <Col xs={24}>
              <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>🎯 启动质量分布</div>
              <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                {breakoutStats.high_quality_count !== undefined && (
                  <Tag color="green">优质启动 {breakoutStats.high_quality_count}只</Tag>
                )}
                {breakoutStats.medium_quality_count !== undefined && (
                  <Tag color="orange">一般质量 {breakoutStats.medium_quality_count}只</Tag>
                )}
                {breakoutStats.trap_risk_count !== undefined && (
                  <Tag color="red">骗炮风险 {breakoutStats.trap_risk_count}只</Tag>
                )}
              </div>
            </Col>
          )}
        </Row>
      </Card>
    )
  }

  const tabItems: TabsProps['items'] = [
    {
      key: 'recommend',
      label: '今日推荐',
      children: (
        <Card>
          {recommendations.length > 0 ? <>
            {/* v6.0: 筛选过程展示 */}
            {renderFilterProcess()}
            
            <div style={{ marginBottom: '16px', display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
              <div style={{ color: '#666' }}>推荐日期: {recommendDate}</div>
              <div style={{ color: '#999', fontSize: '12px' }}>共 {recommendations.length} 只推荐</div>
            </div>
            <Table 
              columns={getRecommendationColumns()} 
              dataSource={recommendations} 
              rowKey="sheep_code" 
              pagination={false}
              scroll={{ x: 'max-content' }}
            />
          </> : (
            <div>
              {recommendDate && diagnosticInfo ? (
                <Alert
                  message="未找到符合条件的推荐肥羊"
                  description={
                    <div>
                      <div style={{ marginBottom: '8px' }}>推荐日期: {recommendDate}</div>
                      <div style={{ fontSize: '12px', color: '#666' }}>
                        <strong>诊断信息：</strong>{diagnosticInfo}
                      </div>
                      <div style={{ marginTop: '12px', fontSize: '12px' }}>
                        <strong>建议：</strong>
                        <ul style={{ margin: '8px 0', paddingLeft: '20px' }}>
                          {diagnosticInfo.includes('市场状态') && <li>当前市场状态可能不适合当前策略，建议根据市场状态调整参数</li>}
                          {diagnosticInfo.includes('Level 2') && <li>尝试放宽MA条件（如从MA60改为MA20）或检查上市天数限制</li>}
                          {diagnosticInfo.includes('Level 4') && <li>尝试关闭AI过滤或降低胜率要求</li>}
                          {diagnosticInfo.includes('Level 1') && <li>检查数据库是否有足够的历史数据（至少90天）</li>}
                          {diagnosticInfo.includes('板块共振') && <li>当前可能缺乏板块共振，建议关注主线板块</li>}
                          <li>尝试选择其他日期或调整其他参数</li>
                        </ul>
                      </div>
                    </div>
                  }
                  type="warning"
                  showIcon
                  style={{ marginBottom: '16px' }}
                />
              ) : (
                <div style={{ textAlign: 'center', padding: '40px', color: '#999' }}>请点击"智能推荐"获取推荐结果</div>
              )}
            </div>
          )}
        </Card>
      )
    },
    {
      key: 'backtest',
      label: '回测仪表盘',
      children: (
        <Card>
          {/* 回测提示信息 */}
          <Alert
            message="回测使用指南"
            description={
              <ul style={{ margin: '8px 0', paddingLeft: '20px', fontSize: '12px' }}>
                <li>建议日期范围：<strong>1-3个月</strong>（7-90天），范围过长可能导致超时</li>
                <li>首次回测建议使用默认参数，确认成功后再调整</li>
                <li>回测结果会自动保存，刷新页面后可恢复</li>
                <li>如果连续失败，请尝试：缩短日期范围、放宽筛选参数</li>
              </ul>
            }
            type="info"
            showIcon
            closable
            style={{ marginBottom: '16px' }}
          />
          
          <div style={{ marginBottom: '16px', display: 'flex', flexDirection: isMobile ? 'column' : 'row', gap: '12px', alignItems: 'center', flexWrap: 'wrap' }}>
            <div style={{ width: '100%' }}>
              <div style={{ marginBottom: '4px', fontSize: '14px', color: '#666' }}>
                选择回测时间范围
                <span style={{ marginLeft: '8px', fontSize: '12px', color: '#999' }}>
                  （{dateRange[0] && dateRange[1] ? `${dateRange[1].diff(dateRange[0], 'day')}天` : '-'}）
                </span>
              </div>
              <RangePicker 
                value={dateRange} 
                onChange={(dates) => { 
                  if (dates && dates[0] && dates[1]) setDateRange([dates[0], dates[1]]) 
                }} 
                format="YYYY-MM-DD"
                presets={[{ label: '最近1个月', value: [dayjs().subtract(1, 'month'), dayjs().subtract(1, 'day')] }, { label: '最近3个月', value: [dayjs().subtract(3, 'month'), dayjs().subtract(1, 'day')] }, { label: '最近6个月', value: [dayjs().subtract(6, 'month'), dayjs().subtract(1, 'day')] }]}
                style={{ width: '100%' }}
              />
            </div>
            <Button 
              type="primary" 
              icon={<ThunderboltOutlined />} 
              onClick={handleBacktest} 
              loading={backtestLoading}
              size="large"
              style={{ marginTop: '24px' }}
            >
              {backtestLoading ? '回测中...' : '执行回测 (Time Travel)'}
            </Button>
          </div>
          
          {/* 回测进度显示 */}
          {backtestLoading && backtestProgress && (
            <Alert
              message={backtestProgress}
              type="warning"
              showIcon
              style={{ marginBottom: '16px' }}
            />
          )}
          {backtestResult?.success && <>
            {/* 回测结果标题栏 */}
            <div style={{ marginBottom: '16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '8px' }}>
              <div>
                <Tag color="green">回测成功</Tag>
                {(backtestResult as any)._dateRange && (
                  <span style={{ fontSize: '12px', color: '#666', marginLeft: '8px' }}>
                    {(backtestResult as any)._dateRange[0]} ~ {(backtestResult as any)._dateRange[1]}
                  </span>
                )}
                {(backtestResult as any)._savedAt && (
                  <span style={{ fontSize: '12px', color: '#999', marginLeft: '8px' }}>
                    （保存于 {new Date((backtestResult as any)._savedAt).toLocaleString()}）
                  </span>
                )}
              </div>
              <Button 
                size="small" 
                danger 
                onClick={() => {
                  setBacktestResult(null)
                  try {
                    localStorage.removeItem('modelk_backtest_result')
                    message.success('已清除回测结果')
                  } catch (e) {
                    console.warn('清除失败:', e)
                  }
                }}
              >
                清除结果
              </Button>
            </div>
            
            {/* 核心指标 */}
            <Row gutter={16} style={{ marginBottom: '16px' }}>
              <Col xs={12} sm={6}>
                <Statistic 
                  title={
                    <Tooltip title="成功定义：最大涨幅≥3%且最终不亏（v2.0优化）">
                      <span>胜率 <QuestionCircleOutlined style={{ fontSize: '12px' }} /></span>
                    </Tooltip>
                  } 
                  value={backtestResult.metrics?.win_rate || 0} 
                  suffix="%" 
                  valueStyle={{ color: '#3f8600' }} 
                />
              </Col>
              <Col xs={12} sm={6}>
                <Statistic 
                  title={
                    <Tooltip title="最大涨幅≥10%且最终盈利≥3%">
                      <span>爆款率 <QuestionCircleOutlined style={{ fontSize: '12px' }} /></span>
                    </Tooltip>
                  } 
                  value={backtestResult.metrics?.alpha_rate || 0} 
                  suffix="%" 
                  valueStyle={{ color: '#cf1322' }} 
                />
              </Col>
              <Col xs={12} sm={6}><Statistic title="总收益率" value={backtestResult.metrics?.total_return || 0} suffix="%" valueStyle={{ color: (backtestResult.metrics?.total_return || 0) >= 0 ? '#3f8600' : '#cf1322' }} /></Col>
              <Col xs={12} sm={6}><Statistic title="最大回撤" value={backtestResult.metrics?.max_drawdown || 0} suffix="%" valueStyle={{ color: '#cf1322' }} /></Col>
            </Row>
            
            {/* v2.0新增：详细分层指标 */}
            <Card size="small" style={{ marginBottom: '16px', background: '#fafafa' }}>
              <Row gutter={[16, 8]}>
                <Col span={24}>
                  <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>📊 分层胜率统计（v2.0改进）</div>
                </Col>
                <Col xs={8} sm={4}>
                  <Statistic 
                    title={<span style={{ fontSize: '12px' }}>优秀</span>}
                    value={backtestResult.metrics?.excellent_rate || 0} 
                    suffix="%" 
                    valueStyle={{ fontSize: '16px', color: '#52c41a' }} 
                  />
                  <div style={{ fontSize: '10px', color: '#999' }}>最大≥10%,终≥3%</div>
                </Col>
                <Col xs={8} sm={4}>
                  <Statistic 
                    title={<span style={{ fontSize: '12px' }}>良好</span>}
                    value={backtestResult.metrics?.good_rate || 0} 
                    suffix="%" 
                    valueStyle={{ fontSize: '16px', color: '#1890ff' }} 
                  />
                  <div style={{ fontSize: '10px', color: '#999' }}>最大≥5%,终≥2%</div>
                </Col>
                <Col xs={8} sm={4}>
                  <Statistic 
                    title={<span style={{ fontSize: '12px' }}>及格</span>}
                    value={backtestResult.metrics?.pass_rate || 0} 
                    suffix="%" 
                    valueStyle={{ fontSize: '16px', color: '#faad14' }} 
                  />
                  <div style={{ fontSize: '10px', color: '#999' }}>最大≥3%,不亏</div>
                </Col>
                <Col xs={8} sm={4}>
                  <Statistic 
                    title={<span style={{ fontSize: '12px' }}>平均收益</span>}
                    value={backtestResult.metrics?.avg_return || 0} 
                    suffix="%" 
                    valueStyle={{ fontSize: '16px', color: (backtestResult.metrics?.avg_return || 0) >= 0 ? '#3f8600' : '#cf1322' }} 
                  />
                </Col>
                <Col xs={8} sm={4}>
                  <Statistic 
                    title={<span style={{ fontSize: '12px' }}>超额收益</span>}
                    value={backtestResult.metrics?.excess_return || 0} 
                    suffix="%" 
                    valueStyle={{ fontSize: '16px', color: (backtestResult.metrics?.excess_return || 0) >= 0 ? '#3f8600' : '#cf1322' }} 
                  />
                </Col>
                <Col xs={8} sm={4}>
                  <Statistic 
                    title={<span style={{ fontSize: '12px' }}>止损触发</span>}
                    value={backtestResult.metrics?.stop_loss_rate || 0} 
                    suffix="%" 
                    valueStyle={{ fontSize: '16px', color: '#ff4d4f' }} 
                  />
                </Col>
              </Row>
            </Card>
            
            <Card title="资金曲线" style={{ marginBottom: '24px' }}>
              <ReactECharts option={getEquityCurveOption()} style={{ height: '400px' }} />
            </Card>
          </>}
          {!backtestResult && <div style={{ textAlign: 'center', padding: '40px', color: '#999' }}>请选择日期范围并点击"执行回测"</div>}
        </Card>
      )
    },
    {
      key: 'history',
      label: '历史战绩',
      children: (
        <Card 
          extra={
            <Space>
              <Button size="small" icon={<ClearOutlined />} onClick={() => handleClearHistory(true)}>清空失败记录</Button>
              <Button size="small" danger icon={<DeleteOutlined />} onClick={() => handleClearHistory(false)}>清空所有记录</Button>
            </Space>
          }
        >
          <div style={{ marginBottom: '12px', padding: '8px 12px', background: '#fafafa', borderRadius: '4px', fontSize: '12px', color: '#666' }}>
            <strong>计算说明：</strong>
            模拟买入价 = 推荐日收盘价 | 
            最大涨幅 = 后5交易日内最高价涨幅 | 
            最终涨幅 = 第5交易日收盘价涨幅 | 
            成功标准 = 最终涨幅 &gt; 5%
          </div>
          <Table 
            columns={historyColumns} 
            dataSource={historyData} 
            rowKey="id" 
            loading={historyLoading} 
            pagination={{ pageSize: 20, showTotal: (total) => `共 ${total} 条记录` }}
            scroll={{ x: 'max-content' }}
          />
        </Card>
      )
    }
  ]

  return (
    <div style={{ padding: '24px' }}>
      <div style={{ marginBottom: '24px' }}>
        <h1 style={{ margin: 0, fontSize: '24px', fontWeight: 'bold' }}>
          <RocketOutlined style={{ marginRight: '8px', color: '#667eea' }} />
          模型老K为您服务
        </h1>
        <p style={{ marginTop: '8px', color: '#666', fontSize: '13px' }}>
          v6.0重构版 | 因子正交化 | Z-Score标准化 | 动态权重 {paramsLoaded && <span style={{ color: '#52c41a' }}>✓ 参数已同步</span>}
        </p>
      </div>
      <Row gutter={24}>
        <Col xs={24} lg={8} xl={6}>
          <Card 
            title={
              <Space>
                <SettingOutlined />
                <span>策略参数配置</span>
              </Space>
            } 
            style={{ marginBottom: '24px' }}
            extra={
              <Tooltip title="调整参数以优化筛选结果">
                <QuestionCircleOutlined style={{ color: '#999' }} />
              </Tooltip>
            }
          >
            <Collapse defaultActiveKey={['basic']} ghost expandIconPosition="end">
              {/* 基础筛选参数 */}
              <Panel header={<span style={{ fontWeight: 'bold' }}>基础设置</span>} key="basic">
                <Space direction="vertical" style={{ width: '100%' }} size="middle">
                  <div>
                    <div style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <span>最小市值 (亿元): {params.min_mv}</span>
                    </div>
                    <Slider 
                      value={params.min_mv} 
                      onChange={(val) => setParams({ ...params, min_mv: val })} 
                      min={10} 
                      max={500} 
                      marks={{ 10: '10', 100: '100', 500: '500' }} 
                    />
                  </div>

                  <div>
                    <div style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <span>RPS强度 (250日): {params.rps_threshold}</span>
                    </div>
                    <Slider 
                      value={params.rps_threshold} 
                      onChange={(val) => setParams({ ...params, rps_threshold: val })} 
                      min={50} 
                      max={95} 
                      marks={{ 50: '50', 75: '75', 90: '90' }} 
                    />
                  </div>

                  <div>
                    <div style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <span>倍量倍数: {params.vol_threshold}x</span>
                    </div>
                    <Slider 
                      value={params.vol_threshold} 
                      onChange={(val) => setParams({ ...params, vol_threshold: val })} 
                      min={1.0} 
                      max={3.0} 
                      step={0.1} 
                      marks={{ 1: '1x', 1.5: '1.5x', 2: '2x', 3: '3x' }} 
                    />
                  </div>
                </Space>
              </Panel>

              {/* 进阶策略参数 */}
              <Panel header={<span style={{ fontWeight: 'bold' }}>进阶策略</span>} key="advanced">
                <Space direction="vertical" style={{ width: '100%' }} size="middle">
                  <div>
                    <div style={{ marginBottom: '4px', fontSize: '12px', display: 'flex', justifyContent: 'space-between' }}>
                      <span>最低AI评分</span>
                      <span style={{ color: '#1890ff' }}>{params.min_ai_score || 40}分</span>
                    </div>
                    <Slider 
                      value={params.min_ai_score || 40} 
                      onChange={(val) => setParams({ ...params, min_ai_score: val })} 
                      min={30} max={70} step={5}
                      marks={{ 30: '宽', 50: '严', 70: '极' }}
                    />
                  </div>

                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: '12px' }}>主力净流入必须为正</span>
                    <Switch 
                      size="small"
                      checked={params.require_positive_inflow || false} 
                      onChange={(checked) => setParams({ ...params, require_positive_inflow: checked })} 
                    />
                  </div>

                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Tooltip title="基于成交量、K线形态、资金流等多维度验证启动质量">
                      <span style={{ fontSize: '12px', cursor: 'help' }}>启动质量验证 <QuestionCircleOutlined style={{ fontSize: '10px' }} /></span>
                    </Tooltip>
                    <Switch 
                      size="small"
                      checked={params.breakout_validation !== false} 
                      onChange={(checked) => setParams({ ...params, breakout_validation: checked })} 
                    />
                  </div>
                  
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: '12px' }}>优先创业板/科创板</span>
                    <Switch 
                      size="small"
                      checked={params.prefer_20cm !== false} 
                      onChange={(checked) => setParams({ ...params, prefer_20cm: checked })} 
                    />
                  </div>

                  <Divider style={{ margin: '4px 0' }} />
                  <div style={{ fontSize: '11px', color: '#999' }}>
                    * 更多参数(换手、涨幅)已根据市场状态自动适配
                  </div>
                </Space>
              </Panel>
            </Collapse>

            <Divider style={{ margin: '16px 0' }} />

            <div>
              <div style={{ marginBottom: '8px' }}>推荐日期（留空使用最近交易日）</div>
              <DatePicker 
                value={selectedRecommendDate}
                onChange={(date) => setSelectedRecommendDate(date)}
                format="YYYY-MM-DD"
                style={{ width: '100%' }}
                placeholder="选择日期（留空使用最近交易日）"
                disabledDate={(current) => current && current > dayjs().endOf('day')}
              />
            </div>

            <div style={{ marginTop: '16px' }}>
              <Popover
                content={
                  recommendLoading ? (
                    <div style={{ fontSize: '13px', minWidth: '260px' }}>
                      <div style={{ marginBottom: '10px', fontWeight: 'bold', color: '#1890ff' }}>
                        🔄 智能筛选执行中...
                      </div>
                      {EXECUTION_STEPS.map((step, idx) => {
                        const isActive = idx === currentStep
                        const isDone = idx < currentStep
                        return (
                          <div 
                            key={idx}
                            style={{ 
                              display: 'flex', 
                              alignItems: 'center', 
                              marginBottom: '6px',
                              padding: '4px 8px',
                              borderRadius: '4px',
                              background: isActive ? '#e6f7ff' : 'transparent',
                              transition: 'all 0.3s'
                            }}
                          >
                            <span style={{ 
                              width: '20px', 
                              height: '20px', 
                              borderRadius: '50%', 
                              display: 'flex', 
                              alignItems: 'center', 
                              justifyContent: 'center',
                              fontSize: '11px',
                              marginRight: '8px',
                              background: isDone ? '#52c41a' : isActive ? '#1890ff' : '#d9d9d9',
                              color: '#fff',
                              fontWeight: 'bold'
                            }}>
                              {isDone ? '✓' : idx + 1}
                            </span>
                            <div style={{ flex: 1 }}>
                              <div style={{ 
                                fontWeight: isActive ? 'bold' : 'normal',
                                color: isActive ? '#1890ff' : isDone ? '#52c41a' : '#666'
                              }}>
                                {step.name}
                              </div>
                              <div style={{ fontSize: '11px', color: '#999' }}>{step.desc}</div>
                            </div>
                            {isActive && <span style={{ color: '#1890ff' }}>⏳</span>}
                          </div>
                        )
                      })}
                      <div style={{ 
                        marginTop: '8px', 
                        paddingTop: '8px', 
                        borderTop: '1px dashed #eee',
                        fontSize: '11px', 
                        color: '#999',
                        textAlign: 'center'
                      }}>
                        预计总耗时 10-30秒
                      </div>
                    </div>
                  ) : funnelData && ((funnelData.total !== undefined && funnelData.total > 0) || (funnelData.L1_pass !== undefined && funnelData.L1_pass > 0)) ? (
                    <div style={{ fontSize: '13px', minWidth: '300px' }}>
                      <div style={{ marginBottom: '10px', fontWeight: 'bold', color: '#52c41a' }}>
                        ✅ 筛选完成 - v6.0 漏斗详情
                      </div>
                      {/* 漏斗流程图 - v6.0 Pipeline */}
                      <div style={{ display: 'flex', flexDirection: 'column', gap: '2px' }}>
                        {funnelData.total !== undefined && (
                          <div style={{ display: 'flex', alignItems: 'center', padding: '4px 0' }}>
                            <div style={{ width: '100px', fontSize: '12px' }}>① 全市场</div>
                            <div style={{ flex: 1, background: '#e6f7ff', borderRadius: '4px', padding: '2px 8px', textAlign: 'right' }}>
                              <strong style={{ color: '#1890ff' }}>{(funnelData.total || 0).toLocaleString()}</strong> 只
                            </div>
                          </div>
                        )}
                        {(funnelData.L0_pass !== undefined || funnelData.L1_pass !== undefined) && (
                          <>
                            <div style={{ textAlign: 'center', color: '#ccc', fontSize: '10px' }}>↓ Filter Layer: SQL层筛选</div>
                            <div style={{ display: 'flex', alignItems: 'center', padding: '4px 0' }}>
                              <div style={{ width: '100px', fontSize: '12px' }}>② Filter</div>
                              <Tooltip title="SQL层筛选: 排除ST/新股/涨停/市值(10-1000亿)/RPS≥50">
                                <QuestionCircleOutlined style={{ fontSize: '10px', color: '#999', marginLeft: '4px' }} />
                              </Tooltip>
                              <div style={{ flex: 1, background: '#e6fffb', borderRadius: '4px', padding: '2px 8px', textAlign: 'right' }}>
                                <strong style={{ color: '#13c2c2' }}>{(funnelData.L0_pass || funnelData.L1_pass || 0).toLocaleString()}</strong> 只
                                <span style={{ fontSize: '10px', color: '#999', marginLeft: '4px' }}>
                                  ({(funnelData.total || 0) > 0 ? (((funnelData.L0_pass || funnelData.L1_pass || 0) / (funnelData.total || 1)) * 100).toFixed(1) : 0}%)
                                </span>
                              </div>
                            </div>
                          </>
                        )}
                        {funnelData.L1_pass !== undefined && (
                          <>
                            <div style={{ textAlign: 'center', color: '#ccc', fontSize: '10px' }}>↓ Feature Layer: 因子提取</div>
                            <div style={{ display: 'flex', alignItems: 'center', padding: '4px 0' }}>
                              <div style={{ width: '100px', fontSize: '12px' }}>③ Feature</div>
                              <Tooltip title="因子提取: 技术因子/资金因子/概念因子独立计算">
                                <QuestionCircleOutlined style={{ fontSize: '10px', color: '#999', marginLeft: '4px' }} />
                              </Tooltip>
                              <div style={{ flex: 1, background: '#f6ffed', borderRadius: '4px', padding: '2px 8px', textAlign: 'right' }}>
                                <strong style={{ color: '#52c41a' }}>{(funnelData.L1_pass || 0).toLocaleString()}</strong> 只
                              </div>
                            </div>
                          </>
                        )}
                        {funnelData.L2_pass !== undefined && (
                          <>
                            <div style={{ textAlign: 'center', color: '#ccc', fontSize: '10px' }}>↓ Score + Validate: 评分+验证</div>
                            <div style={{ display: 'flex', alignItems: 'center', padding: '4px 0' }}>
                              <div style={{ width: '100px', fontSize: '12px' }}>④ Score</div>
                              <Tooltip title="Z-Score标准化 + 动态权重 + 启动质量验证(扣分制)">
                                <QuestionCircleOutlined style={{ fontSize: '10px', color: '#999', marginLeft: '4px' }} />
                              </Tooltip>
                              <div style={{ flex: 1, background: '#fffbe6', borderRadius: '4px', padding: '2px 8px', textAlign: 'right' }}>
                                <strong style={{ color: '#faad14' }}>{funnelData.L2_pass || 0}</strong> 只
                                <span style={{ fontSize: '10px', color: '#999', marginLeft: '4px' }}>
                                  ({(funnelData.L1_pass || 0) > 0 ? (((funnelData.L2_pass || 0) / (funnelData.L1_pass || 1)) * 100).toFixed(1) : 0}%)
                                </span>
                              </div>
                            </div>
                          </>
                        )}
                        {(funnelData.L3_pass !== undefined || funnelData.final !== undefined) && (
                          <>
                            <div style={{ textAlign: 'center', color: '#ccc', fontSize: '10px' }}>↓ Final Filter: 最终筛选</div>
                            <div style={{ display: 'flex', alignItems: 'center', padding: '4px 0' }}>
                              <div style={{ width: '100px', fontSize: '12px' }}>⑤ Final</div>
                              <Tooltip title="最终筛选: 涨幅、概念共振、量比、换手率筛选">
                                <QuestionCircleOutlined style={{ fontSize: '10px', color: '#999', marginLeft: '4px' }} />
                              </Tooltip>
                              <div style={{ flex: 1, background: '#f9f0ff', borderRadius: '4px', padding: '2px 8px', textAlign: 'right' }}>
                                <strong style={{ color: '#722ed1' }}>{funnelData.L3_pass || funnelData.final || 0}</strong> 只
                              </div>
                            </div>
                          </>
                        )}
                        {funnelData.final !== undefined && (
                          <>
                            <div style={{ textAlign: 'center', color: '#ccc', fontSize: '10px' }}>↓ AI评分 + 质量门槛</div>
                            <div style={{ display: 'flex', alignItems: 'center', padding: '4px 0' }}>
                              <div style={{ width: '100px', fontSize: '12px' }}>⑥ 优选推荐</div>
                              <Tooltip title="AI评分≥50，启动质量≥55，TopN限制">
                                <QuestionCircleOutlined style={{ fontSize: '10px', color: '#999', marginLeft: '4px' }} />
                              </Tooltip>
                              <div style={{ flex: 1, background: '#fff1f0', borderRadius: '4px', padding: '2px 8px', textAlign: 'right' }}>
                                <strong style={{ color: '#ff4d4f', fontSize: '14px' }}>{funnelData.final || 0}</strong> 只
                              </div>
                            </div>
                          </>
                        )}
                      </div>
                      {/* 筛选统计 */}
                      {(funnelData.final !== undefined && funnelData.total !== undefined) && (
                        <div style={{ 
                          marginTop: '10px', 
                          paddingTop: '8px', 
                          borderTop: '1px dashed #eee',
                          display: 'flex',
                          justifyContent: 'space-between',
                          fontSize: '11px',
                          color: '#666'
                        }}>
                          <span>总筛选率: <strong>{(funnelData.total || 0) > 0 ? (((funnelData.final || 0) / (funnelData.total || 1)) * 100).toFixed(3) : 0}%</strong></span>
                          <span>淘汰: <strong>{(funnelData.total || 0) - (funnelData.final || 0)}</strong> 只</span>
                        </div>
                      )}
                    </div>
                  ) : (
                    <div style={{ fontSize: '13px', color: '#666', minWidth: '240px' }}>
                      <div style={{ marginBottom: '8px', fontWeight: 'bold' }}>📋 点击开始智能推荐</div>
                      <div style={{ fontSize: '12px', color: '#999' }}>
                        <div>• T7概念资金双驱模型 v6.0</div>
                        <div>• 5层管道: Filter→Feature→Score→Validate→Final</div>
                        <div>• Z-Score标准化 + 动态权重</div>
                        <div>• 因子正交化，避免干扰</div>
                        <div>• 最多推荐 {params.max_recommendations || 20} 只</div>
                      </div>
                    </div>
                  )
                }
                title={recommendLoading ? `执行中 (${currentStep + 1}/${EXECUTION_STEPS.length})` : "筛选漏斗"}
                trigger="hover"
                placement="top"
              >
                <Button 
                  type="primary" 
                  icon={<RocketOutlined />} 
                  block 
                  size="large"
                  onClick={handleGetRecommendations} 
                  loading={recommendLoading}
                  style={{ height: '48px', fontSize: '16px' }}
                >
                  {recommendLoading ? '智能筛选中...' : '智能推荐 (Get Alpha)'}
                </Button>
              </Popover>
            </div>
          </Card>
        </Col>
        <Col xs={24} lg={16} xl={18}>
          <Tabs defaultActiveKey="recommend" items={tabItems} />
        </Col>
      </Row>
      
      {/* K线图弹窗（参考Tab1的实现） */}
      <Modal
        title={
          <Space>
            <span>{selectedStock.name || selectedStock.code} - K线图</span>
          </Space>
        }
        open={klineVisible}
        onCancel={() => setKlineVisible(false)}
        footer={null}
        width={1200}
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
              height: '600px', 
              width: '100%' 
            }}
          />
        ) : (
          <div style={{ textAlign: 'center', padding: 50, color: '#999' }}>
            暂无K线数据
          </div>
        )}
      </Modal>
    </div>
  )
}

export default ModelK
