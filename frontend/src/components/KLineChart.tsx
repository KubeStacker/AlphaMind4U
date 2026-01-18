/**
 * K线图弹窗组件 - 同花顺风格
 * 
 * 功能：
 * 1. 专业K线图（蜡烛图）
 * 2. 成交量柱状图
 * 3. 主力资金流入情况
 * 4. 左右箭头移动K线
 * 5. 数据在图表上方显示
 */
import React, { useState, useEffect, useCallback } from 'react'
import { Modal, Spin, Button, Space, Tag, Segmented, message } from 'antd'
import { LeftOutlined, RightOutlined, ZoomInOutlined, ZoomOutOutlined, ReloadOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { sheepApi, SheepDailyData, CapitalFlowData } from '../api/sheep'

interface KLineChartProps {
  visible: boolean
  onClose: () => void
  sheepCode: string
  sheepName: string
}

// K线颜色配置（同花顺风格）
const COLORS = {
  up: '#ff4d4f',      // 涨 - 红色
  down: '#52c41a',    // 跌 - 绿色
  ma5: '#f5a623',     // MA5 - 橙色
  ma10: '#1890ff',    // MA10 - 蓝色
  ma20: '#722ed1',    // MA20 - 紫色
  ma60: '#13c2c2',    // MA60 - 青色
  volume: '#597ef7',  // 成交量 - 蓝色
  mainInflow: '#ff4d4f',   // 主力流入 - 红色
  mainOutflow: '#52c41a',  // 主力流出 - 绿色
}

const KLineChart: React.FC<KLineChartProps> = ({ visible, onClose, sheepCode, sheepName }) => {
  const [loading, setLoading] = useState(false)
  const [dailyData, setDailyData] = useState<SheepDailyData[]>([])
  const [capitalFlowData, setCapitalFlowData] = useState<CapitalFlowData[]>([])
  const [days, setDays] = useState<number>(60)  // 默认60天
  const [dataZoomStart, setDataZoomStart] = useState(70)  // 默认显示后30%
  const [dataZoomEnd, setDataZoomEnd] = useState(100)
  
  // 加载数据
  const loadData = useCallback(async () => {
    if (!sheepCode) return
    
    setLoading(true)
    try {
      const [daily, flow] = await Promise.all([
        sheepApi.getSheepDaily(sheepCode, days),
        sheepApi.getCapitalFlow(sheepCode, days)
      ])
      setDailyData(daily || [])
      setCapitalFlowData(flow || [])
    } catch (error) {
      console.error('加载K线数据失败:', error)
      message.error('加载数据失败')
    } finally {
      setLoading(false)
    }
  }, [sheepCode, days])
  
  useEffect(() => {
    if (visible && sheepCode) {
      loadData()
    }
  }, [visible, sheepCode, days, loadData])
  
  // 计算均线
  const calculateMA = (data: number[], period: number): (number | null)[] => {
    const result: (number | null)[] = []
    for (let i = 0; i < data.length; i++) {
      if (i < period - 1) {
        result.push(null)
      } else {
        let sum = 0
        for (let j = 0; j < period; j++) {
          sum += data[i - j]
        }
        result.push(parseFloat((sum / period).toFixed(2)))
      }
    }
    return result
  }
  
  // 准备图表数据
  const prepareChartData = () => {
    if (!dailyData || dailyData.length === 0) {
      return { dates: [], ohlc: [], volumes: [], ma5: [], ma10: [], ma20: [], ma60: [], mainFlow: [] }
    }
    
    // 按日期升序排序
    const sortedData = [...dailyData].sort((a, b) => 
      new Date(a.trade_date).getTime() - new Date(b.trade_date).getTime()
    )
    
    const dates = sortedData.map(d => d.trade_date)
    const closes = sortedData.map(d => d.close_price)
    
    // K线数据 [open, close, low, high]
    const ohlc = sortedData.map(d => [d.open_price, d.close_price, d.low_price, d.high_price])
    
    // 成交量
    const volumes = sortedData.map(d => ({
      value: d.volume,
      itemStyle: {
        color: d.close_price >= d.open_price ? COLORS.up : COLORS.down
      }
    }))
    
    // 均线
    const ma5 = calculateMA(closes, 5)
    const ma10 = calculateMA(closes, 10)
    const ma20 = calculateMA(closes, 20)
    const ma60 = calculateMA(closes, 60)
    
    // 主力资金流
    const flowMap = new Map(capitalFlowData.map(f => [f.trade_date, f]))
    const mainFlow = sortedData.map(d => {
      const flow = flowMap.get(d.trade_date)
      return flow ? flow.main_net_inflow / 10000 : 0  // 转换为万元
    })
    
    return { dates, ohlc, volumes, ma5, ma10, ma20, ma60, mainFlow }
  }
  
  const chartData = prepareChartData()
  
  // 左移K线
  const moveLeft = () => {
    const range = dataZoomEnd - dataZoomStart
    const newStart = Math.max(0, dataZoomStart - 10)
    const newEnd = newStart + range
    setDataZoomStart(newStart)
    setDataZoomEnd(Math.min(100, newEnd))
  }
  
  // 右移K线
  const moveRight = () => {
    const range = dataZoomEnd - dataZoomStart
    const newEnd = Math.min(100, dataZoomEnd + 10)
    const newStart = newEnd - range
    setDataZoomStart(Math.max(0, newStart))
    setDataZoomEnd(newEnd)
  }
  
  // 放大
  const zoomIn = () => {
    const center = (dataZoomStart + dataZoomEnd) / 2
    const newRange = Math.max(10, (dataZoomEnd - dataZoomStart) * 0.7)
    setDataZoomStart(Math.max(0, center - newRange / 2))
    setDataZoomEnd(Math.min(100, center + newRange / 2))
  }
  
  // 缩小
  const zoomOut = () => {
    const center = (dataZoomStart + dataZoomEnd) / 2
    const newRange = Math.min(100, (dataZoomEnd - dataZoomStart) * 1.4)
    setDataZoomStart(Math.max(0, center - newRange / 2))
    setDataZoomEnd(Math.min(100, center + newRange / 2))
  }
  
  // 获取最新数据用于显示
  const getLatestData = () => {
    if (!dailyData || dailyData.length === 0) return null
    const latest = dailyData[dailyData.length - 1]
    return latest
  }
  
  const latestData = getLatestData()
  
  // ECharts配置
  const getOption = (): EChartsOption => {
    return {
      animation: false,
      backgroundColor: '#1a1a2e',
      
      // 图例
      legend: {
        top: 10,
        left: 'center',
        textStyle: { color: '#aaa', fontSize: 11 },
        data: ['MA5', 'MA10', 'MA20', 'MA60']
      },
      
      // 提示框
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          crossStyle: { color: '#666' },
          lineStyle: { color: '#666', type: 'dashed' }
        },
        backgroundColor: 'rgba(0,0,0,0.8)',
        borderColor: '#333',
        textStyle: { color: '#fff', fontSize: 12 },
        formatter: (params: any) => {
          if (!params || params.length === 0) return ''
          const date = params[0].axisValue
          let html = `<div style="font-weight:bold;margin-bottom:5px">${date}</div>`
          
          // K线数据
          const kline = params.find((p: any) => p.seriesName === 'K线')
          if (kline && kline.data) {
            const [open, close, low, high] = kline.data
            const change = ((close - open) / open * 100).toFixed(2)
            const color = close >= open ? COLORS.up : COLORS.down
            html += `<div>开盘: <span style="color:${color}">${open}</span></div>`
            html += `<div>收盘: <span style="color:${color}">${close}</span></div>`
            html += `<div>最高: <span style="color:${color}">${high}</span></div>`
            html += `<div>最低: <span style="color:${color}">${low}</span></div>`
            html += `<div>涨幅: <span style="color:${color}">${change}%</span></div>`
          }
          
          // 成交量
          const vol = params.find((p: any) => p.seriesName === '成交量')
          if (vol && vol.data) {
            const volValue = typeof vol.data === 'object' ? vol.data.value : vol.data
            html += `<div style="margin-top:5px">成交量: ${(volValue / 10000).toFixed(0)}万手</div>`
          }
          
          // 主力资金
          const flow = params.find((p: any) => p.seriesName === '主力净流入')
          if (flow && flow.data !== undefined) {
            const flowColor = flow.data >= 0 ? COLORS.mainInflow : COLORS.mainOutflow
            html += `<div>主力: <span style="color:${flowColor}">${flow.data >= 0 ? '+' : ''}${flow.data.toFixed(0)}万</span></div>`
          }
          
          return html
        }
      },
      
      // 坐标轴指示器
      axisPointer: {
        link: [{ xAxisIndex: 'all' }],
        label: { backgroundColor: '#333' }
      },
      
      // 网格布局
      grid: [
        { left: '8%', right: '3%', top: '12%', height: '45%' },   // K线图
        { left: '8%', right: '3%', top: '62%', height: '13%' },   // 成交量
        { left: '8%', right: '3%', top: '78%', height: '13%' }    // 主力资金
      ],
      
      // X轴
      xAxis: [
        {
          type: 'category',
          data: chartData.dates,
          gridIndex: 0,
          axisLine: { lineStyle: { color: '#333' } },
          axisLabel: { show: false },
          axisTick: { show: false },
          splitLine: { show: false }
        },
        {
          type: 'category',
          data: chartData.dates,
          gridIndex: 1,
          axisLine: { lineStyle: { color: '#333' } },
          axisLabel: { show: false },
          axisTick: { show: false },
          splitLine: { show: false }
        },
        {
          type: 'category',
          data: chartData.dates,
          gridIndex: 2,
          axisLine: { lineStyle: { color: '#333' } },
          axisLabel: { color: '#666', fontSize: 10 },
          axisTick: { show: false },
          splitLine: { show: false }
        }
      ],
      
      // Y轴
      yAxis: [
        {
          type: 'value',
          gridIndex: 0,
          scale: true,
          splitNumber: 4,
          axisLine: { show: false },
          axisLabel: { color: '#666', fontSize: 10 },
          splitLine: { lineStyle: { color: '#222' } }
        },
        {
          type: 'value',
          gridIndex: 1,
          scale: true,
          splitNumber: 2,
          axisLine: { show: false },
          axisLabel: { color: '#666', fontSize: 10, formatter: (v: number) => (v / 10000).toFixed(0) + '万' },
          splitLine: { lineStyle: { color: '#222' } }
        },
        {
          type: 'value',
          gridIndex: 2,
          scale: true,
          splitNumber: 2,
          axisLine: { show: false },
          axisLabel: { color: '#666', fontSize: 10, formatter: (v: number) => v.toFixed(0) + '万' },
          splitLine: { lineStyle: { color: '#222' } }
        }
      ],
      
      // 数据缩放
      dataZoom: [
        {
          type: 'inside',
          xAxisIndex: [0, 1, 2],
          start: dataZoomStart,
          end: dataZoomEnd
        },
        {
          type: 'slider',
          xAxisIndex: [0, 1, 2],
          start: dataZoomStart,
          end: dataZoomEnd,
          height: 20,
          bottom: 5,
          borderColor: '#333',
          backgroundColor: '#1a1a2e',
          fillerColor: 'rgba(24, 144, 255, 0.2)',
          handleStyle: { color: '#1890ff' },
          textStyle: { color: '#666' }
        }
      ],
      
      // 数据系列
      series: [
        // K线
        {
          name: 'K线',
          type: 'candlestick',
          data: chartData.ohlc,
          xAxisIndex: 0,
          yAxisIndex: 0,
          itemStyle: {
            color: COLORS.up,        // 阳线填充色
            color0: COLORS.down,     // 阴线填充色
            borderColor: COLORS.up,  // 阳线边框色
            borderColor0: COLORS.down // 阴线边框色
          }
        },
        // MA5
        {
          name: 'MA5',
          type: 'line',
          data: chartData.ma5,
          xAxisIndex: 0,
          yAxisIndex: 0,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 1, color: COLORS.ma5 }
        },
        // MA10
        {
          name: 'MA10',
          type: 'line',
          data: chartData.ma10,
          xAxisIndex: 0,
          yAxisIndex: 0,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 1, color: COLORS.ma10 }
        },
        // MA20
        {
          name: 'MA20',
          type: 'line',
          data: chartData.ma20,
          xAxisIndex: 0,
          yAxisIndex: 0,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 1, color: COLORS.ma20 }
        },
        // MA60
        {
          name: 'MA60',
          type: 'line',
          data: chartData.ma60,
          xAxisIndex: 0,
          yAxisIndex: 0,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 1, color: COLORS.ma60 }
        },
        // 成交量
        {
          name: '成交量',
          type: 'bar',
          data: chartData.volumes,
          xAxisIndex: 1,
          yAxisIndex: 1,
          barWidth: '60%'
        },
        // 主力净流入
        {
          name: '主力净流入',
          type: 'bar',
          data: chartData.mainFlow.map(v => ({
            value: v,
            itemStyle: { color: v >= 0 ? COLORS.mainInflow : COLORS.mainOutflow }
          })),
          xAxisIndex: 2,
          yAxisIndex: 2,
          barWidth: '60%'
        }
      ]
    }
  }
  
  // 处理dataZoom变化
  const handleDataZoom = (params: any) => {
    if (params.start !== undefined && params.end !== undefined) {
      setDataZoomStart(params.start)
      setDataZoomEnd(params.end)
    }
  }
  
  return (
    <Modal
      title={null}
      open={visible}
      onCancel={onClose}
      footer={null}
      width={900}
      bodyStyle={{ padding: 0, background: '#1a1a2e' }}
      style={{ top: 20 }}
      destroyOnClose
    >
      <Spin spinning={loading}>
        {/* 顶部信息栏 */}
        <div style={{ 
          padding: '12px 16px', 
          background: '#16213e', 
          borderBottom: '1px solid #333',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <span style={{ color: '#fff', fontSize: '16px', fontWeight: 'bold' }}>
              {sheepName}
            </span>
            <Tag color="blue">{sheepCode}</Tag>
            {latestData && (
              <>
                <span style={{ 
                  color: (latestData.change_pct || 0) >= 0 ? COLORS.up : COLORS.down, 
                  fontSize: '20px', 
                  fontWeight: 'bold' 
                }}>
                  {latestData.close_price.toFixed(2)}
                </span>
                <span style={{ 
                  color: (latestData.change_pct || 0) >= 0 ? COLORS.up : COLORS.down,
                  fontSize: '14px'
                }}>
                  {(latestData.change_pct || 0) >= 0 ? '+' : ''}{(latestData.change_pct || 0).toFixed(2)}%
                </span>
              </>
            )}
          </div>
          
          <Space>
            <Segmented
              size="small"
              value={days}
              onChange={(v) => setDays(v as number)}
              options={[
                { label: '30天', value: 30 },
                { label: '60天', value: 60 },
                { label: '120天', value: 120 },
                { label: '250天', value: 250 },
              ]}
            />
            <Button size="small" icon={<ReloadOutlined />} onClick={loadData}>刷新</Button>
          </Space>
        </div>
        
        {/* 数据指标栏 */}
        {latestData && (
          <div style={{ 
            padding: '8px 16px', 
            background: '#1a1a2e', 
            borderBottom: '1px solid #222',
            display: 'flex',
            gap: '24px',
            fontSize: '12px'
          }}>
            <span style={{ color: '#666' }}>
              开: <span style={{ color: latestData.close_price >= latestData.open_price ? COLORS.up : COLORS.down }}>
                {latestData.open_price.toFixed(2)}
              </span>
            </span>
            <span style={{ color: '#666' }}>
              高: <span style={{ color: COLORS.up }}>{latestData.high_price.toFixed(2)}</span>
            </span>
            <span style={{ color: '#666' }}>
              低: <span style={{ color: COLORS.down }}>{latestData.low_price.toFixed(2)}</span>
            </span>
            <span style={{ color: '#666' }}>
              量: <span style={{ color: '#fff' }}>{(latestData.volume / 10000).toFixed(0)}万</span>
            </span>
            <span style={{ color: '#666' }}>
              换手: <span style={{ color: '#fff' }}>{latestData.turnover_rate?.toFixed(2)}%</span>
            </span>
            <span style={{ color: COLORS.ma5 }}>MA5: {chartData.ma5[chartData.ma5.length - 1]?.toFixed(2) || '-'}</span>
            <span style={{ color: COLORS.ma10 }}>MA10: {chartData.ma10[chartData.ma10.length - 1]?.toFixed(2) || '-'}</span>
            <span style={{ color: COLORS.ma20 }}>MA20: {chartData.ma20[chartData.ma20.length - 1]?.toFixed(2) || '-'}</span>
          </div>
        )}
        
        {/* 图表区域 */}
        <div style={{ height: '480px', background: '#1a1a2e' }}>
          {chartData.dates.length > 0 ? (
            <ReactECharts
              option={getOption()}
              style={{ height: '100%', width: '100%' }}
              onEvents={{
                datazoom: handleDataZoom
              }}
            />
          ) : (
            <div style={{ 
              height: '100%', 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'center',
              color: '#666'
            }}>
              暂无数据
            </div>
          )}
        </div>
        
        {/* 底部控制栏 */}
        <div style={{ 
          padding: '8px 16px', 
          background: '#16213e', 
          borderTop: '1px solid #333',
          display: 'flex',
          justifyContent: 'center',
          gap: '8px'
        }}>
          <Button size="small" icon={<LeftOutlined />} onClick={moveLeft}>左移</Button>
          <Button size="small" icon={<ZoomInOutlined />} onClick={zoomIn}>放大</Button>
          <Button size="small" icon={<ZoomOutOutlined />} onClick={zoomOut}>缩小</Button>
          <Button size="small" icon={<RightOutlined />} onClick={moveRight}>右移</Button>
        </div>
      </Spin>
    </Modal>
  )
}

export default KLineChart
