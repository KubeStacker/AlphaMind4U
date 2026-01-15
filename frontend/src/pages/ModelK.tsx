import React, { useState, useEffect } from 'react'
import { Card, Button, Slider, InputNumber, Switch, Space, message, Tabs, Table, Tag, Statistic, Row, Col, Modal, DatePicker, Alert, Collapse, Divider, Tooltip } from 'antd'
import { ThunderboltOutlined, RocketOutlined, DeleteOutlined, ClearOutlined, QuestionCircleOutlined, SettingOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import { modelKApi, BacktestParams, BacktestResult, Recommendation, RecommendationHistory } from '../api/modelK'
import dayjs, { Dayjs } from 'dayjs'
import type { TabsProps } from 'antd'

const { RangePicker } = DatePicker
const { Panel } = Collapse

const ModelK: React.FC = () => {
  const [params, setParams] = useState<BacktestParams>({
    // 基础筛选参数
    min_mv: 50, 
    max_mv: 300, 
    rps_threshold: 80,
    vol_threshold: 1.5,
    // 核心参数（简化版）
    min_change_pct: 2.0,
    max_change_pct: 9.5,
    concept_boost: true,
    ai_filter: true,
    min_win_probability: 45,
    // 兼容旧参数（自动映射）
    change_pct_required: true,
  })
  const [backtestLoading, setBacktestLoading] = useState(false)
  const [backtestResult, setBacktestResult] = useState<BacktestResult | null>(null)
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(1, 'year'), dayjs().subtract(1, 'day')])
  const [recommendLoading, setRecommendLoading] = useState(false)
  const [recommendations, setRecommendations] = useState<Recommendation[]>([])
  const [recommendDate, setRecommendDate] = useState<string>('')
  const [selectedRecommendDate, setSelectedRecommendDate] = useState<Dayjs | null>(null) // 用户选择的推荐日期
  const [diagnosticInfo, setDiagnosticInfo] = useState<string>('')
  const [historyLoading, setHistoryLoading] = useState(false)
  const [historyData, setHistoryData] = useState<RecommendationHistory[]>([])

  const handleBacktest = async () => {
    if (!dateRange[0] || !dateRange[1]) { message.warning('请选择回测日期范围'); return }
    setBacktestLoading(true)
    try {
      const result = await modelKApi.runBacktest({
        start_date: dateRange[0].format('YYYY-MM-DD'),
        end_date: dateRange[1].format('YYYY-MM-DD'),
        params
      })
      if (result.success) { setBacktestResult(result); message.success('回测完成') }
      else { message.error(result.message || '回测失败') }
    } catch (error: any) { message.error(error.response?.data?.detail || '回测失败') }
    finally { setBacktestLoading(false) }
  }

  const handleGetRecommendations = async () => {
    setRecommendLoading(true)
    try {
      // 如果用户选择了日期，使用用户选择的日期；否则使用null（后端会自动使用最近的交易日）
      const tradeDate = selectedRecommendDate ? selectedRecommendDate.format('YYYY-MM-DD') : undefined
      const result = await modelKApi.getRecommendations(params, tradeDate)
      setRecommendations(result.recommendations || [])
      setRecommendDate(result.trade_date || '')
      setDiagnosticInfo(result.diagnostic_info || '')
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
    }
    finally { setRecommendLoading(false) }
  }

  const loadHistory = async () => {
    setHistoryLoading(true)
    try {
      const result = await modelKApi.getHistory()
      setHistoryData(result.recommendations)
    } catch (error: any) { message.error('加载历史记录失败') }
    finally { setHistoryLoading(false) }
  }

  useEffect(() => { loadHistory() }, [])

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

  // 今日推荐不显示涨幅列
  const getRecommendationColumns = () => {
    return [
      { title: '肥羊代码', dataIndex: 'sheep_code', key: 'sheep_code', width: 100, fixed: 'left' as const },
      { title: '肥羊名称', dataIndex: 'sheep_name', key: 'sheep_name', width: 120 },
      { title: '现价', dataIndex: 'entry_price', key: 'entry_price', width: 90, render: (price: number) => `¥${price.toFixed(2)}` },
      { title: 'AI打分', dataIndex: 'ai_score', key: 'ai_score', width: 90, render: (score: number) => <Tag color={score > 50 ? 'green' : score > 30 ? 'orange' : 'red'}>{score.toFixed(1)}</Tag> },
      { title: '胜率', dataIndex: 'win_probability', key: 'win_probability', width: 80, render: (prob: number) => `${prob.toFixed(0)}%` },
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
        title: '板块', 
        dataIndex: 'sector_trend', 
        key: 'sector_trend', 
        width: 100, 
        ellipsis: true,
        render: (sector: string) => sector || '-'
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

  const historyColumns = [
    { title: '推荐日期', dataIndex: 'run_date', key: 'run_date', width: 120 },
    { title: '肥羊', key: 'stock', width: 150, render: (_: any, record: RecommendationHistory) => <div><div>{record.sheep_code}</div><div style={{ fontSize: '12px', color: '#999' }}>{record.sheep_name}</div></div> },
    { title: '当时参数', dataIndex: 'params_snapshot', key: 'params_snapshot', width: 200, ellipsis: true, render: (params: BacktestParams) => <div style={{ fontSize: '12px' }}><div>倍量: {params.vol_threshold}x</div><div>RPS: {params.rps_threshold}</div></div> },
    { title: '后5日最大涨幅', dataIndex: 'max_return_5d', key: 'max_return_5d', width: 130, render: (max_pct: number | undefined, record: RecommendationHistory) => {
      if (!record.is_verified) return <Tag color="default">未验证</Tag>
      const pct = max_pct || 0
      return <Tag color={pct > 0 ? 'red' : pct < 0 ? 'green' : 'default'}>{pct > 0 ? '+' : ''}{pct.toFixed(2)}%</Tag>
    }},
    { title: '后5日涨幅', dataIndex: 'final_return_5d', key: 'final_return_5d', width: 120, render: (return_pct: number | undefined, record: RecommendationHistory) => {
      if (!record.is_verified) return <Tag color="default">未验证</Tag>
      const pct = return_pct || 0
      return <Tag color={pct > 0 ? 'red' : pct < 0 ? 'green' : 'default'}>{pct > 0 ? '+' : ''}{pct.toFixed(2)}%</Tag>
    }},
    { title: '结果', dataIndex: 'final_result', key: 'final_result', width: 100, render: (result: string | undefined) => {
      if (!result) return <Tag color="default">未验证</Tag>
      return <Tag color={result === 'SUCCESS' ? 'green' : 'red'}>{result === 'SUCCESS' ? '成功' : '失败'}</Tag>
    }}
  ]

  const tabItems: TabsProps['items'] = [
    {
      key: 'recommend',
      label: '今日推荐',
      children: (
        <Card>
          {recommendations.length > 0 ? <>
            <div style={{ marginBottom: '16px', color: '#666' }}>
              推荐日期: {recommendDate}
              {recommendations.length > 0 && recommendations[0].market_regime && (
                <span style={{ marginLeft: '16px' }}>
                  市场状态: 
                  <Tag 
                    color={
                      recommendations[0].market_regime === 'Attack' ? 'red' : 
                      recommendations[0].market_regime === 'Defense' ? 'blue' : 
                      'default'
                    }
                    style={{ marginLeft: '8px' }}
                  >
                    {recommendations[0].market_regime === 'Attack' ? '进攻' : 
                     recommendations[0].market_regime === 'Defense' ? '防守' : 
                     '震荡'}
                  </Tag>
                </span>
              )}
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
          <div style={{ marginBottom: '16px', display: 'flex', gap: '12px', alignItems: 'center', flexWrap: 'wrap' }}>
            <div>
              <div style={{ marginBottom: '4px', fontSize: '14px', color: '#666' }}>选择回测时间范围</div>
              <RangePicker 
                value={dateRange} 
                onChange={(dates) => { 
                  if (dates && dates[0] && dates[1]) setDateRange([dates[0], dates[1]]) 
                }} 
                format="YYYY-MM-DD" 
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
              执行回测 (Time Travel)
            </Button>
          </div>
          {backtestResult?.success && <>
            <Row gutter={16} style={{ marginBottom: '24px' }}>
              <Col xs={12} sm={6}><Statistic title="胜率" value={backtestResult.metrics?.win_rate || 0} suffix="%" valueStyle={{ color: '#3f8600' }} /></Col>
              <Col xs={12} sm={6}><Statistic title="爆款率" value={backtestResult.metrics?.alpha_rate || 0} suffix="%" valueStyle={{ color: '#cf1322' }} /></Col>
              <Col xs={12} sm={6}><Statistic title="总收益率" value={backtestResult.metrics?.total_return || 0} suffix="%" valueStyle={{ color: '#1890ff' }} /></Col>
              <Col xs={12} sm={6}><Statistic title="最大回撤" value={backtestResult.metrics?.max_drawdown || 0} suffix="%" valueStyle={{ color: '#cf1322' }} /></Col>
            </Row>
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
          模型老K - T7概念资金双驱模型 v2.0
        </h1>
        <p style={{ marginTop: '8px', color: '#666' }}>
          简化配置，智能推荐 | 市场状态自动识别 | 概念竞速引擎 + 资金流验证
        </p>
      </div>
      <Row gutter={24}>
        <Col xs={24} lg={8}>
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
            <Collapse defaultActiveKey={['basic', 'core']} ghost>
              {/* 基础筛选参数 */}
              <Panel header={<span style={{ fontWeight: 'bold' }}>基础筛选</span>} key="basic">
                <Space direction="vertical" style={{ width: '100%' }} size="middle">
                  <div>
                    <div style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <span>市值范围（亿元）</span>
                      <Tooltip title="筛选市值在此范围内的股票">
                        <QuestionCircleOutlined style={{ fontSize: '12px', color: '#999' }} />
                      </Tooltip>
                    </div>
                    <Space>
                      <InputNumber 
                        value={params.min_mv} 
                        onChange={(val) => setParams({ ...params, min_mv: val || 50 })} 
                        min={10} 
                        max={1000} 
                        style={{ width: '100px' }} 
                        addonBefore="最小"
                      />
                      <span>~</span>
                      <InputNumber 
                        value={params.max_mv} 
                        onChange={(val) => setParams({ ...params, max_mv: val || 300 })} 
                        min={10} 
                        max={1000} 
                        style={{ width: '100px' }} 
                        addonBefore="最大"
                      />
                    </Space>
                  </div>

                  <div>
                    <div style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <span>RPS阈值: {params.rps_threshold}</span>
                      <Tooltip title="相对强度排名，数值越高表示股票越强势">
                        <QuestionCircleOutlined style={{ fontSize: '12px', color: '#999' }} />
                      </Tooltip>
                    </div>
                    <Slider 
                      value={params.rps_threshold} 
                      onChange={(val) => setParams({ ...params, rps_threshold: val })} 
                      min={50} 
                      max={100} 
                      marks={{ 50: '50', 75: '75', 100: '100' }} 
                    />
                  </div>

                  <div>
                    <div style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <span>倍量定义: {params.vol_threshold}x</span>
                      <Tooltip title="成交量相对于均量的倍数">
                        <QuestionCircleOutlined style={{ fontSize: '12px', color: '#999' }} />
                      </Tooltip>
                    </div>
                    <Slider 
                      value={params.vol_threshold} 
                      onChange={(val) => setParams({ ...params, vol_threshold: val })} 
                      min={1.0} 
                      max={5.0} 
                      step={0.1} 
                      marks={{ 1: '1x', 2: '2x', 3: '3x', 4: '4x', 5: '5x' }} 
                    />
                  </div>
                </Space>
              </Panel>

              {/* 核心参数 */}
              <Panel header={<span style={{ fontWeight: 'bold' }}>核心参数</span>} key="core">
                <Space direction="vertical" style={{ width: '100%' }} size="middle">
                  <div>
                    <div style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <span>涨幅范围</span>
                      <Tooltip title="震荡模式下的涨幅范围，市场状态会自动调整">
                        <QuestionCircleOutlined style={{ fontSize: '12px', color: '#999' }} />
                      </Tooltip>
                    </div>
                    <Space>
                      <InputNumber 
                        value={params.min_change_pct} 
                        onChange={(val) => setParams({ ...params, min_change_pct: val || 0 })} 
                        min={0} 
                        max={10} 
                        step={0.5}
                        style={{ width: '100px' }} 
                        addonAfter="%"
                        placeholder="最小"
                      />
                      <span>~</span>
                      <InputNumber 
                        value={params.max_change_pct} 
                        onChange={(val) => setParams({ ...params, max_change_pct: val || 9.5 })} 
                        min={0} 
                        max={20} 
                        step={0.5}
                        style={{ width: '100px' }} 
                        addonAfter="%"
                        placeholder="最大"
                      />
                    </Space>
                    <div style={{ marginTop: '4px', fontSize: '12px', color: '#999' }}>
                      提示：市场状态会自动识别并调整涨幅范围（进攻/防守/震荡）
                    </div>
                  </div>

                  <Divider style={{ margin: '12px 0' }} />

                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <span>概念共振</span>
                      <Tooltip title="启用概念共振加分，提升推荐质量">
                        <QuestionCircleOutlined style={{ fontSize: '12px', color: '#999' }} />
                      </Tooltip>
                    </span>
                    <Switch 
                      checked={params.concept_boost || false} 
                      onChange={(checked) => setParams({ ...params, concept_boost: checked })} 
                    />
                  </div>
                </Space>
              </Panel>

              {/* AI过滤参数 */}
              <Panel header={<span style={{ fontWeight: 'bold' }}>AI过滤</span>} key="ai">
                <Space direction="vertical" style={{ width: '100%' }} size="middle">
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <span>AI过滤</span>
                      <Tooltip title="使用AI规则过滤假突破，提升推荐质量">
                        <QuestionCircleOutlined style={{ fontSize: '12px', color: '#999' }} />
                      </Tooltip>
                    </span>
                    <Switch 
                      checked={params.ai_filter || false} 
                      onChange={(checked) => setParams({ ...params, ai_filter: checked })} 
                    />
                  </div>

                  {params.ai_filter && (
                    <div>
                      <div style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                        <span>最小胜率: {params.min_win_probability}%</span>
                        <Tooltip title="AI预测的胜率阈值，低于此值的股票将被过滤">
                          <QuestionCircleOutlined style={{ fontSize: '12px', color: '#999' }} />
                        </Tooltip>
                      </div>
                      <Slider 
                        value={params.min_win_probability || 45} 
                        onChange={(val) => setParams({ ...params, min_win_probability: val })} 
                        min={40} 
                        max={70} 
                        step={5}
                        marks={{ 40: '40%', 50: '50%', 60: '60%', 70: '70%' }}
                      />
                    </div>
                  )}
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
              <Button 
                type="primary" 
                icon={<RocketOutlined />} 
                block 
                size="large"
                onClick={handleGetRecommendations} 
                loading={recommendLoading}
                style={{ height: '48px', fontSize: '16px' }}
              >
                智能推荐 (Get Alpha)
              </Button>
            </div>
          </Card>
        </Col>
        <Col xs={24} lg={16}>
          <Tabs defaultActiveKey="recommend" items={tabItems} />
        </Col>
      </Row>
    </div>
  )
}

export default ModelK
