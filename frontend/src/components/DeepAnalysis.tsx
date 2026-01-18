import React, { useState, useEffect } from 'react'
import { Card, Row, Col, Statistic, Tag, Progress, Alert, Spin, Button, Tooltip, Divider, Space, Collapse, Table } from 'antd'
import { 
  RiseOutlined, 
  FallOutlined, 
  LineChartOutlined, 
  DollarOutlined,
  WarningOutlined,
  CheckCircleOutlined,
  QuestionCircleOutlined,
  ReloadOutlined
} from '@ant-design/icons'
import { sheepApi, DeepAnalysisResult, PredictionResult, PatternResult, StopLevels, Assessment } from '../api/sheep'

const { Panel } = Collapse

interface DeepAnalysisProps {
  sheepCode: string
  sheepName?: string
  onClose?: () => void
}

// 获取预测方向的颜色和图标
const getPredictionStyle = (direction: string) => {
  switch (direction) {
    case '看涨':
    case '强烈看好':
      return { color: '#f5222d', icon: <RiseOutlined />, bgColor: 'rgba(245, 34, 45, 0.1)' }
    case '偏多':
    case '看好':
      return { color: '#fa8c16', icon: <RiseOutlined />, bgColor: 'rgba(250, 140, 22, 0.1)' }
    case '持平':
    case '中性':
      return { color: '#1890ff', icon: <LineChartOutlined />, bgColor: 'rgba(24, 144, 255, 0.1)' }
    case '偏空':
    case '看淡':
      return { color: '#52c41a', icon: <FallOutlined />, bgColor: 'rgba(82, 196, 26, 0.1)' }
    case '看跌':
    case '强烈看淡':
      return { color: '#52c41a', icon: <FallOutlined />, bgColor: 'rgba(82, 196, 26, 0.1)' }
    default:
      return { color: '#666', icon: <LineChartOutlined />, bgColor: '#f5f5f5' }
  }
}

// 获取风险等级的颜色
const getRiskColor = (riskLevel: string) => {
  switch (riskLevel) {
    case '低':
      return 'green'
    case '中低':
      return 'cyan'
    case '中':
      return 'blue'
    case '中高':
      return 'orange'
    case '高':
      return 'red'
    default:
      return 'default'
  }
}

// 预测卡片组件
const PredictionCard: React.FC<{ title: string; data: PredictionResult }> = ({ title, data }) => {
  const style = getPredictionStyle(data.direction)
  
  return (
    <Card 
      size="small" 
      style={{ 
        background: style.bgColor, 
        borderLeft: `4px solid ${style.color}`,
        height: '100%'
      }}
    >
      <div style={{ marginBottom: 8 }}>
        <span style={{ fontWeight: 'bold', fontSize: 14 }}>{title}</span>
        <Tag color={getRiskColor(data.risk_level)} style={{ marginLeft: 8, fontSize: 11 }}>
          风险{data.risk_level}
        </Tag>
      </div>
      
      <Row gutter={8}>
        <Col span={12}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            {style.icon}
            <span style={{ color: style.color, fontWeight: 'bold', fontSize: 20 }}>
              {data.direction}
            </span>
          </div>
          <div style={{ fontSize: 12, color: '#666', marginTop: 4 }}>
            概率 {data.probability}%
          </div>
        </Col>
        <Col span={12}>
          <Statistic
            title={<span style={{ fontSize: 11 }}>预期涨跌幅</span>}
            value={data.expected_change}
            precision={2}
            valueStyle={{ 
              color: data.expected_change >= 0 ? '#f5222d' : '#52c41a',
              fontSize: 16
            }}
            suffix="%"
            prefix={data.expected_change >= 0 ? '+' : ''}
          />
        </Col>
      </Row>
      
      <div style={{ marginTop: 8 }}>
        <Progress 
          percent={data.score} 
          size="small"
          strokeColor={style.color}
          format={() => `${data.score.toFixed(0)}分`}
        />
      </div>
      
      {data.reasons && data.reasons.length > 0 && (
        <div style={{ marginTop: 8, fontSize: 11, color: '#666' }}>
          <Collapse ghost size="small">
            <Panel header="预测理由" key="1">
              <ul style={{ margin: 0, paddingLeft: 16 }}>
                {data.reasons.map((reason, idx) => (
                  <li key={idx} style={{ marginBottom: 4 }}>{reason}</li>
                ))}
              </ul>
            </Panel>
          </Collapse>
        </div>
      )}
    </Card>
  )
}

// 形态识别组件
const PatternCard: React.FC<{ pattern: PatternResult }> = ({ pattern }) => {
  const getPatternColor = (type: string) => {
    switch (type) {
      case '底部反转':
      case '放量突破':
        return 'red'
      case '顶部翻转':
        return 'green'
      case '震荡洗盘':
      case '缩量整理':
        return 'blue'
      case '快速拉升':
        return 'volcano'
      default:
        return 'default'
    }
  }
  
  return (
    <Card 
      title={
        <Space>
          <LineChartOutlined />
          <span>技术形态识别</span>
        </Space>
      }
      size="small"
    >
      <div style={{ marginBottom: 16 }}>
        <Row align="middle" gutter={16}>
          <Col>
            <Tag 
              color={getPatternColor(pattern.type)} 
              style={{ fontSize: 16, padding: '6px 16px' }}
            >
              {pattern.type}
            </Tag>
          </Col>
          <Col>
            <Tooltip title="形态识别置信度">
              <Progress 
                type="circle" 
                percent={pattern.confidence} 
                width={60}
                strokeColor={pattern.confidence >= 70 ? '#52c41a' : pattern.confidence >= 50 ? '#faad14' : '#f5222d'}
              />
            </Tooltip>
          </Col>
        </Row>
      </div>
      
      <div style={{ marginBottom: 12 }}>
        <div style={{ fontWeight: 'bold', marginBottom: 4 }}>形态描述</div>
        <div style={{ color: '#666', fontSize: 13 }}>{pattern.description}</div>
      </div>
      
      {pattern.signals && pattern.signals.length > 0 && (
        <div style={{ marginBottom: 12 }}>
          <div style={{ fontWeight: 'bold', marginBottom: 4 }}>识别信号</div>
          <Space wrap>
            {pattern.signals.map((signal, idx) => (
              <Tag key={idx} color="blue">{signal}</Tag>
            ))}
          </Space>
        </div>
      )}
      
      {pattern.operation_hint && (
        <Alert
          message="操作建议"
          description={pattern.operation_hint}
          type="info"
          showIcon
          style={{ marginTop: 8 }}
        />
      )}
      
      {pattern.all_patterns && pattern.all_patterns.length > 1 && (
        <div style={{ marginTop: 12, fontSize: 12, color: '#999' }}>
          其他识别形态：{pattern.all_patterns.slice(1).join('、')}
        </div>
      )}
    </Card>
  )
}

// 止盈止损组件
const StopLevelsCard: React.FC<{ stopLevels: StopLevels; currentPrice: number }> = ({ stopLevels, currentPrice }) => {
  const columns = [
    {
      title: '类型',
      dataIndex: 'type',
      key: 'type',
      width: 100,
    },
    {
      title: '价格',
      dataIndex: 'price',
      key: 'price',
      render: (price: number, record: any) => (
        <span style={{ color: record.color, fontWeight: 'bold' }}>
          ¥{price}
        </span>
      ),
    },
    {
      title: '幅度',
      dataIndex: 'percentage',
      key: 'percentage',
      render: (pct: number, record: any) => (
        <span style={{ color: record.color }}>
          {pct >= 0 ? '+' : ''}{pct.toFixed(2)}%
        </span>
      ),
    },
    {
      title: '说明',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
  ]
  
  const data = [
    {
      key: 'stop_loss',
      type: '止损位',
      price: stopLevels.stop_loss.price,
      percentage: stopLevels.stop_loss.percentage,
      description: stopLevels.stop_loss.method,
      color: '#52c41a',
    },
    {
      key: 'take_profit_1',
      type: '第一止盈',
      price: stopLevels.take_profit_1.price,
      percentage: stopLevels.take_profit_1.percentage,
      description: stopLevels.take_profit_1.description,
      color: '#f5222d',
    },
    {
      key: 'take_profit_2',
      type: '第二止盈',
      price: stopLevels.take_profit_2.price,
      percentage: stopLevels.take_profit_2.percentage,
      description: stopLevels.take_profit_2.description,
      color: '#f5222d',
    },
  ]
  
  return (
    <Card
      title={
        <Space>
          <DollarOutlined />
          <span>止盈止损位</span>
        </Space>
      }
      size="small"
    >
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={8}>
          <Statistic
            title="当前价格"
            value={currentPrice}
            precision={2}
            prefix="¥"
          />
        </Col>
        <Col span={8}>
          <Statistic
            title="盈亏比"
            value={stopLevels.risk_reward_ratio}
            precision={2}
            prefix="1:"
            valueStyle={{ 
              color: stopLevels.risk_reward_ratio >= 2 ? '#52c41a' : 
                     stopLevels.risk_reward_ratio >= 1.5 ? '#faad14' : '#f5222d'
            }}
          />
        </Col>
        <Col span={8}>
          <Statistic
            title="ATR波动率"
            value={stopLevels.atr_pct}
            precision={2}
            suffix="%"
            valueStyle={{ 
              color: stopLevels.atr_pct <= 3 ? '#52c41a' : 
                     stopLevels.atr_pct <= 5 ? '#faad14' : '#f5222d'
            }}
          />
        </Col>
      </Row>
      
      <Table
        columns={columns}
        dataSource={data}
        pagination={false}
        size="small"
      />
      
      <div style={{ marginTop: 12, fontSize: 12, color: '#999' }}>
        <Tooltip title="ATR(真实波幅)反映股价的波动程度，波动率越高风险越大">
          <QuestionCircleOutlined /> ATR: {stopLevels.atr.toFixed(2)} 元
        </Tooltip>
      </div>
    </Card>
  )
}

// 综合评估组件
const AssessmentCard: React.FC<{ assessment: Assessment }> = ({ assessment }) => {
  const getRatingStyle = (rating: string) => {
    switch (rating) {
      case '强烈看好':
        return { color: '#f5222d', bgColor: 'rgba(245, 34, 45, 0.1)' }
      case '看好':
        return { color: '#fa8c16', bgColor: 'rgba(250, 140, 22, 0.1)' }
      case '中性':
        return { color: '#1890ff', bgColor: 'rgba(24, 144, 255, 0.1)' }
      case '看淡':
        return { color: '#52c41a', bgColor: 'rgba(82, 196, 26, 0.1)' }
      case '强烈看淡':
        return { color: '#237804', bgColor: 'rgba(35, 120, 4, 0.1)' }
      default:
        return { color: '#666', bgColor: '#f5f5f5' }
    }
  }
  
  const style = getRatingStyle(assessment.overall_rating)
  
  return (
    <Card
      title={
        <Space>
          <CheckCircleOutlined />
          <span>综合评估与操作建议</span>
        </Space>
      }
      style={{ 
        borderTop: `4px solid ${style.color}`,
      }}
    >
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={12}>
          <div style={{ 
            padding: 16, 
            background: style.bgColor, 
            borderRadius: 8,
            textAlign: 'center'
          }}>
            <div style={{ fontSize: 14, color: '#666', marginBottom: 8 }}>整体评级</div>
            <div style={{ fontSize: 28, fontWeight: 'bold', color: style.color }}>
              {assessment.overall_rating}
            </div>
          </div>
        </Col>
        <Col span={12}>
          <div style={{ 
            padding: 16, 
            background: '#f5f5f5', 
            borderRadius: 8,
            textAlign: 'center'
          }}>
            <div style={{ fontSize: 14, color: '#666', marginBottom: 8 }}>风险等级</div>
            <Tag 
              color={getRiskColor(assessment.risk_level)} 
              style={{ fontSize: 20, padding: '8px 20px' }}
            >
              {assessment.risk_level}
            </Tag>
          </div>
        </Col>
      </Row>
      
      <Divider style={{ margin: '16px 0' }} />
      
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontWeight: 'bold', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 8 }}>
          <CheckCircleOutlined style={{ color: '#52c41a' }} />
          操作建议
        </div>
        {assessment.operation_advice.map((advice, idx) => (
          <Alert
            key={idx}
            message={advice}
            type={idx === 0 ? 'success' : 'info'}
            style={{ marginBottom: 8 }}
            showIcon={false}
          />
        ))}
      </div>
      
      {assessment.key_points && assessment.key_points.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <div style={{ fontWeight: 'bold', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 8 }}>
            <LineChartOutlined style={{ color: '#1890ff' }} />
            关键点位
          </div>
          <Space wrap>
            {assessment.key_points.map((point, idx) => (
              <Tag key={idx} color="blue">{point}</Tag>
            ))}
          </Space>
        </div>
      )}
      
      {assessment.attention_items && assessment.attention_items.length > 0 && (
        <div>
          <div style={{ fontWeight: 'bold', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 8 }}>
            <WarningOutlined style={{ color: '#faad14' }} />
            注意事项
          </div>
          {assessment.attention_items.map((item, idx) => (
            <Alert
              key={idx}
              message={item}
              type="warning"
              style={{ marginBottom: 8 }}
              showIcon={false}
            />
          ))}
        </div>
      )}
    </Card>
  )
}

// 主组件
const DeepAnalysis: React.FC<DeepAnalysisProps> = ({ sheepCode, sheepName }) => {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<DeepAnalysisResult | null>(null)
  
  const loadAnalysis = async () => {
    if (!sheepCode) return
    
    setLoading(true)
    setError(null)
    
    try {
      const result = await sheepApi.getDeepAnalysis(sheepCode)
      if (result.success) {
        setData(result)
      } else {
        setError(result.message || '分析失败')
      }
    } catch (err: any) {
      console.error('深度分析失败:', err)
      setError(err?.response?.data?.detail || err?.message || '分析请求失败')
    } finally {
      setLoading(false)
    }
  }
  
  useEffect(() => {
    loadAnalysis()
  }, [sheepCode])
  
  if (loading) {
    return (
      <Card>
        <div style={{ textAlign: 'center', padding: 50 }}>
          <Spin size="large" />
          <div style={{ marginTop: 16, color: '#999' }}>
            正在进行深度分析，请稍候...
          </div>
        </div>
      </Card>
    )
  }
  
  if (error) {
    return (
      <Card>
        <Alert
          message="分析失败"
          description={error}
          type="error"
          showIcon
          action={
            <Button size="small" onClick={loadAnalysis}>
              重试
            </Button>
          }
        />
      </Card>
    )
  }
  
  if (!data) {
    return null
  }
  
  return (
    <div>
      <Card 
        title={
          <Space>
            <LineChartOutlined />
            <span>
              {sheepName || sheepCode} 深度分析
            </span>
            <Tag color="blue">{data.trade_date}</Tag>
          </Space>
        }
        extra={
          <Button 
            icon={<ReloadOutlined />} 
            onClick={loadAnalysis}
            loading={loading}
          >
            刷新分析
          </Button>
        }
        style={{ marginBottom: 16 }}
      >
        {/* 走势预判 */}
        <div style={{ marginBottom: 24 }}>
          <div style={{ fontWeight: 'bold', fontSize: 16, marginBottom: 12 }}>
            走势预判
          </div>
          <Row gutter={16}>
            <Col xs={24} sm={8}>
              <PredictionCard title="3日预判" data={data.predictions['3d']} />
            </Col>
            <Col xs={24} sm={8}>
              <PredictionCard title="5日预判" data={data.predictions['5d']} />
            </Col>
            <Col xs={24} sm={8}>
              <PredictionCard title="10日预判" data={data.predictions['10d']} />
            </Col>
          </Row>
        </div>
        
        <Divider />
        
        {/* 形态识别和止盈止损 */}
        <Row gutter={16}>
          <Col xs={24} lg={12}>
            <PatternCard pattern={data.pattern} />
          </Col>
          <Col xs={24} lg={12}>
            <StopLevelsCard 
              stopLevels={data.stop_levels} 
              currentPrice={data.current_price}
            />
          </Col>
        </Row>
        
        <Divider />
        
        {/* 综合评估 */}
        <AssessmentCard assessment={data.assessment} />
      </Card>
    </div>
  )
}

export default DeepAnalysis
