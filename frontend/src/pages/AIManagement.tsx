import React, { useState, useEffect } from 'react'
import { Card, Tabs, Form, Input, Button, message, Space, Typography, Alert, Table, Switch, Modal, Popconfirm } from 'antd'
import { RobotOutlined, SaveOutlined, ArrowUpOutlined, ArrowDownOutlined, EditOutlined, DeleteOutlined, FileTextOutlined } from '@ant-design/icons'
import { aiApi, AIModel } from '../api/ai'
import { useAuth } from '../contexts/AuthContext'

const { Title, Text } = Typography

const AIManagement: React.FC = () => {
  const { user } = useAuth()
  const isAdmin = user?.is_admin || user?.username === 'admin'
  const [loading, setLoading] = useState(false)
  const [models, setModels] = useState<AIModel[]>([])
  const [modelEditModalVisible, setModelEditModalVisible] = useState(false)
  const [editingModel, setEditingModel] = useState<AIModel | null>(null)
  const [modelForm] = Form.useForm()
  const [recommendPromptForm] = Form.useForm()
  const [analyzePromptForm] = Form.useForm()

  useEffect(() => {
    loadData()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const handleClearCache = async () => {
    try {
      const result = await aiApi.clearCache()
      message.success(`缓存清空成功，已删除 ${result.deleted_count} 条缓存记录`)
    } catch (error: any) {
      message.error(`清空缓存失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    }
  }

  const defaultRecommendPrompt = `## 【角色设定】
你是一位拥有20年实战经验的A股"机构派游资"。你既看重公募基金的安全垫（业绩、壁垒、无雷），又擅长捕捉顶级游资的技术买点（洗盘结束、强力反转）。

## 【输入信息】
当前日期：{date}
核心热点/主线：{hot_sectors}

热门股票数据：
{data}

## 【任务目标】
基于上述市场环境，挖掘 3只 符合"绩优白马 + 底部旭日东升（强力反转）"特征的潜力个股。

## 【严格筛选标准】

### 1. 硬核风控（一票否决制）
基本面底线：必须是细分行业龙头、隐形冠军或绩优股。要有真实的业绩支撑或不可替代的技术壁垒。
排雷行动：严禁推荐业绩连年亏损、有违规减持历史、大额商誉减值风险或有财务造假嫌疑的标的。

### 2. 技术形态（特定反转逻辑）
洗盘阶段（缩量缓跌）：个股前期经历了一段明显的"缩量缓跌"（阴跌）调整，换手率降低，表明浮筹清洗充分，空头力量衰竭。
启动信号（放量大阳）：近期（1-3日内）突然走出一根"放量大阳线"（涨幅>5%）。
强度确认（光头阳线）：该阳线收盘价必须接近全天最高价（无明显上影线，实体饱满），且收盘站稳，确立主力做多意志坚决，非试盘行为。

### 3. 股性特征
优先选择 科创板(688) 或 创业板(300)，具备 20CM 高弹性基因，历史股性活跃。

## 【输出格式】
请严格按以下结构输出 3只 标的分析报告：

### [股票名称 & 代码]

1. 基本面逻辑（机构安全垫）：
用简练语言概括其核心业绩增长点或行业地位（为什么跌下来机构敢接？）。

2. 技术面解析（游资爆发力）：
洗盘特征：描述此前缩量调整的周期和幅度。
反转信号：分析这根"启动大阳线"的量能倍数（如：量比>3）及K线形态（是否光头、是否反包）。

3. 实战策略：
低吸位：[依托大阳线实体的支撑价格，如：阳线实体中下部]
防守位（止损）：[跌破启动阳线的最低价]
目标位：[上方筹码密集区或重要均线压力位]`

  const defaultAnalyzePrompt = `你现在是一名资深趋势型游资分析师，擅长从资金流向、量价关系、板块轮动周期研判个股趋势机会，不涉及打板、连板等短线打板逻辑。

分析日期：{date}
标的名称：{stock_name}
所属板块：{sectors}

请你分析以下数据，按照以下框架输出分析报告：
1.  趋势定性：当前处于趋势的哪个阶段（萌芽/启动/加速/滞涨/反转）？用1-2个核心指标（如均线排列、MACD形态、量能趋势）佐证。
2.  资金验证：近5个交易日的主力资金流向（净流入/流出）、龙虎榜机构/游资席位动向（如有），判断资金是否具备持续性。
3.  支撑压力：当前股价的关键支撑位（强支撑/弱支撑）、压力位（强压力/弱压力），说明判断依据。
4.  板块联动：是否属于某个热门板块？所属热门板块的趋势强度如何？该股在板块中是龙头/跟风/补涨？板块轮动周期是否匹配个股趋势。
5.  操作建议：趋势型游资视角下，适合持仓/加仓/减仓/清仓的信号是什么？明确触发条件。
6.  风险预警：哪些信号出现意味着趋势可能终结（如量价背离、均线破位、板块退潮）？

数据：
{data}

要求：结论先行，语言简洁，数据支撑，拒绝空话套话。`

  const loadModels = async () => {
    try {
      const response = await aiApi.getAIModels()
      setModels(response.models)
    } catch (error) {
      console.error('加载模型列表失败:', error)
    }
  }

  const handleUpdateModel = async (modelId: number, field: string, value: any) => {
    try {
      await aiApi.updateAIModel(modelId, { [field]: value })
      message.success('模型配置更新成功')
      loadModels()
    } catch (error: any) {
      message.error(`更新失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    }
  }

  const handleMoveModel = async (modelId: number, direction: 'up' | 'down') => {
    try {
      const model = models.find(m => m.id === modelId)
      if (!model) return
      
      const currentIndex = models.findIndex(m => m.id === modelId)
      if (direction === 'up' && currentIndex === 0) return
      if (direction === 'down' && currentIndex === models.length - 1) return
      
      const targetIndex = direction === 'up' ? currentIndex - 1 : currentIndex + 1
      const targetModel = models[targetIndex]
      
      // 交换排序顺序
      await Promise.all([
        aiApi.updateAIModel(modelId, { sort_order: targetModel.sort_order }),
        aiApi.updateAIModel(targetModel.id, { sort_order: model.sort_order })
      ])
      
      message.success('模型顺序更新成功')
      loadModels()
    } catch (error: any) {
      message.error(`更新失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    }
  }

  const handleEditModel = (model: AIModel) => {
    setEditingModel(model)
    modelForm.setFieldsValue({
      api_key: model.api_key || ''
    })
    setModelEditModalVisible(true)
  }

  const handleSaveModelAPIKey = async () => {
    try {
      const values = await modelForm.validateFields()
      if (editingModel) {
        await aiApi.updateAIModel(editingModel.id, { api_key: values.api_key })
        message.success('模型API Key更新成功')
        setModelEditModalVisible(false)
        loadModels()
      }
    } catch (error: any) {
      if (error?.errorFields) {
        return
      }
      message.error(`保存失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    }
  }

  const loadData = async () => {
    setLoading(true)
    try {
      if (isAdmin) {
        // 加载Prompt模板
        const promptsData = await aiApi.getPrompts()
        const recommendPrompt = promptsData.prompts.recommend || defaultRecommendPrompt
        const analyzePrompt = promptsData.prompts.analyze || defaultAnalyzePrompt
        
        recommendPromptForm.setFieldsValue({
          prompt: recommendPrompt
        })
        analyzePromptForm.setFieldsValue({
          prompt: analyzePrompt
        })
      }
      
      // 加载模型列表
      await loadModels()
    } catch (error: any) {
      const errorMsg = error?.response?.data?.detail || error?.message || '未知错误'
      message.error(`加载数据失败: ${errorMsg}`)
    } finally {
      setLoading(false)
    }
  }

  const modelColumns = [
    {
      title: '排序',
      key: 'sort',
      width: 100,
      render: (_: any, record: AIModel, index: number) => (
        <Space>
          <Button
            type="link"
            icon={<ArrowUpOutlined />}
            disabled={index === 0}
            onClick={() => handleMoveModel(record.id, 'up')}
            title="上移"
          />
          <Button
            type="link"
            icon={<ArrowDownOutlined />}
            disabled={index === models.length - 1}
            onClick={() => handleMoveModel(record.id, 'down')}
            title="下移"
          />
        </Space>
      ),
    },
    {
      title: '模型名称',
      dataIndex: 'model_display_name',
      key: 'model_display_name',
    },
    {
      title: '模型代码',
      dataIndex: 'model_name',
      key: 'model_name',
    },
    {
      title: 'API Key',
      key: 'api_key',
      render: (_: any, record: AIModel) => (
        <Space>
          <span>{record.api_key ? `${record.api_key.substring(0, 8)}...` : '未配置'}</span>
          <Button
            type="link"
            size="small"
            icon={<EditOutlined />}
            onClick={() => handleEditModel(record)}
          >
            编辑
          </Button>
        </Space>
      ),
    },
    {
      title: '启用状态',
      dataIndex: 'is_active',
      key: 'is_active',
      render: (isActive: boolean, record: AIModel) => (
        <Switch
          checked={isActive}
          onChange={(checked) => handleUpdateModel(record.id, 'is_active', checked)}
        />
      ),
    },
  ]

  const tabItems = [
    ...(isAdmin ? [{
      key: 'models',
      label: (
        <span>
          <RobotOutlined />
          模型配置
        </span>
      ),
      children: (
        <div>
          <Title level={4}>AI模型配置管理</Title>
          <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
            配置可用的AI模型和对应的API Key。模型按排序顺序使用，排在前面的优先使用。
          </Text>
          <Table
            dataSource={models}
            columns={modelColumns}
            rowKey="id"
            pagination={false}
            style={{ marginTop: 16 }}
          />
          <Modal
            title="编辑模型API Key"
            open={modelEditModalVisible}
            onOk={handleSaveModelAPIKey}
            onCancel={() => {
              setModelEditModalVisible(false)
              setEditingModel(null)
              modelForm.resetFields()
            }}
            okText="保存"
            cancelText="取消"
          >
            <Form form={modelForm} layout="vertical">
              <Form.Item
                name="api_key"
                label={`${editingModel?.model_display_name} API Key`}
                rules={[{ required: true, message: '请输入API Key' }]}
              >
                <Input.Password
                  placeholder="请输入API Key"
                  maxLength={200}
                />
              </Form.Item>
            </Form>
          </Modal>
        </div>
      ),
    }] : []),
    ...(isAdmin ? [{
      key: 'prompt_recommend',
      label: (
        <span>
          <FileTextOutlined />
          推荐Prompt
        </span>
      ),
      children: (
        <div>
          <Title level={4}>股票推荐Prompt模板</Title>
          <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
            自定义AI推荐股票时的Prompt模板。支持的变量：{'{date}'}（日期）、{'{hot_sectors}'}（热门板块列表）、{'{data}'}（股票数据）。
          </Text>
          <Form
            form={recommendPromptForm}
            layout="vertical"
          >
            <Form.Item
              name="prompt"
              rules={[{ required: true, message: '请输入Prompt内容' }]}
            >
              <Input.TextArea
                rows={15}
                style={{ fontFamily: 'monospace' }}
              />
            </Form.Item>
            <Form.Item>
              <Space>
                <Button
                  type="primary"
                  icon={<SaveOutlined />}
                  onClick={async () => {
                    try {
                      const values = await recommendPromptForm.validateFields()
                      await aiApi.updatePrompt('recommend', values.prompt)
                      message.success('推荐Prompt保存成功')
                    } catch (error: any) {
                      if (error?.errorFields) {
                        return
                      }
                      message.error(`保存失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
                    }
                  }}
                >
                  保存
                </Button>
                <Button
                  onClick={() => {
                    recommendPromptForm.setFieldsValue({ prompt: defaultRecommendPrompt })
                  }}
                >
                  恢复默认
                </Button>
                <Popconfirm
                  title="确定要清空所有AI分析缓存吗？"
                  description="此操作将删除所有已缓存的AI推荐和分析结果，无法恢复。"
                  onConfirm={handleClearCache}
                  okText="确定"
                  cancelText="取消"
                >
                  <Button
                    danger
                    icon={<DeleteOutlined />}
                  >
                    清空缓存
                  </Button>
                </Popconfirm>
              </Space>
            </Form.Item>
          </Form>
        </div>
      ),
    }] : []),
    ...(isAdmin ? [{
      key: 'prompt_analyze',
      label: (
        <span>
          <FileTextOutlined />
          分析Prompt
        </span>
      ),
      children: (
        <div>
          <Title level={4}>股票分析Prompt模板</Title>
          <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
            自定义AI分析股票时的Prompt模板。支持的变量：{'{date}'}（日期）、{'{stock_name}'}（标的名称）、{'{sectors}'}（所属板块）、{'{data}'}（股票数据）。
          </Text>
          <Form
            form={analyzePromptForm}
            layout="vertical"
          >
            <Form.Item
              name="prompt"
              rules={[{ required: true, message: '请输入Prompt内容' }]}
            >
              <Input.TextArea
                rows={15}
                style={{ fontFamily: 'monospace' }}
              />
            </Form.Item>
            <Form.Item>
              <Space>
                <Button
                  type="primary"
                  icon={<SaveOutlined />}
                  onClick={async () => {
                    try {
                      const values = await analyzePromptForm.validateFields()
                      await aiApi.updatePrompt('analyze', values.prompt)
                      message.success('分析Prompt保存成功')
                    } catch (error: any) {
                      if (error?.errorFields) {
                        return
                      }
                      message.error(`保存失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
                    }
                  }}
                >
                  保存
                </Button>
                <Button
                  onClick={() => {
                    analyzePromptForm.setFieldsValue({ prompt: defaultAnalyzePrompt })
                  }}
                >
                  恢复默认
                </Button>
              </Space>
            </Form.Item>
          </Form>
        </div>
      ),
    }] : []),
  ]

  return (
    <div>
      <Card
        title={
          <Space>
            <RobotOutlined />
            <span>AI管理</span>
          </Space>
        }
        loading={loading}
      >
        <Alert
          message="AI功能说明"
          description={isAdmin 
            ? "管理员可以管理AI模型、配置Prompt模板。每个模型需要配置独立的API Key才能使用。"
            : "AI功能使用管理员配置的模型和API Key。如果未配置API Key，请在AI管理设置中配置。"
          }
          type="info"
          showIcon
          style={{ marginBottom: 24 }}
        />

        {tabItems.length > 0 ? (
          <Tabs
            defaultActiveKey={isAdmin ? "models" : undefined}
            items={tabItems}
          />
        ) : (
          <Alert
            message="提示"
            description="您没有权限访问AI管理功能，请联系管理员。"
            type="warning"
            showIcon
          />
        )}
      </Card>
    </div>
  )
}

export default AIManagement
