import React, { useState, useEffect } from 'react'
import { Card, Tabs, Form, Input, Button, message, Space, Typography, Alert } from 'antd'
import { RobotOutlined, KeyOutlined, FileTextOutlined, SaveOutlined } from '@ant-design/icons'
import { aiApi } from '../api/ai'

const { TextArea } = Input
const { Title, Text } = Typography

const AIManagement: React.FC = () => {
  const [loading, setLoading] = useState(false)
  const [configForm] = Form.useForm()
  const [recommendPromptForm] = Form.useForm()
  const [analyzePromptForm] = Form.useForm()

  useEffect(() => {
    loadData()
  }, [])

  const handleSaveApiKey = async () => {
    try {
      const values = await configForm.validateFields()
      await aiApi.updateConfig('api_key', values.api_key, 'DeepSeek API Key')
      message.success('API Key保存成功')
      loadData()
    } catch (error: any) {
      if (error?.errorFields) {
        return // 表单验证错误
      }
      message.error(`保存失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    }
  }

  const handleSaveRecommendPrompt = async () => {
    try {
      const values = await recommendPromptForm.validateFields()
      await aiApi.updatePrompt('recommend', values.prompt)
      message.success('推荐Prompt保存成功')
      loadData()
    } catch (error: any) {
      if (error?.errorFields) {
        return
      }
      message.error(`保存失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    }
  }

  const handleSaveAnalyzePrompt = async () => {
    try {
      const values = await analyzePromptForm.validateFields()
      await aiApi.updatePrompt('analyze', values.prompt)
      message.success('分析Prompt保存成功')
      loadData()
    } catch (error: any) {
      if (error?.errorFields) {
        return
      }
      message.error(`保存失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    }
  }

  const defaultRecommendPrompt = `你是一位资深的股票投资分析师。请根据以下热门股票数据，分析并推荐最有投资价值的股票。

热门股票数据：
{data}

请从以下角度进行分析：
1. 行业趋势和板块热度
2. 技术面分析（价格走势、成交量等）
3. 资金流向
4. 市场情绪和热度持续性

请给出3-5只最值得关注的股票推荐，并说明推荐理由。格式要求：
- 股票代码：股票名称
- 推荐理由：（简要说明）
- 风险提示：（如有）`

  const defaultAnalyzePrompt = `你是一位资深的股票投资分析师。请对以下股票进行深度分析。

股票信息：
{data}

请从以下角度进行专业分析：
1. 基本面分析（行业地位、财务状况等）
2. 技术面分析（K线形态、均线系统、成交量等）
3. 资金流向分析
4. 市场情绪和热度分析
5. 风险评估

请给出专业的投资建议和操作建议。`

  const loadData = async () => {
    setLoading(true)
    try {
      const [configsData, promptsData] = await Promise.all([
        aiApi.getConfig(),
        aiApi.getPrompts()
      ])
      
      // 设置表单初始值
      if (configsData.configs.api_key) {
        configForm.setFieldsValue({
          api_key: configsData.configs.api_key.value
        })
      }
      
      // Prompt处理：如果后端有保存的值则使用，否则使用默认值（首次编辑时可直接编辑默认值）
      const recommendPrompt = promptsData.prompts.recommend || defaultRecommendPrompt
      const analyzePrompt = promptsData.prompts.analyze || defaultAnalyzePrompt
      
      recommendPromptForm.setFieldsValue({
        prompt: recommendPrompt
      })
      analyzePromptForm.setFieldsValue({
        prompt: analyzePrompt
      })
    } catch (error: any) {
      message.error(`加载数据失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`)
    } finally {
      setLoading(false)
    }
  }

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
          description="本系统使用DeepSeek API提供AI推荐和分析功能。请先配置API Key，然后可以自定义Prompt模板。"
          type="info"
          showIcon
          style={{ marginBottom: 24 }}
        />

        <Tabs
          defaultActiveKey="token"
          items={[
            {
              key: 'token',
              label: (
                <span>
                  <KeyOutlined />
                  Token设置
                </span>
              ),
              children: (
                <div>
                  <Title level={4}>DeepSeek API Key配置</Title>
                  <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
                    请前往 DeepSeek 官网获取API Key，并在此处配置。API Key将用于调用AI推荐和分析功能。
                  </Text>
                  <Form
                    form={configForm}
                    layout="vertical"
                    style={{ maxWidth: 600 }}
                  >
                    <Form.Item
                      name="api_key"
                      label="API Key"
                      rules={[{ required: true, message: '请输入API Key' }]}
                      extra="API Key将加密存储，仅用于调用DeepSeek API"
                    >
                      <Input.Password
                        placeholder="请输入DeepSeek API Key"
                        maxLength={200}
                      />
                    </Form.Item>
                    <Form.Item>
                      <Button
                        type="primary"
                        icon={<SaveOutlined />}
                        onClick={handleSaveApiKey}
                      >
                        保存
                      </Button>
                    </Form.Item>
                  </Form>
                </div>
              ),
            },
            {
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
                    自定义AI推荐股票时的Prompt模板。使用 {'{data}'} 作为数据占位符。
                  </Text>
                  <Form
                    form={recommendPromptForm}
                    layout="vertical"
                  >
                    <Form.Item
                      name="prompt"
                      rules={[{ required: true, message: '请输入Prompt内容' }]}
                    >
                      <TextArea
                        rows={15}
                        style={{ fontFamily: 'monospace' }}
                      />
                    </Form.Item>
                    <Form.Item>
                      <Space>
                        <Button
                          type="primary"
                          icon={<SaveOutlined />}
                          onClick={handleSaveRecommendPrompt}
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
                      </Space>
                    </Form.Item>
                  </Form>
                </div>
              ),
            },
            {
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
                    自定义AI分析股票时的Prompt模板。使用 {'{data}'} 作为数据占位符。
                  </Text>
                  <Form
                    form={analyzePromptForm}
                    layout="vertical"
                  >
                    <Form.Item
                      name="prompt"
                      rules={[{ required: true, message: '请输入Prompt内容' }]}
                    >
                      <TextArea
                        rows={15}
                        style={{ fontFamily: 'monospace' }}
                      />
                    </Form.Item>
                    <Form.Item>
                      <Space>
                        <Button
                          type="primary"
                          icon={<SaveOutlined />}
                          onClick={handleSaveAnalyzePrompt}
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
            },
          ]}
        />
      </Card>
    </div>
  )
}

export default AIManagement
