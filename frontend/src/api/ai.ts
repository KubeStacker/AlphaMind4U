import { apiClient } from './client'

export interface AIConfig {
  value: string
  description?: string
  updated_at?: string
}

export interface AIConfigsResponse {
  configs: Record<string, AIConfig>
}

export interface AIPromptsResponse {
  prompts: {
    recommend: string
    analyze: string
  }
}

export interface AIRecommendResponse {
  recommendation: string
  timestamp: string
}

export interface AIAnalyzeResponse {
  analysis: string
  stock_code: string
  stock_name: string
  timestamp: string
}

export const aiApi = {
  // 获取AI配置
  getConfig: async (): Promise<AIConfigsResponse> => {
    const response = await apiClient.get('/ai/config')
    return response.data
  },

  // 更新AI配置
  updateConfig: async (configKey: string, configValue: string, description?: string): Promise<void> => {
    await apiClient.put(`/ai/config/${configKey}`, {
      config_value: configValue,
      description
    })
  },

  // 获取Prompt模板
  getPrompts: async (): Promise<AIPromptsResponse> => {
    const response = await apiClient.get('/ai/prompts')
    return response.data
  },

  // 更新Prompt模板
  updatePrompt: async (promptType: 'recommend' | 'analyze', promptContent: string): Promise<void> => {
    await apiClient.put(`/ai/prompts/${promptType}`, {
      prompt_content: promptContent
    })
  },

  // AI推荐股票
  recommendStocks: async (): Promise<AIRecommendResponse> => {
    const response = await apiClient.post('/ai/recommend-stocks', {}, {
      timeout: 180000 // 180秒，AI推荐可能需要更长时间
    })
    return response.data
  },

  // AI分析股票
  analyzeStock: async (stockCode: string): Promise<AIAnalyzeResponse> => {
    const response = await apiClient.post(`/ai/analyze-stock/${stockCode}`, {}, {
      timeout: 180000 // 180秒，AI分析可能需要更长时间
    })
    return response.data
  }
}
