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
  sheep_code: string
  sheep_name: string
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

  // AI推荐肥羊
  recommendSheeps: async (modelName?: string, customPrompt?: string): Promise<AIRecommendResponse> => {
    const response = await apiClient.post('/ai/recommend-sheep', { 
      model_name: modelName,
      custom_prompt: customPrompt
    }, {
      timeout: 180000 // 180秒，AI推荐可能需要更长时间
    })
    return response.data
  },

  // AI分析肥羊
  analyzeSheep: async (stockCode: string, modelName?: string, customPrompt?: string): Promise<AIAnalyzeResponse> => {
    const response = await apiClient.post(`/ai/analyze-sheep/${stockCode}`, { 
      model_name: modelName,
      custom_prompt: customPrompt
    }, {
      timeout: 180000 // 180秒，AI分析可能需要更长时间
    })
    return response.data
  },

  // AI模型配置管理（所有用户可查看）
  getAIModels: async (): Promise<{ models: AIModel[] }> => {
    const response = await apiClient.get('/ai/models')
    return response.data
  },

  getActiveAIModels: async (): Promise<{ models: AIModel[] }> => {
    const response = await apiClient.get('/ai/models/active')
    return response.data
  },

  // AI模型配置管理（仅admin）
  updateAIModel: async (modelId: number, config: { api_key?: string; sort_order?: number; is_active?: boolean }): Promise<void> => {
    await apiClient.put(`/ai/models/${modelId}`, config)
  },

  createAIModel: async (config: { model_name: string; model_display_name: string; api_key: string; api_url: string; sort_order: number }): Promise<{ message: string; model_id: number }> => {
    const response = await apiClient.post('/ai/models', config)
    return response.data
  },

  deleteAIModel: async (modelId: number): Promise<void> => {
    await apiClient.delete(`/ai/models/${modelId}`)
  },

  // 清空AI缓存
  clearCache: async (): Promise<{ message: string; deleted_count: number }> => {
    const response = await apiClient.post('/ai/clear-cache')
    return response.data
  },
}

export interface AIModel {
  id: number
  model_name: string
  model_display_name: string
  api_key: string
  api_url: string
  sort_order: number
  is_active: boolean
}
