import client from './client'

export interface SheepDailyData {
  trade_date: string
  open_price: number
  close_price: number
  high_price: number
  low_price: number
  volume: number
  amount: number
  change_pct?: number  // 涨跌幅（百分比）
  turnover_rate?: number  // 换手率（百分比）
  ma5?: number
  ma10?: number
  ma20?: number
  ma30?: number
  ma60?: number
}

export interface CapitalFlowData {
  trade_date: string
  main_net_inflow: number
  super_large_inflow?: number
  large_inflow?: number
}

export interface SheepInfo {
  code: string
  name: string
  sector?: string
}

// 走势预判结果
export interface PredictionResult {
  direction: string  // 看涨/偏多/持平/偏空/看跌
  probability: number  // 概率
  expected_change: number  // 预期涨跌幅
  risk_level: string  // 风险等级
  score: number  // 评分
  reasons?: string[]  // 预测理由
}

// 形态识别结果
export interface PatternResult {
  type: string  // 形态类型
  confidence: number  // 置信度
  description: string  // 描述
  signals: string[]  // 信号
  operation_hint: string  // 操作建议
  all_patterns?: string[]  // 所有识别到的形态
}

// 止盈止损位
export interface StopLevelDetail {
  price: number
  percentage: number
  method?: string
  description?: string
  details?: {
    atr_stop?: number
    support_stop?: number
    ma_stop?: number
  }
}

export interface StopLevels {
  stop_loss: StopLevelDetail
  take_profit_1: StopLevelDetail
  take_profit_2: StopLevelDetail
  risk_reward_ratio: number
  atr: number
  atr_pct: number
}

// 综合评估
export interface Assessment {
  overall_rating: string  // 整体评级
  risk_level: string  // 风险等级
  operation_advice: string[]  // 操作建议
  key_points: string[]  // 关键点位
  attention_items: string[]  // 注意事项
}

// 深度分析结果
export interface DeepAnalysisResult {
  success: boolean
  sheep_code: string
  trade_date: string
  current_price: number
  predictions: {
    '3d': PredictionResult
    '5d': PredictionResult
    '10d': PredictionResult
  }
  pattern: PatternResult
  stop_levels: StopLevels
  factors: Record<string, number>
  assessment: Assessment
  message?: string
}

export const sheepApi = {
  searchSheeps: async (query: string): Promise<SheepInfo[]> => {
    const response = await client.get('/sheep/search', { params: { q: query } })
    return response.data.sheep || []
  },

  getSheepDaily: async (stockCode: string, days: number = 120): Promise<SheepDailyData[]> => {
    const response = await client.get(`/sheep/${stockCode}/daily`, { params: { days } })
    return response.data.data || []
  },

  getCapitalFlow: async (stockCode: string, days: number = 60): Promise<CapitalFlowData[]> => {
    const response = await client.get(`/sheep/${stockCode}/capital-flow`, { params: { days } })
    return response.data.data || []
  },

  refreshCapitalFlow: async (stockCode: string): Promise<{ message: string; data_count: number; refreshed: boolean }> => {
    const response = await client.post(`/sheep/${stockCode}/capital-flow/refresh`)
    return response.data
  },

  // 刷新肥羊最新数据（仅在交易时段）
  refreshSheepData: async (stockCode: string): Promise<void> => {
    await client.post(`/sheep/${stockCode}/refresh`)
  },

  getSheepConcepts: async (stockCode: string): Promise<Array<{ concept_name: string; weight: number }>> => {
    const response = await client.get(`/sheep/${stockCode}/concepts`)
    return response.data.concepts || []
  },

  // 深度分析（走势预判+形态识别+止盈止损）
  getDeepAnalysis: async (stockCode: string, tradeDate?: string): Promise<DeepAnalysisResult> => {
    const params: Record<string, string> = {}
    if (tradeDate) {
      params.trade_date = tradeDate
    }
    const response = await client.post(`/sheep/${stockCode}/deep-analysis`, null, { params })
    return response.data
  },

  // 获取走势预判（简化版）
  getPrediction: async (stockCode: string): Promise<{
    sheep_code: string
    trade_date: string
    current_price: number
    predictions: {
      '3d': PredictionResult
      '5d': PredictionResult
      '10d': PredictionResult
    }
    pattern: PatternResult
    assessment: Assessment
  }> => {
    const response = await client.get(`/sheep/${stockCode}/prediction`)
    return response.data
  },
}
