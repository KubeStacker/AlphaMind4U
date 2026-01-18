import client from './client'

export interface HotSheep {
  sheep_code: string
  sheep_name: string
  source: string
  rank: number
  trade_date: string
  consecutive_days: number
  volume?: number
  sectors?: string[]  // 所属板块（最多3个）
  current_price?: number
  change_pct?: number
  avg_change_7d?: number
}

export interface SectorInfo {
  sector_name: string
  hot_count?: number
  hot_score?: number  // 兼容字段
  color?: string  // 颜色标识：red/orange/blue
  hot_sheep?: SectorSheep[]  // 该板块下的热门肥羊列表
}

export interface SectorSheep {
  sheep_code: string
  sheep_name: string
  rank?: number
  consecutive_days: number
}

export interface SectorStockByChange {
  sheep_code: string
  sheep_name: string
  change_pct: number
  current_price?: number
  rank?: number
}

export interface TrendingSector {
  sector_name: string
  inflow_amount: number
  super_large_inflow: number
  large_inflow: number
  avg_change_pct: number
  stock_count: number
  top_stocks: Array<{
    sheep_code: string
    sheep_name: string
    change_pct?: number
    current_price?: number
    rank?: number
  }>
  score: number
  trend_strength: string
  recommendation_reason: string
}

export interface TrendingSectorResponse {
  sectors: TrendingSector[]
  timestamp: string
  limit: number
}

export const trendingSectorApi = {
  getTrendingSectors: async (limit: number = 10): Promise<TrendingSectorResponse> => {
    const response = await client.get('/trending-sectors', { params: { limit } })
    return response.data
  },
}

export const hotApi = {
  getHotSheeps: async (source?: string): Promise<HotSheep[]> => {
    const response = await client.get('/hot-sheep', { params: { source } })
    return response.data.sheep || []
  },

  getHotSectors: async (): Promise<SectorInfo[]> => {
    const response = await client.get('/hot-sectors')
    return response.data.sectors || []
  },

  getSectorDaily: async (sectorName: string) => {
    const response = await client.get(`/sectors/${encodeURIComponent(sectorName)}/daily`)
    return response.data.data || []
  },

  getSectorSheeps: async (sectorName: string): Promise<SectorSheep[]> => {
    const response = await client.get(`/sectors/${encodeURIComponent(sectorName)}/sheep`)
    return response.data.sheep || []
  },

  getSectorStocksByChange: async (sectorName: string, limit: number = 10): Promise<SectorStockByChange[]> => {
    const response = await client.get(`/sectors/${encodeURIComponent(sectorName)}/stocks-by-change`, { params: { limit } })
    return response.data.stocks || []
  },

  refreshHotSheeps: async (): Promise<void> => {
    await client.post('/refresh-hot-sheep')
  },
}

export interface CapitalInflowStock {
  sheep_code: string
  sheep_name: string
  continuous_days: number
  total_inflow: number  // 总流入（亿元）
  max_single_day_inflow: number  // 单日最大流入（亿元）
  avg_daily_inflow: number  // 日均流入（亿元）
}

export const capitalInflowApi = {
  getRecommendations: async (days: number = 5): Promise<{ stocks: CapitalInflowStock[], days: number }> => {
    const response = await client.get('/capital-inflow/recommend', { params: { days } })
    return response.data
  },
}

export interface SectorMoneyFlowDailyData {
  trade_date: string
  main_net_inflow: number  // 主力净流入（万元）
  super_large_inflow?: number
  large_inflow?: number
}

export interface SectorMoneyFlowInfo {
  sector_name: string
  trade_date?: string
  main_net_inflow: number  // 主力净流入（万元）
  super_large_inflow?: number
  large_inflow?: number
  total_inflow?: number  // 总流入（用于多天统计，万元）
  total_super_large?: number
  total_large?: number
  latest_date?: string
  daily_data?: SectorMoneyFlowDailyData[]  // 每日详细数据（用于多天统计）
}

export interface SectorMoneyFlowMetadata {
  total_days_in_db: number
  actual_days_used: number
  requested_days: number
  has_sufficient_data: boolean
  warning?: string
}

export const sectorMoneyFlowApi = {
  getRecommendations: async (days: number = 1, limit: number = 30): Promise<{ sectors: SectorMoneyFlowInfo[], days: number, metadata?: SectorMoneyFlowMetadata }> => {
    // 限制最大返回数量为30
    const actualLimit = limit > 30 ? 30 : limit
    const response = await client.get('/sector-money-flow/recommend', { params: { days, limit: actualLimit } })
    return response.data
  },
}

// ========== 下个交易日预测相关接口 ==========

// 板块预测信息
export interface SectorPrediction {
  sector_name: string
  score: number
  prediction_level: 'high' | 'medium' | 'low'
  reasons: string[]
  details: {
    money_score: number
    hot_score: number
    hot_count: number
    inflow_total: number
  }
  top_stocks: Array<{
    sheep_code: string
    sheep_name: string
    rank: number
    hot_score: number
  }>
}

// 个股推荐信息
export interface StockRecommendation {
  sheep_code: string
  sheep_name: string
  sector_name: string
  score: number
  hot_rank: number
  reasons: string[]
  details: {
    change_pct: number | null
    current_price: number | null
    main_net_inflow: number | null
    volume_ratio: number | null
  }
}

// 下个交易日预测结果
export interface NextDayPrediction {
  success: boolean
  target_date: string
  data_date: string
  generated_at: string
  description: string
  sector_predictions: SectorPrediction[]
  stock_recommendations: StockRecommendation[]
  analysis_summary: {
    top_sectors_count: number
    recommended_stocks_count: number
    data_freshness: 'real-time' | 'post-market'
  }
  message?: string
}

export const nextDayPredictionApi = {
  // 获取下个交易日预测
  getPrediction: async (): Promise<NextDayPrediction> => {
    const response = await client.get('/next-day-prediction')
    return response.data
  },
  
  // 手动刷新预测（管理员）
  refreshPrediction: async (): Promise<{ message: string; status: string }> => {
    const response = await client.post('/next-day-prediction/refresh')
    return response.data
  },
}
