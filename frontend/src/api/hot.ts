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

export const sectorMoneyFlowApi = {
  getRecommendations: async (days: number = 1, limit: number = 20): Promise<{ sectors: SectorMoneyFlowInfo[], days: number }> => {
    const response = await client.get('/sector-money-flow/recommend', { params: { days, limit } })
    return response.data
  },
}
