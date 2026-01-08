import client from './client'

export interface HotSector {
  sector_name: string
  hot_count: number
  hot_stocks?: Array<{
    stock_code: string
    stock_name: string
    rank?: number
    consecutive_days: number
  }>
}

export interface HotStock {
  stock_code: string
  stock_name: string
  sector?: string
  change_pct?: number
  rank?: number
  current_price?: number
  avg_change_7d?: number
  consecutive_boards?: number
}

export interface CapitalFlowStock {
  stock_code: string
  stock_name: string
  avg_inflow: number
  positive_days: number
}

export interface Recommendations {
  hot_sectors: HotSector[]
  hot_stocks: HotStock[]
  capital_flow_stocks: CapitalFlowStock[]
}

export const recommendationsApi = {
  getRecommendations: async (): Promise<Recommendations> => {
    const response = await client.get('/recommendations')
    return response.data
  },
}
