import client from './client'

export interface HotStock {
  stock_code: string
  stock_name: string
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
  hot_count: number
  hot_stocks?: SectorStock[]  // 该板块下的热门股票列表
}

export interface SectorStock {
  stock_code: string
  stock_name: string
  rank?: number
  consecutive_days: number
}

export const hotApi = {
  getHotStocks: async (source?: string): Promise<HotStock[]> => {
    const response = await client.get('/hot-stocks', { params: { source } })
    return response.data.stocks || []
  },

  getHotSectors: async (): Promise<SectorInfo[]> => {
    const response = await client.get('/hot-sectors')
    return response.data.sectors || []
  },

  getSectorDaily: async (sectorName: string) => {
    const response = await client.get(`/sectors/${encodeURIComponent(sectorName)}/daily`)
    return response.data.data || []
  },

  getSectorStocks: async (sectorName: string): Promise<SectorStock[]> => {
    const response = await client.get(`/sectors/${encodeURIComponent(sectorName)}/stocks`)
    return response.data.stocks || []
  },

  refreshHotStocks: async (): Promise<void> => {
    await client.post('/refresh-hot-stocks')
  },
}
