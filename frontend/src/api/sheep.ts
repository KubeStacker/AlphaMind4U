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

export const sheepApi = {
  searchSheeps: async (query: string): Promise<SheepInfo[]> => {
    const response = await client.get('/sheep/search', { params: { q: query } })
    return response.data.sheep || []
  },

  getSheepDaily: async (stockCode: string): Promise<SheepDailyData[]> => {
    const response = await client.get(`/sheep/${stockCode}/daily`)
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
}
