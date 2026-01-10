import client from './client'

export interface StockDailyData {
  trade_date: string
  open_price: number
  close_price: number
  high_price: number
  low_price: number
  volume: number
  amount: number
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

export interface StockInfo {
  code: string
  name: string
  sector?: string
}

export const stockApi = {
  searchStocks: async (query: string): Promise<StockInfo[]> => {
    const response = await client.get('/stocks/search', { params: { q: query } })
    return response.data.stocks || []
  },

  getStockDaily: async (stockCode: string): Promise<StockDailyData[]> => {
    const response = await client.get(`/stocks/${stockCode}/daily`)
    return response.data.data || []
  },

  getCapitalFlow: async (stockCode: string): Promise<CapitalFlowData[]> => {
    const response = await client.get(`/stocks/${stockCode}/capital-flow`)
    return response.data.data || []
  },

  getStockConcepts: async (stockCode: string): Promise<Array<{ concept_name: string; weight: number }>> => {
    const response = await client.get(`/stocks/${stockCode}/concepts`)
    return response.data.concepts || []
  },
}
