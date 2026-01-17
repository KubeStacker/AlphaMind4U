import client from './client'

export interface DataCollectionType {
  value: string
  label: string
  description: string
  requires_trading_day: boolean
}

export interface DataCollectionResult {
  success: boolean
  message: string
  data_type?: string
  elapsed_time?: number
}

export interface CollectAllResult {
  success: boolean
  results: {
    [key: string]: {
      success: boolean
      message: string
    }
  }
  total_time: number
  success_count: number
  total_count: number
}

export const dataCollectionApi = {
  // 获取数据类型列表
  async getTypes(): Promise<DataCollectionType[]> {
    const response = await client.get<{ types: DataCollectionType[] }>('/admin/data-collection/types')
    return response.data.types
  },

  // 采集所有数据
  async collectAll(forceTradingDay: boolean = false): Promise<CollectAllResult> {
    const response = await client.post<CollectAllResult>('/admin/data-collection/collect-all', null, {
      params: { force_trading_day: forceTradingDay }
    })
    return response.data
  },

  // 采集特定数据
  async collectSpecific(
    dataType: string,
    options?: {
      days?: number
      targetDate?: string
      force?: boolean
    }
  ): Promise<DataCollectionResult> {
    const response = await client.post<DataCollectionResult>('/admin/data-collection/collect-specific', null, {
      params: {
        data_type: dataType,
        ...options
      }
    })
    return response.data
  }
}
