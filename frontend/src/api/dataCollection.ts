import client from './client'

export interface DataCollectionStats {
  table_name: string
  schedule_time: string
  retention_days: number | null
  trading_days_in_period: number | null
  actual_data_days: number | null
}

export interface DataCollectionType {
  value: string
  label: string
  description: string
  requires_trading_day: boolean
  stats: DataCollectionStats
}

export interface DataCollectionResult {
  success: boolean
  message: string
  data_type?: string
  elapsed_time?: number
  status?: string
}

export const dataCollectionApi = {
  // 获取数据类型列表
  async getTypes(): Promise<DataCollectionType[]> {
    const response = await client.get<{ types: DataCollectionType[] }>('/admin/data-collection/types')
    return response.data.types
  },

  // 采集特定数据
  async collectSpecific(
    dataType: string,
    options?: {
      days?: number
      targetDate?: string
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
