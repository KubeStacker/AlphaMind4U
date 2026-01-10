import client from './client'

export interface SectorMapping {
  id: number
  source_sector: string
  target_sector: string
  description?: string
  is_active: boolean
  created_at?: string
  updated_at?: string
}

export interface SectorMappingCreate {
  source_sector: string
  target_sector: string
  description?: string
}

export interface SectorMappingUpdate {
  target_sector?: string
  description?: string
  is_active?: boolean
}

export const sectorApi = {
  getMappings: async (): Promise<SectorMapping[]> => {
    const response = await client.get('/sector-mappings')
    return response.data.mappings || []
  },

  createMapping: async (mapping: SectorMappingCreate): Promise<void> => {
    await client.post('/sector-mappings', mapping)
  },

  updateMapping: async (id: number, mapping: SectorMappingUpdate): Promise<void> => {
    await client.put(`/sector-mappings/${id}`, mapping)
  },

  deleteMapping: async (id: number): Promise<void> => {
    await client.delete(`/sector-mappings/${id}`)
  },

  refreshCache: async (): Promise<{ message: string; count: number }> => {
    const response = await client.post('/sector-mappings/refresh-cache')
    return response.data
  },
}
