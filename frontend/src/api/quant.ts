import { apiFetch } from './http'

// 量化后端数据集元信息。
export interface DatasetInfo {
  name: string
  description: string
  date_column: string | null
}

// 数据集字段定义。
export interface DatasetColumn {
  name: string
  type: string
  nullable: boolean
  primary_key: boolean
}

// 数据集结构详情。
export interface DatasetSchema {
  name: string
  description: string
  date_column: string | null
  columns: DatasetColumn[]
}

// 数据集查询参数。
export interface DatasetQueryParams {
  dataset: string
  columns?: string[] | null
  filters?: Record<string, unknown>
  start_date?: string | null
  end_date?: string | null
  order_by?: string | null
  descending?: boolean
  limit?: number
  offset?: number
}

// 数据集查询结果。
export interface DatasetQueryResult {
  dataset: string
  columns: string[]
  rows: Record<string, unknown>[]
  count: number
  limit: number
  offset: number
}

// 获取数据集清单（真接口 GET /admin/datasets）。
export async function listDatasets(): Promise<DatasetInfo[]> {
  const data = (await apiFetch('/admin', '/datasets')) as { datasets: DatasetInfo[] }
  return data.datasets || []
}

// 获取单个数据集结构（真接口 GET /admin/datasets/{name}/schema）。
export async function describeDataset(name: string): Promise<DatasetSchema> {
  return (await apiFetch('/admin', `/datasets/${encodeURIComponent(name)}/schema`)) as DatasetSchema
}

// 查询数据集行数据（真接口 POST /admin/datasets/query）。
export async function queryDataset(params: DatasetQueryParams): Promise<DatasetQueryResult> {
  const body = {
    dataset: params.dataset,
    columns: params.columns ?? null,
    filters: params.filters ?? {},
    start_date: params.start_date ?? null,
    end_date: params.end_date ?? null,
    order_by: params.order_by ?? null,
    descending: params.descending ?? false,
    limit: Math.max(1, Math.min(1000, params.limit ?? 100)),
    offset: Math.max(0, Math.min(1000000, params.offset ?? 0)),
  }
  return (await apiFetch('/admin', '/datasets/query', {
    method: 'POST',
    body: JSON.stringify(body),
  })) as DatasetQueryResult
}

// 单表覆盖快照（截至/完整/缺口/原因/何时补）。
export interface TableCoverage {
  name: string
  description: string
  min_date: string
  max_date: string
  total: number
  recent: { date: string; count: number }[]
  as_of: string
  complete: boolean
  ratio: number
  missing: string[]
  reason: string
  next: string
}

// 全库覆盖快照（真接口 GET /admin/quality/coverage）。
export async function getCoverage(): Promise<{ universe: number; latest_trading_day: string; tables: TableCoverage[] }> {
  const data = (await apiFetch('/admin', '/quality/coverage')) as { universe: number; latest_trading_day: string; tables: TableCoverage[] }
  return { universe: data.universe || 0, latest_trading_day: data.latest_trading_day || '', tables: data.tables || [] }
}
