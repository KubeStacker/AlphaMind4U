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


export interface SectorStockByChange {
  sheep_code: string
  sheep_name: string
  change_pct: number
  current_price?: number
  rank?: number
}


export const hotApi = {
  getHotSheeps: async (source?: string): Promise<HotSheep[]> => {
    const response = await client.get('/hot-sheep', { params: { source } })
    return response.data.sheep || []
  },

  getSectorStocksByChange: async (sectorName: string, limit: number = 10): Promise<SectorStockByChange[]> => {
    const response = await client.get(`/sectors/${encodeURIComponent(sectorName)}/stocks-by-change`, { params: { limit } })
    return response.data.stocks || []
  },
}

export interface CapitalInflowStock {
  sheep_code: string
  sheep_name: string
  continuous_days?: number  // 连续流入天数（仅连续流入接口有）
  total_inflow: number  // 总流入（亿元）
  max_single_day_inflow?: number  // 单日最大流入（亿元，仅连续流入接口有）
  avg_daily_inflow: number  // 日均流入（亿元）
  latest_trade_date?: string  // 最新交易日期（仅Top接口有）
  daily_data?: Array<{
    trade_date: string
    main_net_inflow: number  // 亿元
  }>  // 每日数据（用于趋势图，仅多天视图时有）
}

export const capitalInflowApi = {
  getRecommendations: async (days: number = 5): Promise<{ stocks: CapitalInflowStock[], days: number }> => {
    const response = await client.get('/capital-inflow/recommend', { params: { days } })
    return response.data
  },
  
  // 获取最近N天净流入Top标的（按净流入合计降序）
  getTop: async (days: number = 1, limit: number = 100): Promise<{ stocks: CapitalInflowStock[], days: number, limit: number }> => {
    const response = await client.get('/capital-inflow/top', { params: { days, limit } })
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
  
  // 获取板块资金流历史数据（用于K线图）
  getMoneyFlowHistory: async (sectorName: string, days: number = 60): Promise<{ data: SectorMoneyFlowDailyData[], sector_name: string, days: number }> => {
    const response = await client.get(`/sectors/${encodeURIComponent(sectorName)}/money-flow`, { params: { days } })
    return response.data
  },
}


// ========== 信号雷达与实时看板相关接口 ==========

// 热门板块摘要
export interface HotSectorSummary {
  sector_name: string
  sector_rps_20: number
  sector_rps_50: number
  limit_up_count: number
  change_pct: number
  main_net_inflow: number
  signal: 'New Cycle' | 'Climax Risk' | null
}

// 推荐个股
export interface RecommendedStock {
  sheep_code: string
  sheep_name: string
  change_pct: number | null
  current_price: number | null
  rank?: number
}

// 信号雷达数据
export interface SignalRadarData {
  trade_date: string
  new_cycle_signals: HotSectorSummary[]
  climax_signals: HotSectorSummary[]
  hot_sectors_summary: HotSectorSummary[]
  hottest_sector: HotSectorSummary | null
  recommended_stocks: RecommendedStock[]
  error?: string  // 可选的错误信息
}

// RPS走势图数据
export interface SectorRpsChartData {
  sector_name: string
  dates: string[]
  rps_20: number[]
  rps_50: number[]
  change_pct: number[]
}

export const signalRadarApi = {
  // 获取信号雷达数据
  getSignalRadar: async (): Promise<SignalRadarData> => {
    const response = await client.get('/signal-radar')
    return response.data
  },
  
  // 获取板块RPS走势图
  getSectorRpsChart: async (sectorName: string, days: number = 60): Promise<SectorRpsChartData> => {
    const response = await client.get(`/sectors/${encodeURIComponent(sectorName)}/rps-chart`, { params: { days } })
    return response.data
  },
}

// ========== 猎鹰雷达相关接口 ==========

// 当日最热板块
export interface HottestSector {
  sector_name: string
  trade_date: string
  main_net_inflow: number
  super_large_inflow: number
  large_inflow: number
  change_pct: number
  limit_up_count: number
  sector_rps_20: number
  sector_rps_50: number
  avg_turnover: number
  display_name?: string
  aggregated_count?: number
}

// 猎鹰推荐个股
export interface FalconRecommendation {
  sheep_code: string
  sheep_name: string
  sector_name?: string
  change_pct?: number
  volume_ratio?: number
  support_price?: number
  platform_top?: number
  volatility?: number
  total_inflow?: number
  amplitude?: number
  flow_data?: Array<{
    trade_date: string
    main_net_inflow: number
  }>
  strategy: 'Leader Pullback' | 'Money Divergence' | 'Box Breakout'
  reason: string
}

// 猎鹰推荐结果
export interface FalconRecommendations {
  leader_pullback: FalconRecommendation[]
  money_divergence: FalconRecommendation[]
  box_breakout: FalconRecommendation[]
  trade_date: string
  error?: string
}

// 市场情绪数据
export interface MarketSentiment {
  trade_date: string
  profit_effect: {
    value: number
    up_count: number
    down_count: number
    total_count: number
    level: 'extreme_cold' | 'cold' | 'neutral' | 'warm' | 'extreme_hot'
    message: string
  }
  consecutive_limit_height: number
  limit_up_failure_rate: {
    value: number
    limit_up_count: number
    failure_count: number
    total: number
    level: 'low_risk' | 'medium_risk' | 'high_risk' | 'neutral'
    message: string
  }
  sentiment_level: string
}

// 智能资金矩阵个股
export interface SmartMoneyStock {
  sheep_code: string
  sheep_name: string
  total_inflow: number
  avg_daily_inflow: number
  latest_trade_date?: string
  change_pct_5d: number
  turnover_rate: number
  ma5_price: number
  current_price: number
  potential_score: number
  is_high_potential: boolean
  potential_reason: string
  daily_data?: Array<{
    trade_date: string
    main_net_inflow: number
  }>
}

// 智能资金矩阵数据
export interface SmartMoneyMatrix {
  stocks: SmartMoneyStock[]
  sectors: SectorMoneyFlowInfo[]
  days: number
  trade_date: string
}

export const falconRadarApi = {
  // 获取当日最热板块
  getHottestSectors: async (limit: number = 10): Promise<{ sectors: HottestSector[], limit: number }> => {
    const response = await client.get('/falcon-radar/hottest', { params: { limit } })
    return response.data
  },
  
  // 获取猎鹰推荐
  getRecommendations: async (): Promise<FalconRecommendations> => {
    const response = await client.get('/falcon-radar/recommendations')
    return response.data
  },
}

export const marketSentimentApi = {
  // 获取市场情绪数据
  getMarketSentiment: async (): Promise<MarketSentiment> => {
    const response = await client.get('/market-sentiment')
    return response.data
  },
}

export const smartMoneyMatrixApi = {
  // 获取智能资金矩阵
  getSmartMoneyMatrix: async (days: number = 1, limit: number = 100): Promise<SmartMoneyMatrix> => {
    const response = await client.get('/smart-money-matrix', { params: { days, limit } })
    return response.data
  },
}
