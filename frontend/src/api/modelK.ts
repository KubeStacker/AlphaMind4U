import client from './client'

export interface BacktestParams {
  min_mv?: number  // 最小市值（亿）
  max_mv?: number  // 最大市值（亿）
  ma_support?: 'MA20' | 'MA60' | 'MA120' | null  // 趋势均线，null表示不限制
  rps_threshold?: number  // RPS阈值
  vol_ratio?: number  // 倍量定义
  vol_threshold?: number  // 倍量阈值
  sector_boost?: boolean  // 板块共振
  ai_filter?: boolean  // AI过滤
  w_tech?: number  // 技术权重
  w_trend?: number  // 趋势权重
  w_hot?: number  // 热度权重
  // Level 2 筛选参数
  min_change_pct?: number  // 最小涨幅（%）
  max_change_pct?: number  // 最大涨幅（%）
  change_pct_required?: boolean  // 是否要求涨幅
  vol_ratio_ma20_threshold?: number  // 量比阈值（Volume/MA20）
  vol_ratio_required?: boolean  // 是否要求量比
  upper_shadow_required?: boolean  // 是否要求上影线比例
  max_upper_shadow_ratio?: number  // 最大上影线比例
  vwap_required?: boolean  // 是否要求VWAP
  vwap_tolerance?: number  // VWAP容忍度（0-1）
  // Attack模式参数
  min_change_pct_attack?: number  // 进攻模式最小涨幅
  max_change_pct_attack?: number  // 进攻模式最大涨幅
  concept_boost?: boolean  // 概念共振优先
  rps_threshold_attack?: number  // 进攻模式RPS阈值
  // Defense模式参数
  max_change_pct_defense?: number  // 防守模式最大涨幅
  // Level 4 AI参数
  min_win_probability?: number  // 最小胜率（%）
  ai_vol_ratio_min?: number  // AI过滤量比要求
  ai_vcp_factor_max?: number  // AI过滤VCP因子最大值
}

export interface BacktestRequest {
  start_date: string
  end_date: string
  params: BacktestParams
}

export interface BacktestResult {
  success: boolean
  message?: string
  trades?: Array<{
    buy_date: string
    sell_date: string
    sheep_code: string
    sheep_name: string
    entry_price: number
    exit_price: number
    return_pct: number
    max_return_5d: number
    result: 'SUCCESS' | 'FAIL'
    params_snapshot: BacktestParams
  }>
  equity_curve?: Array<{
    date: string
    capital: number
    return_pct: number
  }>
  metrics?: {
    total_trades: number
    win_rate: number
    alpha_rate: number
    total_return: number
    max_drawdown: number
    benchmark_return: number
    final_capital: number
  }
}

export interface Recommendation {
  sheep_code: string
  sheep_name: string
  entry_price: number
  ai_score: number
  win_probability: number
  reason_tags: string
  stop_loss_price: number
  vol_ratio?: number
  rps_250?: number
  vcp_factor?: number
  return_5d?: number  // 5日涨幅（%）
  return_10d?: number  // 10日涨幅（%）
  return_nd?: number  // 最近N天涨幅（%，10个交易日内时使用）
  market_regime?: string  // 市场状态：Attack/Defense/Balance
  concept_trend?: string  // 驱动概念（原sector_trend）
  sector_trend?: string  // 所属板块（兼容旧字段）
  resonance_score?: number  // 板块共振分数
  tag_total_inflow?: number  // 驱动概念总资金流入（万元）
  tag_avg_pct?: number  // 驱动概念平均涨幅（%）
  is_star_market?: boolean  // 是否科创板
  is_gem?: boolean  // 是否创业板
}

export interface RecommendResponse {
  trade_date: string
  recommendations: Recommendation[]
  count: number
  diagnostic_info?: string
  metadata?: {
    market_regime?: string  // 市场状态：Attack/Defense/Balance
    funnel_data?: {
      total: number  // 全市场扫描
      L1_pass: number  // 初筛合格
      L2_pass: number  // 资金/形态过滤
      final: number  // 最终优选
    }
  }
}

export interface RecommendationHistory {
  id: number
  run_date: string
  strategy_version: string
  sheep_code: string
  sheep_name: string
  params_snapshot: BacktestParams
  entry_price: number
  ai_score: number
  win_probability: number
  reason_tags: string
  stop_loss_price: number
  is_verified: boolean
  max_return_5d?: number
  final_return_5d?: number
  final_result?: 'SUCCESS' | 'FAIL'
  create_time: string
  update_time: string
}

export const modelKApi = {
  // 执行回测
  runBacktest: async (request: BacktestRequest): Promise<BacktestResult> => {
    const response = await client.post('/model-k/backtest', request)
    return response.data
  },

  // 获取智能推荐
  getRecommendations: async (params: BacktestParams, tradeDate?: string, topN?: number): Promise<RecommendResponse> => {
    const response = await client.post('/model-k/recommend', {
      params,
      trade_date: tradeDate,
      top_n: topN || 20  // 默认返回20只，避免返回过多数据导致超时
    })
    return response.data
  },

  // 获取推荐历史
  getHistory: async (runDate?: string, limit: number = 100, offset: number = 0): Promise<{ recommendations: RecommendationHistory[], count: number }> => {
    const response = await client.get('/model-k/history', {
      params: {
        run_date: runDate,
        limit,
        offset
      }
    })
    return response.data
  },

  // 清空历史记录
  clearHistory: async (failedOnly: boolean = false): Promise<{ message: string }> => {
    const response = await client.delete('/model-k/history', {
      params: {
        failed_only: failedOnly
      }
    })
    return response.data
  }
}
