import client from './client'

export interface BacktestParams {
  // T10 结构狙击者参数
  vol_ratio_max?: number  // 极致缩量阈值（Vol/MA5 < 0.6）
  turnover_min?: number   // 最优换手率下限
  turnover_max?: number   // 最优换手率上限
  golden_pit_change_min?: number // 黄金坑涨跌幅下限
  golden_pit_change_max?: number // 黄金坑涨跌幅上限
  min_score?: number      // 最低评分门槛
  max_recommendations?: number // 最大推荐数量
  prefer_negative_change?: boolean // 优先阴线低吸
  require_sector_bullish?: boolean // 要求板块多头
  
  // 兼容旧版 T7 参数
  min_mv?: number  // 最小市值（亿）
  max_mv?: number  // 最大市值（亿）
  rps_threshold?: number  // RPS阈值
  vol_threshold?: number  // 倍量阈值
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
    result_grade?: 'excellent' | 'good' | 'pass' | 'fail'  // v2.0新增：分层评级
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
    excellent_rate?: number  // v2.0新增：优秀率
    good_rate?: number       // v2.0新增：良好率
    pass_rate?: number       // v2.0新增：及格率
    alpha_rate: number
    total_return: number
    avg_return?: number      // v2.0新增：平均收益
    max_drawdown: number
    stop_loss_rate?: number  // v2.0新增：止损触发率
    benchmark_return: number
    excess_return?: number   // v2.0新增：超额收益
    final_capital: number
  }
  stats?: {  // v2.0新增：详细统计
    total_days: number
    days_with_recommendations: number
    days_without_recommendations: number
    excellent_trades: number
    good_trades: number
    pass_trades: number
    fail_trades: number
    stop_loss_triggered: number
  }
}

export interface Recommendation {
  sheep_code: string
  sheep_name: string
  entry_price: number
  ai_score: number
  reason_tags: string
  stop_loss_price: number
  vol_ratio?: number
  turnover_rate?: number
  change_pct?: number
  rps_250?: number
  industry?: string
  is_star_market?: boolean
  is_gem?: boolean
  estimated_mv?: number
  // T10特有字段
  f1_vol_score?: number
  f2_turnover_score?: number
  f3_rps_score?: number
  is_negative_day?: boolean
  is_extreme_shrink?: boolean
  sniper_setup?: boolean
  model_version?: string
}

export interface RecommendResponse {
  trade_date: string
  recommendations: Recommendation[]
  count: number
  diagnostic_info?: string
  metadata?: {
    market_regime?: string  // 市场状态：Attack/Defense/Balance
    regime_score?: number  // 综合评分（-1到+1）
    funnel_data?: {
      total: number  // 全市场扫描
      L1_pass: number  // Layer 1通过
      L2_pass: number  // Layer 2通过
      L3_pass: number  // Layer 3通过
      L4_pass?: number // Layer 4通过
      final: number  // 最终优选
    }
    // v6.0新增：市场状态各维度评分
    regime_details?: {
      rsrs_score: number
      rsrs_zscore: number
      sector_rotation_score?: number  // v6.0新增：板块轮动强度
      market_breadth_score: number
      volume_score: number
      ma_score: number
      sentiment_score: number
      up_count: number
      down_count: number
      limit_up_count: number
      limit_down_count: number
    }
    // v6.0新增：筛选统计
    filter_stats?: {
      level1_before: number
      level1_after: number
      level2_after: number
      level3_after_quality: number
      level3_after_score: number
    }
    // v3.0新增：启动质量统计
    breakout_stats?: {
      high_quality_count: number  // 优质启动
      medium_quality_count: number  // 中等质量
      trap_risk_count: number  // 骗炮风险
    }
    // 使用的参数快照
    params_used?: Record<string, any>
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

export interface DefaultParamsResponse {
  params: BacktestParams
  veto_conditions: Record<string, number>
  version: string
}

export const modelKApi = {
  // 获取后端默认参数（前端自动同步）
  getDefaultParams: async (): Promise<DefaultParamsResponse> => {
    const response = await client.get('/model-k/default-params')
    return response.data
  },

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
      top_n: topN || 20
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
