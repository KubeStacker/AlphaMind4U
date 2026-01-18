import client from './client'

export interface BacktestParams {
  min_mv?: number  // 最小市值（亿）
  max_mv?: number  // 最大市值（亿）
  ma_support?: 'MA20' | 'MA60' | 'MA120' | null  // 趋势均线，null表示不限制
  rps_threshold?: number  // RPS阈值
  vol_ratio?: number  // 倍量定义
  vol_threshold?: number  // 倍量阈值
  sector_boost?: boolean  // 板块共振
  ai_filter?: boolean  // AI过滤（v3.0已移除，保留兼容）
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
  // v3.0新增：资金流参数
  min_main_inflow?: number  // 最小主力净流入（万元）
  require_positive_inflow?: boolean  // 是否要求主力净流入为正
  // v3.0新增：换手率控制
  min_turnover?: number  // 最小换手率(%)
  max_turnover?: number  // 最大换手率(%)
  // v3.0新增：启动质量控制
  breakout_validation?: boolean  // 启动有效性验证开关
  min_breakout_quality?: number  // 最低启动质量分（0-100）
  // v6.0新增：严苛筛选（提升成功率）
  min_ai_score?: number  // 最低AI评分（0-100），v6.0默认50
  max_recommendations?: number  // 单日最多推荐数量
  require_concept_resonance?: boolean  // 必须有概念共振支撑（概念共振>=20）
  enable_sector_linkage?: boolean  // v6.0新增：启用板块联动筛选（板块联动强度>=0.2）
  // v3.0新增：其他
  prefer_20cm?: boolean  // 偏好20cm肥羊
  // 已移除的参数（保留兼容）
  min_win_probability?: number  // 最小胜率（%）- v3.0已移除
  ai_vol_ratio_min?: number  // AI过滤量比要求 - v3.0已移除
  ai_vcp_factor_max?: number  // AI过滤VCP因子最大值 - v3.0已移除
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
  win_probability: number  // v3.0映射为breakout_quality
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
  // v3.0新增：启动质量
  breakout_quality?: number  // 启动质量分（0-100）
  breakout_warning?: string  // 风险警告
  // v3.1新增：估算市值
  estimated_mv?: number  // 估算流通市值（亿元）
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
      L0_pass: number  // Filter Layer通过
      L1_pass: number  // Feature Layer通过
      L2_pass: number  // Score Layer通过
      L3_pass: number  // Final Filter通过
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
