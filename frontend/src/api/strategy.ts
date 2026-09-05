import { apiFetch } from './http'

// 新回测允许的研究版本；S0仅供基准对照，历史结果的版本保持原值。
export type StrategyVersion = 'S0' | 'S1' | 'S2' | 'F1'

// 策略调优参数。
export interface StrategyParams {
  ma_fast?: number
  ma_slow?: number
  atr_n?: number
  momentum_n?: number
  slope_n?: number
  breakout_n?: number
  amount_n?: number
  amount_low?: number
  amount_high?: number
  max_extension?: number
  init_stop_atr?: number
  trail_atr?: number
  ma_exit_days?: number
  cooldown_days?: number
  gap_up_limit?: number
  commission?: number
  min_commission?: number
  slippage_bps?: number
  stamp_rate?: number
}

// 回测请求参数。
export interface BacktestParams {
  data_policy?: 'strict' | 'available_candidates'
  version: StrategyVersion
  effect_months: string[]
  from: string
  to: string
  account?: Partial<StrategyAccount>
  params?: Partial<StrategyParams>
}

// 已实现的账户参数，其余字段由服务端拒绝。
export interface StrategyAccount {
  initial_capital: number
  gross_cap_mode: 'nav_fraction' | 'fixed_amount' | 'both'
  gross_cap_ratio: number
  gross_cap_amount: number
  cash_reserve: number
  single_cap_ratio: number
  single_risk: number
  total_risk: number
  max_positions: 1 | 2
  dd_reduce: number
  dd_freeze: number
  dd_halt: number
  target_multiple: number
}

// 保存的研究任务，只有完成状态携带结果。
export interface StrategyRun {
  data_quality?: BacktestResult['data_quality']
  run_id: string
  status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'BLOCKED' | 'FAILED' | 'CANCELLED'
  progress: number
  error: string
  input_hash: string
  model_version: string
  request?: BacktestParams
  result?: BacktestResult
}

// 回测净值点。
export interface BacktestPoint {
  date: string
  nav: number
  drawdown: number
  held: number
  cash_rate: number
}

// 回测机会漏斗。
export interface BacktestFunnel {
  raw20: number
  tradable: number
  market_pass: number
  signal_hit: number
  filled: number
  no_cash: number
  rejected: number
}

// 未执行意图（区间末仍保留：方向/代码/股数/信号日/理由）。
export interface OpenIntent {
  side: string
  ts_code: string
  shares: number
  signal_date: string
  ref_price: number
  stop_price: number
  market_cap: number
  reason: string
}

// 成交流水，不包含未成交意图。
export interface StrategyFill {
  date: string
  signal_date: string
  side: string
  ts_code: string
  shares: number
  price: number
  fee: number
  reason: string
  cash_after: number
}

// 期末持仓，均价不含佣金。
export interface ClosingPosition {
  ts_code: string
  shares: number
  avg_cost: number
  stop: number
}

// 公司行动流水（除权除息、分红、送转）。
export interface CorporateActionFill {
  date: string
  ts_code: string
  old_factor: number
  new_factor: number
  shares_before: number
  shares_after: number
  cash_div: number
  cash_after: number
  reason: string
}

// 一次建仓至完全退出的周期，未完成时净盈亏为空。
export interface TradeCycle {
  ts_code: string
  entry_date: string
  exit_date: string
  shares_remaining: number
  buy_amount: number
  sell_amount: number
  fees: number
  dividends?: number
  net_pnl: number | null
}

// 基础描述统计，无有效分母时不提供比率。
export interface TradeSummary {
  closed_count: number
  open_count: number
  wins: number
  losses: number
  flats: number
  closed_net_pnl: number
  win_rate: number | null
  profit_factor: number | null
}

// 回测结果。
export interface BacktestResult {
  evidence_level?: 'engineering_replay' | 'limited_research'
  data_quality?: { policy: string; unavailable_rows: number; samples_truncated: boolean;
    samples: { ts_code: string; date: string; reason: string }[] }
  limitations?: string[]
  cycles?: TradeCycle[]
  trade_summary?: TradeSummary
  fills?: StrategyFill[]
  corporate_actions?: CorporateActionFill[]
  dividends_received?: number
  final_cash?: number
  fees_paid?: number
  closing_positions?: ClosingPosition[]
  account: StrategyAccount
  params?: StrategyParams
  input_hash: string
  model_version: string
  pools: Record<string, PoolSnapshot>
  version: string
  effect_months: string[]
  points: number
  trades: number
  halted: boolean
  final_nav: number
  total_return: number
  max_drawdown: number
  funnel: BacktestFunnel
  series: BacktestPoint[]
  open_intents: OpenIntent[]
}

// 股票池快照明细。
export interface PoolEntry {
  rank: number
  ts_code: string
  amount: number
}

// 股票池快照。
export interface PoolSnapshot {
  month: string
  range_from: string
  range_to: string
  dataset_v: string
  entries: PoolEntry[]
}

// 提交后台回测，只返回任务编号。
export async function submitBacktest(params: BacktestParams): Promise<StrategyRun> {
  return await apiFetch('/admin', '/strategy/backtests', {
    method: 'POST', body: JSON.stringify(params),
  }) as StrategyRun
}

// 读取保存的研究任务及结果。
export async function getBacktest(id: string): Promise<StrategyRun> {
  return await apiFetch('/admin', `/strategy/backtests/${encodeURIComponent(id)}`) as StrategyRun
}

// 取消活动研究任务，不删除历史。
export async function cancelBacktest(id: string): Promise<void> {
  await apiFetch('/admin', `/strategy/backtests/${encodeURIComponent(id)}`, { method: 'DELETE' })
}

// 读取最近任务供跨页面恢复查看。
export async function listBacktests(): Promise<StrategyRun[]> {
  const data = await apiFetch('/admin', '/strategy/backtests?limit=20') as { runs: StrategyRun[] }
  return data.runs || []
}

// 查询月份股票池快照（真接口 GET /admin/strategy/pools）。
export async function getPool(month: string): Promise<PoolSnapshot> {
  return (await apiFetch('/admin', `/strategy/pools?month=${encodeURIComponent(month)}`)) as PoolSnapshot
}
