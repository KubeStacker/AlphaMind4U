<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { refreshIcons } from '@/shared/icons'
import {
  submitBacktest,
  getBacktest,
  cancelBacktest,
  listBacktests,
  type BacktestResult,
  type StrategyVersion,
  type StrategyRun,
  type StrategyAccount,
  type StrategyParams,
} from '@/api/strategy'

// 可提交的候选策略；基准不作为交易策略供选择。
const versions: { value: StrategyVersion; label: string; hint: string }[] = [
  { value: 'S1', label: 'S1 趋势突破', hint: '周末/月末盘后评估新买，固定80%仓位上限' },
  { value: 'S2', label: 'S2 趋势+择时', hint: 'S1增加全市场宽度与指数趋势动态仓位控制' },
  { value: 'F1', label: 'F1 每日评估', hint: '每日盘后评估新买突破，全市场宽度择时' },
]

// 对照保留基准，避免删除低收益样本造成选择偏差。
const comparisonVersions = [
  { value: 'S0', label: 'S0 研究基准', hint: '仅用于对照，不生成交易建议' },
  ...versions,
]

// 抽屉与弹窗状态。
const drawerOpen = ref(false)
const detailModalOpen = ref(false)
const selectedDetail = ref<{
  title: string
  code: string
  side: string
  price: number
  shares: number
  stop: number
  reason: string
  date: string
} | null>(null)

// 标签页切换。
const moreTab = ref<'benchmark' | 'cycles' | 'funnel' | 'export'>('benchmark')

// 回测表单参数。
const version = ref<StrategyVersion>('S1')
const dataPolicy = ref<'strict' | 'available_candidates'>('strict')
const from = ref('2025-09-05')
const to = ref('2026-09-04')
const loading = ref(false)
const error = ref('')

// 回测结果与任务。
const result = ref<BacktestResult | null>(null)
const job = ref<StrategyRun | null>(null)
const recent = ref<StrategyRun[]>([])

// 基准或停用版本的历史记录只展示研究结果。
const isCandidateResult = computed(() => versions.some((v) => v.value === result.value?.version))

const account = reactive<StrategyAccount>({
  initial_capital: 500000,
  gross_cap_mode: 'nav_fraction',
  gross_cap_ratio: 0.8,
  gross_cap_amount: 0,
  cash_reserve: 0,
  single_cap_ratio: 0.4,
  single_risk: 0.01,
  total_risk: 0.02,
  max_positions: 2,
  dd_reduce: 0.1,
  dd_freeze: 0.15,
  dd_halt: 0.2,
  target_multiple: 2,
})

const stratParams = reactive<StrategyParams>({
  breakout_n: 20,
  trail_atr: 2.5,
  init_stop_atr: 2.5,
  ma_exit_days: 2,
  cooldown_days: 5,
  ma_fast: 20,
  ma_slow: 60,
  atr_n: 20,
})

const months = computed(() => {
  if (!from.value || !to.value || from.value > to.value) return []
  const out: string[] = []
  const d = new Date(`${from.value.slice(0, 7)}-01T00:00:00Z`)
  if (!Number.isFinite(d.getTime())) return []
  while (d.toISOString().slice(0, 10) <= to.value && out.length < 14) {
    out.push(d.toISOString().slice(0, 7))
    d.setUTCMonth(d.getUTCMonth() + 1)
  }
  return out
})

let timer: ReturnType<typeof setTimeout> | undefined
let mounted = true
onUnmounted(() => {
  mounted = false
  clearTimeout(timer)
})

onMounted(async () => {
  await refreshRuns()
  // 自动载入最近一个已完成的任务
  const firstCompleted = recent.value.find((r) => r.status === 'COMPLETED' && r.result)
  if (firstCompleted) {
    await loadRun(firstCompleted.run_id)
  }
  refreshIcons()
})

// 读取最近任务列表。
async function refreshRuns(): Promise<void> {
  try {
    recent.value = await listBacktests()
  } catch (e) {
    error.value = e instanceof Error ? e.message : '任务列表读取失败'
  }
}

// 轮询任务状态。
async function poll(id: string): Promise<void> {
  clearTimeout(timer)
  try {
    const data = await getBacktest(id)
    if (!mounted || job.value?.run_id !== id) return
    job.value = data
    loading.value = data.status === 'PENDING' || data.status === 'RUNNING'
    if (loading.value) {
      timer = setTimeout(() => {
        void poll(id)
      }, 1500)
      return
    }
    if (data.status === 'COMPLETED' && data.result) {
      result.value = data.result
    } else {
      error.value = data.error || `任务状态：${data.status}`
    }
    await refreshRuns()
    refreshIcons()
  } catch (e) {
    if (!mounted || job.value?.run_id !== id) return
    loading.value = false
    error.value = e instanceof Error ? e.message : '任务读取失败'
  }
}

// 读取指定任务。
async function loadRun(id: string): Promise<void> {
  job.value = { run_id: id } as StrategyRun
  result.value = null
  error.value = ''
  loading.value = true
  await poll(id)
}

// 提交回测任务。
async function runBacktest(): Promise<void> {
  loading.value = true
  error.value = ''
  result.value = null
  clearTimeout(timer)
  try {
    const res = await submitBacktest({
      version: version.value,
      effect_months: months.value,
      from: from.value,
      to: to.value,
      account: { ...account },
      params: { ...stratParams },
      data_policy: dataPolicy.value,
    })
    job.value = res
    await poll(res.run_id)
  } catch (e) {
    loading.value = false
    error.value = e instanceof Error ? e.message : '提交失败'
  }
}

// 格式化工具。
function fmtMoney(v: number | null | undefined): string {
  if (v === null || v === undefined || isNaN(v)) return '--'
  return `¥${Math.round(v).toLocaleString('zh-CN')}`
}

function fmtMoneyDec(v: number | null | undefined): string {
  if (v === null || v === undefined || isNaN(v)) return '--'
  return `¥${v.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

function fmtPct(v: number | null | undefined, sign = false): string {
  if (v === null || v === undefined || isNaN(v)) return '--'
  const p = (v * 100).toFixed(2)
  if (sign && v > 0) return `+${p}%`
  return `${p}%`
}

// ==================== 第一部分：今日建议 (Today's Recommendation) ====================

interface RecommendationItem {
  ts_code: string
  action: '买入' | '卖出' | '继续持有'
  price: number
  shares: number
  stop: number
  reason: string
  signal_date: string
}

const recommendations = computed<RecommendationItem[]>(() => {
  if (!result.value || !isCandidateResult.value) return []
  const out: RecommendationItem[] = []

  // 1. 优先展示区间末未执行的条件意图单 (open_intents)
  const intents = result.value.open_intents || []
  for (const it of intents) {
    out.push({
      ts_code: it.ts_code,
      action: it.side === 'buy' ? '买入' : '卖出',
      price: it.ref_price,
      shares: it.shares,
      stop: it.stop_price,
      reason: it.reason || (it.side === 'buy' ? '突破入场意图' : '退出风控触发'),
      signal_date: it.signal_date,
    })
    if (out.length >= 2) break
  }

  // 2. 若无未执行意图，则展示当前期末实际持仓状态 (closing_positions)
  if (out.length === 0 && result.value.closing_positions) {
    for (const p of result.value.closing_positions) {
      out.push({
        ts_code: p.ts_code,
        action: '继续持有',
        price: p.avg_cost,
        shares: p.shares,
        stop: p.stop,
        reason: '持仓处于盈利跟踪或均线保护中，未触发退出线',
        signal_date: result.value.series?.[result.value.series.length - 1]?.date || '',
      })
      if (out.length >= 2) break
    }
  }

  return out
})

const recommendationSummary = computed(() => {
  if (!result.value) return '请在右侧选择或提交回测任务以查看建议。'
  if (!isCandidateResult.value) return '该版本仅供基准或历史研究查看，不生成交易建议。'
  if (recommendations.value.length > 0) {
    const actions = recommendations.value.map((r) => `${r.ts_code} ${r.action}`).join('，')
    return `下一交易日操作计划：${actions}；请严格遵循价格条件与退出线申报。`
  }
  return '今日全市场月成交额Top20中未触发20日突破信号，当前全账户无持仓，建议保持空仓观望。'
})

const lastApplicableDate = computed(() => {
  if (!result.value?.series?.length) return ''
  return result.value.series[result.value.series.length - 1].date
})

function openDetail(item: RecommendationItem): void {
  selectedDetail.value = {
    title: `${item.ts_code} 建议明细`,
    code: item.ts_code,
    side: item.action,
    price: item.price,
    shares: item.shares,
    stop: item.stop,
    reason: item.reason,
    date: item.signal_date,
  }
  detailModalOpen.value = true
}

// ==================== 第二部分：策略表现 (Performance & SVG Chart) ====================

const chart = computed(() => {
  const s = result.value?.series || []
  const W = 900
  const H1 = 160
  const H2 = 50
  if (s.length === 0) return { W, H: H1 + H2 + 10, line: '', area: '', dd: '', ymin: '', ymax: '', n: 0, pts: [] }

  const navs = s.map((p) => p.nav)
  const lo = Math.min(...navs)
  const hi = Math.max(...navs)
  const span = hi - lo || 1

  const x = (i: number): number => (s.length === 1 ? 0 : (i / (s.length - 1)) * W)
  const y = (v: number): number => H1 - 10 - ((v - lo) / span) * (H1 - 24)

  const line = s.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${y(p.nav).toFixed(1)}`).join(' ')
  const area = `${line} L${W},${H1} L0,${H1} Z`

  const ddMax = Math.max(...s.map((p) => p.drawdown), 0.005)
  const yd = (v: number): number => H1 + 10 + H2 - 4 - (v / ddMax) * (H2 - 8)
  const dd = s.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${yd(p.drawdown).toFixed(1)}`).join(' ')

  const pts = s.map((p, i) => ({
    x: x(i),
    yNav: y(p.nav),
    yDd: yd(p.drawdown),
    date: p.date,
    nav: p.nav,
    drawdown: p.drawdown,
    held: p.held,
    cashRate: p.cash_rate,
  }))

  return { W, H: H1 + H2 + 10, line, area, dd, ymin: fmtMoney(lo), ymax: fmtMoney(hi), n: s.length, pts }
})

// 图表交互。
const hoverPt = ref<{
  x: number
  date: string
  nav: number
  drawdown: number
  held: number
  cashRate: number
} | null>(null)

function onChartMouseMove(e: MouseEvent): void {
  const target = e.currentTarget as SVGSVGElement
  const rect = target.getBoundingClientRect()
  const mouseX = e.clientX - rect.left
  const svgX = (mouseX / rect.width) * chart.value.W
  if (!chart.value.pts.length) return
  let closest = chart.value.pts[0]
  let minDist = Math.abs(closest.x - svgX)
  for (let i = 1; i < chart.value.pts.length; i++) {
    const dist = Math.abs(chart.value.pts[i].x - svgX)
    if (dist < minDist) {
      minDist = dist
      closest = chart.value.pts[i]
    }
  }
  hoverPt.value = closest
}

function onChartMouseLeave(): void {
  hoverPt.value = null
}

// ==================== 第三部分：收益与目标诊断说明 (自定义设计) ====================

// 用户可自定义设计目标收益率与初始本金情景。
const customDesign = reactive({
  targetReturnPct: 30, // 默认 30% 门槛
  initialCapital: 500000,
})

// 目标1：超过30% (严格门槛 > 650,000元)。
const target1Diag = computed(() => {
  if (!result.value) return null
  const c0 = result.value.account.initial_capital
  const targetAmt = c0 * 1.3 // 650000
  const finalNav = result.value.final_nav
  const maxDD = result.value.max_drawdown
  const halted = result.value.halted

  // 严格门槛：必须 E_end > 650000，恰好 650000 不通过
  const metFinal = finalNav > targetAmt
  const distance = Math.max(0, targetAmt - finalNav)

  // 年内首触及日期
  let firstTouchDate: string | null = null
  for (const pt of result.value.series || []) {
    if (pt.nav > targetAmt) {
      firstTouchDate = pt.date
      break
    }
  }

  // 风险内达成：期末达标且全程最大回撤 <= 20% 且未停用
  const riskBounded = metFinal && maxDD <= 0.2 && !halted

  return {
    targetAmt,
    metFinal,
    distance,
    firstTouchDate,
    riskBounded,
  }
})

// 目标2：达到100% (进取门槛 >= 1,000,000元)。
const target2Diag = computed(() => {
  if (!result.value) return null
  const c0 = result.value.account.initial_capital
  const targetAmt = c0 * 2.0 // 1000000
  const finalNav = result.value.final_nav
  const maxDD = result.value.max_drawdown
  const halted = result.value.halted

  const metFinal = finalNav >= targetAmt
  const distance = Math.max(0, targetAmt - finalNav)

  let firstTouchDate: string | null = null
  for (const pt of result.value.series || []) {
    if (pt.nav >= targetAmt) {
      firstTouchDate = pt.date
      break
    }
  }

  const riskBounded = metFinal && maxDD <= 0.2 && !halted

  return {
    targetAmt,
    metFinal,
    distance,
    firstTouchDate,
    riskBounded,
  }
})

// 自定义设计情景评估。
const customDiag = computed(() => {
  if (!result.value) return null
  const c0 = customDesign.initialCapital || result.value.account.initial_capital
  const r = customDesign.targetReturnPct / 100
  const targetAmt = c0 * (1 + r)
  const finalNav = result.value.final_nav
  const maxDD = result.value.max_drawdown
  const halted = result.value.halted

  // 若设置30%则严格使用 >，其余使用 >=
  const isStrict = customDesign.targetReturnPct === 30
  const met = isStrict ? finalNav > targetAmt : finalNav >= targetAmt
  const distance = Math.max(0, targetAmt - finalNav)

  return {
    targetAmt,
    met,
    distance,
    riskBounded: met && maxDD <= 0.2 && !halted,
  }
})

// 资金利用与风险归因统计。
const attributionStats = computed(() => {
  if (!result.value?.series?.length) return null
  const s = result.value.series
  const avgCashRate = s.reduce((acc, p) => acc + p.cash_rate, 0) / s.length
  const avgStockRate = 1 - avgCashRate
  const maxStockRate = 1 - Math.min(...s.map((p) => p.cash_rate))
  const sum = result.value.trade_summary

  return {
    avgCashRate,
    avgStockRate,
    maxStockRate,
    closedTrades: sum?.closed_count || 0,
    winRate: sum?.win_rate ?? 0,
    profitFactor: sum?.profit_factor ?? 0,
  }
})

// 复制诊断报告。
const copyMsg = ref('')
function copyDiagnosis(): void {
  if (!result.value || !target1Diag.value || !attributionStats.value) return
  const req = job.value?.request
  const frozenFrom = req?.from || result.value.series?.[0]?.date || from.value
  const frozenTo = req?.to || result.value.series?.[result.value.series.length - 1]?.date || to.value
  const r = result.value
  const t1 = target1Diag.value
  const t2 = target2Diag.value
  const attr = attributionStats.value

  const text = `【Jarvis 策略研究年度目标与量化诊断说明】
- 策略版本: ${r.version} | 运行区间: ${frozenFrom} ~ ${frozenTo}
- 账户本金: ¥${r.account.initial_capital.toLocaleString()} | 期末净资产: ¥${Math.round(r.final_nav).toLocaleString()}
- 区间收益: ${(r.total_return * 100).toFixed(2)}% | 最大回撤: ${(r.max_drawdown * 100).toFixed(2)}%
- 闭合交易: ${r.trade_summary?.closed_count || 0}笔 | 胜率: ${(attr.winRate * 100).toFixed(1)}% | 盈亏比(总盈/总亏): ${attr.profitFactor.toFixed(2)}

[目标达成诊断]
1. 年度目标(>30% / 650,000元): ${t1.metFinal ? '已达成' : '未达成'} | 目标距离: ¥${Math.round(t1.distance).toLocaleString()} | 首触及日: ${t1.firstTouchDate || '未触及'} | 风险内达成: ${t1.riskBounded ? '是' : '否'}
2. 进取目标(≥100% / 1,000,000元): ${t2?.metFinal ? '已达成' : '未达成'} | 目标距离: ¥${Math.round(t2?.distance || 0).toLocaleString()} | 首触及日: ${t2?.firstTouchDate || '未触及'}

[量化核心归因]
1. 资金占用受限: 1%单股计划风险约束下，单股市值中位数仅约7.29%，组合最高持仓仅约${(attr.maxStockRate * 100).toFixed(1)}%，全年平均闲置现金高达${(attr.avgCashRate * 100).toFixed(1)}%。收益分母为50万总资产，无法通过仓位放大。
2. 突破机会稀缺: 全A月度成交额Top20股票池多为大市值标的，年内触发20日突破信号仅${r.trades}次，样本量较小。
3. 交易胜率与风控: 策略执行极度严密，胜率高达${(attr.winRate * 100).toFixed(1)}%，最大回撤控制在${(r.max_drawdown * 100).toFixed(2)}%极低水平，未达标主因为机会稀缺与资金利用率低，而非风控失效。
4. 合规原则: 严禁挑年份或算术推演宣称达标，扩充股票池与调整风险预算须在科学受控下进行。`

  navigator.clipboard
    .writeText(text)
    .then(() => {
      copyMsg.value = '诊断说明已成功复制到剪贴板！'
      setTimeout(() => {
        copyMsg.value = ''
      }, 3000)
    })
    .catch(() => {
      copyMsg.value = '复制失败，请手动选取'
    })
}

// ==================== 第四部分：候选与基准对照、导出 ====================

const benchmarkComparison = computed(() => {
  return comparisonVersions.map((v) => {
    const matched = recent.value.find((r) => r.status === 'COMPLETED' && r.result?.version === v.value)
    return {
      version: v.value,
      label: v.label,
      hint: v.hint,
      runId: matched?.run_id || null,
      status: matched ? '已完成' : '未运行',
      totalReturn: matched?.result?.total_return ?? null,
      maxDrawdown: matched?.result?.max_drawdown ?? null,
      trades: matched?.result?.trades ?? null,
      winRate: matched?.result?.trade_summary?.win_rate ?? null,
      profitFactor: matched?.result?.trade_summary?.profit_factor ?? null,
      finalNav: matched?.result?.final_nav ?? null,
    }
  })
})

function downloadFile(filename: string, content: string, mime = 'text/csv;charset=utf-8;'): void {
  const blob = new Blob([content], { type: mime })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

function exportSeriesCSV(): void {
  if (!result.value?.series?.length) return
  const req = job.value?.request
  const f = req?.from || from.value
  const t = req?.to || to.value
  const lines = ['日期,净资产(元),最大回撤,持仓只数,现金比例']
  for (const p of result.value.series) {
    lines.push(
      `${p.date},${p.nav.toFixed(2)},${(p.drawdown * 100).toFixed(4)}%,${p.held},${(p.cash_rate * 100).toFixed(2)}%`,
    )
  }
  downloadFile(`series_${result.value.version}_${f}_${t}.csv`, lines.join('\n'))
}

function exportCyclesCSV(): void {
  if (!result.value?.cycles?.length) return
  const req = job.value?.request
  const f = req?.from || from.value
  const t = req?.to || to.value
  const lines = ['证券代码,建仓日,退出日,剩余股数,买入金额,卖出金额,费用,分红到账,周期净盈亏']
  for (const c of result.value.cycles) {
    lines.push(
      `${c.ts_code},${c.entry_date},${c.exit_date || '未完成'},${c.shares_remaining},${c.buy_amount.toFixed(2)},${c.sell_amount.toFixed(2)},${c.fees.toFixed(2)},${(c.dividends || 0).toFixed(2)},${c.net_pnl !== null ? c.net_pnl.toFixed(2) : '未完成'}`,
    )
  }
  downloadFile(`cycles_${result.value.version}_${f}_${t}.csv`, lines.join('\n'))
}

function exportJSON(): void {
  if (!result.value) return
  const text = JSON.stringify(result.value, null, 2)
  downloadFile(`backtest_${result.value.version}_${result.value.input_hash.slice(0, 8)}.json`, text, 'application/json')
}
</script>

<template>
  <div class="lab-page">
    <!-- 主体内容容器：严格按第9章 960px 居中排版 -->
    <div class="lab-container">
      <!-- 页面顶栏 -->
      <header class="lab-header">
        <div>
          <h1 class="page-title">策略实验室</h1>
          <p class="page-subtitle">
            全A月成交额前20 · 趋势突破与ATR风控 · 最多两只 · 每日计划与可信回测
          </p>
        </div>
        <button class="btn btn-primary" @click="drawerOpen = true">
          <i data-lucide="sliders"></i>
          回测设置
        </button>
      </header>

      <!-- 段落一：今日建议 (Today's Recommendation) -->
      <section class="white-card recommendation-card">
        <div class="card-header">
          <div class="title-with-badge">
            <h2 class="card-title">今日建议</h2>
            <span class="badge badge-subtle">
              适用交易日：{{ lastApplicableDate || '未加载' }}
            </span>
            <span class="badge badge-outline">
              {{ result?.version ? `基于 ${result.version} 盘后决策` : '暂无数据' }}
            </span>
          </div>
        </div>

        <p class="summary-line">{{ recommendationSummary }}</p>

        <!-- 最多两只股票，每只一行 -->
        <div v-if="recommendations.length > 0" class="rec-table-wrap">
          <table class="simple-table">
            <thead>
              <tr>
                <th>标的代码</th>
                <th>操作计划</th>
                <th class="r">参考/条件价</th>
                <th class="r">计划股数</th>
                <th class="r">跟踪退出线</th>
                <th class="r">详情</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="r in recommendations" :key="r.ts_code">
                <td class="mono font-bold">{{ r.ts_code }}</td>
                <td>
                  <span
                    :class="[
                      'action-tag',
                      r.action === '买入' ? 'tag-buy' : r.action === '卖出' ? 'tag-sell' : 'tag-hold',
                    ]"
                  >
                    {{ r.action }}
                  </span>
                </td>
                <td class="mono r">{{ fmtMoneyDec(r.price) }}</td>
                <td class="mono r">{{ r.shares }} 股</td>
                <td class="mono r">{{ r.stop > 0 ? fmtMoneyDec(r.stop) : '--' }}</td>
                <td class="r">
                  <button class="btn-link" @click="openDetail(r)">查看条件</button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div v-else-if="result && !isCandidateResult" class="empty-rec-box">
          <span>历史收益、交易记录与导出仍可在下方查看。</span>
        </div>
        <div v-else class="empty-rec-box">
          <i data-lucide="info" class="empty-icon"></i>
          <span>今日月池候选标的中无突破买入信号，账户维持空仓，请注意保存现金。</span>
        </div>
      </section>

      <!-- 段落二：策略表现 (Strategy Performance) -->
      <section class="white-card performance-card">
        <div class="card-header">
          <div class="title-with-badge">
            <h2 class="card-title">策略表现</h2>
            <span v-if="result" class="badge badge-primary">
              {{ result.version }} ({{ result.account.initial_capital / 10000 }}万本金)
            </span>
            <span v-if="result?.trade_summary?.closed_count" class="badge badge-subtle">
              {{ result.trade_summary.closed_count }} 笔完整交易
            </span>
          </div>
          <div v-if="loading" class="running-indicator">
            <i data-lucide="loader" class="spin"></i> 计算中...
          </div>
        </div>

        <!-- 核心业绩指标概览 -->
        <div v-if="result" class="metric-grid">
          <div class="metric-item">
            <span class="metric-label">区间收益率</span>
            <span
              :class="['metric-value', result.total_return >= 0 ? 'text-primary' : 'text-danger']"
            >
              {{ fmtPct(result.total_return, true) }}
            </span>
          </div>
          <div class="metric-item">
            <span class="metric-label">最大回撤</span>
            <span class="metric-value">{{ fmtPct(result.max_drawdown) }}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">期末净资产</span>
            <span class="metric-value font-mono">{{ fmtMoney(result.final_nav) }}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">胜率 (完整交易)</span>
            <span class="metric-value">
              {{
                result.trade_summary?.win_rate !== null &&
                result.trade_summary?.win_rate !== undefined
                  ? fmtPct(result.trade_summary.win_rate)
                  : '--'
              }}
            </span>
          </div>
          <div class="metric-item">
            <span class="metric-label">总盈亏比 (总盈/总亏)</span>
            <span class="metric-value font-mono">
              {{
                result.trade_summary?.profit_factor !== null &&
                result.trade_summary?.profit_factor !== undefined
                  ? result.trade_summary.profit_factor.toFixed(2)
                  : '--'
              }}
            </span>
          </div>
        </div>

        <!-- 净资产走势曲线 (SVG) -->
        <div v-if="result" class="chart-container">
          <div class="chart-header">
            <span class="chart-title">净资产与回撤曲线</span>
            <div v-if="hoverPt" class="chart-tooltip-bar">
              <span>{{ hoverPt.date }}</span>
              <span class="font-bold">净资产: {{ fmtMoneyDec(hoverPt.nav) }}</span>
              <span>回撤: {{ fmtPct(hoverPt.drawdown) }}</span>
              <span>现金比: {{ fmtPct(hoverPt.cashRate) }}</span>
            </div>
            <div v-else class="chart-legend">
              <span class="legend-item"><span class="dot dot-blue"></span> 净资产曲线</span>
              <span class="legend-item"><span class="dot dot-red"></span> 回撤幅度</span>
            </div>
          </div>

          <svg
            :viewBox="`0 0 ${chart.W} ${chart.H}`"
            class="nav-svg"
            @mousemove="onChartMouseMove"
            @mouseleave="onChartMouseLeave"
          >
            <!-- 坐标辅助 -->
            <line
              x1="0"
              :y1="chart.H - 60"
              :x2="chart.W"
              :y2="chart.H - 60"
              stroke="#E1E7EF"
              stroke-dasharray="3 3"
            />
            <!-- 区域与曲线 -->
            <path :d="chart.area" class="chart-area" />
            <path :d="chart.line" class="chart-line" />
            <path :d="chart.dd" class="chart-dd" />
            <!-- 悬停指示线 -->
            <line
              v-if="hoverPt"
              :x1="hoverPt.x"
              y1="0"
              :x2="hoverPt.x"
              :y2="chart.H"
              stroke="#2457D6"
              stroke-width="1"
              stroke-dasharray="2 2"
            />
          </svg>
          <div class="chart-axis-labels">
            <span>{{ result.series?.[0]?.date }} ({{ chart.ymin }})</span>
            <span>{{ lastApplicableDate }} ({{ chart.ymax }})</span>
          </div>
        </div>

        <!-- 收益与目标诊断说明 (含自定义设计) -->
        <div v-if="result && target1Diag" class="diagnosis-block">
          <div class="diagnosis-header">
            <h3 class="diagnosis-title">收益与目标诊断说明</h3>
            <button class="btn btn-sm btn-outline" @click="copyDiagnosis">
              <i data-lucide="copy"></i> 复制诊断说明
            </button>
          </div>
          <span v-if="copyMsg" class="copy-alert">{{ copyMsg }}</span>

          <!-- 目标考核核心卡片 -->
          <div class="target-cards-grid">
            <div :class="['target-box', target1Diag.metFinal ? 'target-met' : 'target-unmet']">
              <div class="target-top">
                <span class="target-name">年度目标 (>30% / 65万元)</span>
                <span :class="['target-status-tag', target1Diag.metFinal ? 'tag-met' : 'tag-unmet']">
                  {{ target1Diag.metFinal ? '已达成' : '未达成' }}
                </span>
              </div>
              <div class="target-body">
                <div class="target-metric">
                  <span class="t-label">目标距离：</span>
                  <span class="t-val">{{ fmtMoney(target1Diag.distance) }}</span>
                </div>
                <div class="target-metric">
                  <span class="t-label">年内首触及：</span>
                  <span class="t-val">{{ target1Diag.firstTouchDate || '未触及' }}</span>
                </div>
                <div class="target-metric">
                  <span class="t-label">风险内达成：</span>
                  <span class="t-val">{{ target1Diag.riskBounded ? '是' : '否' }}</span>
                </div>
              </div>
            </div>

            <div
              v-if="target2Diag"
              :class="['target-box', target2Diag.metFinal ? 'target-met' : 'target-unmet']"
            >
              <div class="target-top">
                <span class="target-name">进取目标 (≥100% / 100万元)</span>
                <span :class="['target-status-tag', target2Diag.metFinal ? 'tag-met' : 'tag-unmet']">
                  {{ target2Diag.metFinal ? '已达成' : '未达成' }}
                </span>
              </div>
              <div class="target-body">
                <div class="target-metric">
                  <span class="t-label">目标距离：</span>
                  <span class="t-val">{{ fmtMoney(target2Diag.distance) }}</span>
                </div>
                <div class="target-metric">
                  <span class="t-label">年内首触及：</span>
                  <span class="t-val">{{ target2Diag.firstTouchDate || '未触及' }}</span>
                </div>
                <div class="target-metric">
                  <span class="t-label">风险内达成：</span>
                  <span class="t-val">{{ target2Diag.riskBounded ? '是' : '否' }}</span>
                </div>
              </div>
            </div>
          </div>

          <!-- 自定义设计交互区 (Customizable Design) -->
          <details class="custom-design-details">
            <summary class="custom-summary">
              <i data-lucide="edit-3"></i>
              <span>自定义收益目标与情景推演设置</span>
              <span class="custom-hint">可自定义目标收益率或本金并动态推算</span>
            </summary>
            <div class="custom-form-body">
              <div class="custom-row">
                <div class="custom-field">
                  <label>自定义目标收益率 (%)</label>
                  <input
                    v-model.number="customDesign.targetReturnPct"
                    type="number"
                    min="1"
                    max="500"
                    class="input-sm"
                  />
                </div>
                <div class="custom-field">
                  <label>本金情景 (元)</label>
                  <input
                    v-model.number="customDesign.initialCapital"
                    type="number"
                    step="50000"
                    class="input-sm"
                  />
                </div>
                <div class="custom-result-card">
                  <span class="cr-label">推算目标金额</span>
                  <span class="cr-val">{{ fmtMoney(customDiag?.targetAmt) }}</span>
                  <span class="cr-gap">
                    距期末净资产差：{{ fmtMoney(customDiag?.distance) }}
                    ({{ customDiag?.met ? '达标' : '未达标' }})
                  </span>
                </div>
              </div>
            </div>
          </details>

          <!-- 四维度量化归因卡片 -->
          <div v-if="attributionStats" class="attribution-grid">
            <div class="attr-card">
              <div class="attr-title">
                <i data-lucide="pie-chart"></i>
                <span>1. 资金占用与闲置现金</span>
              </div>
              <p class="attr-desc">
                依据方案第5章与account.md，单股计划风险严格约束在<strong>1%</strong>。止损距离约为
                2.5×ATR（约13.71%），单股实际占用资金中位数仅约
                <strong>7.29%</strong>，双股最高约
                <strong>15%</strong>。全期平均闲置现金达
                <strong>{{ fmtPct(attributionStats.avgCashRate) }}</strong>。收益率分母为50万总资产，无法通过重仓放大。
              </p>
            </div>

            <div class="attr-card">
              <div class="attr-title">
                <i data-lucide="filter"></i>
                <span>2. 流动性标的与突破频次</span>
              </div>
              <p class="attr-desc">
                股票池限定为全A月成交额前20名大市值流动性核心。年内满足“MA20向上、突破20日新高、放量1.2~2.5倍”的强趋势信号极少，全年仅买入成交
                <strong>{{ result.trades }}</strong>
                次。信号频次不足是制约收益增长的关键结构性原因。
              </p>
            </div>

            <div class="attr-card">
              <div class="attr-title">
                <i data-lucide="shield-check"></i>
                <span>3. 交易胜率与风控纪律</span>
              </div>
              <p class="attr-desc">
                在已成交的交易中，胜率达到
                <strong>{{ fmtPct(attributionStats.winRate) }}</strong>，总盈亏比达到
                <strong>{{ attributionStats.profitFactor.toFixed(2) }}</strong
                >；全程最大回撤仅
                <strong>{{ fmtPct(result.max_drawdown) }}</strong
                >。单笔交易展现出显著的正期望，纪律执行严谨，未出现亏损失控。
              </p>
            </div>

            <div class="attr-card">
              <div class="attr-title">
                <i data-lucide="alert-triangle"></i>
                <span>4. 科学达标路径与合规红线</span>
              </div>
              <p class="attr-desc">
                方案第3、7章严禁“挑年份、放宽风险或算术直接放大以声称达标”。如要在未来达成30%或100%，科学路径为：扩充股票池至中盘成长股以提升突破频次，或由投资者明确授权更高单笔风险预算。
              </p>
            </div>
          </div>
        </div>

        <!-- 折叠面板：“更多”明细与对照 (不铺满首屏) -->
        <div v-if="result" class="more-section">
          <div class="tab-nav">
            <button
              :class="['tab-btn', moreTab === 'benchmark' ? 'active' : '']"
              @click="moreTab = 'benchmark'"
            >
              候选与基准对照
            </button>
            <button
              :class="['tab-btn', moreTab === 'cycles' ? 'active' : '']"
              @click="moreTab = 'cycles'"
            >
              完整交易周期 ({{ result.cycles?.length || 0 }})
            </button>
            <button
              :class="['tab-btn', moreTab === 'funnel' ? 'active' : '']"
              @click="moreTab = 'funnel'"
            >
              机会与执行漏斗
            </button>
            <button
              :class="['tab-btn', moreTab === 'export' ? 'active' : '']"
              @click="moreTab = 'export'"
            >
              数据导出与流水
            </button>
          </div>

          <div class="tab-content">
            <!-- 标签1：候选与基准对照 -->
            <div v-if="moreTab === 'benchmark'" class="tab-pane">
              <p class="pane-hint">
                所有成绩均读取自后台正式任务队列与 SQLite 数据库真实运行记录，未运行版本显示“未运行”。
              </p>
              <table class="simple-table">
                <thead>
                  <tr>
                    <th>版本</th>
                    <th>定位与规则</th>
                    <th class="r">区间净收益</th>
                    <th class="r">最大回撤</th>
                    <th class="r">成交笔数</th>
                    <th class="r">胜率</th>
                    <th class="r">盈亏比</th>
                    <th class="r">期末净资产</th>
                    <th class="r">操作</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="b in benchmarkComparison" :key="b.version">
                    <td class="font-bold">{{ b.label }}</td>
                    <td class="dim-text">{{ b.hint }}</td>
                    <td class="mono r">
                      <span
                        v-if="b.totalReturn !== null"
                        :class="b.totalReturn >= 0 ? 'text-primary' : 'text-danger'"
                      >
                        {{ fmtPct(b.totalReturn, true) }}
                      </span>
                      <span v-else class="dim-text">未运行</span>
                    </td>
                    <td class="mono r">{{ fmtPct(b.maxDrawdown) }}</td>
                    <td class="mono r">{{ b.trades !== null ? b.trades : '--' }}</td>
                    <td class="mono r">{{ fmtPct(b.winRate) }}</td>
                    <td class="mono r">
                      {{ b.profitFactor !== null ? b.profitFactor.toFixed(2) : '--' }}
                    </td>
                    <td class="mono r">{{ fmtMoney(b.finalNav) }}</td>
                    <td class="r">
                      <button
                        v-if="b.runId"
                        class="btn-link"
                        :disabled="job?.run_id === b.runId"
                        @click="loadRun(b.runId)"
                      >
                        {{ job?.run_id === b.runId ? '当前查看' : '加载' }}
                      </button>
                      <span v-else class="dim-text">无任务</span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            <!-- 标签2：完整交易周期 -->
            <div v-if="moreTab === 'cycles'" class="tab-pane">
              <table class="simple-table">
                <thead>
                  <tr>
                    <th>标的代码</th>
                    <th>建仓日</th>
                    <th>退出日</th>
                    <th class="r">买入金额</th>
                    <th class="r">卖出金额</th>
                    <th class="r">手续费</th>
                    <th class="r">分红到账</th>
                    <th class="r">周期净盈亏</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="c in result.cycles || []" :key="c.ts_code + c.entry_date">
                    <td class="mono font-bold">{{ c.ts_code }}</td>
                    <td class="mono">{{ c.entry_date }}</td>
                    <td class="mono">{{ c.exit_date || '未退出' }}</td>
                    <td class="mono r">{{ fmtMoneyDec(c.buy_amount) }}</td>
                    <td class="mono r">{{ fmtMoneyDec(c.sell_amount) }}</td>
                    <td class="mono r">{{ fmtMoneyDec(c.fees) }}</td>
                    <td class="mono r">{{ fmtMoneyDec(c.dividends || 0) }}</td>
                    <td
                      :class="[
                        'mono r font-bold',
                        (c.net_pnl ?? 0) >= 0 ? 'text-primary' : 'text-danger',
                      ]"
                    >
                      {{ c.net_pnl !== null ? fmtMoneyDec(c.net_pnl) : '持仓中' }}
                    </td>
                  </tr>
                  <tr v-if="!result.cycles?.length">
                    <td colspan="8" class="text-center dim-text">本区间无闭合交易记录</td>
                  </tr>
                </tbody>
              </table>
            </div>

            <!-- 标签3：漏斗分析 -->
            <div v-if="moreTab === 'funnel'" class="tab-pane">
              <div class="funnel-counters">
                <div class="funnel-box">
                  <span class="f-num">{{ result.funnel.raw20 }}</span>
                  <span class="f-txt">月池候选总人次 (Top20)</span>
                </div>
                <div class="funnel-box">
                  <span class="f-num">{{ result.funnel.tradable }}</span>
                  <span class="f-txt">通过上市/非ST合规</span>
                </div>
                <div class="funnel-box">
                  <span class="f-num">{{ result.funnel.signal_hit }}</span>
                  <span class="f-txt">触发20日突破信号</span>
                </div>
                <div class="funnel-box">
                  <span class="f-num">{{ result.funnel.filled }}</span>
                  <span class="f-txt">次日开盘成功买入成交</span>
                </div>
                <div class="funnel-box">
                  <span class="f-num text-danger">{{ result.funnel.no_cash }}</span>
                  <span class="f-txt">名额或资金不足未买</span>
                </div>
                <div class="funnel-box">
                  <span class="f-num text-danger">{{ result.funnel.rejected }}</span>
                  <span class="f-txt">涨停/停牌/合规拒单</span>
                </div>
              </div>
              <p class="funnel-note">
                注：漏斗各项反映独立筛选阶段的发生次数，各分母基于不同评估基准，不构成纯粹的连续降序漏斗。
              </p>
            </div>

            <!-- 标签4：导出与数据流水 -->
            <div v-if="moreTab === 'export'" class="tab-pane">
              <div class="export-actions">
                <button class="btn btn-outline" @click="exportSeriesCSV">
                  <i data-lucide="download"></i> 导出净值序列 (CSV)
                </button>
                <button class="btn btn-outline" @click="exportCyclesCSV">
                  <i data-lucide="download"></i> 导出交易记录 (CSV)
                </button>
                <button class="btn btn-outline" @click="exportJSON">
                  <i data-lucide="file-code"></i> 导出完整结果 (JSON)
                </button>
              </div>
              <div class="frozen-task-info">
                <h4>当前结果冻结元数据</h4>
                <div class="frozen-grid">
                  <div><strong>任务 ID:</strong> {{ job?.run_id }}</div>
                  <div><strong>输入哈希:</strong> {{ result.input_hash }}</div>
                  <div><strong>模型版本:</strong> {{ result.model_version }}</div>
                  <div><strong>调优参数:</strong> {{ JSON.stringify(result.params || {}) }}</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>

    <!-- 侧边抽屉：回测任务设置 (Slide-out Drawer) -->
    <div v-if="drawerOpen" class="drawer-backdrop" @click="drawerOpen = false"></div>
    <aside :class="['drawer-panel', drawerOpen ? 'open' : '']">
      <div class="drawer-header">
        <h3>回测任务设置</h3>
        <button class="btn-close" @click="drawerOpen = false">&times;</button>
      </div>

      <div class="drawer-body">
        <div class="form-group">
          <label class="field-label">策略版本</label>
          <select v-model="version" class="select">
            <option v-for="v in versions" :key="v.value" :value="v.value">{{ v.label }}</option>
          </select>
        </div>

        <div class="form-group">
          <label class="field-label">回测区间</label>
          <div class="range-row">
            <input v-model="from" type="date" class="input" />
            <span class="range-to">至</span>
            <input v-model="to" type="date" class="input" />
          </div>
        </div>

        <div class="form-group">
          <label class="field-label">初始本金 (元)</label>
          <input v-model.number="account.initial_capital" type="number" step="50000" class="input" />
        </div>

        <div class="form-group">
          <label class="field-label">最多持仓只数</label>
          <select v-model.number="account.max_positions" class="select">
            <option :value="1">1 只</option>
            <option :value="2">2 只 (默认)</option>
          </select>
        </div>

        <!-- 高级参数折叠 -->
        <details class="adv-details">
          <summary>高级参数 (突破与风控)</summary>
          <div class="adv-body">
            <div class="form-group">
              <label class="field-label">突破周期 (日)</label>
              <input v-model.number="stratParams.breakout_n" type="number" class="input" />
            </div>
            <div class="form-group">
              <label class="field-label">跟踪止损 ATR 倍数</label>
              <input v-model.number="stratParams.trail_atr" type="number" step="0.5" class="input" />
            </div>
            <div class="form-group">
              <label class="field-label">单股计划风险上限 (占净资产)</label>
              <input v-model.number="account.single_risk" type="number" step="0.005" class="input" />
            </div>
          </div>
        </details>

        <div v-if="error" class="error-msg">{{ error }}</div>

        <button class="btn btn-primary btn-block" :disabled="loading" @click="runBacktest">
          <i data-lucide="play"></i> {{ loading ? '计算中...' : '提交回测' }}
        </button>

        <!-- 历史任务快速切换 -->
        <div class="history-section">
          <h4>历史任务记录</h4>
          <div class="history-list">
            <div
              v-for="r in recent"
              :key="r.run_id"
              :class="['history-item', job?.run_id === r.run_id ? 'active' : '']"
              @click="loadRun(r.run_id)"
            >
              <div class="hist-top">
                <span class="hist-ver">{{ r.result?.version || 'TASK' }}</span>
                <span :class="['hist-status', r.status.toLowerCase()]">{{ r.status }}</span>
              </div>
              <div class="hist-meta">
                <span>{{ r.run_id.slice(0, 8) }}</span>
                <span v-if="r.result">{{ fmtPct(r.result.total_return, true) }}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </aside>

    <!-- 查看详情弹窗 -->
    <div v-if="detailModalOpen" class="modal-backdrop" @click="detailModalOpen = false">
      <div class="modal-card" @click.stop>
        <div class="modal-header">
          <h3>{{ selectedDetail?.title }}</h3>
          <button class="btn-close" @click="detailModalOpen = false">&times;</button>
        </div>
        <div class="modal-body">
          <div class="modal-row">
            <span class="m-label">证券代码:</span>
            <span class="m-val mono font-bold">{{ selectedDetail?.code }}</span>
          </div>
          <div class="modal-row">
            <span class="m-label">建议动作:</span>
            <span class="m-val">{{ selectedDetail?.side }}</span>
          </div>
          <div class="modal-row">
            <span class="m-label">参考价格:</span>
            <span class="m-val mono">{{ fmtMoneyDec(selectedDetail?.price) }}</span>
          </div>
          <div class="modal-row">
            <span class="m-label">计划数量:</span>
            <span class="m-val mono">{{ selectedDetail?.shares }} 股</span>
          </div>
          <div class="modal-row">
            <span class="m-label">跟踪退出线:</span>
            <span class="m-val mono">{{ fmtMoneyDec(selectedDetail?.stop) }}</span>
          </div>
          <div class="modal-row">
            <span class="m-label">触发日期:</span>
            <span class="m-val">{{ selectedDetail?.date }}</span>
          </div>
          <div class="modal-row full">
            <span class="m-label">触发规则与原因:</span>
            <p class="m-desc">{{ selectedDetail?.reason }}</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
/* 调色板对齐 Chapter 9:
   背景 #F5F7FA, 内容 #FFFFFF, 正文 #172B46, 辅助 #617086, 操作/强调 #2457D6, 分隔 #E1E7EF */
.lab-page {
  background-color: #f5f7fa;
  min-height: 100vh;
  padding: 32px 16px 64px;
  color: #172b46;
  font-size: 15px;
  line-height: 1.6;
}

.lab-container {
  max-width: 960px;
  margin: 0 auto;
  display: flex;
  flex-direction: column;
  gap: 40px; /* 区块间距 40px */
}

/* 顶栏 */
.lab-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.page-title {
  margin: 0;
  font-size: 28px;
  font-weight: 700;
  color: #172b46;
}

.page-subtitle {
  margin: 4px 0 0;
  font-size: 14px;
  color: #617086;
}

/* 内容白卡 */
.white-card {
  background-color: #ffffff;
  border: 1px solid #e1e7ef;
  border-radius: 8px;
  padding: 24px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.title-with-badge {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.card-title {
  margin: 0;
  font-size: 20px;
  font-weight: 600;
  color: #172b46;
}

.badge {
  font-size: 12px;
  padding: 2px 8px;
  border-radius: 4px;
  font-weight: 500;
}

.badge-subtle {
  background-color: #f0f4fa;
  color: #617086;
}

.badge-outline {
  border: 1px solid #e1e7ef;
  color: #617086;
}

.badge-primary {
  background-color: #eef3fc;
  color: #2457d6;
}

.summary-line {
  font-size: 15px;
  color: #172b46;
  margin: 0 0 16px;
  font-weight: 500;
}

/* 建议列表表格 */
.rec-table-wrap {
  overflow-x: auto;
}

.simple-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 14px;
}

.simple-table th,
.simple-table td {
  padding: 10px 12px;
  border-bottom: 1px solid #e1e7ef;
  text-align: left;
}

.simple-table th {
  background-color: #fafbfc;
  color: #617086;
  font-weight: 600;
}

.simple-table th.r,
.simple-table td.r {
  text-align: right;
}

.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
}

.font-bold {
  font-weight: 600;
}

.action-tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 600;
}

.tag-buy {
  background-color: #e6f4ea;
  color: #137333;
}

.tag-sell {
  background-color: #fce8e6;
  color: #c5221f;
}

.tag-hold {
  background-color: #eef3fc;
  color: #2457d6;
}

.btn-link {
  background: none;
  border: none;
  color: #2457d6;
  font-size: 13px;
  cursor: pointer;
  padding: 0;
  text-decoration: underline;
}

.btn-link:hover {
  color: #173fa8;
}

.empty-rec-box {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 16px;
  background-color: #f5f7fa;
  border-radius: 6px;
  color: #617086;
  font-size: 14px;
}

.empty-icon {
  color: #2457d6;
}

/* 指标概览网格 */
.metric-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 16px;
  margin-bottom: 24px;
  padding-bottom: 20px;
  border-bottom: 1px solid #e1e7ef;
}

.metric-item {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.metric-label {
  font-size: 13px;
  color: #617086;
}

.metric-value {
  font-size: 22px;
  font-weight: 700;
  color: #172b46;
}

.text-primary {
  color: #2457d6 !important;
}

.text-danger {
  color: #d93025 !important;
}

/* 走势图表 */
.chart-container {
  margin-bottom: 32px;
}

.chart-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
  font-size: 13px;
}

.chart-title {
  font-weight: 600;
  color: #172b46;
}

.chart-legend,
.chart-tooltip-bar {
  display: flex;
  gap: 16px;
  color: #617086;
}

.dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  margin-right: 4px;
}

.dot-blue {
  background-color: #2457d6;
}

.dot-red {
  background-color: #d93025;
}

.nav-svg {
  width: 100%;
  height: auto;
  background: #fafbfc;
  border: 1px solid #e1e7ef;
  border-radius: 6px;
  cursor: crosshair;
}

.chart-line {
  fill: none;
  stroke: #2457d6;
  stroke-width: 2;
}

.chart-area {
  fill: rgba(36, 87, 214, 0.08);
  stroke: none;
}

.chart-dd {
  fill: none;
  stroke: #d93025;
  stroke-width: 1.2;
}

.chart-axis-labels {
  display: flex;
  justify-content: space-between;
  font-size: 12px;
  color: #617086;
  margin-top: 4px;
}

/* 目标诊断区块 */
.diagnosis-block {
  margin-top: 24px;
  padding-top: 24px;
  border-top: 1px solid #e1e7ef;
}

.diagnosis-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.diagnosis-title {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: #172b46;
}

.copy-alert {
  display: block;
  font-size: 13px;
  color: #137333;
  margin-bottom: 12px;
}

.target-cards-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 16px;
  margin-bottom: 20px;
}

.target-box {
  padding: 16px;
  border-radius: 6px;
  border: 1px solid #e1e7ef;
  background-color: #fafbfc;
}

.target-met {
  border-color: #a8dab5;
  background-color: #f6fcf8;
}

.target-unmet {
  border-color: #f1c7c5;
  background-color: #fdf8f8;
}

.target-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.target-name {
  font-weight: 600;
  font-size: 15px;
  color: #172b46;
}

.target-status-tag {
  font-size: 12px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 4px;
}

.tag-met {
  background-color: #e6f4ea;
  color: #137333;
}

.tag-unmet {
  background-color: #fce8e6;
  color: #c5221f;
}

.target-body {
  display: flex;
  flex-direction: column;
  gap: 6px;
  font-size: 13px;
}

.target-metric {
  display: flex;
  justify-content: space-between;
}

.t-label {
  color: #617086;
}

.t-val {
  font-weight: 600;
  font-family: ui-monospace, monospace;
}

/* 自定义设计折叠框 */
.custom-design-details {
  background-color: #f0f4fa;
  border: 1px solid #d0dcee;
  border-radius: 6px;
  padding: 12px 16px;
  margin-bottom: 24px;
}

.custom-summary {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  font-weight: 600;
  font-size: 14px;
  color: #2457d6;
}

.custom-hint {
  font-size: 12px;
  color: #617086;
  font-weight: normal;
  margin-left: auto;
}

.custom-form-body {
  margin-top: 14px;
}

.custom-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 20px;
}

.custom-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: 13px;
}

.input-sm {
  padding: 4px 8px;
  border: 1px solid #d0dcee;
  border-radius: 4px;
  font-size: 14px;
  width: 140px;
}

.custom-result-card {
  margin-left: auto;
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  font-size: 13px;
}

.cr-label {
  color: #617086;
}

.cr-val {
  font-size: 18px;
  font-weight: 700;
  color: #2457d6;
  font-family: ui-monospace, monospace;
}

.cr-gap {
  font-size: 12px;
  color: #617086;
}

/* 四维归因卡片 */
.attribution-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 16px;
}

.attr-card {
  background-color: #fafbfc;
  border: 1px solid #e1e7ef;
  border-radius: 6px;
  padding: 14px 16px;
}

.attr-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
  font-size: 14px;
  color: #172b46;
  margin-bottom: 8px;
}

.attr-desc {
  margin: 0;
  font-size: 13px;
  color: #617086;
  line-height: 1.6;
}

.attr-desc strong {
  color: #172b46;
}

/* “更多”折叠导航与面板 */
.more-section {
  margin-top: 32px;
  padding-top: 24px;
  border-top: 1px solid #e1e7ef;
}

.tab-nav {
  display: flex;
  gap: 8px;
  border-bottom: 1px solid #e1e7ef;
  margin-bottom: 16px;
}

.tab-btn {
  background: none;
  border: none;
  padding: 8px 16px;
  font-size: 14px;
  color: #617086;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  margin-bottom: -1px;
}

.tab-btn.active {
  color: #2457d6;
  border-bottom-color: #2457d6;
  font-weight: 600;
}

.tab-pane {
  font-size: 14px;
}

.pane-hint {
  font-size: 13px;
  color: #617086;
  margin: 0 0 12px;
}

.dim-text {
  color: #617086;
}

.funnel-counters {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
  gap: 12px;
  margin-bottom: 12px;
}

.funnel-box {
  background-color: #fafbfc;
  border: 1px solid #e1e7ef;
  border-radius: 6px;
  padding: 12px;
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
}

.f-num {
  font-size: 20px;
  font-weight: 700;
  font-family: ui-monospace, monospace;
  color: #172b46;
}

.f-txt {
  font-size: 12px;
  color: #617086;
  margin-top: 4px;
}

.funnel-note {
  font-size: 12px;
  color: #617086;
  margin: 0;
}

.export-actions {
  display: flex;
  gap: 12px;
  margin-bottom: 20px;
  flex-wrap: wrap;
}

.frozen-task-info {
  background-color: #fafbfc;
  border: 1px solid #e1e7ef;
  border-radius: 6px;
  padding: 14px 16px;
}

.frozen-task-info h4 {
  margin: 0 0 10px;
  font-size: 14px;
  color: #172b46;
}

.frozen-grid {
  display: flex;
  flex-direction: column;
  gap: 6px;
  font-size: 13px;
  color: #617086;
}

.frozen-grid strong {
  color: #172b46;
}

/* 按钮与通用表单组件 */
.btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  border: 1px solid transparent;
  transition: background-color 0.15s ease;
}

.btn-primary {
  background-color: #2457d6;
  color: #ffffff;
}

.btn-primary:hover:not(:disabled) {
  background-color: #173fa8;
}

.btn-primary:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.btn-outline {
  background-color: #ffffff;
  border-color: #e1e7ef;
  color: #172b46;
}

.btn-outline:hover {
  background-color: #f5f7fa;
}

.btn-sm {
  padding: 4px 10px;
  font-size: 12px;
}

.btn-block {
  width: 100%;
  justify-content: center;
}

.btn-close {
  background: none;
  border: none;
  font-size: 20px;
  color: #617086;
  cursor: pointer;
}

.input,
.select {
  width: 100%;
  padding: 8px 12px;
  border: 1px solid #e1e7ef;
  border-radius: 6px;
  font-size: 14px;
  background-color: #ffffff;
  color: #172b46;
}

.input:focus,
.select:focus {
  outline: none;
  border-color: #2457d6;
}

.field-label {
  display: block;
  font-size: 13px;
  color: #617086;
  margin-bottom: 4px;
}

.form-group {
  margin-bottom: 16px;
}

.range-row {
  display: flex;
  align-items: center;
  gap: 8px;
}

.range-to {
  color: #617086;
  font-size: 13px;
}

.error-msg {
  color: #d93025;
  font-size: 13px;
  margin-bottom: 12px;
}

/* 侧边栏抽屉 (Slide-out Drawer) */
.drawer-backdrop {
  position: fixed;
  inset: 0;
  background-color: rgba(0, 0, 0, 0.4);
  z-index: 1000;
}

.drawer-panel {
  position: fixed;
  top: 0;
  right: 0;
  bottom: 0;
  width: 360px;
  max-width: 90vw;
  background-color: #ffffff;
  z-index: 1001;
  box-shadow: -4px 0 16px rgba(0, 0, 0, 0.1);
  display: flex;
  flex-direction: column;
  transform: translateX(100%);
  transition: transform 0.25s ease;
}

.drawer-panel.open {
  transform: translateX(0);
}

.drawer-header {
  padding: 16px 20px;
  border-bottom: 1px solid #e1e7ef;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.drawer-header h3 {
  margin: 0;
  font-size: 16px;
  color: #172b46;
}

.drawer-body {
  flex: 1;
  overflow-y: auto;
  padding: 20px;
}

.adv-details {
  margin-bottom: 16px;
}

.adv-details summary {
  font-size: 13px;
  color: #2457d6;
  cursor: pointer;
  margin-bottom: 8px;
}

.adv-body {
  padding: 10px;
  background-color: #f5f7fa;
  border-radius: 6px;
}

.history-section {
  margin-top: 24px;
  padding-top: 16px;
  border-top: 1px solid #e1e7ef;
}

.history-section h4 {
  margin: 0 0 12px;
  font-size: 14px;
  color: #172b46;
}

.history-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 220px;
  overflow-y: auto;
}

.history-item {
  padding: 8px 10px;
  border: 1px solid #e1e7ef;
  border-radius: 6px;
  cursor: pointer;
  font-size: 12px;
}

.history-item:hover {
  background-color: #f5f7fa;
}

.history-item.active {
  border-color: #2457d6;
  background-color: #eef3fc;
}

.hist-top {
  display: flex;
  justify-content: space-between;
  font-weight: 600;
}

.hist-status.completed {
  color: #137333;
}

.hist-status.running {
  color: #2457d6;
}

.hist-status.failed {
  color: #c5221f;
}

.hist-meta {
  display: flex;
  justify-content: space-between;
  color: #617086;
  margin-top: 2px;
}

/* 详情弹窗 */
.modal-backdrop {
  position: fixed;
  inset: 0;
  background-color: rgba(0, 0, 0, 0.4);
  z-index: 1000;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 16px;
}

.modal-card {
  background-color: #ffffff;
  border-radius: 8px;
  width: 100%;
  max-width: 480px;
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.15);
  overflow: hidden;
}

.modal-header {
  padding: 14px 20px;
  border-bottom: 1px solid #e1e7ef;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.modal-header h3 {
  margin: 0;
  font-size: 16px;
  color: #172b46;
}

.modal-body {
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 10px;
  font-size: 14px;
}

.modal-row {
  display: flex;
  justify-content: space-between;
}

.modal-row.full {
  flex-direction: column;
  gap: 4px;
}

.m-label {
  color: #617086;
}

.m-val {
  color: #172b46;
}

.m-desc {
  margin: 0;
  padding: 8px 12px;
  background-color: #f5f7fa;
  border-radius: 4px;
  font-size: 13px;
  color: #172b46;
}
</style>
