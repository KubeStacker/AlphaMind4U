<template>
  <section class="space-y-3">
    <div class="flex items-center gap-2 overflow-x-auto pb-2 md:flex-wrap">
      <select v-model="selectedStrategyKey" class="shrink-0 rounded-lg border border-white/[0.08] bg-[#08090d] px-3 py-2 text-sm text-slate-200 [&>option]:bg-[#08090d] [&>option]:text-slate-200">
        <option value="">选择策略</option>
        <option v-for="item in strategies" :key="item.strategy_key" :value="item.strategy_key">{{ item.name }}</option>
      </select>
      <input v-model="selectedDate" type="date" class="shrink-0 rounded-lg border border-white/[0.08] bg-[#08090d] px-3 py-2 text-sm text-slate-200" />
      <button class="shrink-0 rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-xs font-semibold text-slate-200" @click="reloadAll">
        刷新
      </button>
    </div>

    <div v-if="!strategies.length && !loading" class="rounded-xl border border-white/[0.06] bg-obsidian-900/50 px-4 py-8 text-center text-sm text-slate-500">
      暂无策略
    </div>

    <div v-else class="rounded-xl border border-white/[0.06] bg-obsidian-900/50 overflow-hidden">
      <div class="border-b border-white/[0.06] px-3 py-2 text-sm font-semibold text-slate-200">新进入观察</div>
      <div v-if="!rows.length && !loading" class="px-3 py-6 text-center text-sm text-slate-500">该日暂无新进入观察的标的</div>
      <table v-else class="w-full text-left text-sm">
        <thead class="bg-white/[0.02] text-[11px] uppercase tracking-[0.16em] text-slate-500">
          <tr>
            <th class="px-3 py-2">标的</th>
            <th class="px-3 py-2">理由</th>
            <th class="px-3 py-2">3日</th>
            <th class="px-3 py-2">5日</th>
            <th class="px-3 py-2">10日</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in rows" :key="`${item.ts_code}-${item.reason}`" class="border-t border-white/[0.05]">
            <td class="px-3 py-2">
              <button class="text-left" data-kline-trigger="1" @click.stop="toggleKline(item, $event)">
                <div class="font-semibold text-slate-100 transition-colors hover:text-signal-accent">{{ item.name }}</div>
              </button>
              <div class="text-xs text-slate-500">{{ item.ts_code }}</div>
            </td>
            <td class="px-3 py-2 text-slate-300">{{ item.reason }}</td>
            <td class="px-3 py-2 text-slate-300">{{ item.ret3dText }}</td>
            <td class="px-3 py-2 text-slate-300">{{ item.ret5dText }}</td>
            <td class="px-3 py-2 text-slate-300">{{ item.ret10dText }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="summary" class="rounded-xl border border-white/[0.06] bg-obsidian-900/50 px-3 py-2">
      <div class="grid grid-cols-2 gap-2 md:grid-cols-4">
        <div v-for="fact in summaryFacts" :key="fact.label">
          <div class="text-[10px] text-slate-500">{{ fact.label }}</div>
          <div class="text-sm font-semibold text-slate-200">{{ fact.value }}</div>
        </div>
      </div>
      <div class="mt-2 text-sm text-slate-300">{{ summary.summary_text }}</div>
    </div>

    <Teleport to="body">
      <div v-if="hoverInfo.visible" class="fixed inset-0 z-50 flex items-center justify-center bg-black/60" @click="hideKline">
        <div ref="klinePopupRef" class="mx-4 max-h-[85vh] w-full max-w-3xl overflow-y-auto rounded-xl border border-white/[0.1] bg-obsidian-900 p-3" @click.stop>
          <div class="mb-2 flex items-center justify-between">
            <div>
              <div class="text-sm font-semibold text-slate-100">{{ hoverInfo.stock?.name || '-' }}</div>
              <div class="text-xs text-slate-500">{{ hoverInfo.stock?.ts_code || '-' }}</div>
            </div>
            <button class="rounded-lg p-1.5 text-slate-500 hover:bg-white/[0.06]" @click="hideKline">
              <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
            </button>
          </div>
          <div v-if="klineLoading" class="flex h-[200px] items-center justify-center text-xs text-slate-600">
            <div class="flex items-center gap-2">
              <div class="h-4 w-4 animate-spin rounded-full border-2 border-signal-accent/40 border-t-signal-accent"></div>
              <span>加载中...</span>
            </div>
          </div>
          <div v-else-if="klineError" class="rounded-lg border border-signal-bear/20 bg-signal-bear/5 px-3 py-2 text-xs text-signal-bear/80">{{ klineError }}</div>
          <template v-else>
            <div v-if="latestKlineData" class="mb-2 grid grid-cols-3 gap-2 text-[10px] md:grid-cols-6 md:text-[11px]">
              <div class="rounded-lg border border-white/[0.04] bg-white/[0.02] px-2 py-1.5"><span class="text-slate-600">收盘</span><p class="mt-0.5 font-mono text-slate-200">{{ fmt(latestKlineData.close, 2) }}</p></div>
              <div class="rounded-lg border border-white/[0.04] bg-white/[0.02] px-2 py-1.5"><span class="text-slate-600">成交额(亿)</span><p class="mt-0.5 font-mono text-slate-200">{{ fmtAmount(latestKlineData.amount) }}</p></div>
              <div class="rounded-lg border border-white/[0.04] bg-white/[0.02] px-2 py-1.5"><span class="text-slate-600">主力(亿)</span><p class="mt-0.5 font-mono" :class="latestKlineData.net_mf_vol >= 0 ? 'text-signal-data' : 'text-signal-bear/80'">{{ fmtMf(latestKlineData.net_mf_vol) }}</p></div>
              <div class="hidden rounded-lg border border-white/[0.04] bg-white/[0.02] px-2 py-1.5 md:block"><span class="text-slate-600">成交量(手)</span><p class="mt-0.5 font-mono text-slate-200">{{ fmtVol(latestKlineData.vol) }}</p></div>
              <div class="hidden rounded-lg border border-white/[0.04] bg-white/[0.02] px-2 py-1.5 md:block"><span class="text-slate-600">MA5/10/20</span><p class="mt-0.5 font-mono text-slate-200">{{ fmt(latestKlineData.ma5, 2) }} / {{ fmt(latestKlineData.ma10, 2) }} / {{ fmt(latestKlineData.ma20, 2) }}</p></div>
              <div class="hidden rounded-lg border border-white/[0.04] bg-white/[0.02] px-2 py-1.5 md:block"><span class="text-slate-600">融资(亿)</span><p class="mt-0.5 font-mono text-slate-200">{{ latestKlineData.rzye ? fmtRzye(latestKlineData.rzye) : '-' }}</p></div>
            </div>
            <div class="h-[200px] md:h-[340px]"><v-chart :option="klineOption" autoresize @updateAxisPointer="handleUpdateAxisPointer" /></div>
          </template>
        </div>
      </div>
    </Teleport>
  </section>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import {
  getStockKline,
  getStrategyPlazaObservations,
  getStrategyPlazaStrategies,
  getStrategyPlazaSummary,
} from '@/services/api'
import { useKlineChart } from '@/composables/useKlineChart'
import { buildSummaryFacts, normalizeObservationRows } from '@/composables/useStrategyPlaza'

const getLocalDateString = () => {
  const now = new Date()
  const year = now.getFullYear()
  const month = String(now.getMonth() + 1).padStart(2, '0')
  const day = String(now.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

const today = getLocalDateString()
const loading = ref(false)
const strategies = ref([])
const selectedStrategyKey = ref('')
const selectedDate = ref(today)
const observationItems = ref([])
const summary = ref(null)
const hoverInfo = reactive({ visible: false, stock: null, klineData: [] })
const klineLoading = ref(false)
const klineError = ref('')
const klineHoveredIndex = ref(-1)
const klinePopupRef = ref(null)
const { createKlineOption, getLatestKlineData } = useKlineChart()

const rows = computed(() => normalizeObservationRows(observationItems.value))
const summaryFacts = computed(() => buildSummaryFacts(summary.value))
const klineOption = computed(() => createKlineOption(hoverInfo.klineData, { showLegend: true, showDataZoom: true, marginType: 'mf', hideMargin: true }))
const latestKlineData = computed(() => {
  const data = hoverInfo.klineData
  if (!data?.length) return null
  const idx = klineHoveredIndex.value >= 0 ? klineHoveredIndex.value : data.length - 1
  const item = data[idx]
  if (!item) return getLatestKlineData(data)
  return {
    trade_date: item.trade_date,
    close: Number(item.close) || null,
    open: Number(item.open) || null,
    high: Number(item.high) || null,
    low: Number(item.low) || null,
    vol: Number(item.vol) || 0,
    amount: (Number(item.amount) || 0) * 1000 / 1e8,
    net_mf_vol: item.net_mf_amount != null ? Number(item.net_mf_amount) / 10000 : null,
    rzye: item.rzye != null ? Number(item.rzye) / 1e8 : null,
    ma5: Number(item.ma5) || null,
    ma10: Number(item.ma10) || null,
    ma20: Number(item.ma20) || null,
  }
})

const fmt = (v, digits = 2) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '-'
  return n.toFixed(digits)
}
const fmtVol = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n) || n === 0) return '-'
  if (Math.abs(n) >= 1e8) return `${(n / 1e8).toFixed(2)}亿手`
  if (Math.abs(n) >= 1e4) return `${(n / 1e4).toFixed(2)}万手`
  return `${Math.round(n)}手`
}
const fmtAmount = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n) || n === 0) return '-'
  return `${n.toFixed(2)}亿`
}
const fmtMf = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n) || n === 0) return '-'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}亿`
}
const fmtRzye = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n) || n === 0) return '-'
  return `${n.toFixed(2)}亿`
}

const handleUpdateAxisPointer = (event) => {
  const info = event.dataIndex
  if (info != null && info >= 0) klineHoveredIndex.value = info
}

const hideKline = () => {
  hoverInfo.visible = false
  klineLoading.value = false
  klineError.value = ''
  klineHoveredIndex.value = -1
}

const showKline = async (stock) => {
  hoverInfo.stock = stock
  hoverInfo.visible = true
  hoverInfo.klineData = []
  klineHoveredIndex.value = -1
  klineLoading.value = true
  klineError.value = ''
  try {
    const res = await getStockKline(stock.ts_code, 80)
    hoverInfo.klineData = res.data?.data || []
  } catch (error) {
    klineError.value = error?.response?.data?.detail || error?.message || 'K线加载失败'
  } finally {
    klineLoading.value = false
  }
}

const toggleKline = async (stock) => {
  if (hoverInfo.visible && hoverInfo.stock?.ts_code === stock?.ts_code) {
    hideKline()
    return
  }
  await showKline(stock)
}

const handleDocumentClick = (event) => {
  if (!hoverInfo.visible) return
  if (klinePopupRef.value && klinePopupRef.value.contains(event.target)) return
  if (event.target?.closest?.('[data-kline-trigger="1"]')) return
  hideKline()
}

const reloadAll = async () => {
  loading.value = true
  try {
    hideKline()
    const strategyRes = await getStrategyPlazaStrategies()
    strategies.value = strategyRes.data?.data?.strategies || []

    if (!selectedStrategyKey.value && strategies.value.length) {
      selectedStrategyKey.value = strategies.value[0].strategy_key
    }

    if (!selectedStrategyKey.value) {
      observationItems.value = []
      summary.value = null
      return
    }

    const [observationRes, summaryRes] = await Promise.all([
      getStrategyPlazaObservations(selectedStrategyKey.value, selectedDate.value),
      getStrategyPlazaSummary(selectedStrategyKey.value, selectedDate.value),
    ])
    observationItems.value = observationRes.data?.data?.items || []
    summary.value = summaryRes.data?.data?.summary || null
  } finally {
    loading.value = false
  }
}

watch([selectedStrategyKey, selectedDate], () => {
  if (selectedStrategyKey.value) void reloadAll()
})

onMounted(() => {
  document.addEventListener('click', handleDocumentClick, true)
  void reloadAll()
})

onBeforeUnmount(() => {
  document.removeEventListener('click', handleDocumentClick, true)
})
</script>
