<template>
  <div class="max-w-6xl mx-auto space-y-4 md:space-y-6 pb-8">
    <div class="bg-business-dark border border-business-light rounded-2xl p-4 shadow-business">
      <div class="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <h2 class="text-sm font-bold text-white tracking-wide">Falcon 引擎</h2>
          <p class="text-[11px] text-slate-400 mt-1">策略运行、推荐结果与回测跟踪</p>
        </div>
        <div class="flex items-center gap-2">
          <button
            @click="handleEvolve"
            :disabled="evolving"
            class="px-3 py-1.5 rounded-lg text-[11px] font-bold border transition"
            :class="evolving ? 'bg-sky-200 text-sky-900 border-sky-200' : 'bg-sky-900/30 text-sky-300 border-sky-700 hover:bg-sky-700 hover:text-white'"
          >
            {{ evolving ? '演进中...' : '策略演进' }}
          </button>
        </div>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-4 gap-2 mt-4">
        <div>
          <label class="text-[10px] text-slate-500">策略</label>
          <select v-model="selectedStrategy" class="mt-1 w-full bg-business-darker border border-business-light rounded-lg px-2 py-2 text-xs text-slate-100">
            <option v-for="s in strategies" :key="s.strategy_id" :value="s.strategy_id">{{ s.name }}</option>
          </select>
        </div>
        <div>
          <label class="text-[10px] text-slate-500">日期</label>
          <input v-model="selectedDate" type="date" class="mt-1 w-full bg-business-darker border border-business-light rounded-lg px-2 py-2 text-xs text-slate-100" />
        </div>
        <div class="md:col-span-2 flex items-end gap-2">
          <button
            @click="handleRun"
            :disabled="running || !selectedStrategy || !selectedDate"
            class="h-9 px-4 rounded-lg text-[11px] font-bold border transition"
            :class="running ? 'bg-amber-200 text-amber-900 border-amber-200' : 'bg-amber-900/30 text-amber-300 border-amber-700 hover:bg-amber-700 hover:text-white'"
          >
            {{ running ? '运行中...' : '运行引擎' }}
          </button>
          <button @click="refreshRuns" class="h-9 px-4 rounded-lg text-[11px] font-bold border bg-slate-800 text-slate-300 border-slate-700 hover:bg-slate-700">
            刷新
          </button>
        </div>
      </div>
    </div>

    <div class="grid grid-cols-2 md:grid-cols-6 gap-2">
      <div class="bg-business-dark border border-business-light rounded-xl p-3">
        <div class="text-[10px] text-slate-500">本次评分</div>
        <div class="text-lg font-bold text-white mt-1">{{ metricText(activeRun?.summary?.avg_score, 1) }}</div>
      </div>
      <div class="bg-business-dark border border-business-light rounded-xl p-3">
        <div class="text-[10px] text-slate-500">+5 命中</div>
        <div class="text-lg font-bold text-emerald-300 mt-1">{{ metricPct(activeRun?.summary?.hit_5d) }}</div>
      </div>
      <div class="bg-business-dark border border-business-light rounded-xl p-3">
        <div class="text-[10px] text-slate-500">+10 命中</div>
        <div class="text-lg font-bold text-cyan-300 mt-1">{{ metricPct(activeRun?.summary?.hit_10d) }}</div>
      </div>
      <div class="bg-business-dark border border-business-light rounded-xl p-3">
        <div class="text-[10px] text-slate-500">候选数</div>
        <div class="text-lg font-bold text-white mt-1">{{ activeRun?.summary?.pick_count ?? '-' }}</div>
      </div>
      <div class="bg-business-dark border border-business-light rounded-xl p-3">
        <div class="text-[10px] text-slate-500">建议仓位</div>
        <div class="text-lg font-bold text-amber-300 mt-1">{{ metricPct(activeRun?.summary?.portfolio_advice?.market_target_position) }}</div>
      </div>
      <div class="bg-business-dark border border-business-light rounded-xl p-3">
        <div class="text-[10px] text-slate-500">单票上限</div>
        <div class="text-lg font-bold text-sky-300 mt-1">{{ metricPct(activeRun?.summary?.portfolio_advice?.max_single_position) }}</div>
      </div>
    </div>

    <div
      v-if="activeRun?.summary?.portfolio_advice"
      class="bg-business-dark border border-business-light rounded-2xl px-4 py-3"
    >
      <div class="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
        <div>
          <div class="text-[10px] text-slate-500">组合执行建议</div>
          <div class="text-sm font-semibold text-slate-100 mt-1">
            {{ activeRun.summary.portfolio_advice.market_label || '-' }}
            <span class="text-slate-400 font-normal">/ {{ activeRun.summary.portfolio_advice.market_signal || '-' }}</span>
          </div>
        </div>
        <div class="flex flex-wrap gap-3 text-[11px] text-slate-400">
          <span>已分配 {{ metricPct(activeRun.summary.portfolio_advice.allocated_position) }}</span>
          <span>现金缓冲 {{ metricPct(activeRun.summary.portfolio_advice.cash_buffer) }}</span>
          <span>建议持股 {{ activeRun.summary.portfolio_advice.suggested_pick_count ?? '-' }} 只</span>
        </div>
      </div>
    </div>

    <div
      v-if="activeRun?.summary?.pool_split"
      class="bg-business-dark border border-business-light rounded-2xl px-4 py-3"
    >
      <div class="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
        <div>
          <div class="text-[10px] text-slate-500">分层结果</div>
          <div class="text-sm font-semibold text-slate-100 mt-1">
            可交易池 {{ activePoolSplit.tradable_count ?? 0 }} 只
            <span class="text-slate-400 font-normal">/ 观察池 {{ activePoolSplit.observation_count ?? 0 }} 只</span>
          </div>
        </div>
        <div v-if="activeObservationSummary" class="flex flex-wrap gap-3 text-[11px] text-slate-400">
          <span>观察池 +5 {{ metricPct(activeObservationSummary.hit_5d) }}</span>
          <span>观察池 +10 {{ metricPct(activeObservationSummary.hit_10d) }}</span>
        </div>
      </div>
    </div>

    <div class="bg-business-dark border border-business-light rounded-2xl p-3">
      <div class="flex items-center justify-between mb-2">
        <h3 class="text-xs font-bold text-slate-200">可交易池</h3>
        <div class="text-[10px] text-slate-500">{{ activeRun?.trade_date || '-' }}</div>
      </div>
      <div class="overflow-auto max-h-[460px]">
        <table class="w-full text-left text-xs">
          <thead class="text-slate-500 border-b border-slate-700 sticky top-0 bg-business-dark">
            <tr>
              <th class="py-2">序</th>
              <th>标的</th>
              <th>策略分</th>
              <th>理由</th>
              <th>+5</th>
              <th>+10</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="r in activeRunPicks"
              :key="`${r.rank_no}-${r.ts_code}`"
              class="border-b border-slate-800/60"
              :class="selectedKlineTsCode === r.ts_code ? 'bg-sky-900/10' : ''"
            >
              <td class="py-2">{{ r.rank_no }}</td>
              <td>
                <button
                  class="text-left"
                  @mouseenter="handleStockHover(r)"
                  @mouseleave="scheduleHideKlinePreview"
                  @focus="handleStockHover(r)"
                >
                  <div class="font-semibold text-slate-100 hover:text-sky-300">{{ r.name || '-' }}</div>
                  <div class="text-[10px] text-slate-500">{{ r.ts_code }}</div>
                </button>
              </td>
              <td>{{ metricText(r.strategy_score, 1) }}</td>
              <td class="text-slate-400 truncate max-w-[120px]" :title="r.reason">{{ r.reason || '-' }}</td>
              <td :class="retClass(r.ret_5d)">{{ retPct(r.ret_5d) }}</td>
              <td :class="retClass(r.ret_10d)">{{ retPct(r.ret_10d) }}</td>
              <td>
                <div class="flex gap-1">
                  <button @click="openScore(r)" class="px-2 py-1 rounded border border-slate-700 text-[10px] text-slate-300 hover:bg-slate-800">评分</button>
                </div>
              </td>
            </tr>
            <tr v-if="activeRunPicks.length === 0"><td colspan="7" class="py-6 text-center text-slate-500">暂无结果</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div v-if="activeObservationPool.length > 0" class="bg-business-dark border border-business-light rounded-2xl p-3">
      <div class="flex items-center justify-between mb-2">
        <h3 class="text-xs font-bold text-slate-200">观察池</h3>
        <div class="text-[10px] text-slate-500">{{ activeObservationPool.length }} 只</div>
      </div>
      <div class="overflow-auto max-h-[360px]">
        <table class="w-full text-left text-xs">
          <thead class="text-slate-500 border-b border-slate-700 sticky top-0 bg-business-dark">
            <tr>
              <th class="py-2">标的</th>
              <th>策略分</th>
              <th>观察原因</th>
              <th>主线</th>
              <th>阶段</th>
              <th>拥挤度</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="r in activeObservationPool"
              :key="`observe-${r.ts_code}`"
              class="border-b border-slate-800/60"
            >
              <td class="py-2">
                <button
                  class="text-left"
                  @mouseenter="handleStockHover(r)"
                  @mouseleave="scheduleHideKlinePreview"
                  @focus="handleStockHover(r)"
                >
                  <div class="font-semibold text-slate-100 hover:text-sky-300">{{ r.name || '-' }}</div>
                  <div class="text-[10px] text-slate-500">{{ r.ts_code }}</div>
                </button>
              </td>
              <td>{{ metricText(r.strategy_score, 1) }}</td>
              <td class="text-slate-400 max-w-[220px]" :title="r.observation_reason">{{ r.observation_reason || '-' }}</td>
              <td class="text-slate-300">{{ r.mainline_name || '-' }}</td>
              <td class="text-slate-400">{{ r.mainline_phase || '-' }}</td>
              <td class="text-amber-300">{{ metricText(r.crowding_score, 1) }}</td>
              <td>
                <button @click="openScore(r)" class="px-2 py-1 rounded border border-slate-700 text-[10px] text-slate-300 hover:bg-slate-800">评分</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div
      v-if="klinePreviewVisible && selectedKlineTsCode"
      class="fixed z-40 left-2 right-2 bottom-14 md:left-auto md:right-4 md:bottom-4 md:w-[70vw] md:max-w-3xl bg-business-dark border border-business-light rounded-2xl p-2.5 md:p-3 shadow-2xl max-h-[78vh] overflow-y-auto"
      @mouseenter="clearHideKlinePreview"
      @mouseleave="scheduleHideKlinePreview"
    >
      <div class="flex flex-wrap items-center justify-between gap-2 mb-2">
        <div>
          <h3 class="text-xs font-bold text-slate-200">K线分析（悬浮预览）</h3>
          <p class="text-[10px] text-slate-500 mt-1">{{ selectedKlineName || '-' }} · {{ selectedKlineTsCode }}</p>
        </div>
        <div class="flex flex-wrap items-center gap-1.5 sm:gap-2">
          <button @click="stepOlder" :disabled="!canStepOlder" class="px-1.5 sm:px-2 py-1 rounded border border-slate-700 text-[9px] sm:text-[10px] text-slate-300 hover:bg-slate-800 disabled:opacity-40">← 往前</button>
          <button @click="stepNewer" :disabled="!canStepNewer" class="px-1.5 sm:px-2 py-1 rounded border border-slate-700 text-[9px] sm:text-[10px] text-slate-300 hover:bg-slate-800 disabled:opacity-40">往后 →</button>
          <button @click="zoomIn" :disabled="klineWindowDays <= 20" class="px-1.5 sm:px-2 py-1 rounded border border-slate-700 text-[9px] sm:text-[10px] text-slate-300 hover:bg-slate-800 disabled:opacity-40">放大</button>
          <button @click="zoomOut" :disabled="klineWindowDays >= 180" class="px-1.5 sm:px-2 py-1 rounded border border-slate-700 text-[9px] sm:text-[10px] text-slate-300 hover:bg-slate-800 disabled:opacity-40">缩小</button>
        </div>
      </div>

      <div class="grid grid-cols-3 sm:grid-cols-6 gap-1.5 mb-2 text-[10px]" v-if="latestKlineData">
        <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
          <span class="text-slate-500">收盘价</span>
          <p class="text-slate-200 mt-0.5 font-mono">{{ fmtPrice(latestKlineData.close) }}</p>
        </div>
        <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
          <span class="text-slate-500">成交量</span>
          <p class="text-slate-200 mt-0.5 font-mono">{{ fmtVolUnit(latestKlineData.vol) }}</p>
        </div>
        <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
          <span class="text-slate-500">成交额</span>
          <p class="text-slate-200 mt-0.5 font-mono">{{ fmtAmountUnit(latestKlineData.amount) }}</p>
        </div>
        <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
          <span class="text-slate-500">主力流入</span>
          <p class="mt-0.5 font-mono" :class="latestKlineData.net_mf_vol >= 0 ? 'text-cyan-400' : 'text-rose-400'">{{ fmtMfUnit(latestKlineData.net_mf_vol) }}</p>
        </div>
        <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
          <span class="text-slate-500">MA5/10/20</span>
          <p class="text-slate-200 mt-0.5 font-mono text-[9px]">{{ fmtPrice(latestKlineData.ma5) }} / {{ fmtPrice(latestKlineData.ma10) }} / {{ fmtPrice(latestKlineData.ma20) }}</p>
        </div>
        <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
          <span class="text-slate-500">融资余额</span>
          <p class="text-slate-200 mt-0.5 font-mono">{{ latestKlineData.rzye ? fmtRzyeUnit(latestKlineData.rzye) : '-' }}</p>
        </div>
      </div>

      <div v-if="klineLoading" class="h-[220px] sm:h-[280px] md:h-[340px] flex items-center justify-center text-slate-400 text-xs">K线加载中...</div>
      <div v-else-if="klineError" class="rounded border border-red-700/40 bg-red-500/10 px-3 py-2 text-xs text-red-300">{{ klineError }}</div>
      <div v-else class="h-[220px] sm:h-[280px] md:h-[340px] lg:h-[380px]">
        <v-chart :option="klineChartOption" autoresize @updateAxisPointer="handleUpdateAxisPointer" />
      </div>
    </div>

    <div class="space-y-3">
      <div class="bg-business-dark border border-business-light rounded-2xl p-3">
        <button class="w-full flex items-center justify-between" @click="panelOpen.dataOps = !panelOpen.dataOps">
          <h3 class="text-xs font-bold text-slate-200">数据管理</h3>
          <span class="text-[10px] text-slate-500">{{ panelOpen.dataOps ? '收起' : '展开' }}</span>
        </button>
        <div v-if="panelOpen.dataOps" class="mt-2">
          <div class="flex gap-2 mb-2">
            <button @click="removeRun(false)" :disabled="!activeRun" class="flex-1 px-3 py-2 rounded border border-rose-700 text-rose-300 text-[11px] hover:bg-rose-900/20 disabled:opacity-50">删除当前运行</button>
          </div>
          <div class="grid grid-cols-2 gap-2 mb-2">
            <input v-model="batchStartDate" type="date" class="w-full bg-business-darker border border-business-light rounded-lg px-2 py-2 text-xs text-slate-100" />
            <input v-model="batchEndDate" type="date" class="w-full bg-business-darker border border-business-light rounded-lg px-2 py-2 text-xs text-slate-100" />
          </div>
          <button
            @click="batchDeleteByRange"
            :disabled="!selectedStrategy || !batchStartDate || !batchEndDate"
            class="w-full px-3 py-2 rounded border border-rose-700 text-rose-300 text-[11px] hover:bg-rose-900/20 disabled:opacity-50"
          >
            按区间清理记录
          </button>
        </div>
      </div>
    </div>

    <div v-if="scoreModal.open" class="fixed inset-0 z-50 bg-black/60 flex items-center justify-center p-4" @click.self="scoreModal.open = false">
      <div class="w-full max-w-md bg-business-dark border border-business-light rounded-2xl p-4">
        <div class="flex items-center justify-between mb-2">
          <h3 class="text-sm font-bold text-slate-200">评分明细</h3>
          <button class="text-slate-400" @click="scoreModal.open = false">关闭</button>
        </div>
        <div class="text-xs space-y-2">
          <div class="rounded bg-slate-900/50 p-2 border border-slate-700/60">
            <div class="text-slate-500">标的</div>
            <div class="text-slate-200 font-semibold">{{ scoreModal.row?.name || '-' }} ({{ scoreModal.row?.ts_code || '-' }})</div>
          </div>
          <div class="rounded bg-slate-900/50 p-2 border border-slate-700/60">
            <pre class="whitespace-pre-wrap text-[11px] text-slate-300">{{ JSON.stringify(scoreModal.row?.score_breakdown || {}, null, 2) }}</pre>
          </div>
          <div class="rounded bg-sky-900/20 p-2 border border-sky-700/50" v-if="strategyScore">
            <div class="text-sky-300 text-[11px]">策略当日评分</div>
            <div class="text-white text-lg font-bold mt-1">{{ metricText(strategyScore.score, 1) }}</div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import {
  deleteFalconRun,
  deleteFalconRunsByRange,
  evolveFalcon,
  executeDBQuery,
  getFalconLatestTradeDate,
  getFalconRunDetail,
  getFalconScore,
  getFalconStrategies,
  getStockKline,
  hardDeleteFalconRunsBatch,
  listFalconDeletedRuns,
  listFalconOps,
  listFalconRuns,
  restoreFalconRun,
  restoreFalconRunsBatch,
  runFalcon,
} from '@/services/api';
import { useKlineChart } from '@/composables/useKlineChart';

const { createKlineOption, getLatestKlineData } = useKlineChart();

const strategies = ref([]);
const selectedStrategy = ref('');
const selectedDate = ref('');
const batchStartDate = ref('');
const batchEndDate = ref('');
const runs = ref([]);
const deletedRuns = ref([]);
const operationLogs = ref([]);
const selectedDeletedRunId = ref(null);
const selectedDeletedRunIds = ref([]);
const activeRun = ref(null);
const activeRunPicks = ref([]);
const running = ref(false);
const evolving = ref(false);
const strategyScore = ref(null);

const panelOpen = ref({ history: true, dataOps: false, trash: false, audit: false });

const selectedKlineTsCode = ref('');
const selectedKlineName = ref('');
const klinePreviewVisible = ref(false);
const klineLoading = ref(false);
const klineError = ref('');
const klineRows = ref([]);
const klineOffset = ref(0);
const klineWindowDays = ref(60);
const klineHoveredIndex = ref(-1);
let klineHideTimer = null;
const klineCache = new Map();

const scoreModal = ref({ open: false, row: null });
const activeObservationPool = computed(() => Array.isArray(activeRun.value?.summary?.observation_pool) ? activeRun.value.summary.observation_pool : []);
const activeObservationSummary = computed(() => activeRun.value?.summary?.observation_summary || null);
const activePoolSplit = computed(() => activeRun.value?.summary?.pool_split || null);

const metricText = (v, d = 2) => {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(d) : '-';
};
const metricPct = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? `${(n * 100).toFixed(1)}%` : '-';
};
const retPct = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? `${(n * 100).toFixed(2)}%` : '-';
};
const retClass = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return 'text-slate-500';
  return n >= 0 ? 'text-rose-400' : 'text-emerald-400';
};

const calcMaSeries = (rows, windowSize) => rows.map((_, idx) => {
  const start = Math.max(0, idx - windowSize + 1);
  const window = rows.slice(start, idx + 1).map((x) => Number(x.close)).filter((x) => Number.isFinite(x));
  if (!window.length || window.length < Math.min(windowSize, idx + 1)) return null;
  const avg = window.reduce((a, b) => a + b, 0) / window.length;
  return Number.isFinite(avg) ? Number(avg.toFixed(2)) : null;
});

const normalizedKlineRows = computed(() => {
  const rows = [...klineRows.value].sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)));
  if (rows.length === 0) return [];
  const total = rows.length;
  const windowDays = Math.max(20, Math.min(klineWindowDays.value, total));
  const maxOffset = Math.max(0, total - windowDays);
  const offset = Math.max(0, Math.min(klineOffset.value, maxOffset));
  const end = total - offset;
  const start = Math.max(0, end - windowDays);
  return rows.slice(start, end);
});

const latestKlinePoint = computed(() => {
  const rows = normalizedKlineRows.value;
  return rows.length ? rows[rows.length - 1] : null;
});

const latestKlineData = computed(() => {
  const rows = normalizedKlineRows.value;
  if (!rows || !rows.length) return null;
  const idx = klineHoveredIndex.value >= 0 ? klineHoveredIndex.value : rows.length - 1;
  const item = rows[idx];
  if (!item) return getLatestKlineData(rows);
  return {
    ...item,
    net_mf_vol: item.net_mf_amount != null ? Number(item.net_mf_amount) / 10000 : null,
    rzye: item.rzye != null ? Number(item.rzye) / 1e8 : null,
  };
});

const handleUpdateAxisPointer = (event) => {
  const info = event.dataIndex;
  if (info != null && info >= 0) {
    klineHoveredIndex.value = info;
  }
};

const fmtPrice = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(2) : '-';
};

const fmtVolUnit = (v) => {
  if (!v || !Number.isFinite(v)) return '-';
  if (Math.abs(v) >= 1e8) return `${(v / 1e8).toFixed(2)}亿手`;
  if (Math.abs(v) >= 1e4) return `${(v / 1e4).toFixed(2)}万手`;
  return `${Math.round(v)}手`;
};

const fmtAmountUnit = (v) => {
  if (!v || !Number.isFinite(v)) return '-';
  return `${v.toFixed(2)}亿`;
};

const fmtMfUnit = (v) => {
  if (!v || !Number.isFinite(v)) return '-';
  const sign = v >= 0 ? '+' : '';
  return `${sign}${v.toFixed(2)}亿`;
};

const fmtRzyeUnit = (v) => {
  if (!v || !Number.isFinite(v)) return '-';
  return `${v.toFixed(2)}亿`;
};

const canStepOlder = computed(() => {
  const total = klineRows.value.length;
  const windowDays = Math.max(20, Math.min(klineWindowDays.value, total || 20));
  return total > windowDays && klineOffset.value < total - windowDays;
});

const canStepNewer = computed(() => klineOffset.value > 0);

const klineChartOption = computed(() => {
  return createKlineOption(normalizedKlineRows.value, { showLegend: true, showDataZoom: true, marginType: 'mf' });
});

const initBase = async () => {
  const [sRes, dRes] = await Promise.all([getFalconStrategies(), getFalconLatestTradeDate()]);
  strategies.value = sRes.data?.data || [];
  if (!selectedStrategy.value && strategies.value.length > 0) {
    selectedStrategy.value = strategies.value[0].strategy_id;
  }
  selectedDate.value = dRes.data?.data?.trade_date || '';
  batchStartDate.value = selectedDate.value;
  batchEndDate.value = selectedDate.value;
};

const refreshRuns = async () => {
  const res = await listFalconRuns(selectedStrategy.value || null, 30);
  runs.value = res.data?.data || [];
  if (activeRun.value && !runs.value.some((x) => x.run_id === activeRun.value.run_id)) {
    activeRun.value = null;
    activeRunPicks.value = [];
  }
  if (!activeRun.value && runs.value.length > 0) {
    await loadRun(runs.value[0].run_id);
  }
};

const refreshDeletedRuns = async () => {
  const res = await listFalconDeletedRuns(selectedStrategy.value || null, 30);
  deletedRuns.value = res.data?.data || [];
  selectedDeletedRunIds.value = selectedDeletedRunIds.value.filter((id) => deletedRuns.value.some((x) => x.run_id === id));
  if (selectedDeletedRunId.value && !deletedRuns.value.some((x) => x.run_id === selectedDeletedRunId.value)) {
    selectedDeletedRunId.value = null;
  }
};

const refreshOps = async () => {
  const res = await listFalconOps(selectedStrategy.value || null, 20);
  operationLogs.value = res.data?.data || [];
};

const toggleDeletedRunSelection = (runId) => {
  if (selectedDeletedRunIds.value.includes(runId)) {
    selectedDeletedRunIds.value = selectedDeletedRunIds.value.filter((x) => x !== runId);
    return;
  }
  selectedDeletedRunIds.value.push(runId);
};

const selectAllDeletedRuns = () => {
  selectedDeletedRunIds.value = deletedRuns.value.map((x) => x.run_id);
};

const clearDeletedRunSelection = () => {
  selectedDeletedRunIds.value = [];
};

const loadRun = async (runId) => {
  const res = await getFalconRunDetail(runId);
  activeRun.value = res.data?.data || null;
  activeRunPicks.value = activeRun.value?.picks || [];
  strategyScore.value = null;
};

const fetchKline = async (tsCode) => {
  const cached = klineCache.get(tsCode);
  if (cached) {
    klineRows.value = cached;
    klineError.value = '';
    return;
  }

  klineLoading.value = true;
  klineError.value = '';
  try {
    const res = await getStockKline(tsCode, 420);
    const rows = res.data?.data || [];

    const baseRows = rows
      .map((r) => {
        const d = String(r.trade_date).slice(0, 10);
        return {
          trade_date: d,
          open: Number(r.open),
          high: Number(r.high),
          low: Number(r.low),
          close: Number(r.close),
          vol: Number(r.vol),
          amount: Number(r.amount),
          net_mf_amount: r.net_mf_amount ?? null,
          rzye: r.rzye ?? null,
        };
      })
      .filter((x) => Number.isFinite(x.open) && Number.isFinite(x.high) && Number.isFinite(x.low) && Number.isFinite(x.close))
      .sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)));

    const ma5 = calcMaSeries(baseRows, 5);
    const ma10 = calcMaSeries(baseRows, 10);
    const ma20 = calcMaSeries(baseRows, 20);
    const ma60 = calcMaSeries(baseRows, 60);
    klineRows.value = baseRows.map((x, idx) => ({
      ...x,
      ma5: ma5[idx],
      ma10: ma10[idx],
      ma20: ma20[idx],
      ma60: ma60[idx],
    }));
    if (klineRows.value.length > 0) {
      klineCache.set(tsCode, klineRows.value);
    }

    if (klineRows.value.length === 0) {
      klineError.value = '暂无K线数据';
    }
  } catch (e) {
    klineError.value = e?.response?.data?.detail || 'K线加载失败';
    klineRows.value = [];
  } finally {
    klineLoading.value = false;
  }
};

const openKline = async (row) => {
  if (!row?.ts_code) return;
  selectedKlineTsCode.value = row.ts_code;
  selectedKlineName.value = row.name || '';
  klineOffset.value = 0;
  klineHoveredIndex.value = -1;
  klinePreviewVisible.value = true;
  await fetchKline(row.ts_code);
};

const clearHideKlinePreview = () => {
  if (klineHideTimer) {
    clearTimeout(klineHideTimer);
    klineHideTimer = null;
  }
};

const scheduleHideKlinePreview = () => {
  clearHideKlinePreview();
  klineHideTimer = setTimeout(() => {
    klinePreviewVisible.value = false;
    klineHoveredIndex.value = -1;
  }, 160);
};

const handleStockHover = async (row) => {
  clearHideKlinePreview();
  await openKline(row);
};

const stepOlder = () => {
  if (!canStepOlder.value) return;
  const total = klineRows.value.length;
  const maxOffset = Math.max(0, total - klineWindowDays.value);
  klineOffset.value = Math.min(maxOffset, klineOffset.value + 10);
  klineHoveredIndex.value = -1;
};

const stepNewer = () => {
  if (!canStepNewer.value) return;
  klineOffset.value = Math.max(0, klineOffset.value - 10);
  klineHoveredIndex.value = -1;
};

const zoomIn = () => {
  klineWindowDays.value = Math.max(20, klineWindowDays.value - 10);
  klineOffset.value = Math.min(klineOffset.value, Math.max(0, klineRows.value.length - klineWindowDays.value));
  klineHoveredIndex.value = -1;
};

const zoomOut = () => {
  klineWindowDays.value = Math.min(180, klineWindowDays.value + 10);
  klineOffset.value = Math.min(klineOffset.value, Math.max(0, klineRows.value.length - klineWindowDays.value));
  klineHoveredIndex.value = -1;
};

const handleRun = async () => {
  if (running.value) return;
  running.value = true;
  try {
    const payload = {
      strategy_id: selectedStrategy.value,
      trade_date: selectedDate.value,
      params: {},
    };
    const res = await runFalcon(payload);
    const runId = res.data?.data?.run_id;
    await refreshRuns();
    if (runId) {
      await loadRun(runId);
    }
  } catch (e) {
    alert(e?.response?.data?.detail || '运行失败');
  } finally {
    running.value = false;
  }
};

const openScore = async (row) => {
  scoreModal.value = { open: true, row };
  strategyScore.value = null;
  try {
    if (!activeRun.value?.strategy_id || !activeRun.value?.trade_date) return;
    const res = await getFalconScore(activeRun.value.strategy_id, activeRun.value.trade_date);
    strategyScore.value = res.data?.data || null;
  } catch (_) {
    strategyScore.value = null;
  }
};

const removeRun = async (hard = false) => {
  if (!activeRun.value) return;
  if (!confirm(`确认删除运行 #${activeRun.value.run_id} ?`)) return;
  await deleteFalconRun(activeRun.value.run_id, hard);
  activeRun.value = null;
  activeRunPicks.value = [];
  await refreshRuns();
  await refreshDeletedRuns();
  await refreshOps();
};

const restoreDeletedRun = async () => {
  if (!selectedDeletedRunId.value) return;
  await restoreFalconRun(selectedDeletedRunId.value);
  selectedDeletedRunId.value = null;
  await refreshRuns();
  await refreshDeletedRuns();
  await refreshOps();
};

const hardDeleteDeletedRun = async () => {
  if (!selectedDeletedRunId.value) return;
  if (!confirm(`确认硬删 #${selectedDeletedRunId.value} ?`)) return;
  await deleteFalconRun(selectedDeletedRunId.value, true);
  selectedDeletedRunId.value = null;
  await refreshDeletedRuns();
  await refreshOps();
};

const restoreDeletedRunsBatch = async () => {
  if (selectedDeletedRunIds.value.length === 0) return;
  await restoreFalconRunsBatch(selectedDeletedRunIds.value);
  selectedDeletedRunIds.value = [];
  await refreshRuns();
  await refreshDeletedRuns();
  await refreshOps();
};

const hardDeleteDeletedRunsBatch = async () => {
  if (selectedDeletedRunIds.value.length === 0) return;
  if (!confirm(`确认硬删 ${selectedDeletedRunIds.value.length} 条记录?`)) return;
  await hardDeleteFalconRunsBatch(selectedDeletedRunIds.value);
  selectedDeletedRunIds.value = [];
  await refreshDeletedRuns();
  await refreshOps();
};

const batchDeleteByRange = async () => {
  if (!selectedStrategy.value || !batchStartDate.value || !batchEndDate.value) return;
  if (!confirm(`确认删除 ${selectedStrategy.value} 在 ${batchStartDate.value} ~ ${batchEndDate.value} 的运行记录?`)) return;
  const res = await deleteFalconRunsByRange(selectedStrategy.value, batchStartDate.value, batchEndDate.value);
  const cnt = res.data?.data?.deleted_count ?? 0;
  alert(`已删除 ${cnt} 条运行记录`);
  await refreshRuns();
  await refreshDeletedRuns();
  await refreshOps();
};

const handleEvolve = async () => {
  if (!selectedStrategy.value || evolving.value) return;
  evolving.value = true;
  try {
    const res = await evolveFalcon(selectedStrategy.value);
    const promoted = res.data?.data?.promoted;
    alert(promoted ? '策略参数已晋级' : '演进完成，本轮未晋级');
  } catch (e) {
    alert(e?.response?.data?.detail || '演进失败');
  } finally {
    evolving.value = false;
  }
};

onMounted(async () => {
  try {
    await initBase();
    await refreshRuns();
    await refreshDeletedRuns();
    await refreshOps();
  } catch (e) {
    alert(e?.response?.data?.detail || 'Falcon 初始化失败');
  }
});

onBeforeUnmount(() => {
  clearHideKlinePreview();
});

watch(selectedStrategy, async () => {
  activeRun.value = null;
  activeRunPicks.value = [];
  selectedDeletedRunId.value = null;
  selectedDeletedRunIds.value = [];
  selectedKlineTsCode.value = '';
  selectedKlineName.value = '';
  klinePreviewVisible.value = false;
  klineRows.value = [];
  klineOffset.value = 0;
  klineCache.clear();
  try {
    await refreshRuns();
    await refreshDeletedRuns();
    await refreshOps();
  } catch (_) {
    // noop
  }
});
</script>

<style scoped>
.shadow-business {
  box-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5);
}
</style>
