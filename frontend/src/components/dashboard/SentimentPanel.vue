<template>
  <section 
    class="dashboard-panel panel-warm bg-business-dark rounded-2xl border border-business-light shadow-business overflow-hidden cursor-pointer transition-all hover:border-amber-500/30"
    @click="showHistoryModal = true"
  >
    <div class="h-px bg-gradient-to-r from-amber-400/70 via-business-highlight/30 to-transparent"></div>
    <div class="p-4 md:p-6">
      <div class="space-y-4">
        <div class="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <div class="flex items-center gap-4">
            <div class="relative h-24 w-24 md:h-28 md:w-28">
              <svg viewBox="0 0 100 100" class="h-full w-full -rotate-90">
                <circle cx="50" cy="50" r="42" fill="none" stroke="#334155" stroke-width="8" />
                <circle
                  cx="50" cy="50" r="42" fill="none"
                  :stroke="sentimentColor" stroke-width="8" stroke-linecap="round"
                  :stroke-dasharray="`${sentimentScore * 2.64} 264`"
                  class="transition-all duration-500"
                />
              </svg>
              <div class="absolute inset-0 flex flex-col items-center justify-center">
                <span class="text-2xl font-black md:text-3xl" :class="sentimentTextColor">{{ sentimentScore }}</span>
                <span class="text-[10px] text-slate-500">{{ sentimentLabel }}</span>
              </div>
            </div>

            <div class="space-y-2">
              <div class="flex flex-wrap items-center gap-2">
                <div class="text-sm font-bold text-white">{{ sentimentDate || '等待数据' }}</div>
                <span class="rounded-full border border-business-light bg-business-darker/70 px-2 py-1 text-[10px] font-bold text-slate-300">
                  {{ sentimentMarketStatusText }}
                </span>
                <span class="rounded-full border border-business-highlight/25 bg-business-highlight/10 px-2 py-1 text-[10px] font-bold text-business-highlight">
                  {{ sentimentAutoRefreshText }}
                </span>
              </div>
              <div class="flex flex-wrap items-center gap-2 text-[11px]">
                <span class="text-slate-300">收盘基线 {{ sentimentBaseScoreText }}</span>
                <span :class="sentimentDeltaClass">{{ sentimentDeltaText }}</span>
                <span class="text-slate-500">{{ sentimentMacroUpdatedText }}</span>
              </div>
              <p class="max-w-3xl text-[12px] leading-5 text-slate-300">
                {{ sentimentOverlaySummary }}
              </p>
              <div class="text-[11px] text-slate-500">点击面板查看历史曲线</div>
            </div>
          </div>

          <div class="flex flex-wrap gap-2">
            <button
              @click.stop="handleSyncSentiment"
              :disabled="syncingSentiment"
              class="inline-flex items-center justify-center rounded-lg border px-3 py-2 text-xs font-bold transition-all disabled:opacity-60"
              :class="syncingSentiment ? 'border-emerald-300 bg-emerald-300 text-emerald-950' : 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300 hover:bg-emerald-500 hover:text-white'"
            >
              {{ syncingSentiment ? '同步中...' : '同步' }}
            </button>
          </div>
        </div>

        <div class="grid gap-2 sm:grid-cols-2 xl:grid-cols-5">
          <div class="rounded-lg border border-slate-700 bg-slate-900/45 px-3 py-2.5">
            <div class="text-[10px] text-slate-500">市场广度</div>
            <div class="mt-1 text-base font-bold text-white">{{ breadthText }}</div>
            <div class="mt-1 text-[10px] text-slate-500">收盘上涨家数占比</div>
          </div>

          <div class="rounded-lg border border-slate-700 bg-slate-900/45 px-3 py-2.5">
            <div class="text-[10px] text-slate-500">涨停 / 跌停</div>
            <div class="mt-1 flex items-center gap-2 text-base font-bold">
              <span class="text-red-400">{{ limitUpCount }}</span>
              <span class="text-slate-500">/</span>
              <span class="text-emerald-400">{{ limitDownCount }}</span>
            </div>
            <div class="mt-1 text-[10px] text-slate-500">收盘强弱结构</div>
          </div>

          <div class="rounded-lg border border-slate-700 bg-slate-900/45 px-3 py-2.5">
            <div class="text-[10px] text-slate-500">上证实时</div>
            <div class="mt-1 text-base font-bold" :class="sseClass">{{ sseText }}</div>
            <div class="mt-1 text-[10px] text-slate-500">{{ sseSubtext }}</div>
          </div>

          <div class="rounded-lg border border-slate-700 bg-slate-900/45 px-3 py-2.5">
            <div class="text-[10px] text-slate-500">10Y 美债</div>
            <div class="mt-1 text-base font-bold" :class="tenYearClass">{{ tenYearText }}</div>
            <div class="mt-1 text-[10px] text-slate-500">{{ tenYearSubtext }}</div>
          </div>

          <div class="rounded-lg border border-slate-700 bg-slate-900/45 px-3 py-2.5">
            <div class="text-[10px] text-slate-500">Pizza 指数</div>
            <div class="mt-1 text-base font-bold" :class="pizzaClass">{{ pizzaText }}</div>
            <div class="mt-1 text-[10px] text-slate-500">{{ pizzaSubtext }}</div>
          </div>
        </div>

        <div class="grid gap-2 xl:grid-cols-[1.08fr,0.92fr]">
          <div class="rounded-xl border px-3 py-3" :class="sentimentRiskClass">
            <div class="flex items-center justify-between gap-3">
              <div>
                <div class="text-[10px] text-slate-500">风险预测</div>
                <div class="mt-1 text-sm font-bold text-white">{{ sentimentRiskHeadline }}</div>
              </div>
              <span class="rounded-full border px-2 py-1 text-[10px] font-bold" :class="sentimentRiskBadgeClass">
                {{ sentimentRiskLevelText }}
              </span>
            </div>
          </div>

          <div class="rounded-xl border border-business-light bg-slate-900/35 px-3 py-3">
            <div class="text-[10px] text-slate-500">叠加说明</div>
            <div class="mt-1 text-[12px] leading-5 text-slate-300">{{ sentimentMonitorHint }}</div>
            <div v-if="sentimentRiskReasons.length" class="mt-2 space-y-1 text-[11px] text-slate-400">
              <div v-for="reason in sentimentRiskReasons" :key="reason">- {{ reason }}</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <div
    v-if="showHistoryModal"
    class="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
    @click.self="showHistoryModal = false"
  >
    <div class="w-full max-w-4xl overflow-hidden rounded-2xl border border-slate-700 bg-slate-950/95 shadow-2xl backdrop-blur">
      <div class="flex items-center justify-between border-b border-slate-800 px-4 py-3">
        <div>
          <h3 class="text-sm font-bold text-slate-200">市场情绪趋势</h3>
          <p class="mt-1 text-[11px] text-slate-500">拖拽查看历史，默认显示最近15个交易日</p>
        </div>
        <button class="text-sm text-slate-400 transition-colors hover:text-white" @click="showHistoryModal = false">关闭</button>
      </div>
      <div class="p-4">
        <div class="h-72 w-full sm:h-96">
          <v-chart :option="historyChartOption" autoresize />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, onMounted, onBeforeUnmount } from 'vue';
import { getMarketSentiment, syncSentiment, getTasksStatus } from '@/services/api';

const latestSentiment = ref(null);
const latestSentimentDate = ref('');
const recentSentiments = ref([]);
const sentimentBoard = ref(null);
const syncingSentiment = ref(false);
const showHistoryModal = ref(false);
let sentimentRefreshTimer = null;

const sentimentLive = computed(() => sentimentBoard.value?.live || null);
const sentimentAutomation = computed(() => sentimentBoard.value?.automation || sentimentLive.value?.automation || {});

const sentimentScore = computed(() => {
  const liveVal = Number(sentimentLive.value?.score);
  if (Number.isFinite(liveVal)) return Math.round(liveVal);
  const val = Number(latestSentiment.value?.value);
  return Number.isFinite(val) ? Math.round(val) : 50;
});

const sentimentLabel = computed(() => {
  if (sentimentLive.value?.label) return sentimentLive.value.label;
  const score = sentimentScore.value;
  if (score >= 85) return '沸腾';
  if (score >= 70) return '高热';
  if (score >= 55) return '修复';
  if (score >= 42) return '拉锯';
  if (score >= 25) return '低温';
  return '冰点';
});

const sentimentColor = computed(() => {
  const score = sentimentScore.value;
  if (score >= 80) return '#ef4444';
  if (score >= 60) return '#f59e0b';
  if (score >= 40) return '#3b82f6';
  return '#64748b';
});

const sentimentTextColor = computed(() => {
  const score = sentimentScore.value;
  if (score >= 80) return 'text-red-400';
  if (score >= 60) return 'text-amber-400';
  if (score >= 40) return 'text-blue-400';
  return 'text-slate-400';
});

const sentimentDate = computed(() => {
  const tradeDate = latestSentimentDate.value || '';
  const boardTime = sentimentLive.value?.board_time || '';
  if (!tradeDate && !boardTime) return '';
  if (!boardTime) return tradeDate;
  return `${tradeDate || boardTime.slice(0, 10)} · ${boardTime.slice(11, 16)}`;
});

const sentimentMarketStatusText = computed(() => (
  sentimentLive.value?.market_status === 'TRADING' ? '交易时段' : '闭市跟踪'
));

const sentimentAutoRefreshText = computed(() => {
  const seconds = Number(sentimentAutomation.value?.dashboard_refresh_seconds);
  if (!Number.isFinite(seconds) || seconds <= 0) return '自动刷新';
  if ((sentimentLive.value?.market_status || '') === 'TRADING') return `${seconds}s 自动刷新`;
  return `闭市 ${seconds}s 刷新`;
});

const sentimentBaseScoreText = computed(() => {
  const val = Number(sentimentLive.value?.base_score ?? latestSentiment.value?.value);
  return Number.isFinite(val) ? `${val.toFixed(1)} 分` : '-';
});

const sentimentDeltaText = computed(() => {
  const delta = Number(sentimentLive.value?.delta);
  if (!Number.isFinite(delta) || Math.abs(delta) < 0.05) return '盘中叠加 0.0';
  return `盘中叠加 ${delta >= 0 ? '+' : ''}${delta.toFixed(1)}`;
});

const sentimentDeltaClass = computed(() => {
  const delta = Number(sentimentLive.value?.delta);
  if (!Number.isFinite(delta) || Math.abs(delta) < 0.05) return 'text-slate-400';
  return delta > 0 ? 'text-red-300' : 'text-emerald-300';
});

const sentimentOverlaySummary = computed(() => (
  sentimentLive.value?.overlay_summary || '收盘情绪会在交易时段自动叠加盘中指数和外部风险信号。'
));

const sentimentMacroUpdatedText = computed(() => {
  const updated = sentimentLive.value?.external_signals?.updated_at;
  if (!updated) return '等待外部信号';
  return `宏观 ${updated.slice(11, 16)}`;
});

const breadthText = computed(() => {
  const details = latestSentiment.value?.details || {};
  const factors = details.factors || {};
  const breadth = Number(factors.breadth);
  return Number.isFinite(breadth) ? `${breadth.toFixed(1)}%` : '-';
});

const limitUpCount = computed(() => {
  const details = latestSentiment.value?.details || {};
  const factors = details.factors || {};
  return factors.limit ?? '-';
});

const limitDownCount = computed(() => {
  const details = latestSentiment.value?.details || {};
  const factors = details.factors || {};
  return factors.limit_down ?? factors.failure ?? '-';
});

const sseSnapshot = computed(() => sentimentLive.value?.sse_snapshot || {});

const sseText = computed(() => {
  const pct = Number(sseSnapshot.value?.pct);
  if (!Number.isFinite(pct)) return '-';
  return `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`;
});

const sseClass = computed(() => {
  const pct = Number(sseSnapshot.value?.pct);
  if (!Number.isFinite(pct)) return 'text-slate-400';
  return pct >= 0 ? 'text-red-400' : 'text-emerald-400';
});

const sseSubtext = computed(() => {
  const quoteTime = sseSnapshot.value?.quote_time;
  if (quoteTime) return `上证 · ${quoteTime}`;
  return '上证指数盘中方向';
});

const tenYearSignal = computed(() => sentimentLive.value?.external_signals?.ten_year_yield || {});

const tenYearText = computed(() => {
  const value = Number(tenYearSignal.value?.yield);
  if (!Number.isFinite(value)) return '-';
  return `${value.toFixed(3)}%`;
});

const tenYearClass = computed(() => {
  const value = Number(tenYearSignal.value?.yield);
  if (!Number.isFinite(value)) return 'text-slate-400';
  if (value >= 4.4) return 'text-rose-300';
  if (value >= 4.38) return 'text-amber-300';
  if (value <= 4.3) return 'text-cyan-300';
  return 'text-slate-100';
});

const tenYearSubtext = computed(() => {
  const source = tenYearSignal.value?.source || '10Y';
  const value = Number(tenYearSignal.value?.yield);
  if (!Number.isFinite(value)) return `${source} 未取到`;
  if (value >= 4.4) return `${source} · 高于 4.4% 风险线`;
  if (value >= 4.38) return `${source} · 逼近 4.4% 风险线`;
  if (value <= 4.3) return `${source} · 靠近 4.3% 观察带`;
  return `${source} · 常规波动区间`;
});

const pizzaSignal = computed(() => sentimentLive.value?.external_signals?.pizza_index || {});

const pizzaText = computed(() => {
  const doughcon = Number(pizzaSignal.value?.doughcon);
  if (Number.isFinite(doughcon)) return `DOUGHCON ${Math.round(doughcon)}`;
  const spike = Number(pizzaSignal.value?.max_spike_pct);
  if (Number.isFinite(spike)) return `${Math.round(spike)}% SPIKE`;
  return '-';
});

const pizzaClass = computed(() => {
  const doughcon = Number(pizzaSignal.value?.doughcon);
  const spike = Number(pizzaSignal.value?.max_spike_pct);
  if (Number.isFinite(doughcon) && doughcon <= 2) return 'text-rose-300';
  if ((Number.isFinite(doughcon) && doughcon <= 3) || (Number.isFinite(spike) && spike >= 150)) return 'text-amber-300';
  return 'text-slate-100';
});

const pizzaSubtext = computed(() => {
  const spike = Number(pizzaSignal.value?.max_spike_pct);
  if (Number.isFinite(spike)) return `低置信度旁证 · ${Math.round(spike)}% 峰值`;
  return '低置信度地缘旁证';
});

const sentimentRisk = computed(() => sentimentLive.value?.risk_prediction || {});

const sentimentRiskClass = computed(() => {
  const level = sentimentRisk.value?.level;
  if (level === 'high') return 'border-rose-400/25 bg-rose-500/10';
  if (level === 'elevated') return 'border-amber-400/25 bg-amber-500/10';
  if (level === 'relief') return 'border-cyan-400/25 bg-cyan-500/10';
  return 'border-business-light bg-slate-900/35';
});

const sentimentRiskBadgeClass = computed(() => {
  const level = sentimentRisk.value?.level;
  if (level === 'high') return 'border-rose-400/30 bg-rose-500/10 text-rose-200';
  if (level === 'elevated') return 'border-amber-400/30 bg-amber-500/10 text-amber-200';
  if (level === 'relief') return 'border-cyan-400/30 bg-cyan-500/10 text-cyan-200';
  return 'border-slate-500/40 bg-slate-500/10 text-slate-300';
});

const sentimentRiskLevelText = computed(() => {
  const level = sentimentRisk.value?.level;
  if (level === 'high') return '高风险';
  if (level === 'elevated') return '升温';
  if (level === 'relief') return '缓和';
  return '中性';
});

const sentimentRiskHeadline = computed(() => (
  sentimentRisk.value?.headline || '外部风险中性，继续按盘中承接判断。'
));

const sentimentRiskReasons = computed(() => (
  Array.isArray(sentimentRisk.value?.reasons) ? sentimentRisk.value.reasons : []
));

const sentimentMonitorHint = computed(() => (
  '10Y 以 4.4% 为上沿风险线、4.3% 为下沿观察线；Pizza 指数只作弱旁证，不单独决定仓位。'
));

const historyChartOption = computed(() => {
  const data = recentSentiments.value || [];
  const dates = data.map(s => { const d = s.trade_date || s.date || ''; return d.length > 5 ? d.slice(5) : d; });
  const scores = data.map(s => Number(s.value) || 0);
  return {
    backgroundColor: 'transparent',
    grid: { top: 40, left: 50, right: 30, bottom: 60, containLabel: true },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' }, backgroundColor: 'rgba(15, 23, 42, 0.95)', borderColor: '#334155', textStyle: { color: '#cbd5e1' }, confine: true },
    dataZoom: [
      { type: 'inside', start: Math.max(0, (1 - 15 / Math.max(dates.length, 1)) * 100), end: 100 },
      { type: 'slider', height: 20, bottom: 10, borderColor: 'transparent', backgroundColor: 'rgba(30, 41, 59, 0.5)', fillerColor: 'rgba(51, 65, 85, 0.5)', handleStyle: { color: '#475569' }, textStyle: { color: '#64748b', fontSize: 10 }, start: Math.max(0, (1 - 15 / Math.max(dates.length, 1)) * 100), end: 100 }
    ],
    xAxis: { type: 'category', data: dates, boundaryGap: false, axisLine: { lineStyle: { color: '#334155' } }, axisLabel: { color: '#64748b', fontSize: 10, rotate: 30 } },
    yAxis: { type: 'value', name: '情绪分数', min: 0, max: 100, splitLine: { lineStyle: { color: '#334155', type: 'dashed' } }, axisLine: { show: false }, axisLabel: { color: '#f59e0b', fontSize: 10 }, nameTextStyle: { color: '#f59e0b', fontSize: 10 } },
    series: [{
      name: '情绪分数', type: 'line', smooth: true, showSymbol: false, data: scores,
      lineStyle: { color: '#f59e0b', width: 3 },
      areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: 'rgba(245, 158, 11, 0.3)' }, { offset: 1, color: 'rgba(245, 158, 11, 0)' }] } },
      markLine: {
        symbol: ['none', 'none'], label: { show: true, position: 'end', formatter: '{b}', fontSize: 10, color: 'inherit' },
        data: [
          { name: '高水位(80)', yAxis: 80, lineStyle: { color: '#ef4444', type: 'dashed', opacity: 0.7 } },
          { name: '低水位(20)', yAxis: 20, lineStyle: { color: '#3b82f6', type: 'dashed', opacity: 0.7 } }
        ]
      }
    }]
  };
});

const waitForSentimentTaskDone = async (timeoutMs = 180000) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await getTasksStatus();
      const status = res.data || {};
      const currentName = status.current_task?.name || '';
      if (!currentName.includes('市场情绪')) return;
    } catch (e) { return; }
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
};

const handleSyncSentiment = async () => {
  if (syncingSentiment.value) return;
  syncingSentiment.value = true;
  try {
    await syncSentiment(250);
    await waitForSentimentTaskDone();
    await fetchMarketSentiment();
  } catch (e) {
    console.error('同步情绪失败', e);
  } finally {
    syncingSentiment.value = false;
  }
};

const clearSentimentRefreshTimer = () => {
  if (sentimentRefreshTimer) { window.clearTimeout(sentimentRefreshTimer); sentimentRefreshTimer = null; }
};

const scheduleSentimentRefresh = () => {
  clearSentimentRefreshTimer();
  const seconds = Number(sentimentAutomation.value?.dashboard_refresh_seconds);
  const nextSeconds = Number.isFinite(seconds) && seconds > 0 ? seconds : 60;
  sentimentRefreshTimer = window.setTimeout(() => { void fetchMarketSentiment(); }, nextSeconds * 1000);
};

const fetchMarketSentiment = async () => {
  clearSentimentRefreshTimer();
  try {
    const res = await getMarketSentiment(250);
    const data = res.data.data || {};
    sentimentBoard.value = { live: data.live || null, automation: data.automation || {} };
    if (!data.dates || data.dates.length === 0) { latestSentimentDate.value = ''; latestSentiment.value = null; recentSentiments.value = []; return; }
    latestSentimentDate.value = data.dates[data.dates.length - 1];
    latestSentiment.value = data.sentiment[data.sentiment.length - 1] || null;
    recentSentiments.value = data.sentiment.slice(-60);
  } catch (e) {
    console.error('Sentiment Error', e);
  } finally {
    scheduleSentimentRefresh();
  }
};

onMounted(async () => { await fetchMarketSentiment(); });
onBeforeUnmount(() => { clearSentimentRefreshTimer(); });
</script>
