<template>
  <div class="space-y-3 md:space-y-4 max-w-6xl mx-auto pb-6">
    <!-- 情绪仪表盘 -->
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
                    cx="50"
                    cy="50"
                    r="42"
                    fill="none"
                    :stroke="sentimentColor"
                    stroke-width="8"
                    stroke-linecap="round"
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

    <!-- 历史情绪趋势弹窗 -->
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

    <section class="dashboard-panel panel-cool bg-business-dark rounded-2xl border border-business-light shadow-business overflow-hidden">
      <div class="h-px bg-gradient-to-r from-business-accent/80 via-business-highlight/35 to-transparent"></div>
      <div class="border-b border-business-light px-2.5 py-2 md:px-3 md:py-2.5">
        <div class="flex flex-col gap-1.5 lg:flex-row lg:items-start lg:justify-between">
          <div class="space-y-1">
            <div class="inline-flex items-center gap-1.5 text-[9px] font-bold uppercase tracking-[0.22em] text-business-accent">
              <span class="h-1.5 w-1.5 rounded-full bg-business-accent"></span>
              主线作战板
            </div>
            <p class="max-w-3xl text-[12px] md:text-[13px] leading-[18px] text-slate-300">
              {{ mainlineHeadline }}
            </p>
          </div>

          <div class="text-[9px] leading-4 text-slate-500">
            <div>{{ mainlineReview?.date_range?.start || '-' }} 至 {{ mainlineReview?.date_range?.end || '-' }}</div>
            <div class="mt-0.5">复盘交易日 {{ mainlineReview?.trade_days || 0 }} 天</div>
          </div>
        </div>
      </div>

      <div class="p-3 md:p-4 space-y-3">
        <div
          v-if="loadingTrend && focusedMainlineCards.length === 0"
          class="py-10 text-center text-sm text-slate-400"
        >
          <div class="mx-auto mb-3 h-8 w-8 animate-spin rounded-full border-2 border-business-accent border-t-transparent"></div>
          主线数据加载中...
        </div>

        <div
          v-else-if="!mainlineOverview.current && !mainlineOverview.continuous && focusedMainlineCards.length === 0"
          class="rounded-xl border border-business-light bg-slate-900/35 px-4 py-8 text-center text-sm text-slate-500"
        >
          暂无主线聚焦数据
        </div>

        <template v-else>
          <div class="grid gap-2.5 xl:grid-cols-[1.14fr,0.86fr]">
            <div class="overflow-hidden rounded-xl border border-business-light bg-slate-900/30">
              <div class="divide-y divide-business-light/70">
                <div
                  v-for="sector in focusedMainlineCards"
                  :key="sector.name"
                  class="cursor-pointer px-2.5 py-2 md:px-3 md:py-2.5 transition-colors"
                  :class="[sector.panelClass, selectedMainlineName === sector.name ? 'ring-1 ring-inset ring-business-accent/35 bg-business-accent/[0.08]' : '']"
                  role="button"
                  tabindex="0"
                  @click="toggleMainlineFocus(sector.name)"
                  @keydown.enter.prevent="toggleMainlineFocus(sector.name)"
                  @keydown.space.prevent="toggleMainlineFocus(sector.name)"
                >
                  <div class="flex items-center gap-2 md:items-start md:gap-3">
                    <span class="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-business-darker text-[9px] font-bold text-white md:h-6 md:w-6 md:text-[10px]">
                      {{ sector.displayRank }}
                    </span>

                    <div class="min-w-0 flex-1">
                      <div class="flex flex-wrap items-center gap-1.5">
                        <div class="truncate text-[13px] font-bold text-white md:text-sm">{{ sector.displayName || sector.name }}</div>
                        <span
                          class="rounded-full border px-1.5 py-0.5 text-[8px] font-bold md:text-[9px]"
                          :class="sector.roleClass"
                        >
                          {{ sector.roleLabel }}
                        </span>
                        <span
                          v-for="qt in sector.quickTags"
                          :key="`qt-${sector.name}-${qt}`"
                          class="rounded-full border px-1.5 py-0.5 text-[8px] font-bold md:text-[9px]"
                          :class="sector.roleClass"
                        >
                          {{ qt }}
                        </span>
                        <span class="text-[11px] font-black text-white md:hidden">{{ fmt(sector.score, 1) }}</span>
                      </div>

                      <div class="mt-0.5 text-[11px] leading-4 text-slate-400 md:mt-1 md:text-[12px] md:leading-5 md:text-slate-300 line-clamp-1 md:line-clamp-none">{{ sector.thesis }}</div>
                      <div v-if="sector.evidenceTags?.length" class="mt-1 flex flex-wrap gap-1 md:gap-1.5">
                        <span
                          v-for="tag in sector.evidenceTags"
                          :key="`${sector.name}-${tag}`"
                          class="rounded-full border border-business-light/70 bg-business-darker/70 px-1.5 py-0.5 text-[8px] text-slate-400 md:px-2 md:text-[9px]"
                        >
                          {{ tag }}
                        </span>
                      </div>
                    </div>

                    <div class="hidden shrink-0 text-right md:block">
                      <div class="text-lg font-black text-white">{{ fmt(sector.score, 1) }}</div>
                      <button
                        @click.stop="toggleMainlineFocus(sector.name)"
                        class="mt-1 inline-flex h-6 min-w-[68px] items-center justify-center rounded-md border border-business-light bg-business-darker/80 px-2 text-[9px] font-bold text-slate-300 transition-all hover:border-business-highlight hover:text-white"
                      >
                        {{ selectedMainlineName === sector.name ? '收起' : '展开' }}
                      </button>
                    </div>
                  </div>

                  <div
                    v-if="selectedMainlineName === sector.name"
                    class="mt-3 rounded-lg border border-business-light/80 bg-business-darker/65 p-2.5"
                  >
                    <div class="flex items-start justify-between gap-2">
                      <div>
                        <p class="text-[9px] font-bold uppercase tracking-[0.18em] text-slate-500">龙头样本</p>
                        <p class="mt-1 text-[10px] leading-4 text-slate-500">{{ getMainlineLeaderCaption(sector) }}</p>
                      </div>
                      <span class="rounded-full border border-business-light/80 bg-slate-950/45 px-2 py-1 text-[9px] font-bold text-slate-300">
                        {{ sector.stocks?.length || 0 }} 只
                      </span>
                    </div>

                    <div
                      v-if="loadingLeaders && !hasLeaderGroup(sector.name)"
                      class="mt-2 rounded-lg border border-business-light/60 bg-slate-950/30 px-2 py-1.5 text-[10px] text-slate-500"
                    >
                      补充龙头强度中，先展示主线历史样本。
                    </div>

                    <div v-if="sector.stocks?.length" class="mt-2.5 grid gap-1.5 sm:grid-cols-2">
                      <div
                        v-for="(stock, sidx) in sector.stocks"
                        :key="`${sector.name}-${stock.tsCode || stock.name}-${sidx}`"
                        class="rounded-lg border border-business-light/70 bg-slate-950/45 px-2.5 py-2"
                      >
                        <div class="flex items-center justify-between gap-2">
                          <div class="min-w-0">
                            <div class="flex items-center gap-1.5">
                              <span class="text-[9px] text-slate-500">{{ sidx + 1 }}</span>
                              <span class="truncate text-[12px] font-semibold text-slate-100">{{ stock.name }}</span>
                            </div>
                          </div>
                          <div class="text-right">
                            <div class="text-[11px]" :class="stock.changeClass">{{ stock.changeText }}</div>
                            <div v-if="stock.badge" class="mt-0.5 text-[9px] text-slate-400">{{ stock.badge }}</div>
                          </div>
                        </div>
                        <div class="mt-1 flex flex-wrap items-center gap-1 text-[9px] text-slate-500">
                          <span class="font-mono">{{ stock.tsCode || '-' }}</span>
                          <span v-if="stock.note">· {{ stock.note }}</span>
                        </div>
                      </div>
                    </div>

                    <div
                      v-else
                      class="mt-2.5 flex min-h-[72px] items-center justify-center rounded-lg border border-business-light/70 bg-slate-950/35 px-3 text-[11px] text-slate-500"
                    >
                      当前方向暂无可展示的龙头样本。
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div class="rounded-xl border border-business-highlight/15 bg-gradient-to-br from-business-highlight/[0.06] to-slate-900/35 p-3">
              <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                <div>
                  <p class="text-[10px] font-bold text-slate-500">趋势证据</p>
                  <h2 class="mt-1 text-sm font-bold text-white">近10日主线强度</h2>
                </div>
                <div class="text-[10px] text-slate-500">{{ mainlineChartCaption }}</div>
              </div>

              <div class="relative mt-3 h-60 w-full sm:h-72 xl:h-80">
                <v-chart
                  v-if="!loadingTrend && mainlineChartOption.series.length > 0"
                  :option="mainlineChartOption"
                  autoresize
                />
                <div v-if="loadingTrend" class="absolute inset-0 flex items-center justify-center">
                  <div class="h-8 w-8 animate-spin rounded-full border-2 border-business-accent border-t-transparent"></div>
                </div>
                <div
                  v-if="!loadingTrend && mainlineChartOption.series.length === 0"
                  class="absolute inset-0 flex flex-col items-center justify-center space-y-2 text-slate-500"
                >
                  <p class="text-xs font-bold uppercase tracking-[0.24em]">暂无近10日主线数据</p>
                </div>
              </div>
            </div>
          </div>
        </template>
      </div>
    </section>
  </div>
</template>

<script setup>
import { computed, ref, onMounted, onBeforeUnmount, watch } from 'vue';
import { getMainlineHistory, getMarketSentiment, syncSentiment, getTasksStatus, getMainlineLeaders } from '@/services/api';

const loadingTrend = ref(false);
const trendAnalysis = ref('');
const mainlineChartDates = ref([]);
const selectedMainlineName = ref('');
const showHistoryModal = ref(false);
const isMobile = ref(false);
const syncingSentiment = ref(false);
const currentMainline = ref({
  name: '',
  displayName: '',
  score: 0,
  reason: '',
  focusTags: [],
  driverSummary: '',
});
const mainlineReview = ref(null);
const mainlineTrendSeries = ref([]);
const mainlineOverview = ref({ current: null, continuous: null });
const mainlineFocusCards = ref([]);
const latestSentiment = ref(null);
const latestSentimentDate = ref('');
const recentSentiments = ref([]);
const sentimentBoard = ref(null);
const minimalTooltipMode = ref(false);
let sentimentRefreshTimer = null;

// 主线龙头推荐数据
const mainlineLeaders = ref(null);
const loadingLeaders = ref(false);
const MAINLINE_CHART_COLORS = ['#3b82f6', '#06b6d4', '#f59e0b'];


// 情绪相关计算属性
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
  if ((sentimentLive.value?.market_status || '') === 'TRADING') {
    return `${seconds}s 自动刷新`;
  }
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
  sentimentLive.value?.overlay_summary
  || '收盘情绪会在交易时段自动叠加盘中指数和外部风险信号。'
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
  if ((Number.isFinite(doughcon) && doughcon <= 3) || (Number.isFinite(spike) && spike >= 150)) {
    return 'text-amber-300';
  }
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

// 历史趋势图表配置
const historyChartOption = computed(() => {
  const data = recentSentiments.value || [];
  const dates = data.map(s => {
    const d = s.trade_date || s.date || '';
    return d.length > 5 ? d.slice(5) : d;
  });
  const scores = data.map(s => Number(s.value) || 0);
  
  return {
    backgroundColor: 'transparent',
    grid: { top: 40, left: 50, right: 30, bottom: 60, containLabel: true },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      backgroundColor: 'rgba(15, 23, 42, 0.95)',
      borderColor: '#334155',
      textStyle: { color: '#cbd5e1' },
      confine: true
    },
    dataZoom: [
      {
        type: 'inside',
        start: Math.max(0, (1 - 15 / Math.max(dates.length, 1)) * 100),
        end: 100
      },
      {
        type: 'slider',
        height: 20,
        bottom: 10,
        borderColor: 'transparent',
        backgroundColor: 'rgba(30, 41, 59, 0.5)',
        fillerColor: 'rgba(51, 65, 85, 0.5)',
        handleStyle: { color: '#475569' },
        textStyle: { color: '#64748b', fontSize: 10 },
        start: Math.max(0, (1 - 15 / Math.max(dates.length, 1)) * 100),
        end: 100
      }
    ],
    xAxis: {
      type: 'category',
      data: dates,
      boundaryGap: false,
      axisLine: { lineStyle: { color: '#334155' } },
      axisLabel: { color: '#64748b', fontSize: 10, rotate: 30 }
    },
    yAxis: {
      type: 'value',
      name: '情绪分数',
      min: 0,
      max: 100,
      splitLine: { lineStyle: { color: '#334155', type: 'dashed' } },
      axisLine: { show: false },
      axisLabel: { color: '#f59e0b', fontSize: 10 },
      nameTextStyle: { color: '#f59e0b', fontSize: 10 }
    },
    series: [
      {
        name: '情绪分数',
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: scores,
        lineStyle: { color: '#f59e0b', width: 3 },
        areaStyle: {
          color: {
            type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(245, 158, 11, 0.3)' },
              { offset: 1, color: 'rgba(245, 158, 11, 0)' }
            ]
          }
        },
        markLine: {
          symbol: ['none', 'none'],
          label: {
            show: true,
            position: 'end',
            formatter: '{b}',
            fontSize: 10,
            color: 'inherit'
          },
          data: [
            { name: '高水位(80)', yAxis: 80, lineStyle: { color: '#ef4444', type: 'dashed', opacity: 0.7 } },
            { name: '低水位(20)', yAxis: 20, lineStyle: { color: '#3b82f6', type: 'dashed', opacity: 0.7 } }
          ]
        }
      }
    ]
  };
});

const getMainlineHeat = (score) => {
  if (score >= 35) {
    return {
      label: '高热',
      heatClass: 'border-rose-400/30 bg-rose-400/10 text-rose-200',
      progressClass: 'bg-gradient-to-r from-rose-500 via-amber-400 to-yellow-300',
      surfaceClass: 'border-rose-400/15 bg-rose-500/6',
    };
  }
  if (score >= 25) {
    return {
      label: '升温',
      heatClass: 'border-amber-400/30 bg-amber-400/10 text-amber-200',
      progressClass: 'bg-gradient-to-r from-amber-500 via-orange-400 to-cyan-300',
      surfaceClass: 'border-amber-400/15 bg-amber-500/6',
    };
  }
  if (score >= 15) {
    return {
      label: '观察',
      heatClass: 'border-cyan-400/30 bg-cyan-400/10 text-cyan-200',
      progressClass: 'bg-gradient-to-r from-cyan-500 via-sky-400 to-emerald-300',
      surfaceClass: 'border-cyan-400/15 bg-cyan-500/6',
    };
  }
  return {
    label: '试探',
    heatClass: 'border-slate-500/40 bg-slate-500/10 text-slate-200',
    progressClass: 'bg-gradient-to-r from-slate-500 via-slate-400 to-slate-300',
    surfaceClass: 'border-white/8 bg-white/[0.02]',
  };
};





const mainlineHeadline = computed(() => (
  mainlineRotationNote.value || '主线暂未收束，先控制试错。'
));

const getMainlinePriorityScore = (sector) => {
  const score = Number(sector?.score) || 0;
  const activeDays = Number(sector?.activeDays) || 0;
  const consecutiveDays = Number(sector?.consecutiveDays) || 0;
  const limitUps = Number(sector?.limitUps) || 0;
  const resonance = Number(sector?.resonance) || 0;
  const labels = sector?.labels || [];

  let priority = score * 1.35;
  if (labels.includes('当前最强')) priority += 18;
  if (labels.includes('10日连续')) priority += 16;
  if (labels.includes('实时龙头')) priority += 10;
  priority += Math.min(activeDays * 1.4, 10);
  priority += Math.min(consecutiveDays * 4, 16);
  priority += Math.min(limitUps * 1.8, 12);
  priority += Math.min(resonance * 0.12, 8);

  if (consecutiveDays < 2 && activeDays < 3 && score < 20) {
    priority -= 12;
  }

  return priority;
};

const getMainlineRole = (sector, index) => {
  const consecutiveDays = Number(sector?.consecutiveDays) || 0;
  const limitUps = Number(sector?.limitUps) || 0;
  const activeDays = Number(sector?.activeDays) || 0;
  const labels = sector?.labels || [];

  if (index === 0 || (labels.includes('当前最强') && (consecutiveDays >= 3 || limitUps >= 3))) {
    return {
      label: '主盯',
      className: 'border-business-accent/30 bg-business-accent/10 text-business-accent',
      note: '满足强度与持续性，优先深看',
    };
  }

  if (consecutiveDays >= 3 || labels.includes('10日连续')) {
    return {
      label: '次盯',
      className: 'border-business-highlight/30 bg-business-highlight/10 text-business-highlight',
      note: '连续性更好，适合跟踪承接',
    };
  }

  if (activeDays >= 2 || limitUps >= 2 || labels.includes('实时龙头')) {
    return {
      label: '备选',
      className: 'border-amber-400/30 bg-amber-400/10 text-amber-200',
      note: '有强度，但还要等连续性确认',
    };
  }

  return {
    label: '观察',
    className: 'border-slate-500/40 bg-slate-500/10 text-slate-300',
    note: '先看是否能走出连续性',
  };
};

const getMainlinePanelClass = (roleLabel) => {
  if (roleLabel === '主盯') {
    return 'bg-business-accent/[0.05]';
  }
  if (roleLabel === '次盯') {
    return 'bg-business-highlight/[0.05]';
  }
  if (roleLabel === '备选') {
    return 'bg-amber-500/[0.05]';
  }
  return 'bg-slate-900/20';
};

const getMainlineExecutionBias = (sector) => {
  const consecutiveDays = Number(sector?.consecutiveDays) || 0;
  const limitUps = Number(sector?.limitUps) || 0;
  const activeDays = Number(sector?.activeDays) || 0;
  const resonance = Number(sector?.resonance) || 0;

  if (consecutiveDays >= 3 && resonance >= 55) {
    return '连续性和板块共振都在前排，分歧后的承接更值得跟。';
  }
  if (consecutiveDays >= 3) {
    return '连续性已经达标，优先看分歧回踩后的承接。';
  }
  if (limitUps >= 3 || activeDays >= 5 || resonance >= 50) {
    return '强度已进入观察名单，但还要确认能否继续走出持续性。';
  }
  return '当前更偏预备方向，先看龙头是否能维持强度。';
};

const mainlinePriorityCards = computed(() => (
  (mainlineFocusCards.value || [])
    .map((sector) => {
      const heat = getMainlineHeat(Number(sector?.score));
      const priorityScore = getMainlinePriorityScore(sector);
      return {
        ...sector,
        priorityScore,
        heatLabel: heat.label,
        heatClass: heat.heatClass,
      };
    })
    .sort((a, b) => Number(b?.priorityScore || 0) - Number(a?.priorityScore || 0))
));

const focusedMainlineCards = computed(() => (
  mainlinePriorityCards.value.slice(0, 3).map((sector, index) => {
    const role = getMainlineRole(sector, index);
    return {
      ...sector,
      displayRank: index + 1,
      panelClass: getMainlinePanelClass(role.label),
      roleLabel: role.label,
      roleClass: role.className,
      thesis: sector.thesis || getMainlineExecutionBias(sector),
    };
  })
));

const selectedMainlineCard = computed(() => (
  focusedMainlineCards.value.find((item) => item.name === selectedMainlineName.value) || null
));

const normalizeSentenceText = (text = '') => text.replace(/[\s，。；：、“”"'（）()【】\[\]\-]/g, '');

const pickDistinctAnalysisSentence = (text, compareTexts = []) => {
  const sentences = `${text || ''}`
    .split(/[。；！!\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
  const comparePool = compareTexts
    .map((item) => normalizeSentenceText(item))
    .filter(Boolean);

  return sentences.find((sentence) => {
    const normalized = normalizeSentenceText(sentence);
    if (normalized.length < 12) return false;
    return !comparePool.some((item) => item.includes(normalized) || normalized.includes(item));
  }) || '';
};

const mainlineChartCaption = computed(() => {
  const names = focusedMainlineCards.value.map((item) => item.name).filter(Boolean);
  if (!names.length) return '图表自动跟随当前聚焦方向';
  return `图表仅保留 ${names.join(' / ')} 三条以内主线`;
});

const getMainlineLeaderCaption = (sector) => {
  if (!sector) return '点开方向卡片后，在卡片内直接看龙头样本。';
  if (sector.driverSummary) return sector.driverSummary;
  const tags = (sector.quickTags || []).slice(0, 2).join(' · ');
  if (tags) return tags;
  if (sector.roleLabel === '主盯') {
    return '优先看龙头承接是否稳定，不需要额外拆一块固定席位。';
  }
  return '作为轮动备份观察，确认强度后再切换注意力。';
};

const mainlineRotationNote = computed(() => {
  const primary = focusedMainlineCards.value[0];
  const secondary = focusedMainlineCards.value[1];
  const selected = selectedMainlineCard.value;
  if (!primary) return '';

  const parts = [];

  if (selected?.name && selected.name !== primary.name) {
    parts.push(`当前展开 ${selected.name} 只是保留轮动备份，不建议和 ${primary.name} 平均分配仓位`);
  } else {
    parts.push(`先盯 ${primary.name}，龙头承接没丢前不需要频繁切换观察中心`);
  }

  if (secondary?.name && secondary.name !== primary.name) {
    parts.push(`只有在 ${primary.name} 承接转弱且 ${secondary.name} 强度继续抬升时，再把注意力切到 ${secondary.name}`);
  }

  const distinctInsight = pickDistinctAnalysisSentence(trendAnalysis.value, parts);
  if (distinctInsight) {
    parts.push(distinctInsight);
  }

  return `${parts.join('；')}。`;
});

const mergeMainlineLeaders = (payload) => {
  const current = Array.isArray(mainlineLeaders.value?.mainlines)
    ? mainlineLeaders.value.mainlines
    : [];
  const incoming = Array.isArray(payload?.mainlines) ? payload.mainlines : [];
  const merged = new Map();

  current.forEach((item) => {
    if (item?.sector) merged.set(item.sector, item);
  });
  incoming.forEach((item) => {
    if (item?.sector) merged.set(item.sector, item);
  });

  mainlineLeaders.value = {
    ...(mainlineLeaders.value || {}),
    ...(payload || {}),
    mainlines: Array.from(merged.values()),
  };
};

const toggleMainlineFocus = (name) => {
  if (!name) return;
  selectedMainlineName.value = selectedMainlineName.value === name ? '' : name;
};

watch([selectedMainlineName, loadingLeaders], ([name, isLoading]) => {
  if (!name || isLoading || hasLeaderGroup(name)) return;
  void fetchMainlineLeaders(name);
});

const waitForSentimentTaskDone = async (timeoutMs = 180000) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await getTasksStatus();
      const status = res.data || {};
      const currentName = status.current_task?.name || '';
      const isRunningSentiment = currentName.includes('市场情绪');
      if (!isRunningSentiment) {
        return;
      }
    } catch (e) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
};

const handleSyncSentiment = async () => {
  if (syncingSentiment.value) return;
  syncingSentiment.value = true;
  try {
    await syncSentiment(250, true);
    await waitForSentimentTaskDone();
    await fetchMarketSentiment();
  } catch (e) {
    alert("同步情绪失败");
  } finally {
    syncingSentiment.value = false;
  }
};

const fmt = (v, digits = 2, suffix = '') => {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return `${n.toFixed(digits)}${suffix}`;
};

const fmtInt = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return `${Math.round(n)}`;
};

const fmtWithMissing = (v, missing, digits = 2, suffix = '') => {
  if (missing) return '缺失';
  return fmt(v, digits, suffix);
};

const fmtIntWithMissing = (v, missing) => {
  if (missing) return '缺失';
  return fmtInt(v);
};

const toSignedPctText = (value, digits = 2) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return '-';
  return `${n >= 0 ? '+' : ''}${n.toFixed(digits)}%`;
};

const toSignedPctClass = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return 'text-slate-500';
  return n >= 0 ? 'text-red-400' : 'text-emerald-400';
};

const hasLeaderGroup = (name) => Boolean(getLeaderGroupByName(name));

const getLeaderGroupByName = (name) => {
  if (!name) return null;
  return (mainlineLeaders.value?.mainlines || []).find((item) => item.sector === name) || null;
};

const getLatestMainlinePoint = (name) => {
  if (!name) return null;
  const series = (mainlineTrendSeries.value || []).find((item) => item.name === name);
  if (!series?.data?.length) return null;

  for (let i = series.data.length - 1; i >= 0; i -= 1) {
    const point = series.data[i];
    if (Number(point?.value) > 0 || (Array.isArray(point?.top_stocks) && point.top_stocks.length)) {
      return point;
    }
  }
  return series.data[series.data.length - 1] || null;
};

const normalizeMainlineStocks = (stocks = []) => (
  (stocks || []).filter(Boolean).slice(0, 5).map((stock) => {
    const pct = Number.isFinite(Number(stock?.pct_chg))
      ? Number(stock.pct_chg)
      : Number.isFinite(Number(stock?.latest_pct))
        ? Number(stock.latest_pct)
        : null;
    const score = Number.isFinite(Number(stock?.score))
      ? Number(stock.score)
      : Number.isFinite(Number(stock?.leader_score))
        ? Number(stock.leader_score)
        : null;
    const activeDays = Number.isFinite(Number(stock?.active_days)) ? Number(stock.active_days) : null;

    let note = '';
    if (stock?.signal) {
      note = stock.signal;
    } else if (
      Number.isFinite(Number(stock?.sector_rank))
      && Number.isFinite(Number(stock?.sector_total))
    ) {
      note = `板块第${stock.sector_rank}/${stock.sector_total}`;
    } else if (Number.isFinite(Number(stock?.recent_active_days))) {
      note = `近5日活跃${Math.round(Number(stock.recent_active_days))}天`;
    }

    let badge = '';
    if (Number.isFinite(score)) {
      badge = `${Math.round(score)}分`;
    } else if (Number.isFinite(activeDays)) {
      badge = `活跃${Math.round(activeDays)}天`;
    }

    return {
      name: stock?.name || stock?.ts_code || '-',
      tsCode: stock?.ts_code || '',
      note,
      badge,
      changeText: Number.isFinite(pct) ? toSignedPctText(pct) : (badge || '-'),
      changeClass: Number.isFinite(pct) ? toSignedPctClass(pct) : 'text-slate-400',
    };
  })
);

const buildMainlineThesis = ({ labels = [], activeDays, consecutiveDays, limitUps, resonance, driverSummary = '' }) => {
  if (driverSummary) {
    if (consecutiveDays >= 3 || resonance >= 50) {
      return `${driverSummary}，优先看细分龙头分歧后的承接。`;
    }
    return `${driverSummary}，先确认这个细分是否还能继续扩散。`;
  }
  if (consecutiveDays >= 3 && resonance >= 55) {
    return '连续性和板块共振都在前排，优先看分歧后的承接。';
  }
  if (labels.includes('当前最强') && limitUps >= 3) {
    return '当前强度最靠前，先盯龙头能否继续维持溢价。';
  }
  if (consecutiveDays >= 3) {
    return '连续性已建立，适合跟踪分歧回踩后的确认点。';
  }
  if (activeDays >= 5 || resonance >= 50) {
    return '近10日反复活跃，但还要等盘中继续走出确认。';
  }
  return '当前仍在确认阶段，先观察龙头是否继续发酵。';
};

const buildMainlineEvidenceTags = ({ activeDays, consecutiveDays, limitUps, resonance, stockCount, focusTags = [] }) => {
  const tags = [];
  // 不再放入板块概念标签（已在标题展示），也不放涨停/连天（已移到header）
  return tags;
};

const buildMainlineQuickTags = ({ consecutiveDays, limitUps }) => {
  const tags = [];
  if (Number.isFinite(limitUps) && limitUps > 0) {
    tags.push(`涨停${Math.round(limitUps)}`);
  }
  if (Number.isFinite(consecutiveDays) && consecutiveDays >= 2) {
    tags.push(`连${Math.round(consecutiveDays)}天`);
  }
  return tags;
};

const buildFocusCard = (name, labels = [], reviewLine = null) => {
  if (!name) return null;

  const leaderGroup = getLeaderGroupByName(name);
  const latestPoint = getLatestMainlinePoint(name);
  const displayName = leaderGroup?.display_sector
    || reviewLine?.display_name
    || (currentMainline.value?.name === name ? currentMainline.value?.displayName : '')
    || name;
  const focusTags = Array.from(new Set([
    ...(reviewLine?.focus_tags || []),
    ...(leaderGroup?.focus_tags || []),
    ...((currentMainline.value?.name === name ? currentMainline.value?.focusTags : []) || []),
  ])).filter(Boolean);
  const driverSummary = leaderGroup?.driver_summary
    || reviewLine?.driver_summary
    || (currentMainline.value?.name === name ? currentMainline.value?.driverSummary : '')
    || '';
  const stocks = normalizeMainlineStocks(
    leaderGroup?.leaders?.length
      ? leaderGroup.leaders
      : reviewLine?.leaders?.length
        ? reviewLine.leaders
        : latestPoint?.top_stocks || []
  );

  const activeDays = Number.isFinite(Number(reviewLine?.active_days))
    ? Number(reviewLine.active_days)
    : Number.isFinite(Number(leaderGroup?.active_days))
      ? Number(leaderGroup.active_days)
      : null;
  const consecutiveDays = Number.isFinite(Number(reviewLine?.consecutive_days))
    ? Number(reviewLine.consecutive_days)
    : Number.isFinite(Number(leaderGroup?.consecutive_days))
      ? Number(leaderGroup.consecutive_days)
      : null;
  const limitUps = Number.isFinite(Number(leaderGroup?.limit_ups))
    ? Number(leaderGroup.limit_ups)
    : Number.isFinite(Number(reviewLine?.max_limit_ups))
      ? Number(reviewLine.max_limit_ups)
      : Number.isFinite(Number(latestPoint?.limit_ups))
        ? Number(latestPoint.limit_ups)
        : null;
  const stockCount = Number.isFinite(Number(leaderGroup?.stock_count))
    ? Number(leaderGroup.stock_count)
    : Number.isFinite(Number(reviewLine?.stock_count))
      ? Number(reviewLine.stock_count)
      : Number.isFinite(Number(latestPoint?.stock_count))
        ? Number(latestPoint.stock_count)
        : null;
  const score = Number.isFinite(Number(leaderGroup?.strength))
    ? Number(leaderGroup.strength)
    : Number.isFinite(Number(reviewLine?.latest_score))
      ? Number(reviewLine.latest_score)
      : Number.isFinite(Number(latestPoint?.value))
        ? Number(latestPoint.value)
        : Number.isFinite(Number(currentMainline.value?.score)) && currentMainline.value?.name === name
          ? Number(currentMainline.value.score)
          : 0;
  const resonance = Number.isFinite(Number(leaderGroup?.resonance))
    ? Number(leaderGroup.resonance)
    : null;

  return {
    name,
    displayName,
    labels: [...labels],
    thesis: buildMainlineThesis({ labels, activeDays, consecutiveDays, limitUps, resonance, driverSummary }),
    evidenceTags: buildMainlineEvidenceTags({ activeDays, consecutiveDays, limitUps, resonance, stockCount, focusTags }),
    quickTags: buildMainlineQuickTags({ consecutiveDays, limitUps }),
    focusTags,
    driverSummary,
    score,
    activeDays,
    consecutiveDays,
    limitUps,
    stockCount,
    resonance,
    stocks,
  };
};

const rebuildMainlineFocus = () => {
  const reviewLines = mainlineReview.value?.mainlines || [];
  const continuousLine = reviewLines[0] || null;
  const currentName = currentMainline.value?.name || (mainlineLeaders.value?.mainlines || [])[0]?.sector || '';
  const currentReview = reviewLines.find((item) => item.name === currentName) || null;

  mainlineOverview.value = {
    current: currentName
      ? {
          name: currentName,
          score: Number.isFinite(Number(currentMainline.value?.score))
            ? Number(currentMainline.value.score)
            : Number(currentReview?.latest_score) || 0,
          reason: currentMainline.value?.reason || '按最新交易日主线强度排序',
        }
      : null,
    continuous: continuousLine
      ? {
          name: continuousLine.name,
          score: Number.isFinite(Number(continuousLine.latest_score))
            ? Number(continuousLine.latest_score)
            : Number(continuousLine.avg_score) || 0,
          activeDays: Number(continuousLine.active_days) || 0,
          consecutiveDays: Number(continuousLine.consecutive_days) || 0,
        }
      : null,
  };

  const cards = new Map();
  const appendCard = (name, label, reviewLine = null) => {
    if (!name) return;
    if (cards.has(name)) {
      const existing = cards.get(name);
      existing.labels = Array.from(new Set([...existing.labels, label]));
      return;
    }
    const card = buildFocusCard(name, [label], reviewLine);
    if (card) {
      cards.set(name, card);
    }
  };

  appendCard(currentName, '当前最强', currentReview);
  appendCard(continuousLine?.name, '10日连续', continuousLine);
  (mainlineLeaders.value?.mainlines || []).slice(0, 4).forEach((item, index) => {
    appendCard(item?.sector, index === 0 ? '实时龙头' : '候选方向');
  });
  reviewLines.slice(0, 4).forEach((item, index) => {
    appendCard(item?.name, index === 0 ? '复盘重点' : '复盘跟踪', item);
  });

  mainlineFocusCards.value = Array.from(cards.values())
    .sort((a, b) => Number(b?.score || 0) - Number(a?.score || 0))
    .slice(0, 6);

  if (
    selectedMainlineName.value
    && !mainlineFocusCards.value.some((item) => item.name === selectedMainlineName.value)
  ) {
    selectedMainlineName.value = '';
  }

  refreshMainlineChart();
};

const updateViewport = () => {
  isMobile.value = window.innerWidth < 768;
  if (isMobile.value) {
    minimalTooltipMode.value = true;
  }
};

const marketSentimentChartOption = ref({
  grid: { top: 40, left: 40, right: 40, bottom: 45, containLabel: true },
  tooltip: { 
    trigger: 'axis', 
    axisPointer: { type: 'cross' },
    backgroundColor: 'rgba(15, 23, 42, 0.95)',
    borderColor: '#334155',
    textStyle: { color: '#cbd5e1' },
    confine: true
  },
  dataZoom: [
    {
      type: 'inside',
      minSpan: 8
    },
    {
      type: 'slider',
      height: 15,
      bottom: 5,
      borderColor: 'transparent',
      backgroundColor: 'rgba(30, 41, 59, 0.5)',
      fillerColor: 'rgba(51, 65, 85, 0.5)',
      handleStyle: { color: '#475569' },
      textStyle: { color: '#64748b', fontSize: 10 },
      minSpan: 8
    }
  ],
  xAxis: {
    type: 'category',
    boundaryGap: false,
    axisLine: { lineStyle: { color: '#334155' } },
    axisLabel: { color: '#64748b', fontSize: 10 }
  },
  legend: { textStyle: { color: '#94a3b8' }, top: 0 },
  yAxis: [
    { 
      type: 'value', 
      name: '情绪强度', 
      min: 0, 
      max: 100, 
      position: 'left',
      splitLine: { lineStyle: { color: '#334155', type: 'dashed' } }, 
      axisLine: { show: false },
      axisLabel: { color: '#f59e0b', fontSize: 9 },
      nameTextStyle: { color: '#f59e0b', fontSize: 9 }
    },
    { 
      type: 'value', 
      name: '指数', 
      position: 'right',
      show: true,
      splitLine: { show: false }, 
      axisLine: { lineStyle: { color: '#64748b', opacity: 0.5 } },
      axisLabel: { color: '#64748b', fontSize: 9 },
      nameTextStyle: { color: '#64748b', fontSize: 9 },
      scale: true 
    }
  ],
  series: []
});

const mainlineChartOption = ref({
  grid: { top: 24, left: 24, right: 12, bottom: 48, containLabel: true },
  tooltip: {},
  legend: { bottom: 0, textStyle: { color: '#64748b', fontSize: 10 } },
  xAxis: { type: 'category', data: [] },
  yAxis: { type: 'value' },
  series: []
});

const getMainlinePointValue = (point) => {
  if (typeof point === 'object' && point !== null) {
    return Number(point.value) || 0;
  }
  return Number(point) || 0;
};

const getLatestSeriesValue = (seriesData = []) => {
  for (let i = seriesData.length - 1; i >= 0; i -= 1) {
    const value = getMainlinePointValue(seriesData[i]);
    if (value > 0) {
      return value;
    }
  }
  return 0;
};

const refreshMainlineChart = () => {
  const dates = mainlineChartDates.value || [];
  const rawSeries = mainlineTrendSeries.value || [];

  if (!dates.length || !rawSeries.length) {
    mainlineChartOption.value = {
      ...mainlineChartOption.value,
      xAxis: { ...mainlineChartOption.value.xAxis, data: [] },
      series: [],
    };
    return;
  }

  const focusNames = focusedMainlineCards.value.map((item) => item.name).filter(Boolean);
  let selectedSeries = focusNames.length
    ? focusNames
      .map((name) => rawSeries.find((item) => item.name === name))
      .filter(Boolean)
    : [];

  if (!selectedSeries.length) {
    selectedSeries = [...rawSeries]
      .sort((a, b) => getLatestSeriesValue(b?.data || []) - getLatestSeriesValue(a?.data || []))
      .slice(0, 3);
  }

  mainlineChartOption.value = {
    backgroundColor: 'transparent',
    grid: { top: 24, left: 16, right: 12, bottom: 52, containLabel: true },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'line' },
      backgroundColor: 'rgba(15, 23, 42, 0.96)',
      borderColor: '#334155',
      textStyle: { color: '#cbd5e1', fontSize: 11 },
      padding: 10,
      confine: true,
      formatter: (params) => {
        const activeParams = (params || [])
          .filter((item) => getMainlinePointValue(item?.data ?? item?.value) > 0)
          .sort((a, b) => getMainlinePointValue(b?.data ?? b?.value) - getMainlinePointValue(a?.data ?? a?.value));

        if (!activeParams.length) return '';

        const date = activeParams[0].axisValue || activeParams[0].name || '-';
        const rows = activeParams.map((param) => {
          const point = param.data || {};
          const score = getMainlinePointValue(point);
          const stocks = Array.isArray(point.top_stocks) && point.top_stocks.length
            ? point.top_stocks
              .slice(0, 3)
              .map((stock) => {
                const pct = Number(stock?.pct_chg);
                return `${stock?.name || '-'}${Number.isFinite(pct) ? ` ${toSignedPctText(pct, 1)}` : ''}`;
              })
              .join('、')
            : '无龙头';

          return `
            <div style="margin-top:8px;">
              <div style="display:flex;justify-content:space-between;gap:8px;">
                <span style="color:${param.color};font-weight:700;">${param.seriesName}</span>
                <span style="color:#f8fafc;font-weight:700;">${score.toFixed(1)}</span>
              </div>
              <div style="margin-top:4px;color:#94a3b8;font-size:10px;line-height:1.5;">${stocks}</div>
            </div>
          `;
        }).join('');

        return `<div style="min-width:180px;"><div style="font-weight:700;color:#e2e8f0;">${date}</div>${rows}</div>`;
      },
    },
    legend: {
      bottom: 0,
      icon: 'circle',
      itemWidth: 8,
      itemHeight: 8,
      textStyle: { color: '#64748b', fontSize: 10 },
    },
    xAxis: {
      type: 'category',
      data: dates,
      boundaryGap: false,
      axisLine: { lineStyle: { color: '#334155' } },
      axisLabel: { color: '#64748b', fontSize: 10, rotate: 30 },
    },
    yAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: '#1e293b', type: 'dashed' } },
      axisLine: { show: false },
      axisLabel: { color: '#64748b', fontSize: 10 },
    },
    series: selectedSeries.map((series, index) => ({
      name: series.name,
      type: 'line',
      smooth: true,
      showSymbol: false,
      connectNulls: true,
      lineStyle: {
        width: 3,
        color: MAINLINE_CHART_COLORS[index % MAINLINE_CHART_COLORS.length],
      },
      areaStyle: {
        color: MAINLINE_CHART_COLORS[index % MAINLINE_CHART_COLORS.length],
        opacity: 0.08,
      },
      itemStyle: {
        color: MAINLINE_CHART_COLORS[index % MAINLINE_CHART_COLORS.length],
      },
      data: (series.data || []).map((point) => {
        if (typeof point === 'object' && point !== null) {
          return {
            value: Number(point.value) || 0,
            top_stocks: point.top_stocks || [],
            limit_ups: point.limit_ups,
            stock_count: point.stock_count,
          };
        }
        return {
          value: Number(point) || 0,
          top_stocks: [],
        };
      }),
    })),
  };
};

const fetchMainlineHistory = async () => {
  loadingTrend.value = true;
  try {
    const res = await getMainlineHistory(10);
    const data = res.data.status === 'success' ? res.data.data : res.data;
    const { dates, series, analysis } = data;
    mainlineChartDates.value = Array.isArray(dates) ? dates : [];
    mainlineTrendSeries.value = Array.isArray(series) ? series : [];

    if (analysis && analysis.deduction) {
      trendAnalysis.value = analysis.deduction;
    } else {
      trendAnalysis.value = "当前周期内无明显主线推演结论。";
    }
    if (analysis?.top_mainline) {
      currentMainline.value = {
        name: analysis.top_mainline.name || '',
        displayName: analysis.top_mainline.display_name || analysis.top_mainline.name || '',
        score: analysis.top_mainline.score || 0,
        reason: analysis.top_mainline.reason || '',
        focusTags: analysis.top_mainline.focus_tags || [],
        driverSummary: analysis.top_mainline.driver_summary || '',
      };
    } else {
      currentMainline.value = { name: '', displayName: '', score: 0, reason: '', focusTags: [], driverSummary: '' };
    }
    mainlineReview.value = analysis?.review_10d || null;
    
    rebuildMainlineFocus();
  } catch (e) {
    console.error("History Error", e);
    mainlineChartDates.value = [];
    mainlineTrendSeries.value = [];
    mainlineReview.value = null;
    currentMainline.value = { name: '', displayName: '', score: 0, reason: '', focusTags: [], driverSummary: '' };
    mainlineFocusCards.value = [];
    mainlineOverview.value = { current: null, continuous: null };
    refreshMainlineChart();
  } finally {
    loadingTrend.value = false;
  }
};

const clearSentimentRefreshTimer = () => {
  if (sentimentRefreshTimer) {
    window.clearTimeout(sentimentRefreshTimer);
    sentimentRefreshTimer = null;
  }
};

const scheduleSentimentRefresh = () => {
  clearSentimentRefreshTimer();
  const seconds = Number(sentimentAutomation.value?.dashboard_refresh_seconds);
  const nextSeconds = Number.isFinite(seconds) && seconds > 0 ? seconds : 60;
  sentimentRefreshTimer = window.setTimeout(() => {
    void fetchMarketSentiment();
  }, nextSeconds * 1000);
};

const fetchMarketSentiment = async () => {
  clearSentimentRefreshTimer();
  try {
    const res = await getMarketSentiment(250); // 获取约1年交易日数据
    const data = res.data.data || {}; // { dates, sentiment, index, live, automation }
    sentimentBoard.value = {
      live: data.live || null,
      automation: data.automation || {},
    };

    if (!data.dates || data.dates.length === 0) {
      latestSentimentDate.value = '';
      latestSentiment.value = null;
      recentSentiments.value = [];
      return;
    }
    latestSentimentDate.value = data.dates[data.dates.length - 1];
    latestSentiment.value = data.sentiment[data.sentiment.length - 1] || null;
    recentSentiments.value = data.sentiment.slice(-60);
    
    
    // 默认显示最近一个月 (约22个交易日)
    const totalPoints = data.dates.length;
    const startValue = Math.max(0, totalPoints - 22);
    
    // 处理标记点
    const markPoints = [];
    data.sentiment.forEach((item, i) => {
      const details = item.details || {};
      const signal = details.signal;
      const val = item.value;
      
      if (signal && signal.includes('BUY')) {
        markPoints.push({
          name: '买入',
          coord: [i, val],
          value: '买',
          itemStyle: { color: '#ef4444', shadowBlur: 10, shadowColor: '#ef4444' }
        });
      } else if (signal && signal.includes('SELL')) {
        markPoints.push({
          name: '卖出',
          coord: [i, val],
          value: '卖',
          symbolOffset: [0, -10],
          itemStyle: { color: '#10b981', shadowBlur: 10, shadowColor: '#10b981' }
        });
      } else if (signal === 'STAY_OUT') {
        markPoints.push({
          name: '空仓',
          coord: [i, val],
          value: '空',
          symbol: 'rect',
          symbolSize: [20, 15],
          itemStyle: { color: '#64748b' }
        });
      }
    });

    const indexSeries = data.dates.map((_, i) => {
      const raw = data.index?.[i];
      if (raw === null || raw === undefined || raw === '') return null;
      const val = Number(raw);
      return Number.isFinite(val) ? val : null;
    });
    const indexPctSeries = indexSeries.map((curr, i) => {
      if (!Number.isFinite(curr) || i === 0 || !Number.isFinite(indexSeries[i - 1]) || indexSeries[i - 1] === 0) {
        return null;
      }
      return ((curr - indexSeries[i - 1]) / indexSeries[i - 1]) * 100;
    });

    marketSentimentChartOption.value = {
      ...marketSentimentChartOption.value,
      tooltip: {
        ...marketSentimentChartOption.value.tooltip,
        formatter: (params) => {
          let html = `<div class="p-2 font-mono text-xs min-w-[220px]">`;
          params.forEach(p => {
            if (p.seriesName === '情绪评分') {
              const compact = minimalTooltipMode.value || isMobile.value;
              const val = typeof p.value === 'object' ? p.value.value : p.value;
              const details = p.data?.details || {};
              const signal = details.signal || 'HOLD';
              const f = details.factors || {};
              const m = details.metrics || {};
              const mf = details.missing?.factors || {};
              const idx = Number.isFinite(p.dataIndex) ? p.dataIndex : -1;
              const liveIndexChg = idx >= 0 ? indexPctSeries[idx] : null;
              const indexChgDisplay = Number.isFinite(liveIndexChg) ? liveIndexChg : Number(f.index_chg);
              
              // 优先使用数据项中的 label (中文)
              const displayLabel = p.data?.label || details.label || signal;
              
              html += `<div class="mb-2 border-b border-slate-700 pb-1 flex justify-between">
                        <span class="text-slate-400">${p.name} 指标</span>
                        <span class="font-bold text-amber-500 text-sm">${fmt(val, 1)}</span>
                      </div>`;
              
              html += `<div class="mb-2 px-2 py-1 bg-slate-800 rounded flex justify-between items-center">
                        <span class="text-[10px] text-slate-500">操作建议</span>
                        <span class="font-bold ${signal.includes('BUY') ? 'text-rose-500' : signal.includes('SELL') ? 'text-emerald-500' : 'text-slate-300'}">${displayLabel}</span>
                      </div>`;

              if (compact) {
                html += `<div class="text-[10px] text-slate-300 space-y-1">
                          <div class="flex justify-between"><span class="text-slate-500">赚钱效应</span><span>${fmtWithMissing(f.breadth, mf.breadth, 1, '%')}</span></div>
                          <div class="flex justify-between"><span class="text-slate-500">中位涨幅</span><span>${fmtWithMissing(f.median_chg, mf.median_chg, 1, '%')}</span></div>
                          <div class="flex justify-between"><span class="text-slate-500">动量(v1/v2)</span><span>${fmt(m.v1, 1)} / ${fmt(m.v2, 1)}</span></div>
                          <div class="flex justify-between"><span class="text-slate-500">策略</span><span>${m.advice || '等待机会'}</span></div>
                        </div>`;
                return;
              }

              html += `<div class="text-[10px] space-y-2">
                        <div class="rounded bg-slate-800/70 px-2 py-1">
                          <div class="text-slate-500 mb-1">交易行为</div>
                          <div class="grid grid-cols-2 gap-x-2 gap-y-1 text-slate-300">
                            <div>涨停数: ${fmtIntWithMissing(f.limit, mf.limit)}</div>
                            <div>炸板数: ${fmtIntWithMissing(f.failure, mf.failure)}</div>
                            <div>涨跌停比: ${fmtWithMissing(f.limit_up_down_ratio, mf.limit_up_down_ratio, 2)}</div>
                            <div>连板高度: ${fmtIntWithMissing(f.max_limit_up_streak, mf.max_limit_up_streak)}</div>
                          </div>
                        </div>
                        <div class="rounded bg-slate-800/70 px-2 py-1">
                          <div class="text-slate-500 mb-1">市场结构</div>
                          <div class="grid grid-cols-2 gap-x-2 gap-y-1 text-slate-300">
                            <div>赚钱效应: ${fmtWithMissing(f.breadth, mf.breadth, 1, '%')}</div>
                            <div>中位涨幅: ${fmtWithMissing(f.median_chg, mf.median_chg, 1, '%')}</div>
                            <div>新高/新低: ${fmtWithMissing(f.new_high_low_ratio, mf.new_high_low_ratio, 2)}</div>
                            <div>量能活跃: ${fmtWithMissing(f.turnover_activity, mf.turnover_activity, 3)}</div>
                          </div>
                        </div>
                        <div class="rounded bg-slate-800/70 px-2 py-1">
                          <div class="text-slate-500 mb-1">波动预期</div>
                          <div class="grid grid-cols-2 gap-x-2 gap-y-1 text-slate-300">
                            <div>指数涨幅: ${fmtWithMissing(indexChgDisplay, mf.index_chg, 2, '%')}</div>
                            <div>波动代理Z: ${fmtWithMissing(f.iv_proxy_z, mf.iv_proxy_z, 2)}</div>
                          </div>
                        </div>
                      </div>`;

              html += `<div class="mt-2 text-[10px] text-slate-400 border-t border-slate-800 pt-2 space-y-1">
                        <div class="flex justify-between"><span>策略:</span><span class="text-slate-200">${m.advice || '等待机会'}</span></div>
                        <div class="flex justify-between"><span>动量 v1:</span><span class="${Number(m.v1) > 0 ? 'text-rose-400' : 'text-emerald-400'}">${fmt(m.v1, 1)}</span></div>
                        <div class="flex justify-between"><span>加速度 v2:</span><span class="${Number(m.v2) > 0 ? 'text-rose-400' : 'text-emerald-400'}">${fmt(m.v2, 1)}</span></div>
                        <div class="flex justify-between"><span>情绪MA5:</span><span>${fmt(m.score_ma5, 1)}</span></div>
                      </div>`;
            }
          });
          html += `</div>`;
          return html;
        }
      },
      xAxis: {
        ...marketSentimentChartOption.value.xAxis,
        data: data.dates
      },
      yAxis: marketSentimentChartOption.value.yAxis,
      dataZoom: [
        { ...marketSentimentChartOption.value.dataZoom[0], startValue: startValue, endValue: totalPoints - 1 },
        { ...marketSentimentChartOption.value.dataZoom[1], startValue: startValue, endValue: totalPoints - 1, show: !isMobile.value }
      ],
      series: [
        {
          name: '情绪评分',
          type: 'line',
          smooth: true,
          showSymbol: false,
          data: data.sentiment.map(item => ({
            value: item.value,
            label: item.label,
            details: item.details
          })),
          lineStyle: { color: '#f59e0b', width: 3 },
          areaStyle: {
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(245, 158, 11, 0.3)' },
                { offset: 1, color: 'rgba(245, 158, 11, 0)' }
              ]
            }
          },
          markPoint: {
            symbol: 'pin',
            symbolSize: 25,
            data: markPoints,
            label: { fontSize: 9, fontWeight: 'bold', color: '#fff' }
          },
          markLine: {
            symbol: ['none', 'none'],
            label: { 
              show: true, 
              position: 'end', 
              formatter: '{b}', 
              fontSize: 8,
              color: 'inherit'
            },
            data: [
              { name: '沸腾', yAxis: 85, lineStyle: { color: '#ef4444', type: 'dashed', opacity: 0.5 } },
              { name: '冰点', yAxis: 25, lineStyle: { color: '#3b82f6', type: 'dashed', opacity: 0.5 } }
            ]
          }
        },
        {
          name: '上证指数',
          type: 'line',
          yAxisIndex: 1,
          smooth: false,
          showSymbol: false,
          connectNulls: true,
          data: indexSeries,
          z: 1,
          lineStyle: { color: '#94a3b8', width: 2, type: 'solid', opacity: 0.9 }
        }
      ]
    };
  } catch (e) {
    console.error("Sentiment Error", e);
  } finally {
    scheduleSentimentRefresh();
  }
};

// 加载主线龙头推荐
const fetchMainlineLeaders = async (sector = '') => {
  loadingLeaders.value = true;
  try {
    const params = { limit: 5, min_score: 60 };
    if (sector) {
      params.sector = sector;
    }
    const res = await getMainlineLeaders(params);
    if (res.data.status === 'success') {
      mergeMainlineLeaders(res.data);
    } else {
      if (!mainlineLeaders.value?.mainlines?.length) {
        mainlineLeaders.value = null;
      }
    }
    rebuildMainlineFocus();
  } catch (e) {
    console.error('加载主线龙头失败', e);
    if (!mainlineLeaders.value?.mainlines?.length) {
      mainlineLeaders.value = null;
    }
    rebuildMainlineFocus();
  } finally {
    loadingLeaders.value = false;
  }
};

onMounted(async () => {
  updateViewport();
  window.addEventListener('resize', updateViewport);
  void fetchMainlineHistory();
  void fetchMarketSentiment();
});

onBeforeUnmount(() => {
  window.removeEventListener('resize', updateViewport);
  clearSentimentRefreshTimer();
});
</script>

<style scoped>
.dashboard-panel {
  position: relative;
}

.dashboard-panel::before {
  content: '';
  position: absolute;
  inset: 0;
  pointer-events: none;
  opacity: 0.9;
}

.panel-warm::before {
  background: radial-gradient(circle at top left, rgba(245, 158, 11, 0.08), transparent 38%);
}

.panel-cool::before {
  background: radial-gradient(circle at top right, rgba(6, 182, 212, 0.08), transparent 38%);
}

::-webkit-scrollbar {
  width: 4px;
}
::-webkit-scrollbar-track {
  background: transparent;
}
::-webkit-scrollbar-thumb {
  background: #334155;
  border-radius: 10px;
}
::-webkit-scrollbar-thumb:hover {
  background: #475569;
}
</style>
