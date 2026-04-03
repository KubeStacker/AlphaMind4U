<template>
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
                role="button" tabindex="0"
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
                      <span class="rounded-full border px-1.5 py-0.5 text-[8px] font-bold md:text-[9px]" :class="sector.roleClass">
                        {{ sector.roleLabel }}
                      </span>
                      <span
                        v-for="qt in sector.quickTags" :key="`qt-${sector.name}-${qt}`"
                        class="rounded-full border px-1.5 py-0.5 text-[8px] font-bold md:text-[9px]" :class="sector.roleClass"
                      >{{ qt }}</span>
                      <span class="text-[11px] font-black text-white md:hidden">{{ fmt(sector.score, 1) }}</span>
                    </div>

                    <div class="mt-0.5 text-[11px] leading-4 text-slate-400 md:mt-1 md:text-[12px] md:leading-5 md:text-slate-300 line-clamp-1 md:line-clamp-none">{{ sector.thesis }}</div>
                    <div v-if="sector.evidenceTags?.length" class="mt-1 flex flex-wrap gap-1 md:gap-1.5">
                      <span v-for="tag in sector.evidenceTags" :key="`${sector.name}-${tag}`"
                        class="rounded-full border border-business-light/70 bg-business-darker/70 px-1.5 py-0.5 text-[8px] text-slate-400 md:px-2 md:text-[9px]">{{ tag }}</span>
                    </div>
                  </div>

                  <div class="hidden shrink-0 text-right md:block">
                    <div class="text-lg font-black text-white">{{ fmt(sector.score, 1) }}</div>
                    <button @click.stop="toggleMainlineFocus(sector.name)"
                      class="mt-1 inline-flex h-6 min-w-[68px] items-center justify-center rounded-md border border-business-light bg-business-darker/80 px-2 text-[9px] font-bold text-slate-300 transition-all hover:border-business-highlight hover:text-white">
                      {{ selectedMainlineName === sector.name ? '收起' : '展开' }}
                    </button>
                  </div>
                </div>

                <div v-if="selectedMainlineName === sector.name" class="mt-3 rounded-lg border border-business-light/80 bg-business-darker/65 p-2.5">
                  <div class="flex items-start justify-between gap-2">
                    <div>
                      <p class="text-[9px] font-bold uppercase tracking-[0.18em] text-slate-500">龙头样本</p>
                      <p class="mt-1 text-[10px] leading-4 text-slate-500">{{ getMainlineLeaderCaption(sector) }}</p>
                    </div>
                    <span class="rounded-full border border-business-light/80 bg-slate-950/45 px-2 py-1 text-[9px] font-bold text-slate-300">
                      {{ sector.stocks?.length || 0 }} 只
                    </span>
                  </div>

                  <div v-if="loadingLeaders && !hasLeaderGroup(sector.name)"
                    class="mt-2 rounded-lg border border-business-light/60 bg-slate-950/30 px-2 py-1.5 text-[10px] text-slate-500">
                    补充龙头强度中，先展示主线历史样本。
                  </div>

                  <div v-if="sector.stocks?.length" class="mt-2.5 grid gap-1.5 sm:grid-cols-2">
                    <div v-for="(stock, sidx) in sector.stocks" :key="`${sector.name}-${stock.tsCode || stock.name}-${sidx}`"
                      class="rounded-lg border border-business-light/70 bg-slate-950/45 px-2.5 py-2">
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

                  <div v-else class="mt-2.5 flex min-h-[72px] items-center justify-center rounded-lg border border-business-light/70 bg-slate-950/35 px-3 text-[11px] text-slate-500">
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
              <v-chart v-if="!loadingTrend && mainlineChartOption.series.length > 0" :option="mainlineChartOption" autoresize />
              <div v-if="loadingTrend" class="absolute inset-0 flex items-center justify-center">
                <div class="h-8 w-8 animate-spin rounded-full border-2 border-business-accent border-t-transparent"></div>
              </div>
              <div v-if="!loadingTrend && mainlineChartOption.series.length === 0"
                class="absolute inset-0 flex flex-col items-center justify-center space-y-2 text-slate-500">
                <p class="text-xs font-bold uppercase tracking-[0.24em]">暂无近10日主线数据</p>
              </div>
            </div>
          </div>
        </div>
      </template>
    </div>
  </section>
</template>

<script setup>
import { computed, ref, onMounted, watch } from 'vue';
import { getMainlineHistory, getMainlineLeaders } from '@/services/api';

const loadingTrend = ref(false);
const trendAnalysis = ref('');
const mainlineChartDates = ref([]);
const selectedMainlineName = ref('');
const currentMainline = ref({ name: '', displayName: '', score: 0, reason: '', focusTags: [], driverSummary: '' });
const mainlineReview = ref(null);
const mainlineTrendSeries = ref([]);
const mainlineOverview = ref({ current: null, continuous: null });
const mainlineFocusCards = ref([]);
const mainlineLeaders = ref(null);
const loadingLeaders = ref(false);
const MAINLINE_CHART_COLORS = ['#3b82f6', '#06b6d4', '#f59e0b'];

const mainlineChartOption = ref({
  grid: { top: 24, left: 24, right: 12, bottom: 48, containLabel: true },
  tooltip: {},
  legend: { bottom: 0, textStyle: { color: '#64748b', fontSize: 10 } },
  xAxis: { type: 'category', data: [] },
  yAxis: { type: 'value' },
  series: []
});

const getMainlineHeat = (score) => {
  if (score >= 35) return { label: '高热', heatClass: 'border-rose-400/30 bg-rose-400/10 text-rose-200', progressClass: 'bg-gradient-to-r from-rose-500 via-amber-400 to-yellow-300', surfaceClass: 'border-rose-400/15 bg-rose-500/6' };
  if (score >= 25) return { label: '升温', heatClass: 'border-amber-400/30 bg-amber-400/10 text-amber-200', progressClass: 'bg-gradient-to-r from-amber-500 via-orange-400 to-cyan-300', surfaceClass: 'border-amber-400/15 bg-amber-500/6' };
  if (score >= 15) return { label: '观察', heatClass: 'border-cyan-400/30 bg-cyan-400/10 text-cyan-200', progressClass: 'bg-gradient-to-r from-cyan-500 via-sky-400 to-emerald-300', surfaceClass: 'border-cyan-400/15 bg-cyan-500/6' };
  return { label: '试探', heatClass: 'border-slate-500/40 bg-slate-500/10 text-slate-200', progressClass: 'bg-gradient-to-r from-slate-500 via-slate-400 to-slate-300', surfaceClass: 'border-white/8 bg-white/[0.02]' };
};

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
  if (consecutiveDays < 2 && activeDays < 3 && score < 20) priority -= 12;
  return priority;
};

const getMainlineRole = (sector, index) => {
  const consecutiveDays = Number(sector?.consecutiveDays) || 0;
  const limitUps = Number(sector?.limitUps) || 0;
  const activeDays = Number(sector?.activeDays) || 0;
  const labels = sector?.labels || [];
  if (index === 0 || (labels.includes('当前最强') && (consecutiveDays >= 3 || limitUps >= 3))) return { label: '主盯', className: 'border-business-accent/30 bg-business-accent/10 text-business-accent', note: '满足强度与持续性，优先深看' };
  if (consecutiveDays >= 3 || labels.includes('10日连续')) return { label: '次盯', className: 'border-business-highlight/30 bg-business-highlight/10 text-business-highlight', note: '连续性更好，适合跟踪承接' };
  if (activeDays >= 2 || limitUps >= 2 || labels.includes('实时龙头')) return { label: '备选', className: 'border-amber-400/30 bg-amber-400/10 text-amber-200', note: '有强度，但还要等连续性确认' };
  return { label: '观察', className: 'border-slate-500/40 bg-slate-500/10 text-slate-300', note: '先看是否能走出连续性' };
};

const getMainlinePanelClass = (roleLabel) => {
  if (roleLabel === '主盯') return 'bg-business-accent/[0.05]';
  if (roleLabel === '次盯') return 'bg-business-highlight/[0.05]';
  if (roleLabel === '备选') return 'bg-amber-500/[0.05]';
  return 'bg-slate-900/20';
};

const getMainlineExecutionBias = (sector) => {
  const consecutiveDays = Number(sector?.consecutiveDays) || 0;
  const limitUps = Number(sector?.limitUps) || 0;
  const activeDays = Number(sector?.activeDays) || 0;
  const resonance = Number(sector?.resonance) || 0;
  if (consecutiveDays >= 3 && resonance >= 55) return '连续性和板块共振都在前排，分歧后的承接更值得跟。';
  if (consecutiveDays >= 3) return '连续性已经达标，优先看分歧回踩后的承接。';
  if (limitUps >= 3 || activeDays >= 5 || resonance >= 50) return '强度已进入观察名单，但还要确认能否继续走出持续性。';
  return '当前更偏预备方向，先看龙头是否能维持强度。';
};

const mainlinePriorityCards = computed(() =>
  (mainlineFocusCards.value || [])
    .map((sector) => ({ ...sector, ...getMainlineHeat(Number(sector?.score)), priorityScore: getMainlinePriorityScore(sector) }))
    .sort((a, b) => Number(b?.priorityScore || 0) - Number(a?.priorityScore || 0))
);

const focusedMainlineCards = computed(() =>
  mainlinePriorityCards.value.slice(0, 3).map((sector, index) => {
    const role = getMainlineRole(sector, index);
    return { ...sector, displayRank: index + 1, panelClass: getMainlinePanelClass(role.label), roleLabel: role.label, roleClass: role.className, thesis: sector.thesis || getMainlineExecutionBias(sector) };
  })
);

const normalizeSentenceText = (text = '') => text.replace(/[\s，。；：、""''（）()【】\[\]\-]/g, '');

const pickDistinctAnalysisSentence = (text, compareTexts = []) => {
  const sentences = `${text || ''}`.split(/[。；！!\n]/).map((item) => item.trim()).filter(Boolean);
  const comparePool = compareTexts.map((item) => normalizeSentenceText(item)).filter(Boolean);
  return sentences.find((sentence) => { const n = normalizeSentenceText(sentence); return n.length >= 12 && !comparePool.some((item) => item.includes(n) || n.includes(item)); }) || '';
};

const mainlineHeadline = computed(() => {
  const primary = focusedMainlineCards.value[0];
  const secondary = focusedMainlineCards.value[1];
  const selected = mainlinePriorityCards.value.find((item) => item.name === selectedMainlineName.value);
  if (!primary) return '主线暂未收束，先控制试错。';
  const parts = [];
  if (selected?.name && selected.name !== primary.name) parts.push(`当前展开 ${selected.name} 只是保留轮动备份，不建议和 ${primary.name} 平均分配仓位`);
  else parts.push(`先盯 ${primary.name}，龙头承接没丢前不需要频繁切换观察中心`);
  if (secondary?.name && secondary.name !== primary.name) parts.push(`只有在 ${primary.name} 承接转弱且 ${secondary.name} 强度继续抬升时，再把注意力切到 ${secondary.name}`);
  const distinctInsight = pickDistinctAnalysisSentence(trendAnalysis.value, parts);
  if (distinctInsight) parts.push(distinctInsight);
  return `${parts.join('；')}。`;
});

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
  if (sector.roleLabel === '主盯') return '优先看龙头承接是否稳定，不需要额外拆一块固定席位。';
  return '作为轮动备份观察，确认强度后再切换注意力。';
};

const mergeMainlineLeaders = (payload) => {
  const current = Array.isArray(mainlineLeaders.value?.mainlines) ? mainlineLeaders.value.mainlines : [];
  const incoming = Array.isArray(payload?.mainlines) ? payload.mainlines : [];
  const merged = new Map();
  current.forEach((item) => { if (item?.sector) merged.set(item.sector, item); });
  incoming.forEach((item) => { if (item?.sector) merged.set(item.sector, item); });
  mainlineLeaders.value = { ...(mainlineLeaders.value || {}), ...(payload || {}), mainlines: Array.from(merged.values()) };
};

const toggleMainlineFocus = (name) => { if (!name) return; selectedMainlineName.value = selectedMainlineName.value === name ? '' : name; };

const hasLeaderGroup = (name) => Boolean(getLeaderGroupByName(name));
const getLeaderGroupByName = (name) => { if (!name) return null; return (mainlineLeaders.value?.mainlines || []).find((item) => item.sector === name) || null; };

const getLatestMainlinePoint = (name) => {
  if (!name) return null;
  const series = (mainlineTrendSeries.value || []).find((item) => item.name === name);
  if (!series?.data?.length) return null;
  for (let i = series.data.length - 1; i >= 0; i -= 1) { const p = series.data[i]; if (Number(p?.value) > 0 || (Array.isArray(p?.top_stocks) && p.top_stocks.length)) return p; }
  return series.data[series.data.length - 1] || null;
};

const normalizeMainlineStocks = (stocks = []) => (
  (stocks || []).filter(Boolean).slice(0, 5).map((stock) => {
    const pct = Number.isFinite(Number(stock?.pct_chg)) ? Number(stock.pct_chg) : Number.isFinite(Number(stock?.latest_pct)) ? Number(stock.latest_pct) : null;
    const score = Number.isFinite(Number(stock?.score)) ? Number(stock.score) : Number.isFinite(Number(stock?.leader_score)) ? Number(stock.leader_score) : null;
    const activeDays = Number.isFinite(Number(stock?.active_days)) ? Number(stock.active_days) : null;
    let note = '';
    if (stock?.signal) note = stock.signal;
    else if (Number.isFinite(Number(stock?.sector_rank)) && Number.isFinite(Number(stock?.sector_total))) note = `板块第${stock.sector_rank}/${stock.sector_total}`;
    else if (Number.isFinite(Number(stock?.recent_active_days))) note = `近5日活跃${Math.round(Number(stock.recent_active_days))}天`;
    let badge = '';
    if (Number.isFinite(score)) badge = `${Math.round(score)}分`;
    else if (Number.isFinite(activeDays)) badge = `活跃${Math.round(activeDays)}天`;
    return { name: stock?.name || stock?.ts_code || '-', tsCode: stock?.ts_code || '', note, badge, changeText: Number.isFinite(pct) ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%` : (badge || '-'), changeClass: Number.isFinite(pct) ? (pct >= 0 ? 'text-red-400' : 'text-emerald-400') : 'text-slate-400' };
  })
);

const buildMainlineThesis = ({ labels = [], activeDays, consecutiveDays, limitUps, resonance, driverSummary = '' }) => {
  if (driverSummary) { if (consecutiveDays >= 3 || resonance >= 50) return `${driverSummary}，优先看细分龙头分歧后的承接。`; return `${driverSummary}，先确认这个细分是否还能继续扩散。`; }
  if (consecutiveDays >= 3 && resonance >= 55) return '连续性和板块共振都在前排，优先看分歧后的承接。';
  if (labels.includes('当前最强') && limitUps >= 3) return '当前强度最靠前，先盯龙头能否继续维持溢价。';
  if (consecutiveDays >= 3) return '连续性已建立，适合跟踪分歧回踩后的确认点。';
  if (activeDays >= 5 || resonance >= 50) return '近10日反复活跃，但还要等盘中继续走出确认。';
  return '当前仍在确认阶段，先观察龙头是否继续发酵。';
};

const buildMainlineQuickTags = ({ consecutiveDays, limitUps }) => {
  const tags = [];
  if (Number.isFinite(limitUps) && limitUps > 0) tags.push(`涨停${Math.round(limitUps)}`);
  if (Number.isFinite(consecutiveDays) && consecutiveDays >= 2) tags.push(`连${Math.round(consecutiveDays)}天`);
  return tags;
};

const buildFocusCard = (name, labels = [], reviewLine = null) => {
  if (!name) return null;
  const leaderGroup = getLeaderGroupByName(name);
  const latestPoint = getLatestMainlinePoint(name);
  const displayName = leaderGroup?.display_sector || reviewLine?.display_name || name;
  const focusTags = Array.from(new Set([...(reviewLine?.focus_tags || []), ...(leaderGroup?.focus_tags || [])])).filter(Boolean);
  const driverSummary = leaderGroup?.driver_summary || reviewLine?.driver_summary || '';
  const stocks = normalizeMainlineStocks(leaderGroup?.leaders?.length ? leaderGroup.leaders : reviewLine?.leaders?.length ? reviewLine.leaders : latestPoint?.top_stocks || []);
  const activeDays = Number.isFinite(Number(reviewLine?.active_days)) ? Number(reviewLine.active_days) : Number.isFinite(Number(leaderGroup?.active_days)) ? Number(leaderGroup.active_days) : null;
  const consecutiveDays = Number.isFinite(Number(reviewLine?.consecutive_days)) ? Number(reviewLine.consecutive_days) : Number.isFinite(Number(leaderGroup?.consecutive_days)) ? Number(leaderGroup.consecutive_days) : null;
  const limitUps = Number.isFinite(Number(leaderGroup?.limit_ups)) ? Number(leaderGroup.limit_ups) : Number.isFinite(Number(reviewLine?.max_limit_ups)) ? Number(reviewLine.max_limit_ups) : Number.isFinite(Number(latestPoint?.limit_ups)) ? Number(latestPoint.limit_ups) : null;
  const stockCount = Number.isFinite(Number(leaderGroup?.stock_count)) ? Number(leaderGroup.stock_count) : Number.isFinite(Number(reviewLine?.stock_count)) ? Number(reviewLine.stock_count) : Number.isFinite(Number(latestPoint?.stock_count)) ? Number(latestPoint.stock_count) : null;
  const score = Number.isFinite(Number(leaderGroup?.strength)) ? Number(leaderGroup.strength) : Number.isFinite(Number(reviewLine?.latest_score)) ? Number(reviewLine.latest_score) : Number.isFinite(Number(latestPoint?.value)) ? Number(latestPoint.value) : 0;
  const resonance = Number.isFinite(Number(leaderGroup?.resonance)) ? Number(leaderGroup.resonance) : null;
  return { name, displayName, labels: [...labels], thesis: buildMainlineThesis({ labels, activeDays, consecutiveDays, limitUps, resonance, driverSummary }), evidenceTags: [], quickTags: buildMainlineQuickTags({ consecutiveDays, limitUps }), focusTags, driverSummary, score, activeDays, consecutiveDays, limitUps, stockCount, resonance, stocks };
};

const rebuildMainlineFocus = () => {
  const reviewLines = mainlineReview.value?.mainlines || [];
  const continuousLine = reviewLines[0] || null;
  const currentName = currentMainline.value?.name || (mainlineLeaders.value?.mainlines || [])[0]?.sector || '';
  const currentReview = reviewLines.find((item) => item.name === currentName) || null;
  mainlineOverview.value = {
    current: currentName ? { name: currentName, score: Number.isFinite(Number(currentMainline.value?.score)) ? Number(currentMainline.value.score) : Number(currentReview?.latest_score) || 0, reason: currentMainline.value?.reason || '按最新交易日主线强度排序' } : null,
    continuous: continuousLine ? { name: continuousLine.name, score: Number.isFinite(Number(continuousLine.latest_score)) ? Number(continuousLine.latest_score) : Number(continuousLine.avg_score) || 0, activeDays: Number(continuousLine.active_days) || 0, consecutiveDays: Number(continuousLine.consecutive_days) || 0 } : null,
  };
  const cards = new Map();
  const appendCard = (name, label, reviewLine = null) => {
    if (!name) return;
    if (cards.has(name)) { cards.get(name).labels = Array.from(new Set([...cards.get(name).labels, label])); return; }
    const card = buildFocusCard(name, [label], reviewLine);
    if (card) cards.set(name, card);
  };
  appendCard(currentName, '当前最强', currentReview);
  appendCard(continuousLine?.name, '10日连续', continuousLine);
  (mainlineLeaders.value?.mainlines || []).slice(0, 4).forEach((item, index) => { appendCard(item?.sector, index === 0 ? '实时龙头' : '候选方向'); });
  reviewLines.slice(0, 4).forEach((item, index) => { appendCard(item?.name, index === 0 ? '复盘重点' : '复盘跟踪', item); });
  mainlineFocusCards.value = Array.from(cards.values()).sort((a, b) => Number(b?.score || 0) - Number(a?.score || 0)).slice(0, 6);
  if (selectedMainlineName.value && !mainlineFocusCards.value.some((item) => item.name === selectedMainlineName.value)) selectedMainlineName.value = '';
  refreshMainlineChart();
};

const getMainlinePointValue = (point) => { if (typeof point === 'object' && point !== null) return Number(point.value) || 0; return Number(point) || 0; };
const getLatestSeriesValue = (seriesData = []) => { for (let i = seriesData.length - 1; i >= 0; i -= 1) { const v = getMainlinePointValue(seriesData[i]); if (v > 0) return v; } return 0; };

const fmt = (v, digits = 2) => { const n = Number(v); return Number.isFinite(n) ? n.toFixed(digits) : '-'; };

const refreshMainlineChart = () => {
  const dates = mainlineChartDates.value || [];
  const rawSeries = mainlineTrendSeries.value || [];
  if (!dates.length || !rawSeries.length) { mainlineChartOption.value = { ...mainlineChartOption.value, xAxis: { ...mainlineChartOption.value.xAxis, data: [] }, series: [] }; return; }
  const focusNames = focusedMainlineCards.value.map((item) => item.name).filter(Boolean);
  let selectedSeries = focusNames.length ? focusNames.map((name) => rawSeries.find((item) => item.name === name)).filter(Boolean) : [];
  if (!selectedSeries.length) selectedSeries = [...rawSeries].sort((a, b) => getLatestSeriesValue(b?.data || []) - getLatestSeriesValue(a?.data || [])).slice(0, 3);
  const toSignedPctText = (value, digits = 2) => { const n = Number(value); if (!Number.isFinite(n)) return '-'; return `${n >= 0 ? '+' : ''}${n.toFixed(digits)}%`; };
  mainlineChartOption.value = {
    backgroundColor: 'transparent',
    grid: { top: 24, left: 16, right: 12, bottom: 52, containLabel: true },
    tooltip: {
      trigger: 'axis', axisPointer: { type: 'line' }, backgroundColor: 'rgba(15, 23, 42, 0.96)', borderColor: '#334155', textStyle: { color: '#cbd5e1', fontSize: 11 }, padding: 10, confine: true,
      formatter: (params) => {
        const activeParams = (params || []).filter((item) => getMainlinePointValue(item?.data ?? item?.value) > 0).sort((a, b) => getMainlinePointValue(b?.data ?? b?.value) - getMainlinePointValue(a?.data ?? a?.value));
        if (!activeParams.length) return '';
        const date = activeParams[0].axisValue || activeParams[0].name || '-';
        const rows = activeParams.map((param) => {
          const point = param.data || {};
          const score = getMainlinePointValue(point);
          const stocks = Array.isArray(point.top_stocks) && point.top_stocks.length ? point.top_stocks.slice(0, 3).map((s) => { const pct = Number(s?.pct_chg); return `${s?.name || '-'}${Number.isFinite(pct) ? ` ${toSignedPctText(pct, 1)}` : ''}`; }).join('、') : '无龙头';
          return `<div style="margin-top:8px;"><div style="display:flex;justify-content:space-between;gap:8px;"><span style="color:${param.color};font-weight:700;">${param.seriesName}</span><span style="color:#f8fafc;font-weight:700;">${score.toFixed(1)}</span></div><div style="margin-top:4px;color:#94a3b8;font-size:10px;line-height:1.5;">${stocks}</div></div>`;
        }).join('');
        return `<div style="min-width:180px;"><div style="font-weight:700;color:#e2e8f0;">${date}</div>${rows}</div>`;
      },
    },
    legend: { bottom: 0, icon: 'circle', itemWidth: 8, itemHeight: 8, textStyle: { color: '#64748b', fontSize: 10 } },
    xAxis: { type: 'category', data: dates, boundaryGap: false, axisLine: { lineStyle: { color: '#334155' } }, axisLabel: { color: '#64748b', fontSize: 10, rotate: 30 } },
    yAxis: { type: 'value', splitLine: { lineStyle: { color: '#1e293b', type: 'dashed' } }, axisLine: { show: false }, axisLabel: { color: '#64748b', fontSize: 10 } },
    series: selectedSeries.map((series, index) => ({
      name: series.name, type: 'line', smooth: true, showSymbol: false, connectNulls: true,
      lineStyle: { width: 3, color: MAINLINE_CHART_COLORS[index % MAINLINE_CHART_COLORS.length] },
      areaStyle: { color: MAINLINE_CHART_COLORS[index % MAINLINE_CHART_COLORS.length], opacity: 0.08 },
      itemStyle: { color: MAINLINE_CHART_COLORS[index % MAINLINE_CHART_COLORS.length] },
      data: (series.data || []).map((point) => {
        if (typeof point === 'object' && point !== null) return { value: Number(point.value) || 0, top_stocks: point.top_stocks || [], limit_ups: point.limit_ups, stock_count: point.stock_count };
        return { value: Number(point) || 0, top_stocks: [] };
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
    if (analysis && analysis.deduction) trendAnalysis.value = analysis.deduction;
    else trendAnalysis.value = '当前周期内无明显主线推演结论。';
    if (analysis?.top_mainline) {
      currentMainline.value = { name: analysis.top_mainline.name || '', displayName: analysis.top_mainline.display_name || analysis.top_mainline.name || '', score: analysis.top_mainline.score || 0, reason: analysis.top_mainline.reason || '', focusTags: analysis.top_mainline.focus_tags || [], driverSummary: analysis.top_mainline.driver_summary || '' };
    } else {
      currentMainline.value = { name: '', displayName: '', score: 0, reason: '', focusTags: [], driverSummary: '' };
    }
    mainlineReview.value = analysis?.review_10d || null;
    rebuildMainlineFocus();
  } catch (e) {
    console.error('History Error', e);
    mainlineChartDates.value = []; mainlineTrendSeries.value = []; mainlineReview.value = null;
    currentMainline.value = { name: '', displayName: '', score: 0, reason: '', focusTags: [], driverSummary: '' };
    mainlineFocusCards.value = []; mainlineOverview.value = { current: null, continuous: null };
    refreshMainlineChart();
  } finally { loadingTrend.value = false; }
};

const fetchMainlineLeaders = async (sector = '') => {
  loadingLeaders.value = true;
  try {
    const params = { limit: 5, min_score: 60 };
    if (sector) params.sector = sector;
    const res = await getMainlineLeaders(params);
    if (res.data.status === 'success') mergeMainlineLeaders(res.data);
    else { if (!mainlineLeaders.value?.mainlines?.length) mainlineLeaders.value = null; }
    rebuildMainlineFocus();
  } catch (e) {
    console.error('加载主线龙头失败', e);
    if (!mainlineLeaders.value?.mainlines?.length) mainlineLeaders.value = null;
    rebuildMainlineFocus();
  } finally { loadingLeaders.value = false; }
};

watch([selectedMainlineName, loadingLeaders], ([name, isLoading]) => {
  if (!name || isLoading || hasLeaderGroup(name)) return;
  void fetchMainlineLeaders(name);
});

onMounted(async () => { await fetchMainlineHistory(); });

defineExpose({ fetchMainlineLeaders });
</script>
