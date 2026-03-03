<template>
  <div class="space-y-4 md:space-y-6 animate-in fade-in duration-500 max-w-6xl mx-auto pb-8">
    <!-- Market Sentiment Overview -->
    <div class="bg-business-dark p-4 rounded-2xl shadow-lg border border-business-light w-full">
      <div class="flex flex-col sm:flex-row sm:items-center justify-between mb-4 gap-3">
        <div class="flex items-center space-x-2">
          <div class="w-1.5 h-4 bg-business-warning rounded-full"></div>
          <h2 class="text-sm font-bold text-slate-400">市场情绪趋势</h2>
        </div>
        <div class="flex flex-wrap items-center gap-2 text-[10px]">
            <button
              @click="openBacktestModal"
              class="px-2 py-0.5 bg-business-accent/20 text-business-accent rounded border border-business-accent/30 hover:bg-business-accent hover:text-white transition"
            >
              查看回测结果
            </button>
            <button
              @click="handleSyncSentiment"
              :disabled="syncingSentiment"
              class="px-2 py-0.5 rounded border transition"
              :class="syncingSentiment ? 'bg-emerald-200 text-emerald-900 border-emerald-200' : 'bg-emerald-800/40 text-emerald-300 border-emerald-700 hover:bg-emerald-700 hover:text-white'"
            >
              {{ syncingSentiment ? '同步中...' : '同步情绪' }}
            </button>
            <button
              @click="handlePreviewSentiment"
              :disabled="predictingSentiment"
              class="px-2 py-0.5 rounded border transition"
              :class="predictingSentiment ? 'bg-amber-200 text-amber-900 border-amber-200' : 'bg-amber-800/40 text-amber-300 border-amber-700 hover:bg-amber-700 hover:text-white'"
            >
              {{ predictingSentiment ? '预测中...' : '预测情绪' }}
            </button>
            <button
              @click="minimalTooltipMode = !minimalTooltipMode"
              class="px-2 py-0.5 rounded border transition"
              :class="minimalTooltipMode ? 'bg-slate-200 text-slate-900 border-slate-200' : 'bg-slate-800 text-slate-300 border-slate-700 hover:bg-slate-700'"
            >
              {{ minimalTooltipMode ? '极简模式: 开' : '极简模式: 关' }}
            </button>
            <div class="flex items-center space-x-2">
              <span class="px-2 py-0.5 bg-red-500/20 text-red-400 rounded">85+ 沸腾/止盈</span>
              <span class="px-2 py-0.5 bg-blue-500/20 text-blue-400 rounded">25- 冰点/潜伏</span>
            </div>
        </div>
      </div>
      <div class="h-52 sm:h-40 w-full">
        <v-chart :option="marketSentimentChartOption" autoresize />
      </div>
    </div>

    <!-- 图表展示区 -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 px-2 md:px-0">
      <!-- 主线演变图表 -->
      <div class="lg:col-span-2 bg-business-dark p-5 rounded-2xl shadow-business border border-business-light flex flex-col">
        <div class="flex items-center justify-between mb-6">
          <div class="flex items-center space-x-3">
            <div class="w-1 h-4 bg-business-highlight rounded-full"></div>
            <h2 class="text-sm font-bold text-white uppercase tracking-wider">核心主线演变趋势</h2>
            <div v-if="currentMainline.name" class="px-2 py-1 rounded bg-business-highlight/10 border border-business-highlight/30">
              <span class="text-[10px] text-business-highlight font-bold">
                {{ currentMainline.name }} {{ currentMainline.score ? `(${currentMainline.score})` : '' }}
              </span>
            </div>
          </div>
          <div class="flex items-center space-x-2">
            <button @click="isTooltipLocked = false" v-if="isTooltipLocked" class="text-[9px] font-bold text-business-accent bg-business-accent/10 px-2 py-0.5 rounded border border-business-accent/20 transition-all hover:bg-business-accent hover:text-white">解锁</button>
            <span class="text-[9px] text-slate-500 font-medium bg-slate-800/50 px-2 py-1 rounded border border-slate-700">点击锁定浮窗</span>
          </div>
        </div>
        <div class="h-64 sm:h-80 w-full relative">
          <v-chart v-if="!loadingTrend && mainlineChartOption.series.length > 0" :option="mainlineChartOption" autoresize @click="handleChartClick" />
          <div v-if="loadingTrend" class="absolute inset-0 flex items-center justify-center">
             <div class="w-8 h-8 border-2 border-business-highlight border-t-transparent rounded-full animate-spin"></div>
          </div>
          <div v-if="!loadingTrend && mainlineChartOption.series.length === 0" class="absolute inset-0 flex flex-col items-center justify-center text-slate-500 space-y-2">
             <p class="text-xs font-bold uppercase tracking-widest">暂无趋势数据</p>
          </div>
        </div>
        
        <!-- 深度推演逻辑 -->
        <div v-if="trendAnalysis" class="mt-4 p-4 bg-slate-900/50 rounded-xl border border-slate-700/50">
           <div class="flex items-start space-x-3">
              <ChatBubbleLeftRightIcon class="w-5 h-5 text-business-accent mt-0.5 shrink-0" />
              <div class="space-y-1">
                 <h3 class="text-[10px] font-bold text-slate-500 uppercase tracking-widest">量化推演结论</h3>
                 <p class="text-xs text-slate-300 leading-relaxed font-medium whitespace-pre-line">
                    {{ trendAnalysis }}
                 </p>
              </div>
           </div>
        </div>
      </div>

      <!-- 热门概念分布 -->
      <div class="bg-business-dark p-5 rounded-2xl shadow-business border border-business-light flex flex-col">
        <div class="flex items-center space-x-3 mb-6">
          <div class="w-1 h-4 bg-business-accent rounded-full"></div>
          <h2 class="text-sm font-bold text-white uppercase tracking-wider">题材热力分布</h2>
        </div>
        <div class="flex-1 min-h-[240px] sm:min-h-[300px]">
          <v-chart :option="hotConceptsChartOption" autoresize />
        </div>
      </div>
    </div>

    <div v-if="tradingBrief.action" class="px-2 md:px-0">
      <div class="bg-business-dark p-4 rounded-2xl shadow-lg border border-business-light">
        <div class="flex items-center justify-between gap-2 mb-2">
          <h3 class="text-xs font-bold text-slate-200 tracking-wide">最新交易日简报</h3>
          <span class="text-[10px] text-slate-400">{{ tradingBrief.tradeDate || '-' }}</span>
        </div>
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-2 text-xs">
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-3 py-2">
            <span class="text-slate-500">市场状态</span>
            <p class="text-slate-200 font-semibold mt-0.5">{{ tradingBrief.regime || '-' }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-3 py-2">
            <span class="text-slate-500">情绪评分</span>
            <p class="text-slate-200 font-semibold mt-0.5">{{ tradingBrief.scoreText || '-' }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-3 py-2">
            <span class="text-slate-500">策略置信度</span>
            <p class="text-slate-200 font-semibold mt-0.5">{{ tradingBrief.confidence || '-' }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-3 py-2 sm:col-span-2">
            <span class="text-slate-500">主线</span>
            <p class="text-slate-200 font-semibold mt-0.5">{{ tradingBrief.mainline || '-' }}</p>
          </div>
        </div>
        <div class="mt-2 rounded border border-amber-700/40 bg-amber-500/10 px-3 py-2">
          <span class="text-[11px] text-amber-300">执行建议</span>
          <p class="text-xs text-amber-100 mt-0.5 leading-relaxed">{{ tradingBrief.action }}</p>
        </div>
      </div>
    </div>

    <div v-if="sentimentPreview || previewError" class="px-2 md:px-0">
      <div class="bg-business-dark p-4 rounded-2xl shadow-lg border border-business-light">
        <div class="flex items-center justify-between gap-2 mb-2">
          <h3 class="text-xs font-bold text-slate-200 tracking-wide">盘中情绪预测</h3>
          <span class="text-[10px] text-slate-400">{{ sentimentPreview?.as_of || '-' }}</span>
        </div>
        <div v-if="previewError" class="rounded border border-red-700/40 bg-red-500/10 px-3 py-2 text-xs text-red-300">
          {{ previewError }}
        </div>
        <div v-else-if="sentimentPreview" class="grid grid-cols-1 sm:grid-cols-2 gap-2 text-xs">
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-3 py-2">
            <span class="text-slate-500">预测交易日</span>
            <p class="text-slate-200 font-semibold mt-0.5">{{ sentimentPreview.predicted_trade_date || '-' }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-3 py-2">
            <span class="text-slate-500">预测评分</span>
            <p class="text-slate-200 font-semibold mt-0.5">{{ fmt(sentimentPreview.projected_score, 1) }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-3 py-2">
            <span class="text-slate-500">沪深300 涨跌幅</span>
            <p class="text-slate-200 font-semibold mt-0.5">{{ fmt(sentimentPreview.index_pct_chg, 3, '%') }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-3 py-2">
            <span class="text-slate-500">科创50 涨跌幅</span>
            <p class="text-slate-200 font-semibold mt-0.5">{{ fmt(sentimentPreview.star50_pct_chg, 3, '%') }}</p>
          </div>
          <div class="rounded border border-amber-700/40 bg-amber-500/10 px-3 py-2 sm:col-span-2">
            <span class="text-[11px] text-amber-300">预测策略</span>
            <p class="text-xs text-amber-100 mt-0.5 leading-relaxed">
              {{ sentimentPreview.plan?.next_day_strategy || '-' }}
            </p>
          </div>
          <div v-if="marketSuggestion" class="rounded border border-sky-700/40 bg-sky-500/10 px-3 py-2 sm:col-span-2">
            <span class="text-[11px] text-sky-300">统一市场建议（预览）</span>
            <div class="grid grid-cols-2 gap-x-2 gap-y-1 mt-1 text-sky-100">
              <div>动作: {{ marketSuggestion.action || '-' }}</div>
              <div>仓位: {{ fmt((Number(marketSuggestion.target_position) || 0) * 100, 0, '%') }}</div>
              <div>置信度: {{ fmt(marketSuggestion.confidence, 1) }}</div>
              <div>主线: {{ marketSuggestion.rationale?.top_mainline || '-' }}</div>
              <div>止损: {{ fmt(marketSuggestion.risk_controls?.stop_loss_pct, 1, '%') }}</div>
              <div>止盈: {{ fmt(marketSuggestion.risk_controls?.take_profit_pct, 1, '%') }}</div>
            </div>
          </div>
          <div v-else-if="suggestionError" class="rounded border border-red-700/40 bg-red-500/10 px-3 py-2 text-xs text-red-300 sm:col-span-2">
            建议生成失败: {{ suggestionError }}
          </div>
        </div>
      </div>
    </div>

    <div
      v-if="showBacktestModal"
      class="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      @click.self="showBacktestModal = false"
    >
      <div class="w-full max-w-md rounded-xl border border-slate-700 bg-slate-900 p-4 shadow-2xl">
        <div class="mb-3 flex items-center justify-between">
          <h3 class="text-sm font-bold text-slate-200">情绪策略回测结果</h3>
          <button class="text-slate-400 hover:text-white" @click="showBacktestModal = false">关闭</button>
        </div>
        <div v-if="loadingBacktest" class="py-8 text-center text-slate-400 text-sm">回测结果加载中...</div>
        <div v-else-if="backtestError" class="py-8 text-center text-red-400 text-sm">{{ backtestError }}</div>
        <div v-else-if="backtestData" class="space-y-2 text-xs">
          <div class="grid grid-cols-2 gap-2 text-slate-300">
            <div class="rounded bg-slate-800 p-2">总收益: <span class="font-bold text-white">{{ backtestData.metrics?.total_return || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">年化: <span class="font-bold text-white">{{ backtestData.metrics?.annual_return || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">回撤: <span class="font-bold text-white">{{ backtestData.metrics?.max_drawdown || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">胜率: <span class="font-bold text-white">{{ backtestData.metrics?.win_rate || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">夏普: <span class="font-bold text-white">{{ backtestData.metrics?.sharpe || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">基准: <span class="font-bold text-white">{{ backtestData.metrics?.benchmark_return || '-' }}</span></div>
          </div>
          <div class="rounded bg-slate-800 p-2 text-slate-400">
            参数: leverage={{ backtestData.policy?.leverage ?? '-' }}, trend_floor={{ backtestData.policy?.trend_floor_pos ?? '-' }}
          </div>
          <div class="text-[11px] text-slate-500">生成时间: {{ backtestData.generated_at || '-' }}</div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue';
import { getMainlineHistory, getMarketSentiment, getSentimentPreview, getMarketSuggestion, getBacktestResult, syncSentiment, getTasksStatus } from '@/services/api';
import { ChatBubbleLeftRightIcon } from '@heroicons/vue/20/solid'

const loadingTrend = ref(false);
const strategyConclusion = ref('');
const trendAnalysis = ref('');
const isTooltipLocked = ref(false);
const showBacktestModal = ref(false);
const loadingBacktest = ref(false);
const backtestError = ref('');
const backtestData = ref(null);
const minimalTooltipMode = ref(false);
const isMobile = ref(false);
const syncingSentiment = ref(false);
const predictingSentiment = ref(false);
const sentimentPreview = ref(null);
const previewError = ref('');
const marketSuggestion = ref(null);
const suggestionError = ref('');
const currentMainline = ref({ name: '', score: 0, reason: '' });
const latestSentiment = ref(null);
const latestSentimentDate = ref('');
const recentSentiments = ref([]);
const tradingBrief = ref({
  tradeDate: '',
  regime: '',
  scoreText: '',
  confidence: '',
  mainline: '',
  action: ''
});

const classifyRegimeByMa5 = (ma5) => {
  if (!Number.isFinite(ma5)) return '震荡';
  if (ma5 >= 78) return '强势';
  if (ma5 >= 62) return '偏强';
  if (ma5 <= 30) return '弱势';
  if (ma5 <= 42) return '偏弱';
  return '震荡';
};

const getStableRegime = (ma5Series, confirmDays = 2) => {
  if (!Array.isArray(ma5Series) || ma5Series.length === 0) return '震荡';
  let stable = classifyRegimeByMa5(ma5Series[0]);
  let candidate = stable;
  let streak = 0;

  for (let i = 1; i < ma5Series.length; i += 1) {
    const raw = classifyRegimeByMa5(ma5Series[i]);
    if (raw === stable) {
      candidate = raw;
      streak = 0;
      continue;
    }
    if (raw === candidate) {
      streak += 1;
    } else {
      candidate = raw;
      streak = 1;
    }
    if (streak >= confirmDays) {
      stable = candidate;
      streak = 0;
    }
  }
  return stable;
};

const toRegimeBaseConfidence = (regime) => {
  if (regime === '强势') return 78;
  if (regime === '偏强') return 66;
  if (regime === '震荡') return 50;
  if (regime === '偏弱') return 36;
  return 26;
};

const calcStd = (arr) => {
  if (!arr.length) return 0;
  const mean = arr.reduce((a, b) => a + b, 0) / arr.length;
  const variance = arr.reduce((a, b) => a + ((b - mean) ** 2), 0) / arr.length;
  return Math.sqrt(variance);
};

const buildTradingBrief = () => {
  const sentiment = latestSentiment.value;
  const tradeDate = latestSentimentDate.value || '';
  if (!sentiment || !tradeDate) {
    tradingBrief.value = { tradeDate: '', regime: '', scoreText: '', confidence: '', mainline: '', action: '' };
    return;
  }

  const score = Number(sentiment.value);
  const details = sentiment.details || {};
  const advice = details.metrics?.advice || '';
  const topName = currentMainline.value?.name || '';
  const topScore = Number(currentMainline.value?.score);
  const metricsMa5 = Number(details.metrics?.score_ma5);

  const tail = recentSentiments.value || [];
  const scoreSeries = tail.map((x) => Number(x?.value)).filter((x) => Number.isFinite(x));
  const latest5 = scoreSeries.slice(-5);
  const prev5 = scoreSeries.slice(-6, -1);
  const ma5 = Number.isFinite(metricsMa5)
    ? metricsMa5
    : (latest5.length ? latest5.reduce((a, b) => a + b, 0) / latest5.length : score);
  const prevMa5 = prev5.length ? prev5.reduce((a, b) => a + b, 0) / prev5.length : ma5;
  const maTrend = ma5 - prevMa5;

  const ma5Series = [];
  for (let i = 4; i < scoreSeries.length; i += 1) {
    const w = scoreSeries.slice(i - 4, i + 1);
    ma5Series.push(w.reduce((a, b) => a + b, 0) / w.length);
  }
  const deltaSeries = [];
  for (let i = 1; i < ma5Series.length; i += 1) {
    deltaSeries.push(ma5Series[i] - ma5Series[i - 1]);
  }
  const vol = calcStd(deltaSeries.slice(-8));
  const recentPeak = ma5Series.length ? Math.max(...ma5Series.slice(-10)) : ma5;
  const pullback = Number.isFinite(recentPeak) ? ma5 - recentPeak : 0;

  const stableRegime = getStableRegime(ma5Series, 2);
  const latestRawRegime = classifyRegimeByMa5(ma5);

  let regime = stableRegime;
  const mainlineStrong = Number.isFinite(topScore) && topScore >= 30;
  const mainlineMid = Number.isFinite(topScore) && topScore >= 18;
  const recovering = maTrend >= 1.8;
  const weakening = maTrend <= -1.8;
  const highVol = vol >= 3.2;
  const pendingSwitch = stableRegime !== latestRawRegime;

  let position = '20%-35%';
  let attack = '仅做主线内回踩，不做情绪追涨';
  let risk = '单笔亏损接近-3%减仓，盈利+8%分批止盈';

  if (regime === '强势') {
    position = (recovering && mainlineStrong) ? '60%-80%' : '50%-70%';
    if (weakening || highVol) position = '45%-60%';
    attack = '主线核心分歧低吸，次日冲高不追';
    risk = '若 MA5 连续2日走弱或回撤扩大，先降20%仓位锁定收益';
  } else if (regime === '偏强') {
    position = (recovering && (mainlineStrong || mainlineMid)) ? '45%-65%' : '35%-55%';
    if (weakening || highVol) position = '30%-45%';
    attack = '优先主线中军和换手龙，分批加仓';
    risk = '连续2日跌破计划买点则退出，避免被动扛单';
  } else if (regime === '弱势') {
    position = '0%-20%';
    attack = '以观察为主，仅保留试错仓';
    risk = '先活下来，等待 MA5 回到42以上再恢复进攻';
  } else if (regime === '偏弱') {
    position = '10%-30%';
    attack = '轻仓快进快出，优先等主线确认后再加仓';
    risk = '亏损单不过夜，盈利单分批落袋';
  } else {
    position = (recovering && mainlineStrong) ? '30%-45%' : '15%-30%';
    attack = '只做赔率明确的回踩点，不做中位追高';
    risk = '无优势日宁可空仓，降低无效交易';
  }

  let confidenceScore = toRegimeBaseConfidence(regime);
  if (recovering) confidenceScore += 8;
  if (weakening) confidenceScore -= 10;
  if (mainlineStrong) confidenceScore += 8;
  else if (mainlineMid) confidenceScore += 4;
  if (highVol) confidenceScore -= 8;
  if (pullback <= -6) confidenceScore -= 6;
  if (pendingSwitch) confidenceScore -= 5;
  confidenceScore = Math.max(5, Math.min(95, Math.round(confidenceScore)));

  const confidenceText = `${confidenceScore}/100${highVol ? ' (波动偏大)' : ''}`;
  const trendText = maTrend >= 1.5 ? '动能回升' : maTrend <= -1.5 ? '动能走弱' : '动能平稳';
  const pendingText = stableRegime !== latestRawRegime ? `，原始分区=${latestRawRegime}(待确认)` : '';

  const mainlinePart = topName
    ? `主线聚焦 ${topName}${Number.isFinite(topScore) ? `(${topScore.toFixed(1)})` : ''}`
    : '主线暂不清晰';
  const advicePart = advice ? `执行上以「${advice}」为主` : '执行上保持纪律化交易';

  tradingBrief.value = {
    tradeDate,
    regime: `${regime} (${trendText}${pendingText})`,
    scoreText: Number.isFinite(ma5) ? `MA5 ${ma5.toFixed(1)} / 100` : '-',
    confidence: confidenceText,
    mainline: mainlinePart,
    action: `建议仓位 ${position}；进攻: ${attack}；风控: ${risk}。按 2-5 个交易日节奏执行，避免因次日单点信号切换策略。${advicePart}。`
  };
};

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

const handlePreviewSentiment = async () => {
  if (predictingSentiment.value) return;
  predictingSentiment.value = true;
  previewError.value = '';
  suggestionError.value = '';
  marketSuggestion.value = null;
  try {
    const res = await getSentimentPreview('dc');
    if (res.data?.status !== 'success') {
      throw new Error(res.data?.message || '盘中情绪预测失败');
    }
    sentimentPreview.value = res.data?.data || null;
    try {
      const sRes = await getMarketSuggestion({ use_preview: true, src: 'dc' });
      if (sRes.data?.status === 'success') {
        marketSuggestion.value = sRes.data?.data || null;
      } else {
        suggestionError.value = sRes.data?.message || '统一市场建议生成失败';
      }
    } catch (e) {
      const detail = e?.response?.data?.detail;
      suggestionError.value = detail || e?.message || '统一市场建议生成失败';
    }
  } catch (e) {
    const detail = e?.response?.data?.detail;
    previewError.value = detail || e?.message || '盘中情绪预测失败';
    sentimentPreview.value = null;
  } finally {
    predictingSentiment.value = false;
  }
};

const openBacktestModal = async () => {
  showBacktestModal.value = true;
  loadingBacktest.value = true;
  backtestError.value = '';
  backtestData.value = null;
  try {
    const res = await getBacktestResult(true);
    if (res.data?.status !== 'success') {
      backtestError.value = res.data?.message || '回测结果获取失败';
      return;
    }
    backtestData.value = res.data?.data || null;
  } catch (e) {
    backtestError.value = '回测结果获取失败';
  } finally {
    loadingBacktest.value = false;
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

const handleChartClick = () => {
  isTooltipLocked.value = !isTooltipLocked.value;
};

const mainlineChartOption = ref({
  grid: { top: 30, left: 30, right: 10, bottom: 60, containLabel: true },
  xAxis: { type: 'category', data: [] },
  yAxis: { type: 'value' },
  series: []
});

const hotConceptsChartOption = ref({
  tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
  series: [{
    type: 'pie',
    radius: ['35%', '60%'],
    avoidLabelOverlap: true,
    itemStyle: { borderRadius: 8, borderColor: '#0f172a', borderWidth: 2 },
    label: { 
      show: true, 
      formatter: '{b}',
      color: '#cbd5e1',
      fontSize: 10,
      fontWeight: 'bold',
      position: 'outside',
      alignTo: 'edge',
      edgeDistance: '5%',
      minMargin: 5
    },
    emphasis: { 
      scale: true,
      scaleSize: 10,
      label: { show: true, fontSize: 12 } 
    },
    labelLine: { 
      show: true,
      length: 15,
      length2: 10,
      maxSurfaceAngle: 80,
      lineStyle: { color: '#334155' }
    },
    data: []
  }]
});

const fetchMainlineHistory = async () => {
  loadingTrend.value = true;
  try {
    const res = await getMainlineHistory(30);
    const data = res.data.status === 'success' ? res.data.data : res.data;
    const { dates, series, analysis } = data;
    
    if (analysis && analysis.deduction) {
      trendAnalysis.value = analysis.deduction;
    } else {
      trendAnalysis.value = "当前周期内无明显主线推演结论。";
    }
    if (analysis?.top_mainline) {
      currentMainline.value = {
        name: analysis.top_mainline.name || '',
        score: analysis.top_mainline.score || 0,
        reason: analysis.top_mainline.reason || ''
      };
    } else {
      currentMainline.value = { name: '', score: 0, reason: '' };
    }
    buildTradingBrief();
    
    if (!dates || !series || dates.length === 0) {
      console.warn("Mainline History: No data returned", data);
      loadingTrend.value = false;
      return;
    }
    
    const pieData = series.map(s => {
      // 取最近 5 日的平均分作为热力分布依据
      const last5Points = s.data.slice(-5);
      const avgScore = last5Points.reduce((acc, curr) => {
        const val = typeof curr === 'object' ? curr.value : curr;
        return acc + val;
      }, 0) / (last5Points.length || 1);
      
      return { name: s.name, value: Math.round(avgScore * 100) / 100 };
    }).filter(d => d.value > 0.5).sort((a, b) => b.value - a.value);
    
    hotConceptsChartOption.value.series[0].data = pieData;
    
    mainlineChartOption.value = {
      backgroundColor: 'transparent',
      grid: { top: 40, left: 10, right: 10, bottom: 60, containLabel: true },
      tooltip: {
        trigger: 'axis',
        enterable: true,
        confine: true,
        position: (point, params, dom, rect, size) => {
          const x = point[0];
          const y = point[1];
          const viewWidth = size.viewSize[0];
          const viewHeight = size.viewSize[1];
          const boxWidth = size.contentSize[0];
          const boxHeight = size.contentSize[1];
          
          let posX = x + 20;
          if (posX + boxWidth > viewWidth) {
            posX = x - boxWidth - 20;
          }
          
          let posY = y - boxHeight - 20;
          if (posY < 0) {
            posY = y + 20;
          }
          
          return [posX, posY];
        },
        triggerOn: 'mousemove|click',
        axisPointer: { type: 'shadow' },
        backgroundColor: 'rgba(15, 23, 42, 0.95)',
        borderColor: '#334155',
        padding: 12,
        textStyle: { color: '#cbd5e1', fontSize: 11 },
        extraCssText: 'backdrop-filter: blur(4px); box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.5); border-radius: 8px; z-index: 1000;',
        formatter: (params) => {
           if (!params || params.length === 0) return '';
           const activeParams = params
               .filter(p => {
                  const val = typeof p.value === 'object' ? p.value.value : p.value;
                  return (val || 0) > 0;
               })
               .sort((a, b) => {
                  const valB = typeof b.value === 'object' ? b.value.value : b.value;
                  const valA = typeof a.value === 'object' ? a.value.value : a.value;
                  return (valB || 0) - (valA || 0);
               });
           
           if (activeParams.length === 0) return '';

           const date = activeParams[0].name;
           const lockHint = isTooltipLocked.value ? 
              '<span class="ml-2 text-[9px] bg-business-accent text-white px-1 rounded animate-pulse">LOCKED</span>' : 
              '<span class="ml-2 text-[9px] text-slate-500">(点击锁定)</span>';

           let html = `<div class="font-bold text-slate-200 text-xs mb-2 border-b border-slate-700 pb-1 flex justify-between items-center">
                         <span class="truncate mr-2">${date} 核心主线</span>
                         ${lockHint}
                       </div>`;
           
           activeParams.slice(0, 5).forEach(param => {
               const concept = param.seriesName;
               const score = typeof param.value === 'object' ? param.value.value : param.value;
               const dataNode = param.data || {};
               
               let stocksHtml = "";
               if (Array.isArray(dataNode.top_stocks)) {
                   if (dataNode.top_stocks.length > 0) {
                       stocksHtml = dataNode.top_stocks.map(s => `
                           <div style='display:flex;justify-content:space-between;align-items:center;line-height:1.4;'>
                               <span style='overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:80px;'>${s.name}</span>
                               <span style='color:#ef4444;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;font-weight:600;margin-left:8px;'>+${(s.pct_chg || 0).toFixed(1)}%</span>
                           </div>
                       `).join('');
                   } else {
                       stocksHtml = "<div class='text-slate-500 italic text-[9px]'>无符合条件的强势股</div>";
                   }
               } else if (typeof dataNode.top_stocks === 'string') {
                   stocksHtml = dataNode.top_stocks;
               } else {
                   stocksHtml = "<div class='text-slate-500 italic text-[9px]'>无数据</div>";
               }
               
               html += `
                 <div class="mb-3 last:mb-0">
                   <div class="flex justify-between items-center mb-1 px-1">
                     <span class="font-bold text-business-highlight text-[11px] truncate mr-2">${concept}</span>
                     <span class="text-[9px] text-slate-400 font-mono bg-slate-800 px-1 py-0.5 rounded shrink-0">强度: ${score}</span>
                   </div>
                   <div class="bg-slate-900/50 rounded-lg p-2 border border-slate-700/30 shadow-inner">
                     <div class="text-[10px] font-medium text-slate-200">
                       ${stocksHtml}
                     </div>
                   </div>
                 </div>
               `;
           });
           
           return `<div style="max-height:280px; overflow-y:auto; padding-right:4px; min-width:160px; max-width:240px;">${html}</div>`;
        }
      },
      legend: {
        bottom: 0,
        icon: 'circle',
        itemWidth: 8,
        itemHeight: 8,
        textStyle: { color: '#64748b', fontSize: 9 },
        pageTextStyle: { color: '#64748b' },
        type: 'scroll'
      },
      xAxis: {
        type: 'category',
        data: dates,
        axisLine: { lineStyle: { color: '#1e293b' } },
        axisLabel: { color: '#475569', fontSize: 9, rotate: 45 }
      },
      yAxis: {
        type: 'value',
        splitLine: { lineStyle: { color: '#1e293b', type: 'dashed' } },
        axisLabel: { color: '#475569', fontSize: 9 }
      },
      series: series.map(s => ({
        name: s.name,
        type: 'bar',
        stack: 'total',
        barWidth: '60%',
        data: s.data.map(d => (typeof d === 'object' ? { value: d.value, top_stocks: d.top_stocks } : d)),
        emphasis: { focus: 'series' },
        itemStyle: { borderRadius: [2, 2, 0, 0] }
      }))
    };
  } catch (e) {
    console.error("History Error", e);
  } finally {
    loadingTrend.value = false;
  }
};

const fetchMarketSentiment = async () => {
  try {
    const res = await getMarketSentiment(250); // 获取约1年交易日数据
    const data = res.data.data; // { dates, sentiment, index }
    
    if (!data.dates || data.dates.length === 0) return;
    latestSentimentDate.value = data.dates[data.dates.length - 1];
    latestSentiment.value = data.sentiment[data.sentiment.length - 1] || null;
    recentSentiments.value = data.sentiment.slice(-60);
    buildTradingBrief();
    
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
  }
};

onMounted(async () => {
  updateViewport();
  window.addEventListener('resize', updateViewport);
  fetchMainlineHistory();
  fetchMarketSentiment();
});

onBeforeUnmount(() => {
  window.removeEventListener('resize', updateViewport);
});
</script>

<style scoped>
.shadow-business {
  box-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5);
}

/* Custom scrollbar for tooltip */
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
