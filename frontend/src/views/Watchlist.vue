<template>
  <div class="space-y-4 md:space-y-6 max-w-6xl mx-auto pb-8">
    <div class="bg-business-dark p-4 rounded-2xl shadow-lg border border-business-light">
      <div class="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div>
          <h2 class="text-sm font-bold text-white tracking-wide">盯盘自选</h2>
          <p class="text-[11px] text-slate-400 mt-1">
            交易时段自动刷新，非交易时段停止刷新。数据源：新浪（Sina）。
          </p>
        </div>
        <div class="flex items-center gap-2 text-[11px]">
          <span
            class="px-2 py-0.5 rounded border"
            :class="isTradingTime ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-300' : 'border-slate-600 bg-slate-800 text-slate-300'"
          >
            {{ isTradingTime ? '交易时段' : '非交易时段' }}
          </span>
          <button
            @click="refreshNow"
            :disabled="loading"
            class="px-2 py-1 rounded border border-business-accent/40 bg-business-accent/10 text-business-accent hover:bg-business-accent hover:text-white transition disabled:opacity-50"
          >
            {{ loading ? '刷新中...' : '手动刷新' }}
          </button>
        </div>
      </div>
      <div class="mt-3 grid grid-cols-1 md:grid-cols-[1fr_auto] gap-2">
        <input
          v-model="codesInput"
          class="bg-slate-900/80 border border-slate-700 rounded px-3 py-2 text-xs text-slate-100 focus:outline-none focus:ring-1 focus:ring-business-accent"
          placeholder="输入股票代码，逗号分隔，例如 600519, 000001, 300750"
        />
        <button
          @click="applyCodes"
          class="px-3 py-2 text-xs font-semibold rounded bg-business-accent text-white hover:brightness-110 transition"
        >
          保存并刷新
        </button>
      </div>
      <p class="mt-2 text-[11px] text-slate-500">
        {{ message || '就绪' }}
      </p>
    </div>

    <div class="bg-business-dark p-4 rounded-2xl shadow-lg border border-business-light overflow-x-auto">
      <table class="w-full text-xs">
        <thead>
          <tr class="text-slate-400 border-b border-slate-700">
            <th class="text-left py-2 pr-2">代码</th>
            <th class="text-left py-2 pr-2">名称</th>
            <th class="text-right py-2 pr-2">最新价</th>
            <th class="text-right py-2 pr-2">涨跌</th>
            <th class="text-right py-2 pr-2">涨跌幅</th>
            <th class="text-right py-2 pr-2">振幅</th>
            <th class="text-right py-2 pr-2">昨收</th>
            <th class="text-right py-2 pr-2">今开</th>
            <th class="text-right py-2 pr-2">最高</th>
            <th class="text-right py-2 pr-2">最低</th>
            <th class="text-right py-2 pr-2">成交额</th>
            <th class="text-left py-2">分析</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="!rows.length" class="text-slate-500">
            <td colspan="12" class="py-6 text-center">暂无数据</td>
          </tr>
          <tr
            v-for="item in rows"
            :key="item.ts_code"
            class="border-b border-slate-800/70 hover:bg-slate-900/30"
          >
            <td class="py-2 pr-2 text-slate-200 font-semibold">{{ item.ts_code }}</td>
            <td class="py-2 pr-2 text-slate-300 whitespace-nowrap">{{ item.name || '-' }}</td>
            <td class="py-2 pr-2 text-right text-slate-200">{{ fmt(item.price, 2) }}</td>
            <td class="py-2 pr-2 text-right" :class="numColor(item.diff)">{{ fmt(item.diff, 2) }}</td>
            <td class="py-2 pr-2 text-right" :class="numColor(item.pct)">{{ fmt(item.pct, 2, '%') }}</td>
            <td class="py-2 pr-2 text-right text-slate-300">{{ fmt(item.amplitude, 2, '%') }}</td>
            <td class="py-2 pr-2 text-right text-slate-400">{{ fmt(item.pre_close, 2) }}</td>
            <td class="py-2 pr-2 text-right" :class="numColor(item.open - item.pre_close)">{{ fmt(item.open, 2) }}</td>
            <td class="py-2 pr-2 text-right" :class="numColor(item.high - item.pre_close)">{{ fmt(item.high, 2) }}</td>
            <td class="py-2 pr-2 text-right" :class="numColor(item.low - item.pre_close)">{{ fmt(item.low, 2) }}</td>
            <td class="py-2 pr-2 text-right text-slate-300">{{ fmtAmount(item.amount) }}</td>
            <td class="py-2 text-slate-300 min-w-[200px]">
              <div class="flex items-center gap-2">
                <span class="truncate">{{ item.analyze?.summary || '-' }}</span>
                <span 
                  v-if="item.analyze?.suggestion"
                  class="px-1.5 py-0.5 rounded text-[10px] font-bold shrink-0"
                  :class="suggestionColor(item.analyze.suggestion)"
                >
                  {{ item.analyze.suggestion }}
                </span>
              </div>
              <!-- 历史 10 日趋势点 -->
              <div class="flex items-center gap-1 mt-1.5">
                <span class="text-[9px] text-slate-500 mr-1 uppercase">10D:</span>
                <div 
                  v-for="(day, idx) in [...(item.analyze?.history || [])].reverse()" 
                  :key="idx"
                  class="w-2.5 h-2.5 rounded-sm transition-transform hover:scale-125 cursor-help"
                  :class="dotColor(day.suggestion, day.tone)"
                  :title="`${day.date}: ${day.suggestion} (${day.tone}) ${day.patterns.join(',')}`"
                ></div>
              </div>
              <div class="text-[10px] text-slate-500 mt-0.5">
                {{ (item.analyze?.patterns || []).join(' / ') || '无' }}
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref } from 'vue';
import { getWatchlistRealtime } from '@/services/api';

const STORAGE_KEY = 'jarvis_watchlist_codes';
const DEFAULT_CODES = '600519,000001,300750,002594';
const REFRESH_MS = 8000;

const codesInput = ref(localStorage.getItem(STORAGE_KEY) || DEFAULT_CODES);
const loading = ref(false);
const isTradingTime = ref(false);
const message = ref('');
const rows = ref([]);
let timer = null;

const fmt = (v, digits = 2, suffix = '') => {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return `${n.toFixed(digits)}${suffix}`;
};

const fmtAmount = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  if (Math.abs(n) >= 1e8) return `${(n / 1e8).toFixed(2)}亿`;
  if (Math.abs(n) >= 1e4) return `${(n / 1e4).toFixed(2)}万`;
  return `${Math.round(n)}`;
};

const numColor = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n) || n === 0) return 'text-slate-300';
  return n > 0 ? 'text-red-400' : 'text-emerald-400';
};

const suggestionColor = (s) => {
  if (!s || s === '观望') return 'bg-slate-700 text-slate-300';
  if (s.includes('关注') || s.includes('试错')) return 'bg-red-500/20 text-red-400 border border-red-500/30';
  if (s.includes('减仓') || s.includes('持币')) return 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30';
  return 'bg-slate-700 text-slate-300';
};

const dotColor = (s, tone) => {
  if (tone.includes('爆发') || s.includes('试错') || s.includes('关注')) return 'bg-red-500';
  if (tone.includes('杀跌') || s.includes('减仓') || s.includes('持币')) return 'bg-emerald-500';
  if (tone.includes('看多')) return 'bg-red-900/60';
  if (tone.includes('看空')) return 'bg-emerald-900/60';
  return 'bg-slate-700';
};

const stopAutoRefresh = () => {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
};

const startAutoRefresh = () => {
  stopAutoRefresh();
  timer = setInterval(() => {
    refreshNow(false);
  }, REFRESH_MS);
};

const refreshNow = async (showLoading = true) => {
  const payloadCodes = codesInput.value
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean)
    .join(',');
  if (!payloadCodes) {
    rows.value = [];
    message.value = '请先输入股票代码';
    return;
  }
  if (showLoading) loading.value = true;
  try {
    const res = await getWatchlistRealtime(payloadCodes, 'sina');
    const body = res.data || {};
    rows.value = body.data || [];
    isTradingTime.value = !!body.is_trading_time;
    message.value = body.message || '';
    if (isTradingTime.value) startAutoRefresh();
    else stopAutoRefresh();
  } catch (e) {
    message.value = e?.response?.data?.detail || e?.message || '刷新失败';
    stopAutoRefresh();
  } finally {
    loading.value = false;
  }
};

const applyCodes = async () => {
  localStorage.setItem(STORAGE_KEY, codesInput.value);
  await refreshNow(true);
};

onMounted(async () => {
  await refreshNow(true);
});

onBeforeUnmount(() => {
  stopAutoRefresh();
});
</script>
