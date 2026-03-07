<template>
  <div class="space-y-4 md:space-y-6 max-w-6xl mx-auto pb-8 relative">
    <!-- 顶部管理栏 -->
    <div v-if="!zenMode" class="bg-business-dark p-4 rounded-2xl shadow-lg border border-business-light">
      <div class="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div>
          <h2 class="text-sm font-bold text-white tracking-wide">盯盘自选</h2>
          <p class="text-[11px] text-slate-400 mt-1">
            交易时段自动刷新。
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
            @click="toggleZenMode"
            class="px-2 py-1 rounded border border-purple-500/40 bg-purple-500/10 text-purple-300 hover:bg-purple-500 hover:text-white transition"
          >
            {{ zenMode ? '退出专注' : '专注模式' }}
          </button>
          <button
            @click="refreshNow(true)"
            :disabled="loading"
            class="px-2 py-1 rounded border border-business-accent/40 bg-business-accent/10 text-business-accent hover:bg-business-accent hover:text-white transition disabled:opacity-50"
          >
            {{ loading ? '刷新中...' : '手动刷新' }}
          </button>
        </div>
      </div>
      
      <!-- 添加自选 -->
      <div class="mt-3 relative">
        <div class="flex items-center gap-2">
          <input
            v-model="newCode"
            @input="handleSearchInput"
            @keyup.enter="handleAdd"
            @focus="handleSearchInput"
            @blur="hideSearchResults"
            class="flex-1 bg-slate-900/80 border border-slate-700 rounded px-3 py-2 text-xs text-slate-100 focus:outline-none focus:ring-1 focus:ring-business-accent"
            placeholder="输入代码/名称/首字母搜索 (例如 600519, 茅台, GJ)"
          />
          <button
            @click="handleAdd"
            class="px-4 py-2 text-xs font-semibold rounded bg-business-accent text-white hover:brightness-110 transition shrink-0"
          >
            添加
          </button>
        </div>
        <!-- 搜索结果下拉 -->
        <div
          v-if="showSearchResults"
          class="absolute z-50 mt-1 w-full bg-slate-800 border border-slate-600 rounded-lg shadow-xl max-h-60 overflow-auto"
        >
          <div
            v-for="stock in searchResults"
            :key="stock.ts_code"
            @mousedown.prevent="selectSearchResult(stock)"
            class="px-3 py-2 hover:bg-slate-700 cursor-pointer flex justify-between items-center border-b border-slate-700/50 last:border-0"
          >
            <span class="text-slate-200 font-mono text-xs">{{ stock.ts_code }}</span>
            <span class="text-slate-400 text-xs">{{ stock.name }}</span>
          </div>
        </div>
      </div>
    </div>

    <!-- 行情列表 -->
    <div v-if="!zenMode" class="bg-business-dark p-4 rounded-2xl shadow-lg border border-business-light overflow-x-auto min-h-[400px]">
      <table class="w-full text-xs">
        <thead>
          <tr class="text-slate-400 border-b border-slate-700">
            <th class="text-left py-2 pr-2 w-20">代码</th>
            <th class="text-left py-2 pr-2 w-20">名称</th>
            <th class="text-right py-2 pr-2 w-16">最新价</th>
            <th class="text-right py-2 pr-2 w-16">涨跌幅</th>
            <th class="text-left py-2">专业点评</th>
            <th class="text-right py-2 w-16">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="!rows.length && !loading" class="text-slate-500">
            <td colspan="6" class="py-12 text-center">暂无自选股，请先添加</td>
          </tr>
          <tr
            v-for="item in rows"
            :key="item.ts_code"
            class="border-b border-slate-800/70 hover:bg-slate-900/40 group transition-colors"
          >
            <td data-kline-trigger="1" class="py-3 pr-2 text-slate-200 font-semibold font-mono cursor-pointer hover:text-business-accent" @click.stop="toggleKline(item, $event)">{{ item.ts_code }}</td>
            <td data-kline-trigger="1" class="py-3 pr-2 text-slate-300 font-bold whitespace-nowrap cursor-pointer hover:text-business-accent" @click.stop="toggleKline(item, $event)">{{ item.name || '-' }}</td>
            <td class="py-3 pr-2 text-right text-slate-200 font-mono text-sm">{{ fmt(item.price, 2) }}</td>
            <td class="py-3 pr-2 text-right font-mono text-sm" :class="numColor(item.pct)">{{ fmt(item.pct, 2, '%') }}</td>
            <td class="py-3 text-slate-300">
              <div class="flex flex-col gap-1.5">
                <div class="flex items-start gap-2">
                  <span
                    v-if="item.analyze?.suggestion"
                    class="px-1.5 py-0.5 rounded text-[10px] font-bold shrink-0"
                    :class="suggestionColor(item.analyze.suggestion)"
                  >
                    {{ item.analyze.suggestion }}
                  </span>
                  <div class="flex-1 min-w-0">
                    <div class="text-slate-200 text-[11px] leading-relaxed space-y-0.5">
                      <div
                        v-for="(segment, idx) in parseCommentary(item.analyze?.summary)"
                        :key="idx"
                        class="flex items-start gap-1.5"
                      >
                        <span
                          v-if="segment.tag"
                          class="px-1 py-0.5 rounded text-[9px] font-bold shrink-0 mt-0.5"
                          :class="tagColor(segment.tag)"
                        >
                          {{ segment.tag }}
                        </span>
                        <span class="flex-1 text-slate-300">{{ segment.text }}</span>
                      </div>
                    </div>
                    <div class="mt-1">
                      <button
                        v-if="item.analyze?.detail"
                        @click.stop="openDetailModal(item)"
                        class="text-[10px] text-sky-400 hover:text-sky-300 transition-colors"
                      >
                        查看详情
                      </button>
                    </div>
                  </div>
                </div>
                <!-- 10日形态轨迹 -->
                <div class="flex items-center gap-1">
                  <span class="text-[9px] text-slate-500 mr-1 uppercase font-mono">10D:</span>
                  <div
                    v-for="(day, idx) in [...(item.analyze?.history || [])].reverse()"
                    :key="idx"
                    class="w-2.5 h-2.5 rounded-sm transition-transform hover:scale-125 cursor-help"
                    :class="dotColor(day.suggestion, day.tone)"
                    :title="`${day.date}: ${day.suggestion} (${day.tone})\n形态: ${day.patterns.join(',')}`"
                  ></div>
                </div>
              </div>
            </td>
            <td class="py-3 text-right">
              <button @click.stop="handleRemove(item.ts_code)" class="text-slate-500 hover:text-red-400 transition-colors px-2">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                </svg>
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Zen模式全屏覆盖 -->
    <div
      v-if="zenMode"
      class="fixed inset-0 z-50 bg-black flex flex-col items-center justify-center"
      @click="zenMode = false"
    >
      <div class="text-center" @click.stop>
        <div class="text-[10px] text-gray-600 mb-12">轻触空白处退出</div>
        <div class="flex flex-wrap justify-center gap-12 max-w-5xl">
          <div
            v-for="item in rows"
            :key="item.ts_code"
            class="flex flex-col items-center min-w-[100px]"
          >
            <div class="text-lg font-medium text-white mb-1">
              {{ item.ts_code.replace('.SH', '').replace('.SZ', '') }}
            </div>
            <div class="text-3xl font-mono text-white">
              {{ fmt(item.price, 2) }}
            </div>
            <div class="text-sm font-mono mt-1" :class="item.pct >= 0 ? 'text-red-400' : 'text-green-400'">
              {{ item.pct >= 0 ? '+' : '' }}{{ fmt(item.pct, 2, '%') }}
            </div>
          </div>
        </div>
        <div v-if="!rows.length" class="text-gray-500 text-lg mt-20">
          暂无自选股
        </div>
      </div>
    </div>

    <!-- K线悬浮窗 -->
    <div
      ref="klinePopupRef"
      v-if="hoverInfo.visible && !zenMode"
      class="fixed z-40 left-2 right-2 bottom-14 md:left-auto md:right-4 md:bottom-4 md:w-[70vw] md:max-w-3xl bg-business-dark border border-business-light rounded-2xl p-2.5 md:p-3 shadow-2xl max-h-[78vh] overflow-y-auto"
      @click.stop
    >
      <div class="flex flex-wrap items-center justify-between gap-2 mb-2">
        <div>
          <h3 class="text-xs font-bold text-slate-200">K线分析（悬浮预览）</h3>
          <p class="text-[10px] text-slate-500 mt-1">{{ hoverInfo.stock?.name || '-' }} · {{ hoverInfo.stock?.ts_code || '-' }}</p>
        </div>
      </div>
      <div v-if="klineLoading" class="h-[220px] sm:h-[280px] md:h-[340px] flex items-center justify-center text-slate-400 text-xs">K线加载中...</div>
      <div v-else-if="klineError" class="rounded border border-red-700/40 bg-red-500/10 px-3 py-2 text-xs text-red-300">{{ klineError }}</div>
      <template v-else>
        <div class="grid grid-cols-3 sm:grid-cols-6 gap-1.5 mb-2 text-[10px]" v-if="latestKlineData">
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
            <span class="text-slate-500">收盘价</span>
            <p class="text-slate-200 mt-0.5 font-mono">{{ fmt(latestKlineData.close, 2) }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
            <span class="text-slate-500">成交量</span>
            <p class="text-slate-200 mt-0.5 font-mono">{{ fmtVol(latestKlineData.vol) }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
            <span class="text-slate-500">成交额</span>
            <p class="text-slate-200 mt-0.5 font-mono">{{ fmtAmount(latestKlineData.amount) }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
            <span class="text-slate-500">主力流入</span>
            <p class="mt-0.5 font-mono" :class="latestKlineData.net_mf_vol >= 0 ? 'text-cyan-400' : 'text-rose-400'">{{ fmtMf(latestKlineData.net_mf_vol) }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
            <span class="text-slate-500">MA5/10/20</span>
            <p class="text-slate-200 mt-0.5 font-mono text-[9px]">{{ fmt(latestKlineData.ma5, 2) }} / {{ fmt(latestKlineData.ma10, 2) }} / {{ fmt(latestKlineData.ma20, 2) }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
            <span class="text-slate-500">融资余额</span>
            <p class="text-slate-200 mt-0.5 font-mono">{{ latestKlineData.rzye ? fmtRzye(latestKlineData.rzye) : '-' }}</p>
          </div>
        </div>
        <div class="h-[220px] sm:h-[280px] md:h-[340px] lg:h-[380px]">
          <v-chart :option="klineOption" autoresize />
        </div>
      </template>
    </div>

    <!-- 专业点评详情弹窗 -->
    <div 
      v-if="detailModal.visible"
      class="fixed inset-0 z-[200] bg-black/80 flex items-center justify-center p-2"
      @click="closeDetailModal"
    >
      <div 
        class="bg-[#1a1a2e] border border-slate-600 rounded-xl w-full max-w-lg max-h-[85vh] overflow-hidden flex flex-col"
        @click.stop
      >
        <div class="flex items-center justify-between p-4 border-b border-slate-700">
          <h3 class="text-white font-bold text-sm">专业分析详情</h3>
          <button @click="closeDetailModal" class="text-slate-400 hover:text-white">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        
        <div class="overflow-y-auto p-4 space-y-4 flex-1">
          <!-- 基本信息 -->
          <div class="flex items-center gap-3 pb-3 border-b border-slate-700/50">
            <div class="text-lg font-bold text-white">{{ detailModal.stock?.ts_code }}</div>
            <div class="text-slate-300 font-medium">{{ detailModal.stock?.name }}</div>
            <div class="ml-auto text-right">
              <div class="text-white font-mono">{{ fmt(detailModal.stock?.price, 2) }}</div>
              <div class="text-xs font-mono" :class="numColor(detailModal.stock?.pct)">{{ fmt(detailModal.stock?.pct, 2, '%') }}</div>
            </div>
          </div>

          <!-- 机构视角 -->
          <div v-if="detailModal.detail?.institution?.length">
            <h4 class="text-blue-400 font-semibold text-xs uppercase mb-2 flex items-center gap-1">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
              </svg>
              机构视角
            </h4>
            <div class="space-y-2">
              <div 
                v-for="(item, idx) in detailModal.detail.institution" 
                :key="idx"
                class="bg-slate-800/50 rounded-lg p-3"
              >
                <div class="flex items-center gap-2 mb-1">
                  <span 
                    class="px-1.5 py-0.5 rounded text-[10px] font-bold"
                    :class="getLevelBg(item.level)"
                  >{{ item.title }}</span>
                </div>
                <p class="text-slate-300 text-xs leading-relaxed">{{ item.desc }}</p>
              </div>
            </div>
          </div>

          <!-- 游资视角 -->
          <div v-if="detailModal.detail?.hotmoney?.length">
            <h4 class="text-orange-400 font-semibold text-xs uppercase mb-2 flex items-center gap-1">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />
              </svg>
              游资视角
            </h4>
            <div class="space-y-2">
              <div 
                v-for="(item, idx) in detailModal.detail.hotmoney" 
                :key="idx"
                class="bg-slate-800/50 rounded-lg p-3"
              >
                <div class="flex items-center gap-2 mb-1">
                  <span 
                    class="px-1.5 py-0.5 rounded text-[10px] font-bold"
                    :class="getLevelBg(item.level)"
                  >{{ item.title }}</span>
                </div>
                <p class="text-slate-300 text-xs leading-relaxed">{{ item.desc }}</p>
              </div>
            </div>
          </div>

          <!-- 形态信号 -->
          <div v-if="detailModal.detail?.patterns?.length">
            <h4 class="text-purple-400 font-semibold text-xs uppercase mb-2 flex items-center gap-1">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
              </svg>
              形态信号
            </h4>
            <div class="space-y-2">
              <div 
                v-for="(item, idx) in detailModal.detail.patterns" 
                :key="idx"
                class="bg-slate-800/50 rounded-lg p-3"
              >
                <div class="flex items-center gap-2 mb-1">
                  <span class="text-purple-300 font-bold text-xs">{{ item.pattern }}</span>
                  <span 
                    class="px-1.5 py-0.5 rounded text-[10px] font-bold"
                    :class="getLevelBg(item.level)"
                  >{{ item.type }}</span>
                </div>
                <p class="text-slate-300 text-xs leading-relaxed">{{ item.desc }}</p>
              </div>
            </div>
          </div>

          <!-- 风险提示 -->
          <div v-if="detailModal.detail?.risk_alert?.length">
            <h4 class="text-red-400 font-semibold text-xs uppercase mb-2 flex items-center gap-1">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
              风险提示
            </h4>
            <div class="space-y-2">
              <div 
                v-for="(item, idx) in detailModal.detail.risk_alert" 
                :key="idx"
                class="bg-red-900/20 border border-red-500/30 rounded-lg p-3"
              >
                <div class="flex items-center gap-2 mb-1">
                  <span class="text-red-300 font-bold text-xs">{{ item.title }}</span>
                </div>
                <p class="text-slate-300 text-xs leading-relaxed">{{ item.desc }}</p>
              </div>
            </div>
          </div>

          <!-- 技术指标摘要 -->
          <div v-if="detailModal.detail?.technical" class="pt-2 border-t border-slate-700/50">
            <h4 class="text-slate-400 font-semibold text-xs uppercase mb-2">技术指标</h4>
            <div class="grid grid-cols-2 gap-2 text-xs">
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">收盘价</span>
                <div class="text-white font-mono">{{ fmt(detailModal.detail.technical.close, 2) }}</div>
              </div>
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">涨跌幅</span>
                <div class="font-mono" :class="numColor(detailModal.detail.technical.pct_today)">{{ fmt(detailModal.detail.technical.pct_today, 2, '%') }}</div>
              </div>
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">成交量比</span>
                <div class="text-white font-mono">{{ ((detailModal.detail.technical.vol_ratio_5 || 0) * 100).toFixed(0) }}%</div>
              </div>
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">换手率</span>
                <div class="text-white font-mono">{{ detailModal.detail.technical.turnover ? fmt(detailModal.detail.technical.turnover, 1, '%') : '-' }}</div>
              </div>
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">MA5</span>
                <div class="text-white font-mono">{{ detailModal.detail.technical.ma5 ? fmt(detailModal.detail.technical.ma5, 2) : '-' }}</div>
              </div>
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">MA20</span>
                <div class="text-white font-mono">{{ detailModal.detail.technical.ma20 ? fmt(detailModal.detail.technical.ma20, 2) : '-' }}</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref, reactive, computed } from 'vue';
import { getWatchlistRealtime, addToWatchlist, removeFromWatchlist, getStockKline, searchStocks } from '@/services/api';
import { use } from 'echarts/core';
import { CanvasRenderer } from 'echarts/renderers';
import { CandlestickChart, LineChart, BarChart } from 'echarts/charts';
import { GridComponent, TooltipComponent, DataZoomComponent, LegendComponent, VisualMapComponent } from 'echarts/components';
import VChart from 'vue-echarts';
import { useKlineChart } from '@/composables/useKlineChart';

use([
  CanvasRenderer, CandlestickChart, LineChart, BarChart,
  GridComponent, TooltipComponent, DataZoomComponent, LegendComponent, VisualMapComponent
]);

const { createKlineOption, getLatestKlineData } = useKlineChart();

const REFRESH_MS_TRADING = 10000;
const REFRESH_MS_IDLE = 60000;
const REFRESH_MS_ZEN = 10000;

const loading = ref(false);
const isTradingTime = ref(false);
const message = ref('');
const rows = ref([]);
const newCode = ref('');
const expandedComments = ref({});  // 展开的专业点评

// Zen模式
const zenMode = ref(false);

const toggleZenMode = () => {
  zenMode.value = !zenMode.value;
  if (zenMode.value) {
    startAutoRefresh();
  }
};

const handleKeydown = (e) => {
  if (e.key === 'Escape' && zenMode.value) {
    zenMode.value = false;
  }
};

// 搜索功能
const searchResults = ref([]);
const searchLoading = ref(false);
const showSearchResults = ref(false);
const klinePopupRef = ref(null);
let searchTimer = null;
let timer = null;

const handleSearchInput = () => {
  if (searchTimer) clearTimeout(searchTimer);
  const q = newCode.value.trim();
  if (q.length < 1) {
    searchResults.value = [];
    showSearchResults.value = false;
    return;
  }
  searchLoading.value = true;
  searchTimer = setTimeout(async () => {
    try {
      const res = await searchStocks(q, 8);
      searchResults.value = res.data?.data || [];
      showSearchResults.value = searchResults.value.length > 0;
    } catch (e) {
      console.error('搜索失败', e);
      searchResults.value = [];
    } finally {
      searchLoading.value = false;
    }
  }, 300);
};

const selectSearchResult = (stock) => {
  newCode.value = stock.ts_code;
  showSearchResults.value = false;
  handleAdd();
};

const hideSearchResults = () => {
  setTimeout(() => {
    showSearchResults.value = false;
  }, 200);
};

const detailModal = reactive({
  visible: false,
  stock: null,
  detail: null
});

const openDetailModal = (stock) => {
  detailModal.stock = stock || null;
  detailModal.detail = stock?.analyze?.detail || null;
  detailModal.visible = true;
};

const closeDetailModal = () => {
  detailModal.visible = false;
  detailModal.stock = null;
  detailModal.detail = null;
};

// 悬浮窗状态
const hoverInfo = reactive({
  visible: false,
  x: 0,
  y: 0,
  stock: null,
  klineData: []
});
const klineLoading = ref(false);
const klineError = ref('');

const showKline = async (stock, event) => {
  const WINDOW_WIDTH = 520;
  const WINDOW_HEIGHT = 420;
  
  const mouseX = event.clientX;
  const mouseY = event.clientY;
  
  let x = mouseX + 20;
  let y = mouseY - 100;
  
  if (x + WINDOW_WIDTH > window.innerWidth) {
    x = mouseX - WINDOW_WIDTH - 20;
  }
  if (x < 10) x = 10;
  if (y + WINDOW_HEIGHT > window.innerHeight) {
    y = window.innerHeight - WINDOW_HEIGHT - 10;
  }
  if (y < 10) y = 10;
  
  hoverInfo.x = x;
  hoverInfo.y = y;
  hoverInfo.stock = stock;
  hoverInfo.visible = true;
  hoverInfo.klineData = [];
  klineLoading.value = true;
  klineError.value = '';

  try {
    const res = await getStockKline(stock.ts_code, 80);
    hoverInfo.klineData = res.data?.data || [];
  } catch (e) {
    console.error('K线加载失败', e);
    klineError.value = e?.response?.data?.detail || e?.message || 'K线加载失败';
  } finally {
    klineLoading.value = false;
  }
};

const toggleKline = async (stock, event) => {
  if (hoverInfo.visible && hoverInfo.stock?.ts_code === stock?.ts_code) {
    hideKline();
    return;
  }
  await showKline(stock, event);
};

const hideKline = () => {
  hoverInfo.visible = false;
  klineLoading.value = false;
  klineError.value = '';
};

const handleDocumentClick = (e) => {
  if (!hoverInfo.visible || zenMode.value) return;
  const target = e.target;
  if (klinePopupRef.value && klinePopupRef.value.contains(target)) return;
  if (target?.closest?.('[data-kline-trigger="1"]')) return;
  hideKline();
};

const klineOption = computed(() => {
  return createKlineOption(hoverInfo.klineData, { showLegend: true, showDataZoom: true, marginType: 'mf' });
});

const latestKlineData = computed(() => {
  return getLatestKlineData(hoverInfo.klineData);
});

const fmtVol = (v) => {
  if (!v || !Number.isFinite(v)) return '-';
  if (Math.abs(v) >= 1e8) return `${(v / 1e8).toFixed(2)}亿手`;
  if (Math.abs(v) >= 1e4) return `${(v / 1e4).toFixed(2)}万手`;
  return `${Math.round(v)}手`;
};

const fmtAmount = (v) => {
  if (!v || !Number.isFinite(v)) return '-';
  return `${v.toFixed(2)}亿`;
};

const fmtMf = (v) => {
  if (!v || !Number.isFinite(v)) return '-';
  const sign = v >= 0 ? '+' : '';
  return `${sign}${v.toFixed(2)}亿`;
};

const fmtRzye = (v) => {
  if (!v || !Number.isFinite(v)) return '-';
  return `${v.toFixed(2)}亿`;
};

const fmt = (v, digits = 2, suffix = '') => {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return `${n.toFixed(digits)}${suffix}`;
};

const numColor = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n) || n === 0) return 'text-slate-300';
  return n > 0 ? 'text-red-400' : 'text-emerald-400';
};

const parseCommentary = (summary) => {
  if (!summary) return [{ tag: null, text: '-' }];

  // 按 | 分割不同视角
  const segments = summary.split('|').map(s => s.trim()).filter(Boolean);

  return segments.map(seg => {
    // 提取标签 【机构】【游资】【形态】
    const tagMatch = seg.match(/^【(.+?)】(.+)$/);
    if (tagMatch) {
      return {
        tag: tagMatch[1],
        text: tagMatch[2].trim()
      };
    }
    return {
      tag: null,
      text: seg
    };
  });
};

const tagColor = (tag) => {
  if (tag === '机构') return 'bg-blue-500/20 text-blue-400 border border-blue-500/30';
  if (tag === '游资') return 'bg-orange-500/20 text-orange-400 border border-orange-500/30';
  if (tag === '形态') return 'bg-purple-500/20 text-purple-400 border border-purple-500/30';
  return 'bg-slate-700 text-slate-300';
};

const getLevelBg = (level) => {
  const normalized = String(level || '').toLowerCase();
  if (['extreme', 'strong', 'high'].includes(normalized)) return 'bg-red-500/20 text-red-300 border border-red-500/40';
  if (['medium', 'neutral'].includes(normalized)) return 'bg-amber-500/20 text-amber-300 border border-amber-500/40';
  if (['warning', 'danger', 'bearish'].includes(normalized)) return 'bg-rose-500/20 text-rose-300 border border-rose-500/40';
  return 'bg-slate-700/80 text-slate-300 border border-slate-600';
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
    clearTimeout(timer);
    timer = null;
  }
};

const startAutoRefresh = () => {
  stopAutoRefresh();
  let interval;
  if (zenMode.value) {
    interval = REFRESH_MS_ZEN;
  } else {
    interval = isTradingTime.value ? REFRESH_MS_TRADING : REFRESH_MS_IDLE;
  }
  timer = setTimeout(() => {
    refreshNow(false);
  }, interval);
};

const refreshNow = async (showLoading = true) => {
  // 如果当前已经在加载中，且不是强制显示 loading 的请求，则跳过
  if (loading.value && !showLoading) return;
  
  if (showLoading) loading.value = true;
  try {
    const res = await getWatchlistRealtime(null, 'sina');
    const body = res.data || {};
    rows.value = body.data || [];
    isTradingTime.value = !!body.is_trading_time;
    message.value = body.message || '';
  } catch (e) {
    message.value = e?.response?.data?.detail || e?.message || '刷新失败';
  } finally {
    loading.value = false;
    // 无论成功还是失败，在结束后开启下一次计时
    startAutoRefresh();
  }
};

const handleAdd = async () => {
  if (!newCode.value) return;
  try {
    message.value = '正在添加...';
    await addToWatchlist({ ts_code: newCode.value });
    newCode.value = '';
    message.value = '添加成功';
    await refreshNow(true);
  } catch (e) {
    message.value = '添加失败: ' + (e.response?.data?.detail || e.message);
  }
};

const handleRemove = async (tsCode) => {
  if (!confirm(`确定要从自选删除 ${tsCode} 吗？`)) return;
  try {
    await removeFromWatchlist(tsCode);
    await refreshNow(true);
  } catch (e) {
    alert('删除失败: ' + e.message);
  }
};

onMounted(async () => {
  window.addEventListener('keydown', handleKeydown);
  document.addEventListener('click', handleDocumentClick, true);
  await refreshNow(true);
});

onBeforeUnmount(() => {
  window.removeEventListener('keydown', handleKeydown);
  document.removeEventListener('click', handleDocumentClick, true);
  stopAutoRefresh();
});
</script>

<style scoped>
</style>
