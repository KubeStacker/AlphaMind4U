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
           <button @click="handleOpenBacktest" class="flex items-center space-x-1 px-3 py-1 bg-business-accent/20 text-business-accent rounded hover:bg-business-accent hover:text-white transition-colors border border-business-accent/30">
              <PresentationChartLineIcon class="w-3 h-3" />
              <span>历史验证</span>
           </button>
           <div class="flex items-center space-x-2">
             <span class="px-2 py-0.5 bg-red-500/20 text-red-400 rounded">85+ 沸腾/止盈</span>
             <span class="px-2 py-0.5 bg-blue-500/20 text-blue-400 rounded">25- 冰点/潜伏</span>
           </div>
        </div>
      </div>
      <div class="h-40 w-full">
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
          </div>
          <div class="flex items-center space-x-2">
            <button @click="isTooltipLocked = false" v-if="isTooltipLocked" class="text-[9px] font-bold text-business-accent bg-business-accent/10 px-2 py-0.5 rounded border border-business-accent/20 transition-all hover:bg-business-accent hover:text-white">解锁</button>
            <span class="text-[9px] text-slate-500 font-medium bg-slate-800/50 px-2 py-1 rounded border border-slate-700">点击锁定浮窗</span>
          </div>
        </div>
        <div class="h-80 w-full relative">
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
        <div class="flex-1 min-h-[300px]">
          <v-chart :option="hotConceptsChartOption" autoresize @click="handlePieClick" />
        </div>
        <p class="text-center text-[9px] text-slate-500 font-medium mt-4 italic">点击板块切片可自动筛选下方推荐</p>
      </div>
    </div>

    <!-- 策略控制中心 (列表显示) -->
    <div id="strategy-control-center" class="bg-business-dark rounded-2xl shadow-business border border-business-light overflow-hidden mx-2 md:mx-0">
      <div class="p-4 md:p-5 border-b border-business-light bg-slate-900/20 flex flex-col lg:flex-row lg:items-center justify-between gap-4">
        <div class="flex items-center space-x-3">
          <div class="w-1 h-6 bg-business-accent rounded-full"></div>
          <h2 class="text-base font-bold text-white">智能策略推荐</h2>
        </div>
        
        <div class="flex flex-wrap items-center gap-2">
          <input v-model="filterDate" type="date" class="bg-business-darker border border-business-light rounded-lg px-2 py-1.5 text-xs font-bold text-white outline-none focus:border-business-accent w-28 sm:w-32" />
          <select v-model="selectedConcept" class="bg-business-darker border border-business-light rounded-lg px-2 py-1.5 text-xs font-bold text-business-highlight outline-none focus:border-business-accent appearance-none min-w-[80px] sm:min-w-[100px]">
            <option :value="null">全市场</option>
            <option v-for="c in hotConcepts" :key="c" :value="c">{{ c }}</option>
          </select>
          <div class="flex items-center gap-2 w-full sm:w-auto mt-2 sm:mt-0">
            <button @click="handleFetch" class="flex-1 sm:flex-none h-8 px-3 sm:px-4 bg-business-accent text-white rounded-lg text-[10px] font-bold uppercase tracking-wider hover:brightness-110 transition-all active:scale-95 shadow-lg shadow-business-accent/20">
              获取推荐
            </button>
            <button @click="handleBacktest" :disabled="backtesting" class="flex-1 sm:flex-none h-8 px-3 sm:px-4 bg-business-success text-white rounded-lg text-[10px] font-bold uppercase tracking-wider hover:brightness-110 transition-all active:scale-95 shadow-lg shadow-business-success/20">
              {{ backtesting ? '分析中' : '收益验证' }}
            </button>
          </div>
        </div>
      </div>

      <div class="overflow-x-auto">
        <table class="w-full text-left border-collapse min-w-[700px]">
          <thead>
            <tr class="text-slate-500 text-[10px] font-bold uppercase tracking-widest border-b border-business-light bg-slate-900/40">
              <th class="px-5 py-3">资产标识</th>
              <th class="px-5 py-3">板块</th>
              <th class="px-5 py-3 text-center">评分</th>
              <th class="px-5 py-3 text-right">5日表现</th>
              <th class="px-5 py-3 text-right">10日表现</th>
              <th class="px-5 py-3 text-center">状态</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-business-light/20">
            <tr v-for="stock in recommendations" :key="stock.ts_code" class="group hover:bg-white/5 transition-all">
              <td class="px-5 py-4">
                <a :href="`http://stockpage.10jqka.com.cn/${stock.ts_code.split('.')[0]}`" target="_blank" class="text-white font-bold text-sm hover:text-business-highlight flex items-center">
                  {{ stock.name }}
                  <ArrowTopRightOnSquareIcon class="w-3 h-3 ml-1 opacity-0 group-hover:opacity-100 transition-all" />
                </a>
                <div class="text-[9px] text-slate-500 font-medium">{{ stock.ts_code }}</div>
              </td>
              <td class="px-5 py-4">
                <span class="px-2 py-0.5 bg-slate-800 text-slate-400 rounded-md text-[9px] font-bold border border-slate-700">
                  {{ stock.industry }}
                </span>
              </td>
              <td class="px-5 py-4 text-center">
                <span class="text-sm font-bold text-business-highlight">{{ stock.score }}</span>
              </td>
              <td class="px-5 py-4 text-right font-bold text-xs tabular-nums" :class="getPriceColor(stock.p5_return)">
                {{ formatPrice(stock.p5_return) }}
              </td>
              <td class="px-5 py-4 text-right font-bold text-xs tabular-nums" :class="getPriceColor(stock.p10_return)">
                {{ formatPrice(stock.p10_return) }}
              </td>
              <td class="px-5 py-4 text-center">
                <div v-if="stock.p5_return !== undefined" class="flex items-center justify-center space-x-1">
                  <div class="w-1 h-1 rounded-full" :class="stock.p5_return !== null ? 'bg-business-success' : 'bg-slate-600'"></div>
                  <span class="text-[9px] font-bold" :class="stock.p5_return !== null ? 'text-business-success' : 'text-slate-600'">
                    {{ stock.p5_return !== null ? 'OK' : 'PEND' }}
                  </span>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
        
        <div v-if="loading" class="p-12 flex flex-col items-center justify-center space-y-3">
          <div class="w-6 h-6 border-2 border-business-light border-t-business-accent rounded-full animate-spin"></div>
          <span class="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Processing...</span>
        </div>

        <div v-if="!loading && recommendations.length === 0" class="p-12 flex flex-col items-center justify-center text-slate-500 space-y-2">
          <InboxIcon class="w-10 h-10 opacity-20" />
          <p class="text-xs font-bold uppercase tracking-widest">暂无符合条件的筛选结果</p>
        </div>
      </div>
    </div>

    <!-- Backtest Modal -->
    <Dialog :open="showBacktestModal" @close="showBacktestModal = false" class="relative z-50">
      <div class="fixed inset-0 bg-business-darker/90 backdrop-blur-sm" />
      <div class="fixed inset-0 flex items-center justify-center p-2 sm:p-4">
        <DialogPanel class="w-full max-w-4xl rounded-2xl bg-business-dark border border-business-light p-4 sm:p-6 shadow-2xl overflow-hidden flex flex-col max-h-[95vh] sm:max-h-[90vh]">
          <div class="flex justify-between items-start sm:items-center mb-4 sm:mb-6 shrink-0">
             <div class="flex items-center space-x-3">
                <div class="w-1.5 h-6 bg-business-accent rounded-full shadow-[0_0_8px_rgba(56,189,248,0.5)]"></div>
                <div>
                   <DialogTitle class="text-base sm:text-xl font-bold text-white tracking-tight">科创50情绪策略验证</DialogTitle>
                   <p class="text-[9px] sm:text-[10px] text-slate-500 font-mono mt-0.5">CAPITAL: ¥100k | BENCHMARK: STAR50</p>
                </div>
             </div>
             <button @click="showBacktestModal = false" class="p-1.5 text-slate-500 hover:text-white rounded-full hover:bg-white/10 transition-colors">
                <span class="sr-only">Close</span>
                <svg class="w-5 h-5 sm:w-6 sm:h-6" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" /></svg>
             </button>
          </div>

          <div v-if="backtestLoading" class="flex-1 flex flex-col items-center justify-center space-y-4 min-h-[300px]">
             <div class="w-10 h-10 border-4 border-business-light border-t-business-accent rounded-full animate-spin"></div>
             <div class="text-[10px] font-bold text-slate-500 uppercase tracking-widest animate-pulse">Running Simulation...</div>
          </div>

          <div v-else class="flex-1 flex flex-col overflow-hidden">
             <!-- KPI Panel -->
             <div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-2 sm:gap-3 mb-4 sm:mb-6 shrink-0">
                <div class="bg-slate-900/50 p-2 sm:p-3 rounded-xl border border-business-light/30 flex flex-col items-center">
                   <span class="text-[8px] sm:text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-1">当前总资产</span>
                   <span class="text-sm sm:text-lg font-bold font-mono text-business-highlight">{{ backtestMetrics.final_value }}</span>
                </div>
                <div class="bg-slate-900/50 p-2 sm:p-3 rounded-xl border border-business-light/30 flex flex-col items-center">
                   <span class="text-[8px] sm:text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-1">策略总收益</span>
                   <span class="text-sm sm:text-lg font-bold font-mono" :class="parseFloat(backtestMetrics.total_return) >= 0 ? 'text-business-success' : 'text-business-danger'">{{ backtestMetrics.total_return }}</span>
                </div>
                <div class="bg-slate-900/50 p-2 sm:p-3 rounded-xl border border-business-light/30 flex flex-col items-center">
                   <span class="text-[8px] sm:text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-1">最大回撤</span>
                   <span class="text-sm sm:text-lg font-bold font-mono text-white">{{ backtestMetrics.max_drawdown }}</span>
                </div>
                <div class="bg-slate-900/50 p-2 sm:p-3 rounded-xl border border-business-light/30 flex flex-col items-center">
                   <span class="text-[8px] sm:text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-1">胜率</span>
                   <span class="text-sm sm:text-lg font-bold font-mono text-amber-500">{{ backtestMetrics.win_rate }}</span>
                </div>
                <div class="bg-slate-900/50 p-2 sm:p-3 rounded-xl border border-business-light/30 flex flex-col items-center col-span-2 sm:col-span-1">
                   <span class="text-[8px] sm:text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-1">基准收益</span>
                   <span class="text-sm sm:text-lg font-bold font-mono text-slate-400">{{ backtestMetrics.benchmark_return }}</span>
                </div>
             </div>

             <!-- Chart -->
             <div class="flex-1 min-h-[250px] sm:min-h-0 relative bg-slate-900/20 rounded-xl border border-business-light/20 p-2">
                <v-chart :option="backtestOption" autoresize />
             </div>
             
             <div class="mt-3 sm:mt-4 shrink-0 flex flex-col sm:flex-row sm:items-center justify-between text-[8px] sm:text-[10px] text-slate-500 px-1 gap-2">
                <div class="flex space-x-4">
                   <span class="flex items-center"><span class="w-1.5 h-1.5 sm:w-2 sm:h-2 rounded-full bg-business-success mr-1.5"></span> Buy (100% / 50%)</span>
                   <span class="flex items-center"><span class="w-1.5 h-1.5 sm:w-2 sm:h-2 rounded-full bg-business-danger mr-1.5"></span> Sell / Stop</span>
                </div>
                <div class="italic opacity-50">Parameters: Fee 0.15% (Trade at Close)</div>
             </div>
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  </div>
</template>

<script setup>
import { ref, onMounted, reactive, computed } from 'vue';
import { Dialog, DialogPanel, DialogTitle } from '@headlessui/vue';
import { getRecommendations, getHotConcepts, triggerBacktest, getBacktestResults, getMainlineHistory, getMarketSentiment, getStar50Backtest } from '@/services/api';
import { 
  ArrowTopRightOnSquareIcon, 
  ChatBubbleLeftRightIcon,
  InboxIcon,
  PresentationChartLineIcon
} from '@heroicons/vue/20/solid'

const loading = ref(false);
const loadingTrend = ref(false);
const recommendations = ref([]);
const hotConcepts = ref([]);
const selectedConcept = ref(null);
const filterDate = ref(null);
const currentRecDate = ref('');
const macroSentiment = ref('');
const strategyConclusion = ref('');
const trendAnalysis = ref('');
const backtesting = ref(false);
const marketStatus = ref({ text: 'STANDBY', color: 'text-slate-500' });
const isTooltipLocked = ref(false);

// Backtest Modal State
const showBacktestModal = ref(false);
const backtestLoading = ref(false);
const backtestMetrics = ref({
  total_return: '--',
  max_drawdown: '--',
  win_rate: '--',
  benchmark_return: '--',
  final_value: '--'
});
const backtestOption = ref({});

const handleOpenBacktest = async () => {
  showBacktestModal.value = true;
  if (backtestOption.value.series && backtestOption.value.series.length > 0) return; 

  backtestLoading.value = true;
  try {
    const res = await getStar50Backtest();
    const data = res.data;
    if (!data || !data.metrics) return;

    backtestMetrics.value = data.metrics;
    
    const dates = data.curves.map(c => c.trade_date);
    const stratVals = data.curves.map(c => c.strategy_nav);
    const benchVals = data.curves.map(c => c.benchmark_nav);
    const positions = data.curves.map(c => ({
       value: c.strategy_nav,
       position: c.position,
       date: c.trade_date
    }));

    const markPoints = [];
    let lastPos = 0;
    data.curves.forEach((c, i) => {
        if (c.position > lastPos && c.position > 0) {
           markPoints.push({ name: 'Buy', coord: [i, c.strategy_nav], value: 'B', itemStyle: { color: '#ef4444' } });
        } else if (c.position < lastPos) {
           markPoints.push({ name: 'Sell', coord: [i, c.strategy_nav], value: 'S', itemStyle: { color: '#10b981' }, symbolRotate: 180, symbolOffset: [0, 10] });
        }
        lastPos = c.position;
    });

    backtestOption.value = {
      backgroundColor: 'transparent',
      tooltip: { 
         trigger: 'axis',
         axisPointer: { type: 'cross' },
         backgroundColor: 'rgba(15, 23, 42, 0.95)',
         borderColor: '#334155',
         textStyle: { color: '#cbd5e1' },
         formatter: (params) => {
            let html = `<div class="font-bold border-b border-slate-700 pb-1 mb-1">${params[0].name}</div>`;
            params.forEach(p => {
               const val = p.value.toLocaleString('zh-CN', { style: 'currency', currency: 'CNY' });
               html += `<div class="flex justify-between w-48"><span style="color:${p.color}">${p.seriesName}</span> <span class="font-bold text-white">${val}</span></div>`;
            });
            const posItem = positions.find(p => p.date === params[0].name);
            if (posItem) {
               html += `<div class="mt-1 pt-1 border-t border-slate-700 flex justify-between"><span class="text-slate-400">模拟持仓</span> <span class="font-bold text-business-highlight">${posItem.position * 100}%</span></div>`;
            }
            return html;
         }
      },
      legend: { textStyle: { color: '#94a3b8' }, top: 0 },
      grid: { top: 40, left: 20, right: 20, bottom: 30, containLabel: true },
      xAxis: { type: 'category', data: dates, axisLabel: { color: '#64748b', fontSize: 10 } },
      yAxis: { 
        type: 'value', 
        scale: true, 
        splitLine: { lineStyle: { color: '#334155', type: 'dashed' } }, 
        axisLabel: { 
          color: '#64748b', 
          fontSize: 9,
          formatter: (v) => (v / 10000).toFixed(1) + 'w' 
        } 
      },
      series: [
        {
          name: '情绪增强策略',
          type: 'line',
          data: stratVals,
          smooth: true,
          lineStyle: { width: 3, color: '#f43f5e', shadowBlur: 10, shadowColor: 'rgba(244, 63, 94, 0.3)' },
          areaStyle: { 
            opacity: 0.1, 
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [{ offset: 0, color: '#f43f5e' }, { offset: 1, color: 'transparent' }]
            }
          },
          markPoint: { symbol: 'pin', symbolSize: 20, data: markPoints, label: { fontSize: 8, fontWeight: 'bold' } }
        },
        {
          name: '上证指数基准',
          type: 'line',
          data: benchVals,
          showSymbol: false,
          lineStyle: { width: 1.5, type: 'dashed', color: '#94a3b8' }
        }
      ]
    };

  } catch (e) {
    console.error("Backtest Error", e);
  } finally {
    backtestLoading.value = false;
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
      show: false, // 隐藏坐标系
      splitLine: { show: false }, 
      axisLine: { show: false }, 
      axisLabel: { show: false },
      scale: true 
    }
  ],
  series: []
});

const handleChartClick = (params) => {
  isTooltipLocked.value = !isTooltipLocked.value;
};

const handlePieClick = (params) => {
  if (params.name) {
    selectedConcept.value = params.name;
    handleFetch();
    document.getElementById('strategy-control-center')?.scrollIntoView({ behavior: 'smooth' });
  }
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
    const data = res.data; // { dates, sentiment, index }
    
    if (!data.dates || data.dates.length === 0) return;
    
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

    marketSentimentChartOption.value = {
      ...marketSentimentChartOption.value,
      tooltip: {
        ...marketSentimentChartOption.value.tooltip,
        formatter: (params) => {
          let html = `<div class="p-2 font-mono text-xs min-w-[180px]">`;
          params.forEach(p => {
            if (p.seriesName === '情绪评分') {
              const val = typeof p.value === 'object' ? p.value.value : p.value;
              const details = p.data?.details || {};
              const signal = details.signal || 'HOLD';
              
              // 优先使用数据项中的 label (中文)
              const displayLabel = p.data?.label || details.label || signal;
              
              html += `<div class="mb-2 border-b border-slate-700 pb-1 flex justify-between">
                        <span class="text-slate-400">${p.name} 指标</span>
                        <span class="font-bold text-amber-500 text-sm">${val}</span>
                      </div>`;
              
              html += `<div class="mb-2 px-2 py-1 bg-slate-800 rounded flex justify-between items-center">
                        <span class="text-[10px] text-slate-500">操作建议</span>
                        <span class="font-bold ${signal.includes('BUY') ? 'text-rose-500' : signal.includes('SELL') ? 'text-emerald-500' : 'text-slate-300'}">${displayLabel}</span>
                      </div>`;

              if (details.factors) {
                const f = details.factors;
                html += `<div class="grid grid-cols-2 gap-x-2 gap-y-1 mb-3 text-[10px] text-slate-500">
                          <div>赚钱效应: <span class="text-slate-300">${f.breadth}%</span></div>
                          <div>中位数涨幅: <span class="${f.median_chg > 0 ? 'text-rose-400' : 'text-emerald-400'}">${f.median_chg}%</span></div>
                          <div>指数涨幅: <span class="text-slate-300">${f.index_chg}%</span></div>
                          <div>净涨停: <span class="text-slate-300">${f.limit}</span></div>
                          <div>炸板压制: <span class="text-slate-300">${f.failure}</span></div>
                          <div>量能共振: <span class="text-slate-300">${f.vol}</span></div>
                        </div>`;
              }
              
              if (details.metrics) {
                const m = details.metrics;
                html += `<div class="mt-2 text-[10px] text-slate-400 border-t border-slate-800 pt-2 space-y-1">
                          <div class="flex justify-between items-center bg-blue-500/10 px-2 py-1 rounded mb-2">
                            <span class="text-blue-400 font-bold">操盘策略:</span> 
                            <span class="text-white font-bold ml-2">${m.advice || '全市场择优'}</span>
                          </div>
                          <div class="flex justify-between"><span>评分动量:</span> <span class="${m.score_delta > 0 ? 'text-rose-400' : 'text-emerald-400'}">${m.score_delta}</span></div>`;
                
                if (m.is_decoupling) {
                  html += `<div class="flex justify-between items-center bg-rose-500/10 px-1 rounded"><span class="text-rose-400 font-bold">剪刀差进攻:</span> <span class="text-rose-400">TRUE</span></div>`;
                }
                if (m.is_suppressed) {
                  html += `<div class="flex justify-between items-center bg-amber-500/10 px-1 rounded"><span class="text-amber-400 font-bold">主力压盘:</span> <span class="text-amber-400">TRUE</span></div>`;
                }

                if (m.limit_ups !== undefined) {
                   html += `<div class="flex justify-between"><span>涨停/跌停:</span> <span>${m.limit_ups} / ${m.limit_downs}</span></div>`;
                }
                html += `</div>`;
              }
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
        { ...marketSentimentChartOption.value.dataZoom[1], startValue: startValue, endValue: totalPoints - 1 }
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
          smooth: true,
          showSymbol: false,
          data: data.index,
          lineStyle: { color: '#64748b', width: 1, type: 'dashed' }
        }
      ]
    };
  } catch (e) {
    console.error("Sentiment Error", e);
  }
};

const handleFetch = async () => {
  loading.value = true;
  try {
    const params = {};
    if (filterDate.value) params.target_date = filterDate.value;
    if (selectedConcept.value) params.concept = selectedConcept.value;
    
    const response = await getRecommendations(params);
    if (response.data.status === 'success') {
      const recData = response.data.data;
      currentRecDate.value = response.data.date;
      macroSentiment.value = response.data.macro_sentiment;
      strategyConclusion.value = response.data.conclusion;
      
      if (!filterDate.value) {
        filterDate.value = currentRecDate.value;
      }
      
      const btRes = await getBacktestResults(currentRecDate.value);
      const btMap = {};
      if (btRes.data) btRes.data.forEach(r => { btMap[r.ts_code] = r; });
      
      recommendations.value = recData.map(item => ({
        ...item,
        p5_return: btMap[item.ts_code]?.p5_return ?? null,
        p10_return: btMap[item.ts_code]?.p10_return ?? null
      }));
    }
  } catch (error) {
    console.error("Fetch Error", error);
  } finally {
    loading.value = false;
  }
};

const handleBacktest = async () => {
  backtesting.value = true;
  try {
    await triggerBacktest();
    await handleFetch();
    alert("表现验证已更新");
  } catch (e) {
    alert("更新失败");
  } finally {
    backtesting.value = false;
  }
};

const formatPrice = (val) => {
  if (val === null || val === undefined) return '--';
  return (val > 0 ? '+' : '') + val.toFixed(2) + '%';
};

const getPriceColor = (val) => {
  if (val === null || val === undefined) return 'text-slate-600';
  return val >= 0 ? 'text-business-success' : 'text-business-danger';
};

onMounted(async () => {
  try {
    const hotRes = await getHotConcepts();
    hotConcepts.value = hotRes.data;
  } catch (e) {}
  handleFetch();
  fetchMainlineHistory();
  fetchMarketSentiment();
  
  const h = new Date().getHours();
  if (h >= 9 && h < 15) {
    marketStatus.value = { text: '正在交易', color: 'text-business-success' };
  } else {
    marketStatus.value = { text: '已休市', color: 'text-slate-500' };
  }
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
