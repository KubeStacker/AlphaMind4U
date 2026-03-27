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
            @click="refreshNow(true, true)"
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
            placeholder="输入代码/名称搜索 (例如 600519, 茅台)"
          />
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
            class="px-3 py-2 hover:bg-slate-700 cursor-pointer flex justify-between items-center border-b border-slate-700/50 last:border-0 group"
          >
            <div class="flex items-center gap-2">
              <span class="text-slate-200 font-mono text-xs">{{ stock.ts_code }}</span>
              <span class="text-slate-400 text-xs">{{ stock.name }}</span>
            </div>
            <button
              @mousedown.prevent.stop="selectSearchResult(stock)"
              class="opacity-0 group-hover:opacity-100 transition-opacity p-1 rounded hover:bg-emerald-500/20 text-emerald-400 hover:text-emerald-300"
              title="添加到自选"
            >
              <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
              </svg>
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- 行情列表 -->
    <div v-if="!zenMode" class="space-y-3">
      <!-- 表头 (仅桌面端) -->
      <div class="hidden md:grid md:grid-cols-12 gap-4 px-6 py-2 text-[10px] font-bold text-slate-500 uppercase tracking-wider border-b border-slate-800">
        <div class="col-span-2">标的信息</div>
        <div class="col-span-1 text-right">持仓/盈亏</div>
        <div class="col-span-1 text-right">最新行情</div>
        <div class="col-span-6">专业分析与 10D 历史</div>
        <div class="col-span-1 text-right">操作</div>
        <div class="col-span-1 text-right">AI</div>
      </div>

      <div v-if="!rows.length && !loading" class="bg-business-dark p-12 rounded-2xl border border-business-light text-center text-slate-500 text-xs">
        暂无自选股，请先添加
      </div>

      <!-- 总仓位汇总 -->
      <div v-if="totalPortfolioValue > 0" class="bg-business-dark rounded-2xl border border-purple-500/30 p-4 md:px-6">
        <div class="flex flex-wrap items-center justify-between gap-4">
          <div class="flex items-center gap-4">
            <span class="text-[10px] font-bold text-purple-400 uppercase tracking-wider">总仓位</span>
            <span class="text-sm font-mono font-bold text-white">{{ fmt(totalPortfolioValue, 0) }}</span>
          </div>
          <div class="flex items-center gap-6 text-[11px]">
            <div class="text-right">
              <span class="text-slate-500">总成本</span>
              <span class="ml-1 font-mono text-slate-300">{{ fmt(totalCost, 0) }}</span>
            </div>
            <div class="text-right">
              <span class="text-slate-500">总盈亏</span>
              <span class="ml-1 font-mono" :class="totalPnL >= 0 ? 'text-red-400' : 'text-emerald-400'">
                {{ totalPnL >= 0 ? '+' : '' }}{{ fmt(totalPnL, 0) }}
                <span class="text-[10px]">({{ totalPnLPct >= 0 ? '+' : '' }}{{ fmt(totalPnLPct, 2, '%') }})</span>
              </span>
            </div>
          </div>
        </div>
      </div>

      <!-- 股票行 -->
      <div
        v-for="item in rows"
        :key="item.ts_code"
        class="bg-business-dark rounded-2xl border border-business-light hover:border-business-accent/40 transition-all shadow-business group"
      >
        <div class="grid grid-cols-1 md:grid-cols-12 gap-3 md:gap-4 p-4 md:px-6 md:py-4 items-center">
          <!-- 1. 标的信息 -->
          <div class="col-span-2 flex items-center gap-3 md:block" data-kline-trigger="1" @click.stop="toggleKline(item, $event)">
            <div class="cursor-pointer">
              <div class="text-[13px] font-bold text-white group-hover:text-business-accent transition-colors">{{ item.name || '-' }}</div>
              <div class="text-[10px] text-slate-500 font-mono mt-0.5">{{ item.ts_code }}</div>
            </div>
            <!-- 移动端标签显示在名称旁 -->
            <span
              v-if="item.analyze?.suggestion"
              class="md:hidden px-1.5 py-0.5 rounded text-[9px] font-black"
              :class="suggestionColor(item.analyze.suggestion)"
            >
              {{ item.analyze.suggestion }}
            </span>
          </div>

          <!-- 2. 持仓/盈亏 -->
          <div class="col-span-1 flex md:flex-col justify-end items-center gap-1">
            <div v-if="holdings[item.ts_code]?.shares > 0" class="text-right">
              <div class="text-[11px] font-mono text-slate-300">{{ holdings[item.ts_code].shares }}股</div>
              <div class="text-[10px] font-mono text-slate-400">成本: {{ fmt(calcPnL(item)?.cost, 0) }}</div>
              <div class="text-[10px] font-mono" :class="calcPnL(item)?.pnl >= 0 ? 'text-red-400' : 'text-emerald-400'">
                {{ calcPnL(item)?.pnl >= 0 ? '+' : '' }}{{ fmt(calcPnL(item)?.pnl, 0) }}
                <span class="text-[9px]">({{ calcPnL(item)?.pnlPct >= 0 ? '+' : '' }}{{ fmt(calcPnL(item)?.pnlPct, 2, '%') }})</span>
              </div>
              <div v-if="totalPortfolioValue > 0" class="text-[9px] font-mono text-slate-500">
                占比 {{ fmt(calcPnL(item)?.current / totalPortfolioValue * 100, 1, '%') }}
              </div>
            </div>
            <button @click.stop="openHoldingModal(item)" class="text-[9px] text-slate-600 hover:text-purple-400 font-bold">
              {{ holdings[item.ts_code]?.shares > 0 ? '编辑' : '+持仓' }}
            </button>
          </div>

          <!-- 3. 行情数据 -->
          <div class="col-span-1 flex md:flex-col justify-end items-center gap-1">
            <div class="text-[13px] font-mono font-bold text-slate-100 text-right">{{ fmt(item.price, 2) }}</div>
            <div class="text-[11px] font-mono font-bold text-right" :class="numColor(item.pct)">
              {{ item.pct >= 0 ? '+' : '' }}{{ fmt(item.pct, 2, '%') }}
            </div>
          </div>

          <!-- 4. 点评与历史 (核心优化区) -->
          <div class="col-span-6 flex flex-col lg:flex-row gap-3 lg:gap-6 pt-3 md:pt-0 border-t md:border-t-0 border-slate-800/50">
            <!-- 建议与历史圆点 -->
            <div class="flex flex-col gap-2 shrink-0 lg:w-40">
              <div class="flex items-center gap-2">
                <span
                  v-if="item.analyze?.suggestion"
                  class="hidden md:inline-block px-1.5 py-0.5 rounded text-[9px] font-black uppercase tracking-tighter"
                  :class="suggestionColor(item.analyze.suggestion)"
                >
                  {{ item.analyze.suggestion }}
                </span>
                <span class="text-[9px] text-slate-500 font-bold font-mono">10D HIST</span>
                <button
                  v-if="item.analyze"
                  @click.stop="openDetailModal(item)"
                  class="ml-auto lg:hidden text-[10px] text-sky-400 font-bold"
                >
                  详情
                </button>
              </div>
              
              <div class="flex items-center gap-1.5">
                <div
                  v-for="(day, idx) in [...(item.analyze?.history || [])].reverse()"
                  :key="idx"
                  class="relative"
                >
                  <div
                    class="w-2.5 h-2.5 md:w-3 md:h-3 rounded-full cursor-pointer border border-black/20 hover:scale-125 transition-transform"
                    :class="dotColor(day.suggestion, day.tone)"
                    @mouseenter="hoveredDay = day; checkTooltipPosition($event)"
                    @mouseleave="hoveredDay = null"
                  ></div>
                  <!-- Tooltip -->
                  <div
                    v-if="hoveredDay === day"
                    :class="[
                      'absolute z-50 left-1/2 -translate-x-1/2 px-2 py-1.5 bg-slate-800 border border-slate-600 rounded-lg shadow-2xl text-[10px] text-white whitespace-nowrap',
                      tooltipPosition === 'top' ? 'bottom-full mb-2' : 'top-full mt-2'
                    ]"
                  >
                    <div class="font-bold border-b border-slate-700 pb-1 mb-1">{{ day.date || '-' }}</div>
                    <div class="flex items-center gap-2">
                      <span :class="String(day.suggestion || '').includes('关注') ? 'text-red-400' : String(day.suggestion || '').includes('减仓') ? 'text-emerald-400' : 'text-slate-300'">{{ day.suggestion || '-' }}</span>
                      <span class="text-slate-500 px-1 border border-slate-700 rounded text-[9px]">{{ day.tone || '-' }}</span>
                    </div>
                    <div class="text-slate-400 mt-1 italic">{{ (day.patterns || []).join(', ') || '无形态' }}</div>
                  </div>
                </div>
              </div>
            </div>

            <!-- 专家点评片段 -->
            <div class="flex-1 space-y-1.5">
              <div class="flex flex-wrap gap-2">
                <div 
                  v-for="(seg, sIdx) in parseCommentary(item.analyze?.summary)" 
                  :key="sIdx"
                  class="flex items-center bg-slate-900/40 rounded-lg border border-slate-800/60 overflow-hidden"
                >
                  <span v-if="seg.tag" class="px-1.5 py-0.5 text-[9px] font-bold text-slate-100" :class="tagColor(seg.tag)">
                    {{ seg.tag }}
                  </span>
                  <span class="px-2 py-0.5 text-[10px] text-slate-300">{{ seg.text }}</span>
                </div>
              </div>
              <!-- 桌面端详情按钮 -->
              <div class="hidden lg:block">
                <button
                  v-if="item.analyze"
                  @click.stop="openDetailModal(item)"
                  class="text-[10px] text-sky-400 hover:text-sky-300 font-bold transition-colors flex items-center gap-1"
                >
                  查看深度分析报告
                  <svg xmlns="http://www.w3.org/2000/svg" class="h-2.5 w-2.5" viewBox="0 0 20 20" fill="currentColor">
                    <path fill-rule="evenodd" d="M10.293 3.293a1 1 0 011.414 0l6 6a1 1 0 010 1.414l-6 6a1 1 0 01-1.414-1.414L14.586 11H3a1 1 0 110-2h11.586l-4.293-4.293a1 1 0 010-1.414z" clip-rule="evenodd" />
                  </svg>
                </button>
              </div>
            </div>
          </div>

          <!-- 4. 操作区 -->
          <div class="col-span-1 flex flex-col items-end justify-center gap-1">
            <button @click.stop="handleRemove(item.ts_code)" class="p-2 text-slate-600 hover:text-rose-400 hover:bg-rose-500/10 rounded-full transition-all">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
            </button>
          </div>

          <!-- 5. AI分析按钮 -->
          <div class="col-span-1 flex justify-end items-center">
            <button 
              @click.stop="analyzeWithAI(item)" 
              :disabled="analyzingStock === item.ts_code"
              class="px-3 py-1.5 rounded-lg text-[10px] font-bold transition-all"
              :class="analyzingStock === item.ts_code ? 'bg-purple-800 text-purple-300' : 'bg-purple-600/30 text-purple-400 hover:bg-purple-600 hover:text-white border border-purple-500/30'"
            >
              {{ analyzingStock === item.ts_code ? '分析中...' : 'AI分析' }}
            </button>
          </div>
        </div>
      </div>
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
          <v-chart :option="klineOption" autoresize @updateAxisPointer="handleUpdateAxisPointer" />
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

          <div v-if="detailModal.loading" class="flex items-center justify-center py-12 text-xs text-slate-400">
            深度分析加载中...
          </div>

          <div v-else-if="detailModal.error" class="rounded border border-red-700/40 bg-red-500/10 px-3 py-2 text-xs text-red-300">
            {{ detailModal.error }}
          </div>

          <template v-else-if="detailModal.detail">
          <div class="rounded-xl border border-slate-700/60 bg-slate-900/40 p-3">
            <div class="flex flex-wrap items-center gap-2">
              <span class="text-[10px] font-bold uppercase text-slate-500">交易结论</span>
              <span
                class="px-2 py-0.5 rounded text-[10px] font-bold"
                :class="suggestionColor(detailModal.detail?.decision?.action)"
              >
                {{ detailModal.detail?.decision?.action || '观望' }}
              </span>
              <span
                class="px-2 py-0.5 rounded text-[10px] font-bold border"
                :class="decisionScoreColor(detailModal.detail?.decision?.score)"
              >
                {{ Number.isFinite(Number(detailModal.detail?.decision?.score)) ? `${detailModal.detail.decision.score}分` : '评分-' }}
              </span>
              <span class="text-[10px] text-slate-400">
                {{ detailModal.detail?.decision?.bias || '-' }} · {{ detailModal.detail?.decision?.style || '-' }} · 置信度 {{ detailModal.detail?.decision?.confidence || '-' }}
              </span>
            </div>
            <p class="mt-2 text-sm leading-relaxed text-slate-200">
              {{ detailModal.detail?.decision?.summary || detailModal.detail?.summary || '暂无结论' }}
            </p>
          </div>

          <div v-if="detailModal.detail?.trade_plan?.entry" class="space-y-2">
            <h4 class="text-emerald-400 font-semibold text-xs uppercase mb-2">执行计划</h4>
            <div class="grid grid-cols-1 gap-2">
              <div class="bg-slate-800/50 rounded-lg p-3">
                <div class="text-[10px] font-bold text-slate-500 uppercase mb-1">开仓/跟踪</div>
                <p class="text-slate-200 text-xs leading-relaxed">{{ detailModal.detail.trade_plan.entry }}</p>
              </div>
              <div class="bg-slate-800/50 rounded-lg p-3">
                <div class="text-[10px] font-bold text-slate-500 uppercase mb-1">加仓条件</div>
                <p class="text-slate-200 text-xs leading-relaxed">{{ detailModal.detail.trade_plan.add || '-' }}</p>
              </div>
              <div class="bg-slate-800/50 rounded-lg p-3">
                <div class="text-[10px] font-bold text-slate-500 uppercase mb-1">减仓条件</div>
                <p class="text-slate-200 text-xs leading-relaxed">{{ detailModal.detail.trade_plan.reduce || '-' }}</p>
              </div>
              <div class="bg-slate-800/50 rounded-lg p-3">
                <div class="text-[10px] font-bold text-slate-500 uppercase mb-1">失效条件</div>
                <p class="text-slate-200 text-xs leading-relaxed">{{ detailModal.detail.trade_plan.invalid || '-' }}</p>
              </div>
              <div class="bg-slate-800/50 rounded-lg p-3">
                <div class="text-[10px] font-bold text-slate-500 uppercase mb-1">仓位建议</div>
                <p class="text-slate-200 text-xs leading-relaxed">{{ detailModal.detail.trade_plan.position || '-' }}</p>
              </div>
            </div>
          </div>

          <div v-if="detailModal.detail?.key_levels?.length">
            <h4 class="text-cyan-400 font-semibold text-xs uppercase mb-2">关键价位</h4>
            <div class="grid grid-cols-2 gap-2">
              <div
                v-for="(item, idx) in detailModal.detail.key_levels"
                :key="`level-${idx}`"
                class="rounded-lg border border-slate-700/60 bg-slate-900/50 px-3 py-2"
              >
                <div class="flex items-center justify-between gap-2">
                  <span class="text-[10px] font-bold text-slate-400 uppercase">{{ item.label }}</span>
                  <span class="text-sm font-mono text-white">{{ fmt(item.price, 2) }}</span>
                </div>
                <div class="mt-1 text-[10px] leading-relaxed text-slate-500">{{ item.note || '-' }}</div>
              </div>
            </div>
          </div>

          <div v-if="detailModal.detail?.observation_points?.length">
            <h4 class="text-slate-400 font-semibold text-xs uppercase mb-2">观察要点</h4>
            <div class="space-y-2">
              <div
                v-for="(point, idx) in detailModal.detail.observation_points"
                :key="`point-${idx}`"
                class="rounded-lg border border-slate-700/50 bg-slate-900/30 px-3 py-2 text-xs leading-relaxed text-slate-300"
              >
                {{ point }}
              </div>
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
          </template>
        </div>
      </div>
    </div>

    <!-- 持仓编辑弹窗 -->
    <div v-if="showHoldingModal" class="fixed inset-0 z-[200] bg-black/80 flex items-center justify-center p-4" @click="showHoldingModal = false">
      <div class="bg-[#1a1a2e] border border-slate-600 rounded-xl w-full max-w-sm overflow-hidden" @click.stop>
        <div class="flex items-center justify-between p-4 border-b border-slate-700">
          <h3 class="text-white font-bold text-sm">编辑持仓</h3>
          <button @click="showHoldingModal = false" class="text-slate-400 hover:text-white">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <div class="p-4 space-y-4">
          <div class="text-center">
            <div class="text-white font-bold">{{ holdingStock?.name }}</div>
            <div class="text-slate-500 text-xs">{{ holdingStock?.ts_code }}</div>
          </div>
          <div class="space-y-2">
            <label class="block text-[10px] font-bold text-slate-500 uppercase">持仓数量</label>
            <input v-model.number="holdingForm.shares" type="number" min="0" step="100" class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-2.5 text-xs font-medium text-white focus:outline-none focus:border-purple-500" />
          </div>
          <div class="space-y-2">
            <label class="block text-[10px] font-bold text-slate-500 uppercase">成本价</label>
            <input v-model.number="holdingForm.avg_cost" type="number" min="0" step="0.01" class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-2.5 text-xs font-medium text-white focus:outline-none focus:border-purple-500" />
          </div>
          <div v-if="holdingForm.shares > 0 && holdingForm.avg_cost > 0 && holdingStock?.price" class="bg-slate-800/50 rounded-lg p-3 text-xs">
            <div class="flex justify-between mb-1">
              <span class="text-slate-500">成本总额</span>
              <span class="text-white font-mono">{{ (holdingForm.shares * holdingForm.avg_cost).toFixed(2) }}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-slate-500">盈亏</span>
              <span :class="((holdingStock.price - holdingForm.avg_cost) / holdingForm.avg_cost) >= 0 ? 'text-red-400' : 'text-emerald-400'" class="font-mono">
                {{ ((holdingStock.price - holdingForm.avg_cost) / holdingForm.avg_cost) >= 0 ? '+' : '' }}{{ (((holdingStock.price - holdingForm.avg_cost) / holdingForm.avg_cost) * 100).toFixed(2) }}%
              </span>
            </div>
          </div>
          <div class="flex gap-3 mt-6">
            <button v-if="holdings[holdingStock?.ts_code]?.shares > 0" @click="removeHoldingOnly" class="flex-1 py-2.5 rounded-lg text-[11px] font-bold text-red-400 border border-red-500/30 hover:bg-red-500/10">删除持仓</button>
            <button @click="saveHolding" class="flex-1 py-2.5 rounded-lg text-[11px] font-bold bg-purple-600 text-white hover:bg-purple-500">保存</button>
          </div>
        </div>
      </div>
    </div>

    <!-- AI分析结果弹窗 -->
    <div v-if="aiAnalysisModal.visible" class="fixed inset-0 z-[200] bg-black/80 flex items-center justify-center p-2" @click="aiAnalysisModal.visible = false">
      <div class="bg-[#1a1a2e] border border-slate-600 rounded-xl w-full max-w-2xl max-h-[85vh] overflow-hidden flex flex-col" @click.stop>
        <div class="flex items-center justify-between p-4 border-b border-slate-700 shrink-0">
          <div class="flex items-center gap-2">
            <SparklesIcon class="w-4 h-4 text-purple-400" />
            <h3 class="text-white font-bold text-sm">AI 智能分析</h3>
            <span v-if="aiAnalysisModal.stock" class="text-slate-400 text-xs">{{ aiAnalysisModal.stock.name }}</span>
            <span v-if="aiAnalysisModal.fromCache" class="text-[9px] px-1.5 py-0.5 rounded bg-slate-700 text-slate-400">缓存</span>
          </div>
          <div class="flex items-center gap-2">
            <button 
              @click="analyzeWithAI(aiAnalysisModal.stock, true)" 
              :disabled="analyzingStock === aiAnalysisModal.stock?.ts_code"
              class="px-2 py-1 rounded text-[10px] font-bold transition-all border border-purple-500/30 text-purple-400 hover:bg-purple-600 hover:text-white disabled:opacity-50"
            >
              {{ analyzingStock === aiAnalysisModal.stock?.ts_code ? '刷新中...' : '强制刷新' }}
            </button>
            <button @click="aiAnalysisModal.visible = false" class="text-slate-400 hover:text-white">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>
        <div class="overflow-y-auto p-4 flex-1">
          <div v-if="aiAnalysisModal.loading" class="flex items-center justify-center h-40">
            <div class="animate-spin w-8 h-8 border-2 border-purple-500 border-t-transparent rounded-full"></div>
          </div>
          <div v-else-if="aiAnalysisModal.result" class="prose prose-sm prose-invert max-w-none text-slate-300 text-xs leading-relaxed ai-analysis-content" v-html="renderMarkdown(aiAnalysisModal.result)"></div>
          <div v-else class="text-center text-slate-500 py-8">
            暂无分析结果
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref, reactive, computed } from 'vue';
import { getWatchlistRealtime, getWatchlistAnalysis, addToWatchlist, removeFromWatchlist, getStockKline, getUserHoldings, updateUserHolding, deleteUserHolding, analyzeStockWithAI } from '@/services/api';
import { marked } from 'marked';
import { useStockSearch } from '@/composables/useStockSearch';
import { use } from 'echarts/core';

marked.setOptions({
  breaks: true,
  gfm: true,
});
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
const hoveredDay = ref(null);  // 当前hover的10D节点
const klineHoveredIndex = ref(-1);  // K线图当前hover的索引

// 持仓相关
const holdings = ref({});  // { ts_code: { shares, avg_cost } }
const showHoldingModal = ref(false);
const holdingStock = ref(null);
const holdingForm = reactive({ shares: 0, avg_cost: 0 });

// AI分析相关
const aiAnalysisModal = ref({ visible: false, stock: null, loading: false, result: '', fromCache: false });
const analyzingStock = ref(null);

const handleUpdateAxisPointer = (event) => {
  const info = event.dataIndex;
  if (info != null && info >= 0) {
    klineHoveredIndex.value = info;
  }
};

const tooltipPosition = ref('top');  // tooltip显示位置

const checkTooltipPosition = (event) => {
  const rect = event.target.getBoundingClientRect();
  const spaceAbove = rect.top;
  const spaceBelow = window.innerHeight - rect.bottom;
  tooltipPosition.value = spaceAbove > spaceBelow ? 'top' : 'bottom';
};

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
const klinePopupRef = ref(null);
let searchTimer = null;
let timer = null;

// 使用新的搜索composable
const {
  searchResults,
  searchLoading,
  showSearchResults,
  search,
  hideResults,
  clearResults
} = useStockSearch();

const handleSearchInput = () => {
  if (searchTimer) clearTimeout(searchTimer);
  const q = newCode.value.trim();
  if (q.length < 1) {
    clearResults();
    return;
  }
  // 使用更短的延迟，提升响应速度
  searchTimer = setTimeout(() => {
    search(q, 8);
  }, 100);
};

const selectSearchResult = (stock) => {
  newCode.value = stock.ts_code;
  clearResults();
  handleAdd();
};

const hideSearchResults = () => {
  hideResults();
};

const detailModal = reactive({
  visible: false,
  stock: null,
  detail: null,
  loading: false,
  error: ''
});

const openDetailModal = async (stock) => {
  detailModal.stock = stock || null;
  detailModal.visible = true;
  detailModal.detail = stock?.analyze?.detail || null;
  detailModal.loading = !detailModal.detail;
  detailModal.error = '';

  if (detailModal.detail || !stock?.ts_code) {
    return;
  }

  try {
    const res = await getWatchlistAnalysis(stock.ts_code);
    detailModal.detail = res.data?.data?.detail || null;
  } catch (e) {
    detailModal.error = e?.response?.data?.detail || e?.message || '加载深度分析失败';
  } finally {
    detailModal.loading = false;
  }
};

const closeDetailModal = () => {
  detailModal.visible = false;
  detailModal.stock = null;
  detailModal.detail = null;
  detailModal.loading = false;
  detailModal.error = '';
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
  klineHoveredIndex.value = -1;
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
  klineHoveredIndex.value = -1;
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
  const data = hoverInfo.klineData;
  if (!data || !data.length) return null;
  const idx = klineHoveredIndex.value >= 0 ? klineHoveredIndex.value : data.length - 1;
  const item = data[idx];
  if (!item) return getLatestKlineData(data);
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
  };
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
  if (tag === '结论') return 'bg-red-500/20 text-red-400 border border-red-500/30';
  if (tag === '计划') return 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30';
  if (tag === '风险') return 'bg-rose-500/20 text-rose-300 border border-rose-500/30';
  if (tag === '机构') return 'bg-blue-500/20 text-blue-400 border border-blue-500/30';
  if (tag === '游资') return 'bg-orange-500/20 text-orange-400 border border-orange-500/30';
  if (tag === '形态') return 'bg-purple-500/20 text-purple-400 border border-purple-500/30';
  return 'bg-slate-700 text-slate-300';
};

const decisionScoreColor = (score) => {
  const value = Number(score);
  if (!Number.isFinite(value)) return 'border-slate-600 bg-slate-700/70 text-slate-300';
  if (value >= 75) return 'border-red-500/40 bg-red-500/15 text-red-300';
  if (value >= 60) return 'border-amber-500/40 bg-amber-500/15 text-amber-300';
  if (value >= 45) return 'border-slate-600 bg-slate-700/70 text-slate-300';
  return 'border-emerald-500/40 bg-emerald-500/15 text-emerald-300';
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
  if (s.includes('减仓') || s.includes('持币') || s.includes('回避')) return 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30';
  return 'bg-slate-700 text-slate-300';
};

const dotColor = (s, tone) => {
  const sStr = String(s || '');
  const toneStr = String(tone || '');
  if (toneStr.includes('爆发') || sStr.includes('试错') || sStr.includes('关注')) return 'bg-red-500';
  if (toneStr.includes('杀跌') || sStr.includes('减仓') || sStr.includes('持币') || sStr.includes('回避')) return 'bg-emerald-500';
  if (toneStr.includes('看多')) return 'bg-red-900/60';
  if (toneStr.includes('看空')) return 'bg-emerald-900/60';
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

const refreshNow = async (showLoading = true, syncHoldings = false) => {
  // 如果当前已经在加载中，且不是强制显示 loading 的请求，则跳过
  if (loading.value && !showLoading) return;
  
  if (showLoading) loading.value = true;
  try {
    const res = await getWatchlistRealtime(null, 'sina', 'compact');
    const body = res.data || {};
    rows.value = body.data || [];
    isTradingTime.value = !!body.is_trading_time;
    message.value = body.message || '';
    if (syncHoldings) {
      await fetchHoldings();
    }
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
    await refreshNow(true, true);
  } catch (e) {
    message.value = '添加失败: ' + (e.response?.data?.detail || e.message);
  }
};

const handleRemove = async (tsCode) => {
  if (!confirm(`确定要从自选删除 ${tsCode} 吗？`)) return;
  try {
    await removeFromWatchlist(tsCode);
    await refreshNow(true, true);
  } catch (e) {
    alert('删除失败: ' + e.message);
  }
};

// 持仓管理
const fetchHoldings = async () => {
  try {
    const res = await getUserHoldings();
    const holdingsList = Array.isArray(res.data) ? res.data : (res.data?.holdings || []);
    const h = {};
    holdingsList.forEach(item => {
      h[item.ts_code] = { shares: item.shares, avg_cost: item.avg_cost };
    });
    holdings.value = h;
  } catch (e) {
    console.error('获取持仓失败', e);
  }
};

const openHoldingModal = (item) => {
  holdingStock.value = item;
  const existing = holdings.value[item.ts_code];
  holdingForm.shares = existing?.shares || 0;
  holdingForm.avg_cost = existing?.avg_cost || 0;
  showHoldingModal.value = true;
};

const saveHolding = async () => {
  if (!holdingStock.value) return;
  try {
    if (holdingForm.shares <= 0) {
      await deleteUserHolding(holdingStock.value.ts_code);
      delete holdings.value[holdingStock.value.ts_code];
    } else {
      await updateUserHolding(holdingStock.value.ts_code, holdingForm);
      holdings.value[holdingStock.value.ts_code] = { ...holdingForm };
    }
    showHoldingModal.value = false;
  } catch (e) {
    alert('保存失败: ' + (e.response?.data?.detail || e.message));
  }
};

const removeHoldingOnly = async () => {
  if (!holdingStock.value) return;
  if (!confirm(`确定要删除 ${holdingStock.value.ts_code} 的持仓记录吗？`)) return;
  try {
    await deleteUserHolding(holdingStock.value.ts_code);
    delete holdings.value[holdingStock.value.ts_code];
    showHoldingModal.value = false;
  } catch (e) {
    alert('删除持仓失败: ' + (e.response?.data?.detail || e.message));
  }
};

const calcPnL = (item) => {
  const h = holdings.value[item.ts_code];
  if (!h || !h.shares || !h.avg_cost) return null;
  const cost = h.shares * h.avg_cost;
  const current = h.shares * item.price;
  const pnl = current - cost;
  const pnlPct = ((item.price - h.avg_cost) / h.avg_cost) * 100;
  return { pnl, pnlPct, cost, current };
};

// 总仓位计算
const totalPortfolioValue = computed(() => {
  let total = 0;
  for (const item of rows.value) {
    const h = holdings.value[item.ts_code];
    if (h?.shares > 0 && item.price) {
      total += h.shares * item.price;
    }
  }
  return total;
});

const totalCost = computed(() => {
  let total = 0;
  for (const item of rows.value) {
    const h = holdings.value[item.ts_code];
    if (h?.shares > 0 && h.avg_cost) {
      total += h.shares * h.avg_cost;
    }
  }
  return total;
});

const totalPnL = computed(() => totalPortfolioValue.value - totalCost.value);

const totalPnLPct = computed(() => {
  if (totalCost.value <= 0) return 0;
  return (totalPnL.value / totalCost.value) * 100;
});

// Markdown渲染
const renderMarkdown = (text) => {
  if (!text) return '';
  try {
    return marked.parse(text);
  } catch (e) {
    return text;
  }
};

// AI分析
const analyzeWithAI = async (item, forceRefresh = false) => {
  analyzingStock.value = item.ts_code;
  if (!forceRefresh) {
    aiAnalysisModal.value = { visible: true, stock: item, loading: true, result: '', fromCache: false };
  } else {
    aiAnalysisModal.value.loading = true;
  }
  try {
    const res = await analyzeStockWithAI(item.ts_code, null, forceRefresh);
    aiAnalysisModal.value.result = res.data.analysis;
    aiAnalysisModal.value.fromCache = res.data.from_cache || false;
  } catch (e) {
    aiAnalysisModal.value.result = '分析失败: ' + (e.response?.data?.detail || e.message);
  } finally {
    aiAnalysisModal.value.loading = false;
    analyzingStock.value = null;
  }
};

onMounted(async () => {
  window.addEventListener('keydown', handleKeydown);
  document.addEventListener('click', handleDocumentClick, true);
  await refreshNow(true, true);
});

onBeforeUnmount(() => {
  window.removeEventListener('keydown', handleKeydown);
  document.removeEventListener('click', handleDocumentClick, true);
  stopAutoRefresh();
});
</script>

<style scoped>
.ai-analysis-content :deep(h1),
.ai-analysis-content :deep(h2),
.ai-analysis-content :deep(h3),
.ai-analysis-content :deep(h4) {
  color: #e2e8f0;
  font-weight: bold;
  margin-top: 1em;
  margin-bottom: 0.5em;
}
.ai-analysis-content :deep(h1) { font-size: 1.1em; }
.ai-analysis-content :deep(h2) { font-size: 1.05em; }
.ai-analysis-content :deep(h3) { font-size: 1em; }

.ai-analysis-content :deep(p) {
  margin-bottom: 0.75em;
  line-height: 1.7;
}

.ai-analysis-content :deep(ul),
.ai-analysis-content :deep(ol) {
  margin-left: 1.2em;
  margin-bottom: 0.75em;
}

.ai-analysis-content :deep(li) {
  margin-bottom: 0.25em;
}

.ai-analysis-content :deep(strong) {
  color: #f1f5f9;
  font-weight: 600;
}

.ai-analysis-content :deep(em) {
  color: #94a3b8;
}

.ai-analysis-content :deep(code) {
  background: #1e293b;
  padding: 0.15em 0.4em;
  border-radius: 4px;
  font-size: 0.9em;
}

.ai-analysis-content :deep(blockquote) {
  border-left: 3px solid #6366f1;
  padding-left: 1em;
  margin-left: 0;
  color: #94a3b8;
}

.ai-analysis-content :deep(table) {
  width: 100%;
  border-collapse: collapse;
  margin-bottom: 1em;
}

.ai-analysis-content :deep(th),
.ai-analysis-content :deep(td) {
  border: 1px solid #334155;
  padding: 0.4em 0.6em;
  text-align: left;
}

.ai-analysis-content :deep(th) {
  background: #1e293b;
  font-weight: 600;
}

.ai-analysis-content :deep(hr) {
  border-color: #334155;
  margin: 1em 0;
}
</style>
