<template>
  <div class="space-y-3 md:space-y-5 max-w-5xl mx-auto pb-8 relative">
    <!-- 顶部管理栏 -->
    <div v-if="!zenMode" class="bg-business-dark p-3 md:p-4 rounded-xl md:rounded-2xl shadow-lg border border-business-light">
      <div class="flex items-center justify-between gap-2">
        <div class="min-w-0">
          <h2 class="text-xs md:text-sm font-bold text-white tracking-wide">盯盘自选</h2>
          <p class="mt-0.5 text-[10px] text-slate-500">先处理动作和风险，再看解释细节</p>
        </div>
        <div class="flex items-center gap-1.5 md:gap-2 text-[10px] md:text-[11px] shrink-0">
          <span
            class="px-1.5 py-0.5 rounded text-[9px] md:text-[10px]"
            :class="isTradingTime ? 'border border-emerald-500/40 bg-emerald-500/10 text-emerald-300' : 'border border-slate-600 bg-slate-800 text-slate-300'"
          >
            {{ isTradingTime ? '交易中' : '非交易' }}
          </span>
          <button
            @click="refreshNow(true, false)"
            :disabled="refreshing"
            class="px-1.5 py-0.5 rounded border border-business-accent/40 bg-business-accent/10 text-business-accent hover:bg-business-accent hover:text-white transition disabled:opacity-50"
          >
            {{ refreshing ? (rows.length ? '更新中' : '加载中') : '刷新' }}
          </button>
          <button
            @click="openImageHoldingModal"
            class="px-1.5 py-0.5 rounded border border-cyan-500/40 bg-cyan-500/10 text-cyan-300 hover:bg-cyan-500 hover:text-white transition"
          >
            图片更新持仓
          </button>
          <button
            @click="toggleZenMode"
            class="hidden md:inline-block px-2 py-1 rounded border border-purple-500/40 bg-purple-500/10 text-purple-300 hover:bg-purple-500 hover:text-white transition"
          >
            专注模式
          </button>
        </div>
      </div>

      <!-- 添加自选 -->
      <div class="mt-2 md:mt-3 relative">
        <input
          v-model="newCode"
          @input="handleSearchInput"
          @keyup.enter="handleAdd"
          @focus="handleSearchInput"
          @blur="hideSearchResults"
          class="w-full bg-slate-900/80 border border-slate-700 rounded-lg px-3 py-1.5 md:py-2 text-xs text-slate-100 focus:outline-none focus:ring-1 focus:ring-business-accent"
          placeholder="搜索股票 (代码/名称/拼音)"
        />
        <!-- 搜索结果下拉 -->
        <div
          v-if="showSearchResults"
          class="absolute z-50 mt-1 w-full bg-slate-800 border border-slate-600 rounded-lg shadow-xl max-h-48 md:max-h-60 overflow-auto"
        >
          <div
            v-for="stock in searchResults"
            :key="stock.ts_code"
            @mousedown.prevent="selectSearchResult(stock)"
            class="px-3 py-2 hover:bg-slate-700 cursor-pointer flex justify-between items-center border-b border-slate-700/50 last:border-0"
          >
            <div class="flex items-center gap-2 min-w-0">
              <span class="text-slate-200 font-mono text-xs shrink-0">{{ stock.ts_code?.replace('.SH','').replace('.SZ','') }}</span>
              <span class="text-slate-400 text-xs truncate">{{ stock.name }}</span>
            </div>
            <span class="text-emerald-400 text-xs ml-2 shrink-0">+</span>
          </div>
        </div>
      </div>

      <div class="mt-3 flex flex-wrap items-center gap-2 text-[10px] md:text-[11px]">
        <button
          @click="focusFilter = 'all'"
          class="rounded-full border px-2.5 py-1 transition"
          :class="focusFilter === 'all' ? 'border-business-accent/40 bg-business-accent/10 text-business-accent' : 'border-slate-700 bg-slate-900/70 text-slate-400 hover:text-slate-200'"
        >
          全部
        </button>
        <button
          @click="focusFilter = 'actionable'"
          class="rounded-full border px-2.5 py-1 transition"
          :class="focusFilter === 'actionable' ? 'border-amber-500/40 bg-amber-500/10 text-amber-300' : 'border-slate-700 bg-slate-900/70 text-slate-400 hover:text-slate-200'"
        >
          只看动作
        </button>
        <button
          @click="focusFilter = 'risk'"
          class="rounded-full border px-2.5 py-1 transition"
          :class="focusFilter === 'risk' ? 'border-rose-500/40 bg-rose-500/10 text-rose-300' : 'border-slate-700 bg-slate-900/70 text-slate-400 hover:text-slate-200'"
        >
          只看风险
        </button>
        <span class="text-slate-500">{{ focusFilterHint }}</span>
      </div>

      <!-- 底部状态栏：移动端隐藏 -->
      <div class="hidden md:flex mt-3 items-center justify-between border-t border-business-light pt-3">
        <div class="flex items-center gap-2 text-[10px]">
          <span class="rounded-full border border-business-accent/30 bg-business-accent/10 px-2.5 py-1 font-bold text-business-accent">
            {{ refreshStatusText }}
          </span>
          <span class="rounded-full border border-slate-700 bg-slate-900/70 px-2.5 py-1 font-mono text-slate-300">
            {{ refreshCycleText }}
          </span>
          <span class="text-slate-500">{{ message || (refreshing && rows.length ? '保持当前快照展示，后台刷新中' : '列表按当前模式自动更新') }}</span>
        </div>
        <span class="text-[10px] font-mono text-slate-500">{{ lastRefreshAt || '--:--:--' }}</span>
      </div>
    </div>

    <!-- 行情列表 -->
    <div v-if="!zenMode" class="space-y-3 md:space-y-4">
      <div
        v-if="!rows.length && !refreshing"
        class="bg-business-dark p-8 md:p-12 rounded-xl md:rounded-2xl border border-business-light text-center text-slate-500 text-xs"
      >
        暂无自选股，请先添加
      </div>

      <!-- 持仓跟踪 -->
      <section class="bg-business-dark rounded-xl md:rounded-2xl border border-business-light shadow-business overflow-hidden">
        <div class="px-3 py-2 md:px-4 md:py-2.5 border-b border-business-light flex items-center justify-between">
          <div class="flex items-center gap-2">
            <h3 class="text-xs md:text-[13px] font-bold text-white">持仓跟踪</h3>
            <span class="rounded bg-business-accent/10 px-1.5 py-0.5 text-[9px] md:text-[10px] font-bold text-business-accent">{{ filteredHoldingRows.length }}/{{ holdingRows.length }}</span>
            <span v-if="riskyHoldingCount > 0" class="rounded bg-rose-500/10 px-1.5 py-0.5 text-[9px] md:text-[10px] font-bold text-rose-300">{{ riskyHoldingCount }} 风险</span>
          </div>
          <!-- 移动端：只显示总盈亏 -->
          <div v-if="totalPortfolioValue > 0" class="flex items-center gap-2 text-[10px]">
            <span class="md:hidden text-slate-500">盈亏<span class="ml-1 font-mono" :class="totalPnL >= 0 ? 'text-red-400' : 'text-emerald-400'">{{ totalPnL >= 0 ? '+' : '' }}{{ fmt(totalPnLPct, 2, '%') }}</span></span>
            <span class="hidden md:inline text-slate-500">总市值<span class="ml-1 font-mono text-slate-200">{{ fmt(totalPortfolioValue, 0) }}</span></span>
            <span class="hidden md:inline text-slate-500">盈亏<span class="ml-1 font-mono" :class="totalPnL >= 0 ? 'text-red-400' : 'text-emerald-400'">{{ totalPnL >= 0 ? '+' : '' }}{{ fmt(totalPnL, 0) }}</span></span>
            <span class="hidden md:inline text-slate-500">收益率<span class="ml-1 font-mono" :class="totalPnLPct >= 0 ? 'text-red-400' : 'text-emerald-400'">{{ totalPnLPct >= 0 ? '+' : '' }}{{ fmt(totalPnLPct, 2, '%') }}</span></span>
          </div>
        </div>

        <div v-if="filteredHoldingRows.length" class="p-2 md:p-4 grid grid-cols-1 lg:grid-cols-2 gap-2">
          <div
            v-for="item in filteredHoldingRows"
            :key="`holding-${item.ts_code}`"
            class="rounded-lg md:rounded-xl border px-3 md:px-4 py-2 md:py-2.5 transition-all"
            :class="watchCardClass(item, 'holding')"
          >
            <!-- 移动端布局 -->
            <div class="md:hidden">
              <div class="flex items-center justify-between">
                <button class="text-left min-w-0" data-kline-trigger="1" @click.stop="toggleKline(item, $event)">
                  <span class="truncate text-xs font-bold text-white">{{ item.name || '-' }}</span>
                  <span class="ml-1 text-[9px] font-mono text-slate-500">{{ item.ts_code?.replace('.SH','').replace('.SZ','') }}</span>
                </button>
                <div class="flex items-center gap-1 shrink-0">
                  <span class="text-xs font-mono font-bold text-white">{{ fmt(item.price, 2) }}</span>
                  <span class="text-[10px] font-mono font-bold" :class="numColor(item.pct)">{{ item.pct >= 0 ? '+' : '' }}{{ fmt(item.pct, 2, '%') }}</span>
                </div>
              </div>
              <div class="flex items-center justify-between mt-1">
                <div class="flex items-center gap-1.5 min-w-0">
                  <span class="rounded px-1 py-0.5 text-[8px] font-bold shrink-0" :class="watchSignalClass(item)">{{ getSignalText(item) }}</span>
                  <span v-if="getAggressiveLabel(item)" class="rounded px-1 py-0.5 text-[8px] font-bold bg-red-500/15 text-red-300 shrink-0">{{ getAggressiveLabel(item) }}</span>
                </div>
                <div class="flex items-center gap-0.5 shrink-0 ml-1">
                   <button @click.stop="openDetailModal(item)" class="p-1 text-slate-500 hover:text-slate-300">
                     <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                   </button>
                   <button @click.stop="openHoldingModal(item)" class="p-1 text-business-accent hover:text-white">
                     <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" /></svg>
                   </button>
                   <button @click.stop="analyzeWithAI(item)" :disabled="analyzingStock === item.ts_code" class="p-1 text-purple-300 hover:text-purple-200 disabled:opacity-60">
                     <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" /></svg>
                   </button>
                   <button @click.stop="handleRemove(item.ts_code)" class="p-1 text-rose-400/60 hover:text-rose-300">
                     <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                   </button>
                 </div>
              </div>
              <div v-if="getSignalReasons(item).length" class="mt-1 flex flex-wrap gap-1">
                <span
                  v-for="(reason, idx) in getSignalReasons(item)"
                  :key="`holding-m-reason-${item.ts_code}-${idx}`"
                  class="inline-flex items-center rounded border px-1.5 py-0.5 text-[8px] leading-none"
                  :class="signalReasonChipClass(reason.kind)"
                >
                  {{ reason.title }}
                </span>
              </div>
              <div class="mt-1 space-y-0.5">
                <p v-if="getTriggerText(item)" class="text-[9px] leading-4 text-red-200/90 line-clamp-1">触发：{{ getTriggerText(item) }}</p>
                <p v-if="getInvalidText(item)" class="text-[9px] leading-4 text-emerald-200/90 line-clamp-1">失效：{{ getInvalidText(item) }}</p>
              </div>
              <div v-if="getVolumeRatio(item) !== null || getTurnoverRate(item) !== null" class="mt-1 flex flex-wrap gap-1 text-[8px]">
                <span v-if="getVolumeRatio(item) !== null" class="inline-flex items-center rounded border border-cyan-500/20 bg-cyan-500/10 px-1.5 py-0.5 font-mono text-cyan-200">量比 {{ fmt(getVolumeRatio(item), 2) }}</span>
                <span v-if="getTurnoverRate(item) !== null" class="inline-flex items-center rounded border border-slate-600 bg-slate-800/70 px-1.5 py-0.5 font-mono text-slate-300">换手 {{ fmt(getTurnoverRate(item), 1, '%') }}</span>
              </div>
              <div v-if="getKeyLevels(item).length" class="mt-1 flex flex-wrap gap-1">
                <span class="text-[8px] text-slate-500">目标位</span>
                <span v-for="level in getKeyLevels(item)" :key="level.label" class="inline-flex items-center gap-0.5 rounded px-1 py-0.5 text-[8px] font-mono" :class="level.isSupport ? 'bg-emerald-500/10 text-emerald-400' : 'bg-rose-500/10 text-rose-400'">
                  <span class="text-[7px] opacity-70">{{ level.shortLabel }}</span>{{ level.priceText }}
                </span>
              </div>
              <p class="mt-1 text-[9px] text-slate-400 line-clamp-2">{{ getConclusionText(item) }}</p>
              <div v-if="holdings[item.ts_code]?.shares > 0" class="mt-1 text-[9px] text-slate-500">
                {{ holdings[item.ts_code]?.shares }}股 @ {{ fmt(holdings[item.ts_code]?.avg_cost, 2) }}
                <span v-if="calcPnL(item)" class="ml-1 font-mono" :class="calcPnL(item)?.pnl >= 0 ? 'text-red-400' : 'text-emerald-400'">
                  {{ calcPnL(item)?.pnl >= 0 ? '+' : '' }}{{ fmt(calcPnL(item)?.pnlPct, 2, '%') }}
                </span>
              </div>
            </div>

            <!-- 桌面端布局 -->
            <div class="hidden md:block">
              <div class="flex items-start justify-between gap-3">
                <div class="min-w-0 flex-1">
                  <button class="text-left w-full" data-kline-trigger="1" @click.stop="toggleKline(item, $event)">
                    <div class="flex items-center gap-1.5">
                      <span class="truncate text-[13px] font-bold text-white hover:text-business-accent transition-colors">{{ item.name || '-' }}</span>
                      <span class="text-[10px] font-mono text-slate-500 shrink-0">{{ item.ts_code?.replace('.SH','').replace('.SZ','') }}</span>
                    </div>
                  </button>
                  <div class="mt-1 flex items-center gap-2">
                    <span class="text-[14px] font-mono font-bold text-white">{{ fmt(item.price, 2) }}</span>
                    <span class="text-[11px] font-mono font-bold" :class="numColor(item.pct)">{{ item.pct >= 0 ? '+' : '' }}{{ fmt(item.pct, 2, '%') }}</span>
                  </div>
                </div>
                <div class="shrink-0 text-[10px] text-slate-400 text-right whitespace-nowrap">
                  <div>{{ holdings[item.ts_code]?.shares || 0 }}股 @ {{ fmt(holdings[item.ts_code]?.avg_cost, 2) }}</div>
                  <template v-if="calcPnL(item)">
                    <div class="mt-0.5 font-mono" :class="calcPnL(item)?.pnl >= 0 ? 'text-red-400' : 'text-emerald-400'">
                      {{ calcPnL(item)?.pnl >= 0 ? '+' : '' }}{{ fmt(calcPnL(item)?.pnl, 0) }} ({{ calcPnL(item)?.pnlPct >= 0 ? '+' : '' }}{{ fmt(calcPnL(item)?.pnlPct, 2, '%') }})
                    </div>
                  </template>
                </div>
              </div>
              <div class="mt-2 flex flex-wrap items-center gap-1.5">
                <span class="rounded px-1.5 py-0.5 text-[9px] font-bold" :class="watchSignalClass(item)">{{ getSignalText(item) }}</span>
                <span v-if="getAggressiveLabel(item)" class="rounded px-1.5 py-0.5 text-[9px] font-bold bg-red-500/15 text-red-300">{{ getAggressiveLabel(item) }}</span>
                <span v-if="getVolumeRatio(item) !== null" class="inline-flex items-center rounded border border-cyan-500/20 bg-cyan-500/10 px-1.5 py-0.5 text-[8px] font-mono text-cyan-200">量比 {{ fmt(getVolumeRatio(item), 2) }}</span>
                <span v-if="getTurnoverRate(item) !== null" class="inline-flex items-center rounded border border-slate-600 bg-slate-800/70 px-1.5 py-0.5 text-[8px] font-mono text-slate-300">换手 {{ fmt(getTurnoverRate(item), 1, '%') }}</span>
                <span
                  v-for="(reason, idx) in getSignalReasons(item)"
                  :key="`holding-d-reason-${item.ts_code}-${idx}`"
                  class="inline-flex items-center rounded border px-1.5 py-0.5 text-[8px] leading-none"
                  :class="signalReasonChipClass(reason.kind)"
                >
                  {{ reason.title }}
                </span>
              </div>
              <div class="mt-2 space-y-0.5">
                <p v-if="getTriggerText(item)" class="text-[10px] leading-4 text-red-200/90 truncate">触发：{{ getTriggerText(item) }}</p>
                <p v-if="getInvalidText(item)" class="text-[10px] leading-4 text-emerald-200/90 truncate">失效：{{ getInvalidText(item) }}</p>
              </div>
              <div v-if="getKeyLevels(item).length" class="mt-2 flex flex-wrap items-center gap-1">
                <span class="text-[8px] uppercase text-slate-500">目标位</span>
                <span v-for="level in getKeyLevels(item)" :key="level.label" class="inline-flex items-center gap-0.5 rounded px-1 py-0.5 text-[9px] font-mono" :class="level.isSupport ? 'bg-emerald-500/10 text-emerald-400' : 'bg-rose-500/10 text-rose-400'">
                  <span class="text-[8px] opacity-70">{{ level.shortLabel }}</span>{{ level.priceText }}
                </span>
              </div>
              <div class="mt-2 flex items-end justify-between gap-3">
                <p class="min-w-0 flex-1 text-[11px] leading-4 text-slate-400 line-clamp-2">{{ getConclusionText(item) }}</p>
                <div class="shrink-0 flex items-center gap-1.5">
                  <button @click.stop="openDetailModal(item)" class="rounded px-2 py-1 text-[10px] font-bold border border-slate-700/50 text-slate-400 hover:text-slate-200 hover:border-slate-500">详情</button>
                  <button @click.stop="openHoldingModal(item)" class="rounded px-2 py-1 text-[10px] font-bold border border-business-accent/30 text-business-accent hover:bg-business-accent/20">持仓</button>
                  <button @click.stop="analyzeWithAI(item)" :disabled="analyzingStock === item.ts_code" class="rounded px-2 py-1 text-[10px] font-bold border border-purple-500/30 text-purple-300 hover:bg-purple-500/20 disabled:opacity-60">{{ analyzingStock === item.ts_code ? '...' : 'AI' }}</button>
                  <button @click.stop="handleRemove(item.ts_code)" class="rounded px-1.5 py-1 text-[10px] font-bold text-rose-400/60 hover:text-rose-300">×</button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div v-else-if="refreshing && !rows.length" class="p-2 md:p-4 grid grid-cols-1 lg:grid-cols-2 gap-2">
          <div
            v-for="n in 4"
            :key="`holding-skeleton-${n}`"
            class="rounded-lg md:rounded-xl border border-white/10 bg-white/[0.03] px-3 md:px-4 py-3 animate-pulse"
          >
            <div class="flex items-start justify-between gap-3">
              <div class="flex-1 min-w-0">
                <div class="h-4 w-24 rounded bg-slate-700/80"></div>
                <div class="mt-2 h-5 w-20 rounded bg-slate-700/60"></div>
              </div>
              <div class="w-24">
                <div class="h-3 w-full rounded bg-slate-700/60"></div>
                <div class="mt-2 h-3 w-16 ml-auto rounded bg-slate-700/50"></div>
              </div>
            </div>
            <div class="mt-3 flex gap-1.5">
              <div class="h-5 w-14 rounded bg-slate-700/70"></div>
              <div class="h-5 w-16 rounded bg-slate-700/50"></div>
              <div class="h-5 w-16 rounded bg-slate-700/50"></div>
            </div>
            <div class="mt-3 space-y-2">
              <div class="h-3 w-full rounded bg-slate-700/50"></div>
              <div class="h-3 w-5/6 rounded bg-slate-700/40"></div>
            </div>
          </div>
        </div>

        <div v-else class="px-4 py-6 md:py-8 text-center text-[11px] text-slate-500">
          {{ holdingRows.length ? '当前筛选下暂无持仓' : '暂无持仓股' }}
        </div>
      </section>

      <!-- 观察列表 -->
      <section class="bg-business-dark rounded-xl md:rounded-2xl border border-business-light shadow-business overflow-hidden">
        <div class="px-3 py-2 md:px-4 md:py-2.5 border-b border-business-light flex items-center justify-between">
          <div class="flex items-center gap-2">
            <h3 class="text-xs md:text-[13px] font-bold text-white">观察列表</h3>
            <span class="rounded bg-slate-800 px-1.5 py-0.5 text-[9px] md:text-[10px] font-bold text-slate-300">{{ filteredObservationRows.length }}/{{ observationRows.length }}</span>
            <span v-if="aggressiveObservationCount > 0" class="rounded bg-red-500/10 px-1.5 py-0.5 text-[9px] md:text-[10px] font-bold text-red-300">{{ aggressiveObservationCount }}红标</span>
          </div>
          <div class="flex items-center gap-2">
            <button
              @click="refreshObservationPanel"
              :disabled="observationRefreshing || observationCollapsed || !observationRows.length"
              class="rounded border px-1.5 py-0.5 text-[9px] font-bold transition"
              :class="observationRefreshing || observationCollapsed || !observationRows.length ? 'border-slate-700 text-slate-500' : 'border-cyan-500/30 text-cyan-300 hover:bg-cyan-500/10'"
            >
              {{ observationRefreshing ? '刷新中...' : '刷新价格' }}
            </button>
            <button
              @click="toggleObservationPanel"
              class="rounded border px-1.5 py-0.5 text-[9px] font-bold transition border-slate-700 text-slate-300 hover:border-slate-500"
            >
              {{ observationCollapsed ? '展开' : '折叠' }}
            </button>
            <span v-if="!observationCollapsed && filteredObservationRows.length > 1 && focusFilter === 'all'" class="text-[9px] text-slate-600">拖拽可调整顺序</span>
          </div>
        </div>

        <div v-if="observationCollapsed" class="px-4 py-3 text-[11px] text-slate-400 flex items-center justify-between">
          <span>观察列表默认折叠，避免干扰；需要时再展开查看。</span>
          <button
            @click="expandAndRefreshObservation"
            :disabled="observationRefreshing || !observationRows.length"
            class="rounded border px-2 py-0.5 text-[10px] font-bold transition"
            :class="observationRefreshing || !observationRows.length ? 'border-slate-700 text-slate-500' : 'border-cyan-500/30 text-cyan-300 hover:bg-cyan-500/10'"
          >
            {{ observationRefreshing ? '加载中...' : '展开并刷新' }}
          </button>
        </div>

        <div v-else-if="filteredObservationRows.length" class="p-2 md:p-4 grid grid-cols-1 sm:grid-cols-2 gap-2">
          <div
            v-for="(item, index) in filteredObservationRows"
            :key="`observation-${item.ts_code}`"
            :draggable="focusFilter === 'all'"
            @dragstart="handleDragStart($event, index)"
            @dragover="handleDragOver($event, index)"
            @dragleave="handleDragLeave"
            @drop="handleDrop($event, index)"
            @dragend="handleDragEnd"
            class="rounded-lg border px-3 py-2 transition-all cursor-grab active:cursor-grabbing"
            :class="[
              watchCardClass(item, 'observation'),
              dragOverIndex === index ? 'ring-2 ring-business-accent/50 border-business-accent/40' : ''
            ]"
          >
            <!-- 移动端布局 -->
            <div class="md:hidden">
              <div class="flex items-center justify-between">
                <button class="text-left min-w-0" data-kline-trigger="1" @click.stop="toggleKline(item, $event)">
                  <span class="truncate text-xs font-bold text-white">{{ item.name || '-' }}</span>
                  <span class="ml-1 text-[9px] font-mono text-slate-500">{{ item.ts_code?.replace('.SH','').replace('.SZ','') }}</span>
                </button>
                <div class="flex items-center gap-1 shrink-0">
                  <span class="text-xs font-mono font-bold text-white">{{ fmt(item.price, 2) }}</span>
                  <span class="text-[10px] font-mono font-bold" :class="numColor(item.pct)">{{ item.pct >= 0 ? '+' : '' }}{{ fmt(item.pct, 2, '%') }}</span>
                </div>
              </div>
              <div class="flex items-center justify-between mt-1">
                 <div class="flex items-center gap-1.5 min-w-0">
                   <span class="rounded px-1 py-0.5 text-[8px] font-bold shrink-0" :class="watchSignalClass(item)">{{ getSignalText(item) }}</span>
                   <span v-if="getAggressiveLabel(item)" class="rounded px-1 py-0.5 text-[8px] font-bold bg-red-500/15 text-red-300 shrink-0">{{ getAggressiveLabel(item) }}</span>
                 </div>
                <div class="flex items-center gap-0.5 shrink-0 ml-1">
                   <button @click.stop="openDetailModal(item)" class="p-1 text-slate-500 hover:text-slate-300">
                     <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                   </button>
                   <button @click.stop="openHoldingModal(item)" class="p-1 text-business-accent hover:text-white">
                     <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6v6m0 0v6m0-6h6m-6 0H6" /></svg>
                   </button>
                   <button @click.stop="analyzeWithAI(item, false, 'observation')" :disabled="analyzingStock === item.ts_code" class="p-1 text-purple-300 hover:text-purple-200 disabled:opacity-60">
                     <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" /></svg>
                   </button>
                   <button @click.stop="handleRemove(item.ts_code)" class="p-1 text-rose-400/60 hover:text-rose-300">
                     <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                   </button>
                 </div>
              </div>
              <div v-if="getSignalReasons(item).length" class="mt-1 flex flex-wrap gap-1">
                <span
                  v-for="(reason, idx) in getSignalReasons(item)"
                  :key="`obs-m-reason-${item.ts_code}-${idx}`"
                  class="inline-flex items-center rounded border px-1.5 py-0.5 text-[8px] leading-none"
                  :class="signalReasonChipClass(reason.kind)"
                >
                  {{ reason.title }}
                </span>
              </div>
              <div v-if="getVolumeRatio(item) !== null" class="mt-1">
                <span class="inline-flex items-center rounded border border-cyan-500/20 bg-cyan-500/10 px-1.5 py-0.5 text-[8px] font-mono text-cyan-200">量比 {{ fmt(getVolumeRatio(item), 2) }}</span>
              </div>
              <div v-if="getKeyLevels(item).length" class="mt-1 flex flex-wrap gap-1">
                <span class="text-[8px] text-slate-500">目标位</span>
                <span v-for="level in getKeyLevels(item)" :key="level.label" class="inline-flex items-center gap-0.5 rounded px-1 py-0.5 text-[7px] font-mono" :class="level.isSupport ? 'bg-emerald-500/10 text-emerald-400' : 'bg-rose-500/10 text-rose-400'">
                  <span class="text-[6px] opacity-70">{{ level.shortLabel }}</span>{{ level.priceText }}
                </span>
              </div>
              <p class="mt-1 text-[9px] leading-4 text-slate-400 line-clamp-2">{{ getConclusionText(item) }}</p>
            </div>

            <!-- 桌面端布局 -->
            <div class="hidden md:block">
              <div class="flex items-center justify-between gap-2">
                <button class="min-w-0 text-left" data-kline-trigger="1" @click.stop="toggleKline(item, $event)">
                  <span class="truncate text-[12px] font-bold text-white hover:text-business-accent transition-colors">{{ item.name || '-' }}</span>
                </button>
                <div class="flex items-center gap-1.5 shrink-0">
                  <span class="text-[12px] font-mono font-bold text-slate-100">{{ fmt(item.price, 2) }}</span>
                  <span class="text-[10px] font-mono font-bold" :class="numColor(item.pct)">{{ item.pct >= 0 ? '+' : '' }}{{ fmt(item.pct, 2, '%') }}</span>
                </div>
              </div>
              <div class="mt-1">
                 <div v-if="getSignalReasons(item).length" class="flex flex-wrap gap-1 mb-1">
                   <span
                     v-for="(reason, idx) in getSignalReasons(item)"
                     :key="`obs-d-reason-${item.ts_code}-${idx}`"
                     class="inline-flex items-center rounded border px-1.5 py-0.5 text-[8px] leading-none"
                     :class="signalReasonChipClass(reason.kind)"
                   >
                     {{ reason.title }}
                   </span>
                 </div>
                 <div v-if="getVolumeRatio(item) !== null" class="mb-1">
                   <span class="inline-flex items-center rounded border border-cyan-500/20 bg-cyan-500/10 px-1.5 py-0.5 text-[8px] font-mono text-cyan-200">量比 {{ fmt(getVolumeRatio(item), 2) }}</span>
                 </div>
                 <div v-if="getKeyLevels(item).length" class="flex flex-wrap items-center gap-1 mb-0.5">
                   <span class="text-[8px] uppercase text-slate-500">目标位</span>
                   <span v-for="level in getKeyLevels(item)" :key="level.label" class="inline-flex items-center gap-0.5 rounded px-1 py-0.5 text-[8px] font-mono" :class="level.isSupport ? 'bg-emerald-500/10 text-emerald-400' : 'bg-rose-500/10 text-rose-400'">
                     <span class="text-[7px] opacity-70">{{ level.shortLabel }}</span>{{ level.priceText }}
                   </span>
                 </div>
                 <p class="text-[10px] leading-4 truncate" :class="isAggressiveItem(item) ? 'text-red-200' : 'text-slate-400'">{{ getConclusionText(item) }}</p>
               </div>
              <div class="flex items-center justify-end gap-1 mt-1">
                <button @click.stop="openDetailModal(item)" class="rounded px-1.5 py-0.5 text-[9px] font-bold border border-slate-700/50 text-slate-400 hover:text-slate-200">详情</button>
                <button @click.stop="openHoldingModal(item)" class="rounded px-1.5 py-0.5 text-[9px] font-bold border border-business-accent/30 text-business-accent hover:bg-business-accent/20">+持仓</button>
                <button @click.stop="analyzeWithAI(item, false, 'observation')" :disabled="analyzingStock === item.ts_code" class="rounded px-1.5 py-0.5 text-[9px] font-bold border border-purple-500/30 text-purple-300 hover:bg-purple-500/20 disabled:opacity-60">{{ analyzingStock === item.ts_code ? '...' : 'AI' }}</button>
                <button @click.stop="handleRemove(item.ts_code)" class="rounded px-1.5 py-0.5 text-[9px] font-bold text-rose-400/60 hover:text-rose-300 hover:bg-rose-500/10">×</button>
              </div>
            </div>
          </div>
        </div>

        <div v-else-if="refreshing && !rows.length" class="px-4 py-6 md:py-8 text-center text-[11px] text-slate-500">
          正在加载观察列表...
        </div>

        <div v-else class="px-4 py-6 md:py-8 text-center text-[11px] text-slate-500">
          {{ observationRows.length ? '当前筛选下暂无观察股' : '暂无观察股' }}
        </div>
      </section>
    </div>

    <!-- Zen模式全屏覆盖 -->
    <div
      v-if="zenMode"
      class="fixed inset-0 z-50 bg-black flex flex-col items-center justify-center"
      @click="exitZenMode"
    >
      <div class="text-center" @click.stop>
        <div class="text-[10px] text-gray-600 mb-12">轻触空白处退出</div>
        <div class="flex flex-wrap justify-center gap-12 max-w-4xl">
          <div
            v-for="item in holdingRows"
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
        <div v-if="!holdingRows.length" class="text-gray-500 text-lg mt-20">
          暂无持仓股
        </div>
      </div>
    </div>

    <!-- K线悬浮窗 -->
    <div
      ref="klinePopupRef"
      v-if="hoverInfo.visible && !zenMode"
      class="fixed z-40 left-0 right-0 bottom-0 md:left-auto md:right-4 md:bottom-4 md:w-[70vw] md:max-w-3xl bg-business-dark border-t md:border border-business-light md:rounded-2xl p-2 md:p-3 shadow-2xl max-h-[70vh] md:max-h-[78vh] overflow-y-auto"
      @click.stop
    >
      <div class="flex items-center justify-between gap-2 mb-2">
        <div class="min-w-0">
          <h3 class="text-xs font-bold text-slate-200">K线分析</h3>
          <p class="text-[10px] text-slate-500 truncate">{{ hoverInfo.stock?.name || '-' }} · {{ hoverInfo.stock?.ts_code || '-' }}</p>
        </div>
        <button @click="hideKline" class="md:hidden shrink-0 p-1 text-slate-400 hover:text-white">
          <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
        </button>
      </div>
      <div v-if="klineLoading" class="h-[200px] md:h-[340px] flex items-center justify-center text-slate-400 text-xs">加载中...</div>
      <div v-else-if="klineError" class="rounded border border-red-700/40 bg-red-500/10 px-3 py-2 text-xs text-red-300">{{ klineError }}</div>
      <template v-else>
        <div class="grid grid-cols-3 md:grid-cols-6 gap-1 md:gap-1.5 mb-2 text-[9px] md:text-[10px]" v-if="latestKlineData">
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-1.5 md:px-2 py-1 md:py-1.5">
            <span class="text-slate-500">收盘</span>
            <p class="text-slate-200 mt-0.5 font-mono">{{ fmt(latestKlineData.close, 2) }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-1.5 md:px-2 py-1 md:py-1.5">
            <span class="text-slate-500">成交额</span>
            <p class="text-slate-200 mt-0.5 font-mono">{{ fmtAmount(latestKlineData.amount) }}</p>
          </div>
          <div class="rounded border border-slate-700/60 bg-slate-900/40 px-1.5 md:px-2 py-1 md:py-1.5">
            <span class="text-slate-500">主力</span>
            <p class="mt-0.5 font-mono" :class="latestKlineData.net_mf_vol >= 0 ? 'text-cyan-400' : 'text-rose-400'">{{ fmtMf(latestKlineData.net_mf_vol) }}</p>
          </div>
          <div class="hidden md:block rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
            <span class="text-slate-500">成交量</span>
            <p class="text-slate-200 mt-0.5 font-mono">{{ fmtVol(latestKlineData.vol) }}</p>
          </div>
          <div class="hidden md:block rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
            <span class="text-slate-500">MA5/10/20</span>
            <p class="text-slate-200 mt-0.5 font-mono text-[9px]">{{ fmt(latestKlineData.ma5, 2) }} / {{ fmt(latestKlineData.ma10, 2) }} / {{ fmt(latestKlineData.ma20, 2) }}</p>
          </div>
          <div class="hidden md:block rounded border border-slate-700/60 bg-slate-900/40 px-2 py-1.5">
            <span class="text-slate-500">融资余额</span>
            <p class="text-slate-200 mt-0.5 font-mono">{{ latestKlineData.rzye ? fmtRzye(latestKlineData.rzye) : '-' }}</p>
          </div>
        </div>
        <div class="h-[200px] md:h-[340px] lg:h-[380px]">
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
            <p v-if="detailModal.detail?.action_signal?.headline" class="mt-2 text-xs leading-relaxed text-slate-300">
              {{ detailModal.detail.action_signal.headline }}
            </p>
          </div>

          <div v-if="detailModal.detail?.trade_plan?.entry" class="space-y-2">
            <h4 class="text-emerald-400 font-semibold text-xs uppercase mb-2">执行计划</h4>
            <div class="grid grid-cols-1 gap-2">
              <div v-if="detailModal.detail.trade_plan.current" class="bg-slate-800/50 rounded-lg p-3">
                <div class="text-[10px] font-bold text-slate-500 uppercase mb-1">当前动作</div>
                <p class="text-slate-200 text-xs leading-relaxed">{{ detailModal.detail.trade_plan.current }}</p>
              </div>
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

          <div v-if="detailModal.detail?.signal_reasons?.length || detailModal.detail?.intraday_context?.snapshot" class="space-y-2">
            <h4 class="text-slate-200 font-semibold text-xs uppercase mb-2">操作依据</h4>
            <div v-if="detailModal.detail?.intraday_context?.snapshot" class="rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs leading-relaxed text-slate-300">
              {{ detailModal.detail.intraday_context.snapshot }} · {{ detailModal.detail.intraday_context.zone || '位置待确认' }}
            </div>
            <div class="space-y-2">
              <div
                v-for="(reason, idx) in detailModal.detail.signal_reasons || []"
                :key="`reason-${idx}`"
                class="rounded-lg border px-3 py-2"
                :class="signalReasonCardClass(reason.kind)"
              >
                <div class="flex items-center gap-2 mb-1">
                  <span class="rounded px-1.5 py-0.5 text-[10px] font-bold" :class="signalReasonChipClass(reason.kind)">
                    {{ reason.title }}
                  </span>
                </div>
                <p class="text-xs leading-relaxed text-slate-200">{{ reason.desc }}</p>
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
                <span class="text-slate-500">量比</span>
                <div class="text-white font-mono">{{ Number.isFinite(Number(detailModal.detail.technical.volume_ratio)) ? fmt(detailModal.detail.technical.volume_ratio, 2) : '-' }}</div>
              </div>
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">换手率</span>
                <div class="text-white font-mono">{{ Number.isFinite(Number(detailModal.detail.technical.turnover)) ? fmt(detailModal.detail.technical.turnover, 1, '%') : '-' }}</div>
              </div>
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">主力净额</span>
                <div
                  class="font-mono"
                  :class="Number.isFinite(Number(detailModal.detail.technical.net_mf_amount)) && Number(detailModal.detail.technical.net_mf_amount) >= 0 ? 'text-red-300' : 'text-emerald-300'"
                >
                  {{ fmtFlowAmount(detailModal.detail.technical.net_mf_amount) }}
                </div>
              </div>
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">综合因子</span>
                <div class="text-white font-mono">{{ Number.isFinite(Number(detailModal.detail.technical.factor_score)) ? fmt(detailModal.detail.technical.factor_score, 1) : '-' }}</div>
              </div>
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">趋势分</span>
                <div class="text-white font-mono">{{ Number.isFinite(Number(detailModal.detail.technical.trend_factor)) ? fmt(detailModal.detail.technical.trend_factor, 1) : '-' }}</div>
              </div>
              <div class="bg-slate-800/30 rounded p-2">
                <span class="text-slate-500">MA20 / MA60</span>
                <div class="text-white font-mono text-[11px]">
                  {{ detailModal.detail.technical.ma20 ? fmt(detailModal.detail.technical.ma20, 2) : '-' }}
                  /
                  {{ detailModal.detail.technical.ma60 ? fmt(detailModal.detail.technical.ma60, 2) : '-' }}
                </div>
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

    <!-- 图片更新持仓 -->
    <div v-if="showImageHoldingModal" class="fixed inset-0 z-[200] bg-black/80 flex items-center justify-center p-2 md:p-4" @click="closeImageHoldingModal">
      <div class="bg-[#1a1a2e] border border-slate-600 rounded-xl w-full max-w-2xl max-h-[88vh] overflow-hidden flex flex-col" @click.stop>
        <div class="flex items-center justify-between p-4 border-b border-slate-700">
          <div>
            <h3 class="text-white font-bold text-sm">图片更新持仓</h3>
            <p class="mt-0.5 text-[10px] text-slate-500">先识别预览，再一键写入持仓并同步到自选</p>
          </div>
          <button @click="closeImageHoldingModal" class="text-slate-400 hover:text-white">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div class="overflow-y-auto p-4 space-y-4 flex-1">
          <input ref="holdingImageInputRef" type="file" accept="image/*" class="hidden" @change="handleHoldingImageChange" />

          <div class="rounded-xl border border-slate-700/60 bg-slate-900/40 p-3">
            <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
              <div class="min-w-0">
                <div class="text-[10px] font-bold uppercase text-slate-500">识别图片</div>
                <p class="mt-1 text-xs text-slate-300 truncate">{{ holdingImageFile?.name || '请选择券商持仓截图' }}</p>
              </div>
              <div class="flex items-center gap-2 shrink-0">
                <button @click="triggerHoldingImageSelect" class="px-3 py-2 rounded-lg text-[11px] font-bold border border-cyan-500/30 text-cyan-300 hover:bg-cyan-500/10">
                  {{ holdingImageFile ? '更换图片' : '选择图片' }}
                </button>
                <button
                  @click="parseHoldingImage"
                  :disabled="!holdingImageFile || holdingImageParsing"
                  class="px-3 py-2 rounded-lg text-[11px] font-bold border border-business-accent/30 text-business-accent hover:bg-business-accent/10 disabled:opacity-50"
                >
                  {{ holdingImageParsing ? '识别中...' : '重新识别' }}
                </button>
              </div>
            </div>
          </div>

          <div class="grid grid-cols-1 md:grid-cols-2 gap-2">
            <label class="flex items-start gap-2 rounded-xl border border-slate-700/60 bg-slate-900/30 px-3 py-2 cursor-pointer">
              <input v-model="holdingImageImportForm.sync_watchlist" type="checkbox" class="mt-0.5 rounded border-slate-600 bg-slate-900 text-cyan-400 focus:ring-cyan-500/40" />
              <div>
                <div class="text-[11px] font-bold text-slate-200">同步到自选</div>
                <p class="mt-0.5 text-[10px] leading-4 text-slate-500">识别出的持仓若不在 Watchlist，会自动加入观察。</p>
              </div>
            </label>
            <label class="flex items-start gap-2 rounded-xl border border-slate-700/60 bg-slate-900/30 px-3 py-2 cursor-pointer">
              <input v-model="holdingImageImportForm.replace_missing" type="checkbox" class="mt-0.5 rounded border-slate-600 bg-slate-900 text-rose-400 focus:ring-rose-500/40" />
              <div>
                <div class="text-[11px] font-bold text-slate-200">按图片全量替换</div>
                <p class="mt-0.5 text-[10px] leading-4 text-slate-500">仅在截图覆盖全部持仓时勾选，未识别到的旧持仓会被删除。</p>
              </div>
            </label>
          </div>

          <div v-if="holdingImageError" class="rounded-lg border border-red-700/40 bg-red-500/10 px-3 py-2 text-xs text-red-300">
            {{ holdingImageError }}
          </div>

          <div v-if="holdingImageParsing" class="rounded-xl border border-slate-700/60 bg-slate-900/30 px-4 py-6 text-center text-xs text-slate-400">
            正在识别截图中的持仓明细...
          </div>

          <template v-else-if="holdingImagePreview">
            <div class="grid grid-cols-3 gap-2">
              <div class="rounded-xl border border-slate-700/60 bg-slate-900/40 px-3 py-2">
                <div class="text-[10px] text-slate-500">识别项</div>
                <div class="mt-1 text-lg font-mono text-white">{{ holdingImagePreview.summary?.total_items || 0 }}</div>
              </div>
              <div class="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-3 py-2">
                <div class="text-[10px] text-slate-500">已匹配</div>
                <div class="mt-1 text-lg font-mono text-white">{{ holdingImagePreview.summary?.matched_count || 0 }}</div>
              </div>
              <div class="rounded-xl border border-amber-500/20 bg-amber-500/10 px-3 py-2">
                <div class="text-[10px] text-slate-500">待确认</div>
                <div class="mt-1 text-lg font-mono text-white">{{ holdingImagePreview.summary?.unmatched_count || 0 }}</div>
              </div>
            </div>

            <div v-if="holdingImagePreview.items?.length" class="rounded-xl border border-slate-700/60 overflow-hidden">
              <div class="px-3 py-2 border-b border-slate-700/60 bg-slate-900/50 text-[10px] font-bold uppercase text-slate-500">识别预览</div>
              <div class="max-h-64 overflow-y-auto">
                <div
                  v-for="item in holdingImagePreview.items"
                  :key="`${item.ts_code || item.source_code || item.name}-${item.shares}`"
                  class="grid grid-cols-[1.6fr_0.8fr_0.8fr] gap-2 px-3 py-2 border-b border-slate-800/70 last:border-0 text-xs"
                >
                  <div class="min-w-0">
                    <div class="flex items-center gap-2 min-w-0">
                      <span class="truncate font-bold text-slate-100">{{ item.name }}</span>
                      <span class="text-[10px] font-mono text-slate-500 shrink-0">{{ item.ts_code || item.source_code || '-' }}</span>
                    </div>
                    <div class="mt-1 flex items-center gap-2 text-[10px]">
                      <span
                        class="rounded px-1.5 py-0.5 font-bold"
                        :class="item.status === 'matched' ? 'bg-emerald-500/15 text-emerald-300' : 'bg-amber-500/15 text-amber-300'"
                      >
                        {{ item.status === 'matched' ? `已匹配 · ${item.matched_by}` : '待确认' }}
                      </span>
                      <span v-if="item.warning" class="truncate text-amber-300">{{ item.warning }}</span>
                    </div>
                  </div>
                  <div class="text-right">
                    <div class="text-[10px] text-slate-500">股数</div>
                    <div class="mt-1 font-mono text-slate-100">{{ item.shares }}</div>
                  </div>
                  <div class="text-right">
                    <div class="text-[10px] text-slate-500">成本</div>
                    <div class="mt-1 font-mono text-slate-100">{{ item.avg_cost == null ? '-' : fmt(item.avg_cost, 2) }}</div>
                  </div>
                </div>
              </div>
            </div>

            <div v-if="holdingImagePreview.notes?.length" class="rounded-xl border border-slate-700/60 bg-slate-900/30 p-3">
              <div class="text-[10px] font-bold uppercase text-slate-500">识别备注</div>
              <div class="mt-2 space-y-1">
                <p v-for="(note, idx) in holdingImagePreview.notes" :key="`note-${idx}`" class="text-xs leading-5 text-slate-300">{{ note }}</p>
              </div>
            </div>
          </template>
        </div>

        <div class="flex items-center justify-between gap-3 p-4 border-t border-slate-700">
          <p class="text-[10px] text-slate-500">默认只更新识别到的持仓，不会删除旧持仓。</p>
          <div class="flex items-center gap-2">
            <button @click="closeImageHoldingModal" class="px-3 py-2 rounded-lg text-[11px] font-bold border border-slate-700 text-slate-300 hover:text-white">关闭</button>
            <button
              @click="applyHoldingImageResult"
              :disabled="holdingImageApplying || !holdingImagePreview?.matched_items?.length"
              class="px-3 py-2 rounded-lg text-[11px] font-bold bg-cyan-600 text-white hover:bg-cyan-500 disabled:opacity-50"
            >
              {{ holdingImageApplying ? '更新中...' : '一键更新持仓' }}
            </button>
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
import { getWatchlistRealtime, getWatchlistAnalysis, addToWatchlist, removeFromWatchlist, reorderWatchlist, getStockKline, getUserHoldings, updateUserHolding, deleteUserHolding, analyzeStockWithAI, parseHoldingsImage, batchUpdateUserHoldings } from '@/services/api';
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
import { SparklesIcon } from '@heroicons/vue/20/solid';
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
const WATCHLIST_CACHE_KEY = 'jarvis-watchlist-snapshot-v2';

const loading = ref(false);
const refreshing = ref(false);
const isTradingTime = ref(false);
const message = ref('');
const lastRefreshAt = ref('');
const rows = ref([]);
const newCode = ref('');
const klineHoveredIndex = ref(-1);  // K线图当前hover的索引

// 持仓相关
const holdings = ref({});  // { ts_code: { shares, avg_cost } }
const showHoldingModal = ref(false);
const holdingStock = ref(null);
const holdingForm = reactive({ shares: 0, avg_cost: 0 });
const showImageHoldingModal = ref(false);
const holdingImageInputRef = ref(null);
const holdingImageFile = ref(null);
const holdingImageParsing = ref(false);
const holdingImageApplying = ref(false);
const holdingImageError = ref('');
const holdingImagePreview = ref(null);
const holdingImageImportForm = reactive({
  replace_missing: false,
  sync_watchlist: true,
});

// AI分析相关
const aiAnalysisModal = ref({ visible: false, stock: null, loading: false, result: '', fromCache: false });
const analyzingStock = ref(null);

// 拖拽排序相关
const dragOverIndex = ref(-1);
const isDragging = ref(false);
let dragSrcCode = null;
const focusFilter = ref('all');
const observationCollapsed = ref(true);
const observationRefreshing = ref(false);

const handleDragStart = (e, index) => {
  if (focusFilter.value !== 'all') return;
  isDragging.value = true;
  dragSrcCode = observationRows.value[index]?.ts_code;
  e.dataTransfer.effectAllowed = 'move';
  e.dataTransfer.setData('text/plain', index);
};

const handleDragOver = (e, index) => {
  if (focusFilter.value !== 'all') return;
  e.preventDefault();
  e.dataTransfer.dropEffect = 'move';
  dragOverIndex.value = index;
};

const handleDragLeave = () => {
  dragOverIndex.value = -1;
};

const handleDrop = async (e, targetIndex) => {
  if (focusFilter.value !== 'all') return;
  e.preventDefault();
  dragOverIndex.value = -1;
  isDragging.value = false;

  if (!dragSrcCode) return;
  const targetCode = observationRows.value[targetIndex]?.ts_code;
  if (!targetCode || dragSrcCode === targetCode) {
    dragSrcCode = null;
    return;
  }

  // 在 rows 中重排
  const allCodes = rows.value.map(r => r.ts_code);
  const srcGlobalIndex = allCodes.indexOf(dragSrcCode);
  const tgtGlobalIndex = allCodes.indexOf(targetCode);
  if (srcGlobalIndex < 0 || tgtGlobalIndex < 0) {
    dragSrcCode = null;
    return;
  }

  const [moved] = allCodes.splice(srcGlobalIndex, 1);
  allCodes.splice(tgtGlobalIndex, 0, moved);

  // 乐观更新 rows 中的顺序
  const newRows = [...rows.value];
  const [movedRow] = newRows.splice(srcGlobalIndex, 1);
  newRows.splice(tgtGlobalIndex, 0, movedRow);
  rows.value = newRows;
  persistWatchlistCache();
  dragSrcCode = null;

  // 只提交非持仓（观察列表）的顺序给后端
  const nonHoldingCodes = allCodes.filter(c => !hasHolding(c));
  try {
    await reorderWatchlist(nonHoldingCodes);
  } catch (err) {
    // ignore
  }
};

const handleDragEnd = () => {
  isDragging.value = false;
  dragOverIndex.value = -1;
  dragSrcCode = null;
};

const handleUpdateAxisPointer = (event) => {
  const info = event.dataIndex;
  if (info != null && info >= 0) {
    klineHoveredIndex.value = info;
  }
};

// Zen模式
const zenMode = ref(false);

const toggleZenMode = () => {
  zenMode.value = !zenMode.value;
  startAutoRefresh();
};

const exitZenMode = () => {
  zenMode.value = false;
  startAutoRefresh();
};

const handleKeydown = (e) => {
  if (e.key === 'Escape' && zenMode.value) {
    exitZenMode();
  }
};

// 搜索功能
const klinePopupRef = ref(null);
let searchTimer = null;
let timer = null;

// 使用新的搜索composable
const {
  searchResults,
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
  const needsFetch = !detailModal.detail || detailModal.detail?._compact;
  detailModal.loading = needsFetch;
  detailModal.error = '';

  if (!needsFetch || !stock?.ts_code) {
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

const fmtFlowAmount = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n) || n === 0) return '-';
  const abs = Math.abs(n);
  const sign = n > 0 ? '+' : '-';
  if (abs >= 10000) return `${sign}${(abs / 10000).toFixed(2)}亿`;
  return `${sign}${abs.toFixed(0)}万`;
};

const fmt = (v, digits = 2, suffix = '') => {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return `${n.toFixed(digits)}${suffix}`;
};

const formatClock = (date = new Date()) => date.toLocaleTimeString('zh-CN', { hour12: false });

const numColor = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n) || n === 0) return 'text-slate-300';
  return n > 0 ? 'text-red-400' : 'text-emerald-400';
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
  if (!s || s === '观望') return 'bg-white/[0.08] text-slate-100 border border-white/15';
  if (s.includes('试错') || s.includes('主动进攻')) return 'bg-red-500/20 text-red-300 border border-red-500/30';
  if (s.includes('关注')) return 'bg-red-500/[0.14] text-red-200 border border-red-500/25';
  if (s.includes('减仓') || s.includes('持币') || s.includes('回避')) return 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30';
  return 'bg-white/[0.08] text-slate-100 border border-white/15';
};

const stopAutoRefresh = () => {
  if (timer) {
    clearTimeout(timer);
    timer = null;
  }
};

const readWatchlistCache = () => {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.sessionStorage.getItem(WATCHLIST_CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch (e) {
    console.error('读取 watchlist 缓存失败', e);
    return null;
  }
};

const persistWatchlistCache = () => {
  if (typeof window === 'undefined') return;
  try {
    window.sessionStorage.setItem(WATCHLIST_CACHE_KEY, JSON.stringify({
      rows: rows.value,
      holdings: holdings.value,
      isTradingTime: isTradingTime.value,
      lastRefreshAt: lastRefreshAt.value,
      cachedAt: Date.now(),
    }));
  } catch (e) {
    console.error('写入 watchlist 缓存失败', e);
  }
};

const restoreWatchlistCache = () => {
  const cached = readWatchlistCache();
  if (!cached) return false;

  rows.value = Array.isArray(cached.rows) ? cached.rows : [];
  holdings.value = cached.holdings && typeof cached.holdings === 'object' ? cached.holdings : {};
  isTradingTime.value = !!cached.isTradingTime;
  lastRefreshAt.value = typeof cached.lastRefreshAt === 'string' ? cached.lastRefreshAt : '';

  const hasSnapshot = rows.value.length > 0 || Object.keys(holdings.value).length > 0;
  if (hasSnapshot) {
    message.value = '已恢复上次快照，后台刷新中';
  }
  return hasSnapshot;
};

const normalizeCodes = (codes = []) => {
  return Array.from(new Set((codes || []).map(code => String(code || '').trim()).filter(Boolean)));
};

const getHoldingCodeList = () => normalizeCodes(
  rows.value
    .filter(item => Number(holdings.value[item.ts_code]?.shares || 0) > 0)
    .map(item => item.ts_code)
);

const getObservationCodeList = () => normalizeCodes(
  rows.value
    .filter(item => Number(holdings.value[item.ts_code]?.shares || 0) <= 0)
    .map(item => item.ts_code)
);

const mergePartialRows = (nextRows = []) => {
  const patchMap = new Map((nextRows || []).map(item => [item.ts_code, item]));
  const merged = rows.value.map(item => patchMap.get(item.ts_code) || item);
  const exists = new Set(merged.map(item => item.ts_code));
  for (const item of nextRows || []) {
    if (item?.ts_code && !exists.has(item.ts_code)) {
      merged.push(item);
      exists.add(item.ts_code);
    }
  }
  rows.value = merged;
};

const resolveRefreshScope = (scope = 'auto') => {
  if (scope === 'auto') {
    const holdingCodes = getHoldingCodeList();
    if (observationCollapsed.value && holdingCodes.length) return 'holding';
    return 'all';
  }
  return scope;
};

const fetchWatchlistRows = async (scope = 'all') => {
  let codes = null;
  if (scope === 'holding') {
    codes = getHoldingCodeList();
  } else if (scope === 'observation') {
    codes = getObservationCodeList();
  }
  const codeParam = Array.isArray(codes) && codes.length ? codes.join(',') : null;
  return getWatchlistRealtime(codeParam, 'sina', 'compact');
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

const refreshNow = async (showLoading = true, syncHoldings = false, scope = 'auto') => {
  if (refreshing.value) return;

  refreshing.value = true;
  loading.value = showLoading && !rows.value.length;
  if (showLoading) {
    message.value = rows.value.length ? '保持当前快照展示，后台刷新中' : '正在加载盯盘列表';
  }
  try {
    const effectiveScope = resolveRefreshScope(scope);
    if (effectiveScope === 'observation' && !getObservationCodeList().length) {
      message.value = '暂无观察股可刷新';
      return;
    }
    const requestScope = effectiveScope === 'holding' && !getHoldingCodeList().length ? 'all' : effectiveScope;
    const holdingsPromise = syncHoldings
      ? fetchHoldingsMap().catch((e) => {
          console.error('刷新时获取持仓失败', e);
          return null;
        })
      : null;

    const [res, nextHoldings] = await Promise.all([
      fetchWatchlistRows(requestScope),
      holdingsPromise || Promise.resolve(null),
    ]);
    const body = res.data || {};
    const nextRows = body.data || [];
    if (nextHoldings) {
      holdings.value = nextHoldings;
    }
    if (requestScope === 'all') {
      rows.value = nextRows;
    } else {
      mergePartialRows(nextRows);
    }
    isTradingTime.value = !!body.is_trading_time;
    if (effectiveScope === 'observation') {
      message.value = `观察列表已刷新（${nextRows.length}）`;
    } else if (effectiveScope === 'holding') {
      message.value = `持仓价格已刷新（${nextRows.length}）`;
    } else {
      message.value = body.message || '';
    }
    lastRefreshAt.value = formatClock();
    persistWatchlistCache();
  } catch (e) {
    message.value = e?.response?.data?.detail || e?.message || '刷新失败';
  } finally {
    refreshing.value = false;
    loading.value = false;
    // 无论成功还是失败，在结束后开启下一次计时
    startAutoRefresh();
  }
};

const refreshObservationPanel = async () => {
  if (!observationRows.value.length) {
    message.value = '暂无观察股可刷新';
    return;
  }
  observationRefreshing.value = true;
  try {
    await refreshNow(false, false, 'observation');
  } finally {
    observationRefreshing.value = false;
  }
};

const toggleObservationPanel = () => {
  observationCollapsed.value = !observationCollapsed.value;
};

const expandAndRefreshObservation = async () => {
  observationCollapsed.value = false;
  await refreshObservationPanel();
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
const buildHoldingsMap = (holdingsList = []) => {
  const nextHoldings = {};
  for (const item of holdingsList || []) {
    if (!item?.ts_code) continue;
    nextHoldings[item.ts_code] = {
      shares: item.shares,
      avg_cost: item.avg_cost,
    };
  }
  return nextHoldings;
};

const fetchHoldingsMap = async () => {
  const res = await getUserHoldings();
  const holdingsList = Array.isArray(res.data) ? res.data : (res.data?.holdings || []);
  return buildHoldingsMap(holdingsList);
};

const fetchHoldings = async () => {
  try {
    holdings.value = await fetchHoldingsMap();
    persistWatchlistCache();
    return holdings.value;
  } catch (e) {
    console.error('获取持仓失败', e);
    return null;
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
    persistWatchlistCache();
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
    persistWatchlistCache();
    showHoldingModal.value = false;
  } catch (e) {
    alert('删除持仓失败: ' + (e.response?.data?.detail || e.message));
  }
};

const resetHoldingImageState = () => {
  holdingImageParsing.value = false;
  holdingImageApplying.value = false;
  holdingImageError.value = '';
  holdingImagePreview.value = null;
  holdingImageFile.value = null;
  holdingImageImportForm.replace_missing = false;
  holdingImageImportForm.sync_watchlist = true;
  if (holdingImageInputRef.value) {
    holdingImageInputRef.value.value = '';
  }
};

const openImageHoldingModal = () => {
  resetHoldingImageState();
  showImageHoldingModal.value = true;
};

const closeImageHoldingModal = () => {
  if (holdingImageParsing.value || holdingImageApplying.value) return;
  showImageHoldingModal.value = false;
};

const triggerHoldingImageSelect = () => {
  holdingImageInputRef.value?.click?.();
};

const handleHoldingImageChange = async (event) => {
  const [file] = event?.target?.files || [];
  if (!file) return;
  holdingImageFile.value = file;
  await parseHoldingImage();
};

const parseHoldingImage = async () => {
  if (!holdingImageFile.value) return;
  holdingImageParsing.value = true;
  holdingImageError.value = '';
  try {
    const formData = new FormData();
    formData.append('file', holdingImageFile.value);
    const res = await parseHoldingsImage(formData);
    holdingImagePreview.value = res.data?.data || null;
  } catch (e) {
    holdingImagePreview.value = null;
    holdingImageError.value = e?.response?.data?.detail || e?.message || '识别失败';
  } finally {
    holdingImageParsing.value = false;
  }
};

const applyHoldingImageResult = async () => {
  const matchedItems = holdingImagePreview.value?.matched_items || [];
  if (!matchedItems.length) {
    holdingImageError.value = '没有可应用的匹配持仓';
    return;
  }

  holdingImageApplying.value = true;
  holdingImageError.value = '';
  try {
    await batchUpdateUserHoldings({
      items: matchedItems.map(item => ({
        ts_code: item.ts_code,
        name: item.name,
        shares: item.shares,
        avg_cost: item.avg_cost ?? 0,
      })),
      replace_missing: holdingImageImportForm.replace_missing,
      sync_watchlist: holdingImageImportForm.sync_watchlist,
    });
    message.value = `已从图片同步 ${matchedItems.length} 条持仓`;
    showImageHoldingModal.value = false;
    await refreshNow(true, true);
  } catch (e) {
    holdingImageError.value = e?.response?.data?.detail || e?.message || '批量更新失败';
  } finally {
    holdingImageApplying.value = false;
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

const getHoldingLossLevel = (item) => {
  const pnl = calcPnL(item);
  if (!pnl) return 'none';
  if (pnl.pnlPct <= -10) return 'critical';  // 亏损超过-10%
  if (pnl.pnlPct <= -6) return 'warning';     // 亏损超过-6%
  return 'normal';
};

const hasHolding = (tsCode) => Number(holdings.value[tsCode]?.shares || 0) > 0;

const isDefensiveItem = (item) => {
  const signal = getSignalText(item);
  return signal.includes('减仓') || signal.includes('回避') || signal.includes('持币');
};

const isAttentionItem = (item) => getSignalText(item).includes('关注');

const isActionableItem = (item, kind = 'observation') => {
  if (isAggressiveItem(item) || isAttentionItem(item) || isDefensiveItem(item)) return true;
  if (kind === 'holding') {
    const lossLevel = getHoldingLossLevel(item);
    return lossLevel === 'critical' || lossLevel === 'warning';
  }
  return false;
};

const isRiskyItem = (item, kind = 'observation') => {
  if (isDefensiveItem(item)) return true;
  if (kind === 'holding') {
    const lossLevel = getHoldingLossLevel(item);
    return lossLevel === 'critical' || lossLevel === 'warning';
  }
  return false;
};

const getHoldingPriorityScore = (item) => {
  let score = 0;
  const lossLevel = getHoldingLossLevel(item);
  if (lossLevel === 'critical') score += 400;
  else if (lossLevel === 'warning') score += 260;
  if (isDefensiveItem(item)) score += 180;
  if (isAggressiveItem(item)) score += 120;
  if (isAttentionItem(item)) score += 80;
  const holdingValue = (holdings.value[item.ts_code]?.shares || 0) * (item.price || 0);
  score += Math.min(holdingValue / 10000, 60);
  return score;
};

const holdingRows = computed(() => {
  return rows.value
    .filter(item => hasHolding(item.ts_code))
    .sort((a, b) => {
      const priorityDelta = getHoldingPriorityScore(b) - getHoldingPriorityScore(a);
      if (priorityDelta !== 0) return priorityDelta;
      const valA = (holdings.value[a.ts_code]?.shares || 0) * (a.price || 0);
      const valB = (holdings.value[b.ts_code]?.shares || 0) * (b.price || 0);
      return valB - valA;
    });
});
const observationRows = computed(() => rows.value.filter(item => !hasHolding(item.ts_code)));

const matchesFocusFilter = (item, kind = 'observation') => {
  if (focusFilter.value === 'all') return true;
  if (focusFilter.value === 'actionable') return isActionableItem(item, kind);
  if (focusFilter.value === 'risk') return isRiskyItem(item, kind);
  return true;
};

const filteredHoldingRows = computed(() => holdingRows.value.filter(item => matchesFocusFilter(item, 'holding')));
const filteredObservationRows = computed(() => observationRows.value.filter(item => matchesFocusFilter(item, 'observation')));
const riskyHoldingCount = computed(() => holdingRows.value.filter(item => isRiskyItem(item, 'holding')).length);
const focusFilterHint = computed(() => {
  if (focusFilter.value === 'actionable') return '只保留需要动作确认的标的';
  if (focusFilter.value === 'risk') return '只保留持仓风险与防守信号';
  return '展示完整列表，但持仓仍按风险优先排序';
});

const refreshStatusText = computed(() => {
  if (refreshing.value) return rows.value.length ? '后台刷新中' : '加载中';
  return isTradingTime.value ? '自动实时刷新' : '自动轮询';
});
const refreshCycleText = computed(() => {
  if (zenMode.value) return `专注模式 ${Math.round(REFRESH_MS_ZEN / 1000)}s`;
  return isTradingTime.value
    ? `盘中 ${Math.round(REFRESH_MS_TRADING / 1000)}s`
    : `盘后 ${Math.round(REFRESH_MS_IDLE / 1000)}s`;
});

const truncateText = (text, limit = 64) => {
  const raw = String(text || '').replace(/\s+/g, ' ').trim();
  if (!raw) return '暂无结论';
  if (raw.length <= limit) return raw;
  return `${raw.slice(0, limit).trim()}...`;
};

const getSignalKind = (item) => {
  const detailColor = String(item?.analyze?.detail?.action_signal?.color || '').trim();
  if (detailColor === 'buy' || detailColor === 'sell' || detailColor === 'watch') return detailColor;

  const signal = String(item?.analyze?.suggestion || '').trim();
  if (signal.includes('主动进攻') || signal.includes('试错') || signal.includes('关注')) return 'buy';
  if (signal.includes('减仓') || signal.includes('回避') || signal.includes('持币')) return 'sell';
  return 'watch';
};

const getRawConclusion = (item) => {
  const headline = String(
    item?.analyze?.detail?.action_signal?.headline
    || item?.analyze?.detail?.trade_plan?.current
    || ''
  ).trim();
  if (headline) return headline;

  const direct = String(item?.analyze?.conclusion || '').trim();
  if (direct) return direct;

  const summary = String(item?.analyze?.summary || '').trim();
  if (summary) {
    const segments = summary.split('|').map(part => part.trim()).filter(Boolean);
    const conclusion = segments.find(part => part.startsWith('【结论】')) || segments[0];
    if (conclusion) {
      return conclusion.replace(/^【[^】]+】/, '').trim();
    }
  }

  const suggestion = String(item?.analyze?.suggestion || '').trim();
  return suggestion ? `建议 ${suggestion}` : '暂无结论';
};

const getConclusionText = (item) => {
  return truncateText(getRawConclusion(item), 70);
};

const getSignalReasons = (item, limit = 2) => {
  const reasons = item?.analyze?.detail?.signal_reasons;
  if (!Array.isArray(reasons) || !reasons.length) return [];
  return reasons.filter(Boolean).slice(0, limit);
};

const getTriggerText = (item) => {
  const text = String(
    item?.analyze?.detail?.action_signal?.trigger
    || item?.analyze?.detail?.trade_plan?.entry
    || ''
  ).trim();
  if (!text) return '';
  return truncateText(text, 56);
};

const getInvalidText = (item) => {
  const text = String(
    item?.analyze?.detail?.action_signal?.fallback
    || item?.analyze?.detail?.trade_plan?.invalid
    || ''
  ).trim();
  if (!text) return '';
  return truncateText(text, 56);
};

const getVolumeRatio = (item) => {
  const direct = Number(item?.volume_ratio);
  if (Number.isFinite(direct) && direct > 0) return direct;

  const detailValue = Number(item?.analyze?.detail?.technical?.volume_ratio);
  if (Number.isFinite(detailValue) && detailValue > 0) return detailValue;

  return null;
};

const getTurnoverRate = (item) => {
  const direct = Number(item?.turnover_rate);
  if (Number.isFinite(direct) && direct >= 0) return direct;

  const detailValue = Number(
    item?.analyze?.detail?.technical?.turnover
    ?? item?.analyze?.detail?.technical?.turnover_rate
  );
  if (Number.isFinite(detailValue) && detailValue >= 0) return detailValue;

  return null;
};

const getKeyLevels = (item) => {
  const levels = item?.analyze?.detail?.key_levels;
  if (!Array.isArray(levels) || !levels.length) return [];
  return levels.map(l => {
    const price = Number(l.price);
    if (!Number.isFinite(price)) return null;
    const label = String(l.label || '');
    const isSupport = label.includes('支撑');
    const shortLabel = label.replace('支撑', '支').replace('压力', '压');
    return {
      label,
      shortLabel,
      price,
      priceText: price.toFixed(2),
      isSupport,
    };
  }).filter(Boolean);
};

const getSignalText = (item) => {
  const suggestion = String(item?.analyze?.suggestion || '').trim();
  if (suggestion) return suggestion;
  if (getRawConclusion(item).includes('主动进攻')) return '主动进攻';
  return '观望';
};

const getAggressiveLabel = (item) => {
  const signal = getSignalText(item);
  const combined = `${item?.analyze?.suggestion || ''} ${item?.analyze?.conclusion || ''} ${item?.analyze?.summary || ''}`;
  if (combined.includes('主动进攻') && !signal.includes('主动进攻')) return '主动进攻';
  if (combined.includes('试错') && !signal.includes('试错')) return '试错';
  return '';
};

const isAggressiveItem = (item) => {
  const signal = getSignalText(item);
  if (signal.includes('主动进攻') || signal.includes('试错')) return true;
  return !!getAggressiveLabel(item);
};

const aggressiveObservationCount = computed(() => observationRows.value.filter(isAggressiveItem).length);

const signalReasonChipClass = (kind) => {
  if (kind === 'buy') return 'border-red-500/25 bg-red-500/10 text-red-200';
  if (kind === 'sell') return 'border-emerald-500/25 bg-emerald-500/10 text-emerald-200';
  return 'border-white/10 bg-white/[0.05] text-slate-200';
};

const signalReasonCardClass = (kind) => {
  if (kind === 'buy') return 'border-red-500/25 bg-red-500/[0.08]';
  if (kind === 'sell') return 'border-emerald-500/25 bg-emerald-500/[0.08]';
  return 'border-white/10 bg-white/[0.04]';
};

const watchSignalClass = (item) => {
  const kind = getSignalKind(item);
  if (kind === 'buy') return 'border-red-500/30 bg-red-500/[0.12] text-red-200';
  if (kind === 'sell') return 'border-emerald-500/30 bg-emerald-500/[0.12] text-emerald-200';
  return 'border-white/15 bg-white/[0.06] text-slate-100';
};

const watchCardClass = (item, kind = 'observation') => {
  // 持仓亏损闪烁：优先级最高
  if (kind === 'holding') {
    const lossLevel = getHoldingLossLevel(item);
    if (lossLevel === 'critical') {
      // 亏损超过-10%：红色闪烁
      return 'animate-blink-red';
    }
    if (lossLevel === 'warning') {
      // 亏损超过-6%：橙色闪烁
      return 'animate-blink-orange';
    }
  }

  // 激进信号：试错/主动进攻 → 整块红色
  if (isAggressiveItem(item)) {
    return 'border-red-500/30 bg-red-500/[0.08] shadow-[inset_0_0_20px_rgba(239,68,68,0.04)]';
  }

  const signalKind = getSignalKind(item);

  if (signalKind === 'buy') {
    return 'border-red-500/22 bg-red-500/[0.05] shadow-[inset_0_0_20px_rgba(239,68,68,0.03)]';
  }

  if (signalKind === 'sell') {
    return 'border-emerald-500/25 bg-emerald-500/[0.06] shadow-[inset_0_0_20px_rgba(16,185,129,0.03)]';
  }

  if (kind === 'holding') {
    return 'border-white/10 bg-white/[0.03]';
  }
  return 'border-white/10 bg-white/[0.025]';
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

onMounted(() => {
  window.addEventListener('keydown', handleKeydown);
  document.addEventListener('click', handleDocumentClick, true);
  const restored = restoreWatchlistCache();
  refreshNow(!restored, true);
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
