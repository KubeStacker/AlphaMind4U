<template>
  <div class="space-y-3 md:space-y-4 max-w-6xl mx-auto pb-6">
    <section class="dashboard-panel panel-warm bg-business-dark rounded-2xl border border-business-light shadow-business overflow-hidden">
      <div class="h-px bg-gradient-to-r from-amber-400/70 via-business-highlight/30 to-transparent"></div>
      <div class="p-2.5 md:p-3.5 border-b border-business-light">
        <div class="flex flex-col gap-2 xl:flex-row xl:items-start xl:justify-between">
          <div class="space-y-1.5">
            <div class="inline-flex items-center gap-2 text-[10px] font-bold uppercase tracking-[0.24em] text-amber-300">
              <span class="h-2 w-2 rounded-full bg-amber-400"></span>
              情绪驾驶舱
            </div>
            <div class="flex flex-wrap items-center gap-2">
              <h1 class="text-base md:text-lg font-bold tracking-tight text-white">
                {{ sentimentHero.title }}
              </h1>
              <span
                class="rounded-full border px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.2em]"
                :class="sentimentHero.badgeClass"
              >
                {{ sentimentHero.bandLabel }}
              </span>
              <span class="text-[10px] text-slate-500">
                最新交易日 {{ tradingBrief.tradeDate || '-' }}
              </span>
            </div>
            <p class="max-w-3xl text-[12px] leading-[18px] text-slate-400">
              {{ sentimentHero.summary }}
            </p>
          </div>

          <div class="flex flex-wrap items-center gap-2 text-[10px]">
            <button
              @click="openBacktestModal"
              class="h-7 px-2.5 rounded-lg border border-business-light bg-slate-900/50 text-slate-300 font-bold transition-all hover:border-business-accent hover:text-white"
            >
              查看回测
            </button>
            <button
              @click="handleSyncSentiment"
              :disabled="syncingSentiment"
              class="h-7 px-2.5 rounded-lg border font-bold transition-all disabled:opacity-60"
              :class="syncingSentiment ? 'border-emerald-300 bg-emerald-300 text-emerald-950' : 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300 hover:bg-emerald-500 hover:text-white'"
            >
              {{ syncingSentiment ? '同步中...' : '同步情绪' }}
            </button>
            <button
              @click="handlePreviewSentiment"
              :disabled="predictingSentiment"
              class="h-7 px-2.5 rounded-lg border font-bold transition-all disabled:opacity-60"
              :class="predictingSentiment ? 'border-amber-300 bg-amber-300 text-amber-950' : 'border-amber-500/30 bg-amber-500/10 text-amber-300 hover:bg-amber-500 hover:text-white'"
            >
              {{ predictingSentiment ? '预测中...' : '预测情绪' }}
            </button>
          </div>
        </div>
      </div>

      <div class="p-2.5 md:p-3.5 space-y-2.5">
        <div class="flex flex-wrap gap-1.5">
          <div
            v-for="card in sentimentMetricCards"
            :key="card.label"
            class="inline-flex items-center gap-1.5 rounded-full border px-2 py-1"
            :class="card.toneClass"
          >
            <div class="text-[9px] text-slate-500">{{ card.label }}</div>
            <div class="text-[11px] font-bold text-white">{{ card.value }}</div>
          </div>
        </div>

        <div class="grid gap-2.5 xl:grid-cols-[1.15fr,0.85fr]">
          <div class="rounded-lg border border-business-light bg-slate-900/35 p-2.5">
            <div class="flex items-center justify-between gap-2">
              <p class="text-[11px] font-bold text-white">今日结论</p>
              <div class="rounded-lg border border-business-accent/30 bg-business-accent/10 px-2.5 py-1 text-[10px] font-bold text-business-accent">
                置信度 {{ tradingBrief.confidence || '-' }}
              </div>
            </div>

            <div class="mt-2 rounded-lg border border-business-light/80 bg-business-darker/45 divide-y divide-business-light/70">
              <div
                v-for="item in sentimentActionItems"
                :key="item.label"
                class="flex items-start gap-2.5 px-2.5 py-2"
              >
                <div class="w-12 shrink-0 text-[10px] text-slate-500">{{ item.label }}</div>
                <div class="flex-1 text-[12px] font-semibold leading-[18px] text-slate-100">{{ item.value }}</div>
              </div>

              <div class="px-2.5 py-2">
                <div class="flex items-start gap-2.5">
                  <div class="w-12 shrink-0 text-[10px] text-slate-500">主线</div>
                  <div class="flex-1">
                    <div class="text-[12px] font-semibold text-slate-100">
                      {{ currentMainline.name || '主线待确认' }}
                    </div>
                    <div class="mt-0.5 text-[10px] leading-4 text-slate-400">
                      {{ currentMainline.reason || '当前没有明确主线时，先按情绪节奏控制仓位。' }}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div class="mt-2 rounded-lg border border-business-accent/20 bg-business-accent/10 px-2.5 py-2">
              <div class="text-[10px] text-business-accent/80">
                {{ sentimentHero.strategyTitle }}
              </div>
              <p class="mt-1 text-[12px] leading-[18px] text-slate-100">
                {{ tradingBrief.action || '暂无执行建议' }}
              </p>
            </div>
          </div>

          <div class="rounded-lg border border-amber-500/15 bg-gradient-to-br from-amber-500/[0.06] via-business-accent/[0.05] to-slate-900/40 p-2.5">
            <div class="flex items-center justify-between gap-2">
              <div>
                <p class="text-[11px] font-bold text-slate-200">盘中辅助</p>
                <p class="mt-0.5 text-[10px] text-slate-500">预测和统一建议集中显示</p>
              </div>
              <span class="text-[10px] text-slate-500">{{ sentimentPreview?.as_of || '未生成' }}</span>
            </div>

            <div v-if="previewError" class="mt-2.5 rounded-lg border border-rose-500/20 bg-rose-500/10 px-2.5 py-2 text-[12px] text-rose-200">
              {{ previewError }}
            </div>

            <div v-else-if="sentimentPreview" class="mt-2.5 grid grid-cols-2 gap-1.5 text-[11px] sm:grid-cols-4">
              <div class="rounded-lg border border-business-light/80 bg-business-darker/60 px-2 py-2">
                <div class="text-slate-500">预测日</div>
                <div class="mt-1 font-semibold text-white">{{ sentimentPreview.predicted_trade_date || '-' }}</div>
              </div>
              <div class="rounded-lg border border-business-light/80 bg-business-darker/60 px-2 py-2">
                <div class="text-slate-500">预测分</div>
                <div class="mt-1 font-semibold text-white">{{ fmt(sentimentPreview.projected_score, 1) }}</div>
              </div>
              <div class="rounded-lg border border-business-light/80 bg-business-darker/60 px-2 py-2">
                <div class="text-slate-500">沪深300</div>
                <div class="mt-1 font-semibold text-white">{{ fmt(sentimentPreview.index_pct_chg, 3, '%') }}</div>
              </div>
              <div class="rounded-lg border border-business-light/80 bg-business-darker/60 px-2 py-2">
                <div class="text-slate-500">科创50</div>
                <div class="mt-1 font-semibold text-white">{{ fmt(sentimentPreview.star50_pct_chg, 3, '%') }}</div>
              </div>
            </div>

            <div v-else class="mt-2.5 rounded-lg border border-business-light bg-business-darker/60 px-2.5 py-2 text-[12px] leading-[18px] text-slate-400">
              点击“预测情绪”后，这里会补充次日预判与统一建议。
            </div>

            <div v-if="sentimentPreview?.plan?.next_day_strategy" class="mt-2.5 rounded-lg border border-amber-500/20 bg-amber-500/10 px-2.5 py-2">
              <div class="text-[10px] text-amber-200/80">次日策略</div>
              <p class="mt-1 text-[12px] leading-[18px] text-amber-50">
                {{ sentimentPreview.plan?.next_day_strategy || '-' }}
              </p>
            </div>

            <div class="mt-2.5 divide-y divide-business-light/70 rounded-lg border border-business-light/80 bg-business-darker/55">
              <div
                v-for="cue in sentimentDecisionCues"
                :key="cue.label"
                class="px-2.5 py-2"
              >
                <div class="text-[10px] text-slate-500">{{ cue.label }}</div>
                <div class="mt-1 text-[12px] leading-[18px] text-slate-200">{{ cue.value }}</div>
              </div>
            </div>

            <div v-if="marketSuggestion" class="mt-2.5 rounded-lg border border-business-accent/20 bg-business-accent/10 px-2.5 py-2">
              <div class="text-[10px] text-business-accent/80">统一市场建议</div>
              <p class="mt-1 text-[12px] leading-[18px] text-slate-200">
                {{ marketSuggestion.action || '-' }} · 仓位 {{ fmt((Number(marketSuggestion.target_position) || 0) * 100, 0, '%') }} · 主线 {{ marketSuggestion.rationale?.top_mainline || '-' }} · 风控 {{ fmt(marketSuggestion.risk_controls?.stop_loss_pct, 1, '%') }} / {{ fmt(marketSuggestion.risk_controls?.take_profit_pct, 1, '%') }}
              </p>
            </div>

            <div v-else-if="suggestionError" class="mt-2.5 rounded-lg border border-rose-500/20 bg-rose-500/10 px-2.5 py-2 text-[12px] text-rose-200">
              建议生成失败: {{ suggestionError }}
            </div>
          </div>
        </div>

        <div class="rounded-lg border border-amber-500/15 bg-gradient-to-br from-amber-500/[0.05] to-slate-900/35 p-2.5">
          <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <div class="flex items-center gap-2">
              <p class="text-[11px] font-bold text-white">市场情绪趋势</p>
              <span class="text-[10px] text-slate-500">证据层</span>
            </div>
            <div class="text-[10px] text-slate-500">
              85+ 沸腾 / 止盈 · 25- 冰点 / 观察
            </div>
          </div>

          <div class="mt-2 h-40 w-full sm:h-48">
            <v-chart :option="marketSentimentChartOption" autoresize />
          </div>
        </div>
      </div>
    </section>

    <section class="dashboard-panel panel-cool bg-business-dark rounded-2xl border border-business-light shadow-business overflow-hidden">
      <div class="h-px bg-gradient-to-r from-business-accent/80 via-business-highlight/35 to-transparent"></div>
      <div class="p-3 md:p-4 border-b border-business-light">
        <div class="flex flex-col gap-2 xl:flex-row xl:items-start xl:justify-between">
          <div class="space-y-2">
            <div class="inline-flex items-center gap-2 text-[10px] font-bold uppercase tracking-[0.24em] text-business-accent">
              <span class="h-2 w-2 rounded-full bg-business-accent"></span>
              主线作战板
            </div>
            <div class="flex flex-wrap items-center gap-2">
              <h2 class="text-lg md:text-xl font-bold tracking-tight text-white">
                {{ mainlineHeadline.title }}
              </h2>
              <span
                v-if="currentMainline.name"
                class="rounded-full border border-business-accent/25 bg-business-accent/10 px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.2em] text-business-accent"
              >
                {{ currentMainline.name }}{{ currentMainline.score ? ` · ${fmt(currentMainline.score, 1)}` : '' }}
              </span>
            </div>
            <p class="max-w-4xl text-[13px] leading-5 text-slate-300">
              {{ mainlineHeadline.summary }}
            </p>
          </div>

          <div class="text-[11px] leading-5 text-slate-500">
            <div>{{ mainlineReview?.date_range?.start || '-' }} 至 {{ mainlineReview?.date_range?.end || '-' }}</div>
            <div class="mt-1">复盘交易日 {{ mainlineReview?.trade_days || 0 }} 天</div>
          </div>
        </div>
      </div>

      <div class="p-3 md:p-4 space-y-3">
        <div v-if="loadingTrend || loadingLeaders" class="py-10 text-center text-sm text-slate-400">
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
          <div class="grid gap-2 md:grid-cols-3">
            <div
              v-for="card in mainlineSummaryCards"
              :key="card.label"
              class="rounded-lg border px-3 py-2"
              :class="card.toneClass"
            >
              <div class="text-[10px] text-slate-500">{{ card.label }}</div>
              <div class="mt-1 text-sm font-bold text-white">{{ card.value }}</div>
              <div class="mt-0.5 text-[10px] leading-4 text-slate-400">{{ card.description }}</div>
            </div>
          </div>

          <div class="grid gap-2.5 xl:grid-cols-[1.15fr,0.85fr]">
            <div class="overflow-hidden rounded-xl border border-business-light bg-slate-900/30">
              <div class="divide-y divide-business-light/70">
                <div
                  v-for="sector in focusedMainlineCards"
                  :key="sector.name"
                  class="px-3 py-2.5"
                  :class="sector.panelClass"
                >
                  <div class="flex items-start gap-3">
                    <span class="mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-business-darker text-[10px] font-bold text-white">
                      {{ sector.displayRank }}
                    </span>

                    <div class="min-w-0 flex-1">
                      <div class="flex flex-wrap items-center gap-1.5">
                        <div class="truncate text-sm font-bold text-white">{{ sector.name }}</div>
                        <span
                          class="rounded-full border px-1.5 py-0.5 text-[9px] font-bold"
                          :class="sector.roleClass"
                        >
                          {{ sector.roleLabel }}
                        </span>
                        <span
                          class="rounded-full border px-1.5 py-0.5 text-[9px] font-semibold"
                          :class="sector.heatClass"
                        >
                          {{ sector.heatLabel }}
                        </span>
                      </div>

                      <div class="mt-1 text-[12px] leading-5 text-slate-300">{{ sector.detail }}</div>
                      <div class="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-slate-400">
                        <span>上榜 {{ sector.activeDaysText }}</span>
                        <span>连续 {{ sector.consecutiveDaysText }}</span>
                        <span>涨停 {{ sector.limitUpsText }}</span>
                        <span>样本 {{ sector.stockCountText }}</span>
                      </div>
                    </div>

                    <div class="shrink-0 text-right">
                      <div class="text-lg font-black text-white">{{ fmt(sector.score, 1) }}</div>
                      <button
                        @click="toggleMainlineExpansion(sector.name)"
                        class="mt-1 h-6 px-2 rounded-md border border-business-light bg-business-darker/80 text-[9px] font-bold text-slate-300 transition-all hover:border-business-highlight hover:text-white"
                      >
                        {{ expandedMainlineName === sector.name ? '收起龙头' : '看龙头' }}
                      </button>
                    </div>
                  </div>

                  <div v-if="expandedMainlineName === sector.name" class="mt-2.5 grid gap-1.5 sm:grid-cols-2">
                    <div
                      v-for="(stock, sidx) in sector.stocks"
                      :key="`${sector.name}-${stock.tsCode || stock.name}-${sidx}`"
                      class="rounded-lg border border-business-light/70 bg-business-darker/60 px-2.5 py-2"
                    >
                      <div class="flex items-center justify-between gap-2">
                        <div class="min-w-0">
                          <div class="flex items-center gap-1.5">
                            <span class="text-[9px] text-slate-500">{{ sidx + 1 }}</span>
                            <span class="truncate text-[12px] font-semibold text-slate-100">{{ stock.name }}</span>
                          </div>
                          <div class="mt-0.5 truncate font-mono text-[9px] text-slate-500">{{ stock.tsCode || '-' }}</div>
                        </div>
                        <div class="text-right">
                          <div class="text-[11px]" :class="stock.changeClass">{{ stock.changeText }}</div>
                          <div v-if="stock.badge" class="mt-0.5 text-[9px] text-slate-400">{{ stock.badge }}</div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div class="space-y-2.5">
              <div class="rounded-xl border border-business-highlight/15 bg-gradient-to-br from-business-highlight/[0.06] to-slate-900/35 p-3">
                <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                  <div>
                    <p class="text-[10px] font-bold text-slate-500">趋势证据</p>
                    <h2 class="mt-1 text-sm font-bold text-white">近10日主线演变</h2>
                  </div>
                  <div class="text-[10px] text-slate-500">图表只保留当前聚焦的最多 3 条主线</div>
                </div>

                <div class="relative mt-3 h-60 w-full sm:h-72">
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

              <div v-if="trendAnalysis" class="rounded-xl border border-business-accent/15 bg-gradient-to-br from-business-accent/[0.05] to-slate-900/35 p-3">
                <div class="flex items-start gap-3">
                  <ChatBubbleLeftRightIcon class="mt-0.5 h-5 w-5 shrink-0 text-business-accent" />
                  <div class="space-y-1">
                    <h3 class="text-[10px] font-bold text-slate-500">量化推演结论</h3>
                    <p class="text-[13px] leading-5 text-slate-300 whitespace-pre-line">
                      {{ trendAnalysis }}
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </template>
      </div>
    </section>

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
        <div v-else-if="backtestData" class="space-y-3 text-xs max-h-[70vh] overflow-y-auto">
          <div class="grid grid-cols-2 gap-2 text-slate-300">
            <div class="rounded bg-slate-800 p-2">总收益: <span class="font-bold text-white">{{ backtestData.metrics?.total_return || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">年化: <span class="font-bold text-white">{{ backtestData.metrics?.annual_return || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">回撤: <span class="font-bold text-white">{{ backtestData.metrics?.max_drawdown || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">胜率: <span class="font-bold text-white">{{ backtestData.metrics?.win_rate || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">日胜率: <span class="font-bold text-white">{{ backtestData.metrics?.day_win_rate || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">交易次数: <span class="font-bold text-white">{{ backtestData.metrics?.total_trades || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">夏普: <span class="font-bold text-white">{{ backtestData.metrics?.sharpe || '-' }}</span></div>
            <div class="rounded bg-slate-800 p-2">基准: <span class="font-bold text-white">{{ backtestData.metrics?.benchmark_return || '-' }}</span></div>
          </div>
          <div class="rounded bg-slate-800 p-2 text-slate-400">
            参数: leverage={{ backtestData.policy?.leverage ?? '-' }}, trend_floor={{ backtestData.policy?.trend_floor_pos ?? '-' }}
          </div>
          <div v-if="backtestData.attribution && Object.keys(backtestData.attribution).length > 0" class="rounded bg-slate-800 p-2">
            <div class="text-slate-300 font-semibold mb-1">归因分析</div>
            <div class="grid grid-cols-2 gap-x-2 gap-y-1">
              <template v-for="(val, key) in backtestData.attribution" :key="key">
                <div class="text-slate-400">{{ key }}</div>
                <div class="text-slate-200">{{ val.count }}次, {{ val.win_rate }}胜, {{ val.avg_pnl }}均</div>
              </template>
            </div>
          </div>
          <div v-if="backtestData.trades && backtestData.trades.length > 0" class="rounded bg-slate-800 p-2">
            <div class="text-slate-300 font-semibold mb-1">交易记录 (最近10笔)</div>
            <div class="max-h-32 overflow-y-auto">
              <table class="w-full text-[10px]">
                <thead class="text-slate-500">
                  <tr>
                    <th class="text-left">入场</th>
                    <th class="text-left">出场</th>
                    <th class="text-right">盈亏</th>
                    <th class="text-right">持仓天</th>
                    <th class="text-left">原因</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(t, i) in backtestData.trades.slice(-10)" :key="i" :class="t.profit_pct > 0 ? 'text-green-400' : 'text-red-400'">
                    <td>{{ t.entry_date?.slice(5) }}</td>
                    <td>{{ t.exit_date?.slice(5) }}</td>
                    <td class="text-right">{{ t.profit_pct }}%</td>
                    <td class="text-right">{{ t.hold_days }}</td>
                    <td>{{ t.reason }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
          <div class="flex gap-2">
            <button @click="loadBacktestGrid" class="flex-1 rounded bg-sky-700 hover:bg-sky-600 px-2 py-1 text-white text-xs" :disabled="loadingBacktestGrid">
              {{ loadingBacktestGrid ? '加载中...' : '网格诊断' }}
            </button>
            <button @click="loadWalkforward" class="flex-1 rounded bg-purple-700 hover:bg-purple-600 px-2 py-1 text-white text-xs" :disabled="loadingWalkforward">
              {{ loadingWalkforward ? '加载中...' : 'Walk-Forward' }}
            </button>
          </div>
          <div v-if="backtestGridData" class="rounded bg-slate-800 p-2">
            <div class="text-slate-300 font-semibold mb-1">网格诊断</div>
            <div class="max-h-32 overflow-y-auto">
              <table class="w-full text-[10px]">
                <thead class="text-slate-500">
                  <tr>
                    <th class="text-left">杠杆</th>
                    <th class="text-right">Floor</th>
                    <th class="text-right">收益</th>
                    <th class="text-right">回撤</th>
                    <th class="text-right">胜率</th>
                    <th class="text-right">交易</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(g, i) in backtestGridData.grid" :key="i" :class="g.dd_passed ? 'text-green-400' : 'text-red-400'">
                    <td>{{ g.leverage }}</td>
                    <td class="text-right">{{ g.floor }}</td>
                    <td class="text-right">{{ g.return }}</td>
                    <td class="text-right">{{ g.max_dd }}</td>
                    <td class="text-right">{{ g.win_rate }}</td>
                    <td class="text-right">{{ g.trades }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
          <div v-if="walkforwardData" class="rounded bg-slate-800 p-2">
            <div class="text-slate-300 font-semibold mb-1">Walk-Forward结果</div>
            <div class="grid grid-cols-2 gap-1 text-slate-400">
              <div>OOS收益: <span class="text-white">{{ walkforwardData.metrics?.oos_total_return }}</span></div>
              <div>OOS年化: <span class="text-white">{{ walkforwardData.metrics?.oos_annual_return }}</span></div>
              <div>OOS回撤: <span class="text-white">{{ walkforwardData.metrics?.oos_max_drawdown }}</span></div>
              <div>OOS胜率: <span class="text-white">{{ walkforwardData.metrics?.oos_win_rate }}</span></div>
              <div>窗口数: <span class="text-white">{{ walkforwardData.metrics?.oos_total_trades }}</span></div>
            </div>
          </div>
          <div class="text-[11px] text-slate-500">生成时间: {{ backtestData.generated_at || '-' }}</div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, onMounted, onBeforeUnmount } from 'vue';
import { getMainlineHistory, getMarketSentiment, getSentimentPreview, getMarketSuggestion, getBacktestResult, getBacktestGrid, getBacktestWalkforward, syncSentiment, getTasksStatus, getMainlineLeaders } from '@/services/api';
import { ChatBubbleLeftRightIcon } from '@heroicons/vue/20/solid'

const loadingTrend = ref(false);
const trendAnalysis = ref('');
const mainlineChartDates = ref([]);
const expandedMainlineName = ref('');
const showBacktestModal = ref(false);
const loadingBacktest = ref(false);
const backtestError = ref('');
const backtestData = ref(null);
const backtestGridData = ref(null);
const walkforwardData = ref(null);
const loadingBacktestGrid = ref(false);
const loadingWalkforward = ref(false);
const minimalTooltipMode = ref(false);
const isMobile = ref(false);
const syncingSentiment = ref(false);
const predictingSentiment = ref(false);
const sentimentPreview = ref(null);
const previewError = ref('');
const marketSuggestion = ref(null);
const suggestionError = ref('');
const currentMainline = ref({ name: '', score: 0, reason: '' });
const mainlineReview = ref(null);
const mainlineTrendSeries = ref([]);
const mainlineOverview = ref({ current: null, continuous: null });
const mainlineFocusCards = ref([]);
const latestSentiment = ref(null);
const latestSentimentDate = ref('');
const recentSentiments = ref([]);
const tradingBrief = ref({
  tradeDate: '',
  regime: '',
  regimeBase: '',
  trendText: '',
  scoreText: '',
  scoreValue: null,
  rawScore: null,
  confidence: '',
  confidenceScore: null,
  mainline: '',
  mainlineShort: '',
  position: '',
  attack: '',
  risk: '',
  advice: '',
  action: ''
});

// 主线龙头推荐数据
const mainlineLeaders = ref(null);
const loadingLeaders = ref(false);
const MAINLINE_CHART_COLORS = ['#3b82f6', '#06b6d4', '#f59e0b'];

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

const clampNumber = (value, min, max) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return min;
  return Math.max(min, Math.min(max, n));
};

const getSentimentBand = (score) => {
  if (!Number.isFinite(score)) {
    return { label: '等待数据', strategyTitle: '先等情绪信号完成' };
  }
  if (score >= 85) {
    return { label: '沸腾区', strategyTitle: '情绪过热，重点是兑现而不是追高' };
  }
  if (score >= 70) {
    return { label: '高热区', strategyTitle: '情绪偏热，顺势做主线但不打高潮尾段' };
  }
  if (score >= 55) {
    return { label: '修复区', strategyTitle: '情绪修复，主线核心可以主动配置' };
  }
  if (score > 42) {
    return { label: '拉锯区', strategyTitle: '情绪反复，只做赔率清晰的位置' };
  }
  if (score > 25) {
    return { label: '低温区', strategyTitle: '情绪偏冷，先防守，等待承接确认' };
  }
  return { label: '冰点区', strategyTitle: '情绪冰点，轻仓观察，不急于抄底' };
};

const getSentimentTheme = (regime) => {
  if (regime === '强势') {
    return {
      badgeClass: 'border-rose-400/30 bg-rose-400/10 text-rose-200',
      panelClass: 'border-rose-400/20 bg-rose-500/10',
      scoreClass: 'text-rose-200',
      progressClass: 'bg-gradient-to-r from-rose-500 via-amber-400 to-yellow-300',
      signalLabel: '进攻窗口',
    };
  }
  if (regime === '偏强') {
    return {
      badgeClass: 'border-amber-400/30 bg-amber-400/10 text-amber-200',
      panelClass: 'border-amber-400/20 bg-amber-500/10',
      scoreClass: 'text-amber-100',
      progressClass: 'bg-gradient-to-r from-amber-500 via-orange-400 to-cyan-300',
      signalLabel: '修复窗口',
    };
  }
  if (regime === '偏弱') {
    return {
      badgeClass: 'border-emerald-400/30 bg-emerald-400/10 text-emerald-200',
      panelClass: 'border-emerald-400/20 bg-emerald-500/10',
      scoreClass: 'text-emerald-100',
      progressClass: 'bg-gradient-to-r from-emerald-500 via-cyan-400 to-slate-300',
      signalLabel: '防守窗口',
    };
  }
  if (regime === '弱势') {
    return {
      badgeClass: 'border-blue-400/30 bg-blue-400/10 text-blue-200',
      panelClass: 'border-blue-400/20 bg-blue-500/10',
      scoreClass: 'text-blue-100',
      progressClass: 'bg-gradient-to-r from-blue-500 via-slate-400 to-slate-300',
      signalLabel: '冷却窗口',
    };
  }
  return {
    badgeClass: 'border-cyan-400/30 bg-cyan-400/10 text-cyan-200',
    panelClass: 'border-cyan-400/20 bg-cyan-500/10',
    scoreClass: 'text-cyan-100',
    progressClass: 'bg-gradient-to-r from-cyan-500 via-sky-400 to-slate-300',
    signalLabel: '拉锯窗口',
  };
};

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

const buildTradingBrief = () => {
  const sentiment = latestSentiment.value;
  const tradeDate = latestSentimentDate.value || '';
  if (!sentiment || !tradeDate) {
    tradingBrief.value = {
      tradeDate: '',
      regime: '',
      regimeBase: '',
      trendText: '',
      scoreText: '',
      scoreValue: null,
      rawScore: null,
      confidence: '',
      confidenceScore: null,
      mainline: '',
      mainlineShort: '',
      position: '',
      attack: '',
      risk: '',
      advice: '',
      action: '',
    };
    return;
  }

  const score = Number(sentiment.value);
  const details = sentiment.details || {};
  const advice = details.metrics?.advice || '';
  const topName = currentMainline.value?.name || '';
  const topScore = Number(currentMainline.value?.score);
  const reviewNames = (mainlineReview.value?.mainlines || []).slice(0, 2).map((item) => item.name).filter(Boolean);
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
    ? `主线聚焦 ${topName}${Number.isFinite(topScore) ? `(${topScore.toFixed(1)})` : ''}${reviewNames.length > 1 ? `；近10日重点 ${reviewNames.join('、')}` : ''}`
    : reviewNames.length
      ? `近10日主线 ${reviewNames.join('、')}`
      : '主线暂不清晰';
  const advicePart = advice ? `执行上以「${advice}」为主` : '执行上保持纪律化交易';

  tradingBrief.value = {
    tradeDate,
    regime: `${regime} (${trendText}${pendingText})`,
    regimeBase: regime,
    trendText,
    scoreText: Number.isFinite(ma5) ? `MA5 ${ma5.toFixed(1)} / 100` : '-',
    scoreValue: Number.isFinite(ma5) ? ma5 : null,
    rawScore: Number.isFinite(score) ? score : null,
    confidence: confidenceText,
    confidenceScore,
    mainline: mainlinePart,
    mainlineShort: topName || reviewNames[0] || '',
    position,
    attack,
    risk,
    advice,
    action: `建议仓位 ${position}；进攻: ${attack}；风控: ${risk}。按 2-5 个交易日节奏执行，避免因次日单点信号切换策略。${advicePart}。`
  };
};

const sentimentHero = computed(() => {
  const score = Number(tradingBrief.value.scoreValue);
  const previewScore = Number(sentimentPreview.value?.projected_score);
  const regime = tradingBrief.value.regimeBase || '';
  const band = getSentimentBand(score);
  const theme = getSentimentTheme(regime);
  const mainlineName = currentMainline.value?.name || tradingBrief.value.mainlineShort || '主线待确认';

  let title = '等待更清晰的情绪确认';
  if (regime === '强势') title = '情绪强势，主线内主动进攻';
  else if (regime === '偏强') title = '情绪修复，核心方向可提仓';
  else if (regime === '震荡') title = '情绪拉锯，只打高胜率回踩';
  else if (regime === '偏弱') title = '情绪偏弱，先防守后出手';
  else if (regime === '弱势') title = '情绪冰冷，先缩手等拐点';

  let previewSummary = '未生成盘中预测，先按收盘信号执行。';
  if (Number.isFinite(previewScore) && Number.isFinite(score)) {
    if (previewScore >= score + 2) {
      previewSummary = `次日预估继续回暖到 ${previewScore.toFixed(1)}，可以优先看主线承接。`;
    } else if (previewScore <= score - 2) {
      previewSummary = `次日预估降温到 ${previewScore.toFixed(1)}，明早先看冲高回落风险。`;
    } else {
      previewSummary = `次日预估维持在 ${previewScore.toFixed(1)} 左右，策略以延续为主。`;
    }
  }

  const summaryParts = [];
  if (tradingBrief.value.position) summaryParts.push(`建议仓位 ${tradingBrief.value.position}`);
  if (mainlineName) summaryParts.push(`主线先看 ${mainlineName}`);
  if (tradingBrief.value.attack) summaryParts.push(tradingBrief.value.attack);
  summaryParts.push(previewSummary);

  return {
    title,
    bandLabel: band.label,
    strategyTitle: band.strategyTitle,
    summary: summaryParts.join('；'),
    scoreDisplay: Number.isFinite(score) ? score.toFixed(1) : '--',
    scoreClass: theme.scoreClass,
    progressClass: theme.progressClass,
    progressWidth: `${clampNumber(Number.isFinite(score) ? score : 0, 4, 100)}%`,
    badgeClass: theme.badgeClass,
    panelClass: theme.panelClass,
    signalLabel: theme.signalLabel,
    asideTitle: Number.isFinite(score)
      ? `${band.label}，${tradingBrief.value.trendText || '跟随强弱变化'}`
      : '等待情绪数据',
  };
});

const sentimentMetricCards = computed(() => {
  const previewCard = sentimentPreview.value
    ? {
        label: '次日预估',
        value: fmt(sentimentPreview.value.projected_score, 1),
        description: `${sentimentPreview.value.predicted_trade_date || '-'} · ${sentimentPreview.value.plan?.next_day_strategy || '等待策略说明'}`,
        toneClass: 'border-emerald-500/18 bg-emerald-500/[0.07]',
      }
    : {
        label: '主线锚点',
        value: currentMainline.value?.name || tradingBrief.value.mainlineShort || '-',
        description: currentMainline.value?.reason || '暂无明确主线',
        toneClass: 'border-cyan-500/18 bg-cyan-500/[0.07]',
      };

  return [
    {
      label: '最新交易日',
      value: tradingBrief.value.tradeDate || '-',
      description: tradingBrief.value.regime || '等待情绪数据完成',
      toneClass: 'border-sky-500/18 bg-sky-500/[0.07]',
    },
    {
      label: '情绪状态',
      value: sentimentHero.value.bandLabel,
      description: tradingBrief.value.scoreText || '-',
      toneClass: 'border-amber-500/18 bg-amber-500/[0.08]',
    },
    {
      label: '建议仓位',
      value: tradingBrief.value.position || '-',
      description: tradingBrief.value.attack || '等待主线确认',
      toneClass: 'border-business-accent/18 bg-business-accent/[0.08]',
    },
    previewCard,
  ];
});

const sentimentActionItems = computed(() => [
  {
    label: '仓位',
    value: tradingBrief.value.position || '等待信号',
    toneClass: 'border-business-accent/18 bg-business-accent/[0.08]',
  },
  {
    label: '进攻方向',
    value: tradingBrief.value.attack || '等待主线确认',
    toneClass: 'border-amber-500/18 bg-amber-500/[0.08]',
  },
  {
    label: '风控底线',
    value: tradingBrief.value.risk || '先控制回撤',
    toneClass: 'border-rose-500/18 bg-rose-500/[0.08]',
  },
]);

const sentimentDecisionCues = computed(() => {
  const previewSummary = sentimentPreview.value
    ? `${sentimentPreview.value.predicted_trade_date || '-'} 预估 ${fmt(sentimentPreview.value.projected_score, 1)}，${sentimentPreview.value.plan?.next_day_strategy || '继续观察'}`
    : '还没有盘中预测，先按收盘情绪和主线强度执行。';

  const suggestionSummary = marketSuggestion.value
    ? `${marketSuggestion.value.action || '-'} / 目标仓位 ${fmt((Number(marketSuggestion.value.target_position) || 0) * 100, 0, '%')} / 主线 ${marketSuggestion.value.rationale?.top_mainline || '-'}`
    : suggestionError.value
      ? `统一建议生成失败: ${suggestionError.value}`
      : '需要时可点击“预测情绪”补充统一市场建议。';

  return [
    {
      label: '市场状态',
      value: tradingBrief.value.regime || '等待情绪数据',
    },
    {
      label: '盘中预判',
      value: previewSummary,
    },
    {
      label: '统一建议',
      value: suggestionSummary,
    },
  ];
});

const mainlineHeadline = computed(() => {
  const current = mainlineOverview.value.current;
  const continuous = mainlineOverview.value.continuous;

  let title = '主线暂未收束，先控制试错';
  if (current?.name && continuous?.name && current.name === continuous.name) {
    title = `${current.name} 是当前最值得聚焦的方向`;
  } else if (current?.name && continuous?.name) {
    title = `先盯 ${current.name}，持续性再看 ${continuous.name}`;
  } else if (current?.name) {
    title = `${current.name} 是当前最强方向`;
  } else if (continuous?.name) {
    title = `${continuous.name} 仍是近10日持续主线`;
  }

  return {
    title,
    summary: mainlineReview.value?.summary
      || current?.detail
      || continuous?.detail
      || '主线强度来自近10日复盘与实时龙头共振。',
  };
});

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

const getMainlineRuleText = (sector) => {
  const consecutiveDays = Number(sector?.consecutiveDays) || 0;
  const limitUps = Number(sector?.limitUps) || 0;
  const activeDays = Number(sector?.activeDays) || 0;

  if (consecutiveDays >= 3 && limitUps >= 3) {
    return '满足“连续 3 日走强 + 强度达标”，可以进入深看名单。';
  }
  if (consecutiveDays >= 3) {
    return '连续性达标，重点看分歧后的承接和龙头换手。';
  }
  if (limitUps >= 3 || activeDays >= 5) {
    return '强度存在，但还需要继续确认是否只是短线脉冲。';
  }
  return '当前更像预备方向，先观察再决定是否提高优先级。';
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
      ruleText: getMainlineRuleText(sector),
      priorityText: role.note,
    };
  })
));

const mainlineSummaryCards = computed(() => [
  {
    label: '主盯方向',
    value: mainlineOverview.value.current?.name || '等待数据',
    description: mainlineOverview.value.current?.detail || '按最新主线强度排序。',
    toneClass: 'border-business-accent/18 bg-business-accent/[0.08]',
  },
  {
    label: '持续方向',
    value: mainlineOverview.value.continuous?.name || '等待数据',
    description: mainlineOverview.value.continuous?.detail || '优先看近10日能持续的方向。',
    toneClass: 'border-business-highlight/18 bg-business-highlight/[0.08]',
  },
  {
    label: '聚焦规则',
    value: focusedMainlineCards.value.length ? `最多 ${focusedMainlineCards.value.length} 条` : '等待数据',
    description: '按持续性、强度、共振筛选，Top5 龙头折叠展示。',
    toneClass: 'border-amber-500/18 bg-amber-500/[0.08]',
  },
]);

const toggleMainlineExpansion = (name) => {
  expandedMainlineName.value = expandedMainlineName.value === name ? '' : name;
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
  backtestGridData.value = null;
  walkforwardData.value = null;
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

const loadBacktestGrid = async () => {
  if (backtestGridData.value) return;
  loadingBacktestGrid.value = true;
  try {
    const res = await getBacktestGrid();
    if (res.data?.status === 'success') {
      backtestGridData.value = res.data || null;
    }
  } catch (e) {
    console.error('网格诊断加载失败', e);
  } finally {
    loadingBacktestGrid.value = false;
  }
};

const loadWalkforward = async () => {
  if (walkforwardData.value) return;
  loadingWalkforward.value = true;
  try {
    const res = await getBacktestWalkforward(120, 40);
    if (res.data?.status === 'success') {
      walkforwardData.value = res.data?.data || null;
    }
  } catch (e) {
    console.error('Walk-Forward加载失败', e);
  } finally {
    loadingWalkforward.value = false;
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

const buildFocusCard = (name, labels = [], reviewLine = null) => {
  if (!name) return null;

  const leaderGroup = getLeaderGroupByName(name);
  const latestPoint = getLatestMainlinePoint(name);
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

  let detail = '';
  if (reviewLine) {
    detail = `近10日上榜${activeDays ?? 0}天，连续${consecutiveDays ?? 0}天`;
  } else if (leaderGroup) {
    detail = `最新主线样本${stockCount ?? '-'}只${Number.isFinite(resonance) ? `，板块共振${Math.round(resonance)}%` : ''}`;
  } else {
    detail = currentMainline.value?.reason || '主线强度来自近10日历史走势';
  }

  let stockHint = '优先展示实时评分龙头';
  if (!leaderGroup?.leaders?.length && reviewLine?.leaders?.length) {
    stockHint = '回退到近10日活跃龙头';
  } else if (!leaderGroup?.leaders?.length && latestPoint?.top_stocks?.length) {
    stockHint = '回退到最新交易日强势股';
  }

  return {
    name,
    labels: [...labels],
    detail,
    score,
    activeDays,
    consecutiveDays,
    limitUps,
    stockCount,
    resonance,
    resonanceText: Number.isFinite(resonance) ? `板块共振 ${Math.round(resonance)}%` : '',
    activeDaysText: Number.isFinite(activeDays) ? `${Math.round(activeDays)}天` : '-',
    consecutiveDaysText: Number.isFinite(consecutiveDays) ? `${Math.round(consecutiveDays)}天` : '-',
    limitUpsText: Number.isFinite(limitUps) ? `${Math.round(limitUps)}家` : '-',
    stockCountText: Number.isFinite(stockCount) ? `${Math.round(stockCount)}只` : '-',
    stockHint,
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
          detail: currentMainline.value?.reason || '按最新交易日主线强度排序',
        }
      : null,
    continuous: continuousLine
      ? {
          name: continuousLine.name,
          score: Number.isFinite(Number(continuousLine.latest_score))
            ? Number(continuousLine.latest_score)
            : Number(continuousLine.avg_score) || 0,
          detail: `近10日上榜${continuousLine.active_days}天，连续${continuousLine.consecutive_days}天`,
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

  if (expandedMainlineName.value && !mainlineFocusCards.value.some((item) => item.name === expandedMainlineName.value)) {
    expandedMainlineName.value = '';
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
        score: analysis.top_mainline.score || 0,
        reason: analysis.top_mainline.reason || ''
      };
    } else {
      currentMainline.value = { name: '', score: 0, reason: '' };
    }
    mainlineReview.value = analysis?.review_10d || null;
    buildTradingBrief();
    rebuildMainlineFocus();
  } catch (e) {
    console.error("History Error", e);
    mainlineChartDates.value = [];
    mainlineTrendSeries.value = [];
    mainlineReview.value = null;
    currentMainline.value = { name: '', score: 0, reason: '' };
    mainlineFocusCards.value = [];
    mainlineOverview.value = { current: null, continuous: null };
    refreshMainlineChart();
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

// 加载主线龙头推荐
const fetchMainlineLeaders = async () => {
  loadingLeaders.value = true;
  try {
    const res = await getMainlineLeaders({ limit: 5, min_score: 60 });
    if (res.data.status === 'success') {
      mainlineLeaders.value = res.data;
    } else {
      mainlineLeaders.value = null;
    }
    rebuildMainlineFocus();
  } catch (e) {
    console.error('加载主线龙头失败', e);
    mainlineLeaders.value = null;
    rebuildMainlineFocus();
  } finally {
    loadingLeaders.value = false;
  }
};

onMounted(async () => {
  updateViewport();
  window.addEventListener('resize', updateViewport);
  fetchMainlineHistory();
  fetchMarketSentiment();
  fetchMainlineLeaders();
});

onBeforeUnmount(() => {
  window.removeEventListener('resize', updateViewport);
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
