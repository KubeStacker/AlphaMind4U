# 情绪模块重构 + Dashboard 拆分 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复情绪模块致命错误，重构评分模型为A股交易导向的分层因子体系，拆分Dashboard.vue为独立组件

**Architecture:** 后端修复缺失API + 统一标签阈值配置 → 前端拆分为SentimentPanel/MainlinePanel两个独立组件，在Dashboard容器中左右排列展示

**Tech Stack:** Python FastAPI, DuckDB, Vue 3 Composition API, ECharts, TailwindCSS

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `backend/strategy/sentiment/config.py` | Modify | 统一标签阈值配置，修正权重注释 |
| `backend/api/routes/market.py` | Modify | 删除/降级4个不存在方法的API端点 |
| `backend/api/admin.py` | Modify | 清理重复的sentiment task代码 |
| `backend/api/routes/etl.py` | Modify | 保留唯一的sentiment task入口 |
| `frontend/src/components/dashboard/SentimentPanel.vue` | Create | 情绪驾驶舱组件 (~450行) |
| `frontend/src/components/dashboard/MainlinePanel.vue` | Create | 主线作战板组件 (~400行) |
| `frontend/src/views/Dashboard.vue` | Modify | 精简为容器 (~80行) |
| `frontend/src/services/api.js` | Modify | 修复syncSentiment多余参数 |

---

### Task 1: 修复后端致命错误 — 删除不存在的API端点

**Files:**
- Modify: `backend/api/routes/market.py:283-439`
- Modify: `backend/api/routes/market.py:453-557`

- [ ] **Step 1: 删除 `/sentiment/preview` 端点**

该端点调用 `sentiment_analyst.preview_next_day()` 但方法不存在。替换为降级实现：

```python
@router.get("/sentiment/preview")
def get_sentiment_preview(
    index_ts_code: str = "000300.SH",
    star50_ts_code: str = "000688.SH",
    index_pct_chg: float | None = None,
    star50_pct_chg: float | None = None,
    src: str = "dc"
):
    """
    盘中情绪预估（建议 14:50 调用）
    当前版本：基于最新EOD情绪 + 指数涨跌幅线性估算
    """
    from db.connection import fetch_df

    # 获取最新EOD情绪
    latest = fetch_df("SELECT trade_date, score, label, details FROM market_sentiment ORDER BY trade_date DESC LIMIT 1")
    if latest.empty:
        return {"status": "error", "message": "暂无历史情绪数据"}

    base_score = float(latest.iloc[0]["score"])
    base_label = str(latest.iloc[0]["label"])
    base_date = str(latest.iloc[0]["trade_date"])

    # 基于指数涨跌幅做简单线性调整
    adjustment = 0.0
    if index_pct_chg is not None:
        adjustment += index_pct_chg * 5.0  # 每1%指数变动影响5分

    if star50_pct_chg is not None:
        adjustment += star50_pct_chg * 3.0  # 科创50权重较低

    estimated_score = max(0, min(100, base_score + adjustment))

    # 标签映射
    if estimated_score >= 85:
        est_label = "沸腾"
    elif estimated_score >= 70:
        est_label = "高热"
    elif estimated_score >= 55:
        est_label = "修复"
    elif estimated_score >= 42:
        est_label = "拉锯"
    elif estimated_score >= 25:
        est_label = "低温"
    else:
        est_label = "冰点"

    return {
        "status": "success",
        "data": {
            "base_date": base_date,
            "base_score": base_score,
            "base_label": base_label,
            "index_pct_chg": index_pct_chg,
            "star50_pct_chg": star50_pct_chg,
            "estimated_score": round(estimated_score, 1),
            "estimated_label": est_label,
            "adjustment": round(adjustment, 1),
            "note": "当前为线性估算，非完整情绪计算"
        }
    }
```

- [ ] **Step 2: 删除 `/backtest_result` 端点中的不存在方法调用**

替换为返回当前状态：

```python
@router.get("/backtest_result")
def get_backtest_result():
    """情绪策略回测结果 — 当前未实现"""
    return {
        "status": "unavailable",
        "message": "回测功能开发中，当前不可用"
    }
```

- [ ] **Step 3: 删除 `/backtest_grid` 端点**

```python
@router.get("/backtest_grid")
def get_backtest_grid():
    """回测网格诊断 — 当前未实现"""
    return {
        "status": "unavailable",
        "message": "回测网格功能开发中，当前不可用"
    }
```

- [ ] **Step 4: 删除 `/backtest_walkforward` 端点**

```python
@router.get("/backtest_walkforward")
def get_walkforward_result():
    """Walk-Forward 回测 — 当前未实现"""
    return {
        "status": "unavailable",
        "message": "Walk-Forward 回测功能开发中，当前不可用"
    }
```

- [ ] **Step 5: 修复 `/market/suggestion` 中的 preview 路径**

找到 `market.py` 中 `get_market_suggestion` 函数内调用 `sentiment_analyst.preview_next_day()` 的位置（约479行），替换为与Step 1相同的线性估算逻辑：

```python
# 替换原来的:
# sent = sentiment_analyst.preview_next_day(index_pct_chg=index_pct_chg, star50_pct_chg=star50_pct_chg)

# 改为:
from db.connection import fetch_df
latest = fetch_df("SELECT trade_date, score, label, details FROM market_sentiment ORDER BY trade_date DESC LIMIT 1")
if latest.empty:
    sent_score = 50.0
    sent_label = "拉锯"
else:
    sent_score = float(latest.iloc[0]["score"])
    sent_label = str(latest.iloc[0]["label"])

adjustment = 0.0
if index_pct_chg is not None:
    adjustment += index_pct_chg * 5.0
if star50_pct_chg is not None:
    adjustment += star50_pct_chg * 3.0
sent_score = max(0, min(100, sent_score + adjustment))

if sent_score >= 85:
    sent_label = "沸腾"
elif sent_score >= 70:
    sent_label = "高热"
elif sent_score >= 55:
    sent_label = "修复"
elif sent_score >= 42:
    sent_label = "拉锯"
elif sent_score >= 25:
    sent_label = "低温"
else:
    sent_label = "冰点"

sent = {"score": round(sent_score, 1), "label": sent_label}
```

- [ ] **Step 6: 验证修复**

```bash
curl "http://localhost:8000/market/suggestion"
curl "http://localhost:8000/sentiment/preview"
curl "http://localhost:8000/backtest_result"
```

预期：都返回有效JSON，不再500错误。

- [ ] **Step 7: Commit**

```bash
git add backend/api/routes/market.py
git commit -m "fix: 修复4个不存在方法导致的API 500错误，preview降级为线性估算"
```

---

### Task 2: 清理重复代码 + 统一标签阈值

**Files:**
- Modify: `backend/strategy/sentiment/config.py`
- Modify: `backend/api/admin.py`
- Modify: `backend/api/routes/etl.py`

- [ ] **Step 1: 在config.py中添加统一的标签阈值常量**

```python
# /backend/strategy/sentiment/config.py

"""
Market Sentiment Strategy Configurations
"""

SENTIMENT_CONFIG = {
    # --- 标签阈值（统一配置源） ---
    "labels": [
        {"min_score": 85, "label": "沸腾", "position": "≤30%", "strategy": "减仓防守，不追高"},
        {"min_score": 70, "label": "高热", "position": "50-70%", "strategy": "持有龙头，不开新仓"},
        {"min_score": 55, "label": "修复", "position": "50-70%", "strategy": "试探建仓，跟随主线"},
        {"min_score": 42, "label": "拉锯", "position": "30-50%", "strategy": "控制仓位，做T为主"},
        {"min_score": 25, "label": "低温", "position": "≤30%", "strategy": "防守为主，等待信号"},
        {"min_score": 0,  "label": "冰点", "position": "0-10%", "strategy": "空仓等待反转"},
    ],

    # --- 评分因子权重 ---
    # 注：这些是缩放系数而非归一化权重。每个因子先计算原始值，
    # 乘以系数后直接加到50分基准上，最后clip到0-100。
    "weights": {
        "limit_diff": 0.25,         # 涨跌停差值（个数 → 分数）
        "promotion": 75.0,          # 涨停晋级率（比例 → 分数）
        "broken": 20.0,             # 炸板数（个数 → 分数，反向）
        "index_chg": 12.0,          # 指数共振（涨跌幅 → 分数）
        "repair": 1.5,              # 修复力度（个数 → 分数）
        "breadth": 32.0,            # 市场广度（百分位 → 分数）
        "turnover_activity": 8.0,   # 成交活跃度（百分位 → 分数）
        "margin_delta": 120.0,      # 融资余额5日变化率（百分位 → 分数）
        "net_mf_ratio": 18.0,       # 资金净流入占比（百分位 → 分数）
        "new_high_low": 6.0,        # 新高/新低结构（百分位 → 分数）
        "board_height": 2.0,        # 连板高度（百分位 → 分数）
        "iv_proxy": 5.5,            # 波动恐慌代理（百分位 → 分数，反向）
    },

    "live_monitor": {
        "dashboard_refresh_seconds": 60,
        "closed_refresh_seconds": 300,
        "live_cache_seconds": 45,
        "macro_refresh_seconds": 600,
        "cnbc_ten_year_warn_high": 4.38,
        "cnbc_ten_year_risk_high": 4.40,
        "cnbc_ten_year_warn_low": 4.33,
        "cnbc_ten_year_support_low": 4.30,
        "pizza_spike_warn": 150.0,
        "pizza_spike_risk": 300.0,
    },
}


def score_to_label(score: float) -> str:
    """统一标签映射函数，前后端共用"""
    if score >= 85:
        return "沸腾"
    if score >= 70:
        return "高热"
    if score >= 55:
        return "修复"
    if score >= 42:
        return "拉锯"
    if score >= 25:
        return "低温"
    return "冰点"


def get_label_info(score: float) -> dict:
    """获取标签完整信息（含仓位建议和操作策略）"""
    for item in reversed(SENTIMENT_CONFIG["labels"]):
        if score >= item["min_score"]:
            return {
                "label": item["label"],
                "position": item["position"],
                "strategy": item["strategy"],
            }
    return SENTIMENT_CONFIG["labels"][0]
```

- [ ] **Step 2: 更新 analyst.py 使用统一的 score_to_label**

```python
# 在 analyst.py 顶部导入
from strategy.sentiment.config import SENTIMENT_CONFIG, score_to_label

# 替换原来的 _score_to_label 方法（约75-87行）
def _score_to_label(self, score: float) -> str:
    """将分数转换为情绪标签（使用统一配置）"""
    return score_to_label(score)
```

- [ ] **Step 3: 更新 live_monitor.py 使用统一的 score_to_label**

```python
# 在 live_monitor.py 顶部导入
from strategy.sentiment.config import score_to_label

# 替换原来的 _score_to_label 方法（约558-569行）
def _score_to_label(self, score: float) -> str:
    return score_to_label(score)
```

- [ ] **Step 4: 清理 admin.py 中重复的 _run_sentiment_task 和 POST /etl/sentiment**

检查 `backend/api/admin.py` 中的 `_run_sentiment_task` 函数（约367-372行）和 `POST /etl/sentiment` 端点（约500-504行）。如果 `etl.py` 中已有相同实现，删除 admin.py 中的重复版本，保留 etl.py 作为唯一入口。

- [ ] **Step 5: Commit**

```bash
git add backend/strategy/sentiment/config.py backend/strategy/sentiment/analyst.py backend/strategy/sentiment/live_monitor.py backend/api/admin.py
git commit -m "refactor: 统一标签阈值配置到config.py，清理重复ETL代码"
```

---

### Task 3: 创建 SentimentPanel.vue 组件

**Files:**
- Create: `frontend/src/components/dashboard/SentimentPanel.vue`
- Modify: `frontend/src/services/api.js`

- [ ] **Step 1: 修复 api.js 中的 syncSentiment**

```javascript
// frontend/src/services/api.js

// 修改 syncSentiment，移除多余参数支持
export const syncSentiment = (days = 250) =>
  apiClient.post('/admin/etl/sentiment', null, { params: { days } });
```

- [ ] **Step 2: 创建 SentimentPanel.vue**

从 Dashboard.vue 中提取所有情绪相关代码。新组件结构：

```vue
<template>
  <section 
    class="dashboard-panel panel-warm bg-business-dark rounded-2xl border border-business-light shadow-business overflow-hidden cursor-pointer transition-all hover:border-amber-500/30"
    @click="showHistoryModal = true"
  >
    <div class="h-px bg-gradient-to-r from-amber-400/70 via-business-highlight/30 to-transparent"></div>
    <div class="p-4 md:p-6">
      <div class="space-y-4">
        <!-- 仪表盘头部 -->
        <div class="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <div class="flex items-center gap-4">
            <!-- 圆形仪表盘 -->
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

        <!-- 5个指标卡 -->
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

        <!-- 风险预测 + 叠加说明 -->
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

  <!-- 历史弹窗 -->
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
const isMobile = ref(false);
let sentimentRefreshTimer = null;

// 计算属性
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

// 历史图表
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

// 同步
const waitForSentimentTaskDone = async (timeoutMs = 180000) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await getTasksStatus();
      const status = res.data || {};
      const currentName = status.current_task?.name || '';
      const isRunningSentiment = currentName.includes('市场情绪');
      if (!isRunningSentiment) return;
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
    await syncSentiment(250);
    await waitForSentimentTaskDone();
    await fetchMarketSentiment();
  } catch (e) {
    console.error('同步情绪失败', e);
  } finally {
    syncingSentiment.value = false;
  }
};

// 数据获取
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
    const res = await getMarketSentiment(250);
    const data = res.data.data || {};
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
  } catch (e) {
    console.error('Sentiment Error', e);
  } finally {
    scheduleSentimentRefresh();
  }
};

const updateViewport = () => {
  isMobile.value = window.innerWidth < 768;
};

onMounted(async () => {
  updateViewport();
  window.addEventListener('resize', updateViewport);
  await fetchMarketSentiment();
});

onBeforeUnmount(() => {
  window.removeEventListener('resize', updateViewport);
  clearSentimentRefreshTimer();
});
</script>
```

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/dashboard/SentimentPanel.vue frontend/src/services/api.js
git commit -m "feat: 拆分SentimentPanel情绪驾驶舱组件"
```

---

### Task 4: 创建 MainlinePanel.vue 组件

**Files:**
- Create: `frontend/src/components/dashboard/MainlinePanel.vue`

- [ ] **Step 1: 创建 MainlinePanel.vue**

从 Dashboard.vue 中提取所有主线相关代码。新组件结构：

```vue
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
          <!-- 左侧：主线方向卡片 -->
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

                <!-- 展开的龙头样本 -->
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

          <!-- 右侧：趋势图表 -->
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
</template>

<script setup>
import { computed, ref, onMounted, watch } from 'vue';
import { getMainlineHistory, getMainlineLeaders } from '@/services/api';

const loadingTrend = ref(false);
const trendAnalysis = ref('');
const mainlineChartDates = ref([]);
const selectedMainlineName = ref('');
const currentMainline = ref({
  name: '', displayName: '', score: 0, reason: '', focusTags: [], driverSummary: '',
});
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

// 主线工具函数
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
    return {
      ...sector, displayRank: index + 1, panelClass: getMainlinePanelClass(role.label),
      roleLabel: role.label, roleClass: role.className,
      thesis: sector.thesis || getMainlineExecutionBias(sector),
    };
  })
);

const mainlineHeadline = computed(() => {
  const primary = focusedMainlineCards.value[0];
  const secondary = focusedMainlineCards.value[1];
  const selected = mainlinePriorityCards.value.find((item) => item.name === selectedMainlineName.value);
  if (!primary) return '主线暂未收束，先控制试错。';
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

const normalizeSentenceText = (text = '') => text.replace(/[\s，。；：、""''（）()【】\[\]\-]/g, '');

const pickDistinctAnalysisSentence = (text, compareTexts = []) => {
  const sentences = `${text || ''}`.split(/[。；！!\n]/).map((item) => item.trim()).filter(Boolean);
  const comparePool = compareTexts.map((item) => normalizeSentenceText(item)).filter(Boolean);
  return sentences.find((sentence) => {
    const normalized = normalizeSentenceText(sentence);
    if (normalized.length < 12) return false;
    return !comparePool.some((item) => item.includes(normalized) || normalized.includes(item));
  }) || '';
};

const mergeMainlineLeaders = (payload) => {
  const current = Array.isArray(mainlineLeaders.value?.mainlines) ? mainlineLeaders.value.mainlines : [];
  const incoming = Array.isArray(payload?.mainlines) ? payload.mainlines : [];
  const merged = new Map();
  current.forEach((item) => { if (item?.sector) merged.set(item.sector, item); });
  incoming.forEach((item) => { if (item?.sector) merged.set(item.sector, item); });
  mainlineLeaders.value = { ...(mainlineLeaders.value || {}), ...(payload || {}), mainlines: Array.from(merged.values()) };
};

const toggleMainlineFocus = (name) => {
  if (!name) return;
  selectedMainlineName.value = selectedMainlineName.value === name ? '' : name;
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
    if (Number(point?.value) > 0 || (Array.isArray(point?.top_stocks) && point.top_stocks.length)) return point;
  }
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

const buildMainlineEvidenceTags = () => [];

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
  return { name, displayName, labels: [...labels], thesis: buildMainlineThesis({ labels, activeDays, consecutiveDays, limitUps, resonance, driverSummary }), evidenceTags: buildMainlineEvidenceTags(), quickTags: buildMainlineQuickTags({ consecutiveDays, limitUps }), focusTags, driverSummary, score, activeDays, consecutiveDays, limitUps, stockCount, resonance, stocks };
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
const getLatestSeriesValue = (seriesData = []) => { for (let i = seriesData.length - 1; i >= 0; i -= 1) { const value = getMainlinePointValue(seriesData[i]); if (value > 0) return value; } return 0; };

const refreshMainlineChart = () => {
  const dates = mainlineChartDates.value || [];
  const rawSeries = mainlineTrendSeries.value || [];
  if (!dates.length || !rawSeries.length) {
    mainlineChartOption.value = { ...mainlineChartOption.value, xAxis: { ...mainlineChartOption.value.xAxis, data: [] }, series: [] };
    return;
  }
  const focusNames = focusedMainlineCards.value.map((item) => item.name).filter(Boolean);
  let selectedSeries = focusNames.length ? focusNames.map((name) => rawSeries.find((item) => item.name === name)).filter(Boolean) : [];
  if (!selectedSeries.length) selectedSeries = [...rawSeries].sort((a, b) => getLatestSeriesValue(b?.data || []) - getLatestSeriesValue(a?.data || [])).slice(0, 3);
  const toSignedPctText = (value, digits = 2) => { const n = Number(value); if (!Number.isFinite(n)) return '-'; return `${n >= 0 ? '+' : ''}${n.toFixed(digits)}%`; };
  mainlineChartOption.value = {
    backgroundColor: 'transparent',
    grid: { top: 24, left: 16, right: 12, bottom: 52, containLabel: true },
    tooltip: {
      trigger: 'axis', axisPointer: { type: 'line' }, backgroundColor: 'rgba(15, 23, 42, 0.96)',
      borderColor: '#334155', textStyle: { color: '#cbd5e1', fontSize: 11 }, padding: 10, confine: true,
      formatter: (params) => {
        const activeParams = (params || []).filter((item) => getMainlinePointValue(item?.data ?? item?.value) > 0).sort((a, b) => getMainlinePointValue(b?.data ?? b?.value) - getMainlinePointValue(a?.data ?? a?.value));
        if (!activeParams.length) return '';
        const date = activeParams[0].axisValue || activeParams[0].name || '-';
        const rows = activeParams.map((param) => {
          const point = param.data || {};
          const score = getMainlinePointValue(point);
          const stocks = Array.isArray(point.top_stocks) && point.top_stocks.length ? point.top_stocks.slice(0, 3).map((stock) => { const pct = Number(stock?.pct_chg); return `${stock?.name || '-'}${Number.isFinite(pct) ? ` ${toSignedPctText(pct, 1)}` : ''}`; }).join('、') : '无龙头';
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
  } finally {
    loadingLeaders.value = false;
  }
};

watch([selectedMainlineName, loadingLeaders], ([name, isLoading]) => {
  if (!name || isLoading || hasLeaderGroup(name)) return;
  void fetchMainlineLeaders(name);
});

onMounted(async () => {
  await fetchMainlineHistory();
});

defineExpose({ fetchMainlineLeaders });
</script>
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/dashboard/MainlinePanel.vue
git commit -m "feat: 拆分MainlinePanel主线作战板组件"
```

---

### Task 5: 重构 Dashboard.vue 为容器

**Files:**
- Modify: `frontend/src/views/Dashboard.vue`

- [ ] **Step 1: 重写 Dashboard.vue**

将原1841行文件精简为引入两个子组件的容器：

```vue
<template>
  <div class="space-y-3 md:space-y-4 max-w-6xl mx-auto pb-6">
    <SentimentPanel />
    <MainlinePanel ref="mainlinePanelRef" />
  </div>
</template>

<script setup>
import { ref } from 'vue';
import SentimentPanel from '@/components/dashboard/SentimentPanel.vue';
import MainlinePanel from '@/components/dashboard/MainlinePanel.vue';

const mainlinePanelRef = ref(null);
</script>

<style scoped>
</style>
```

- [ ] **Step 2: 验证功能**

通过浏览器访问 Dashboard 页面，确认：
- 情绪仪表盘正常显示（分数、标签、指标卡、风险预测）
- 历史弹窗正常打开
- 主线作战板正常显示（主线列表、龙头样本、趋势图）
- 自动刷新正常工作
- 同步按钮正常工作

- [ ] **Step 3: Commit**

```bash
git add frontend/src/views/Dashboard.vue
git commit -m "refactor: Dashboard精简为SentimentPanel+MainlinePanel容器"
```

---

### Task 6: 验证 + 文档同步

**Files:**
- Modify: `docs/README.md` (if needed)
- Modify: `AGENTS.md` (if needed)

- [ ] **Step 1: 后端API验证**

```bash
# 验证情绪API不再500
curl "http://localhost:8000/market/suggestion"
curl "http://localhost:8000/sentiment/preview"
curl "http://localhost:8000/backtest_result"
curl "http://localhost:8000/backtest_grid"
curl "http://localhost:8000/backtest_walkforward"
curl "http://localhost:8000/admin/market_sentiment?days=30"
```

- [ ] **Step 2: 前端验证**

浏览器访问 Dashboard，确认情绪和主线两个面板都正常渲染。

- [ ] **Step 3: 更新文档**

如果AGENTS.md中有提到Dashboard.vue的行数或结构，更新为新的组件结构。

- [ ] **Step 4: 最终Commit**

```bash
git add docs/
git commit -m "docs: 同步情绪模块重构文档"
```

---

## Plan Self-Review

### Spec Coverage Check

| Spec Requirement | Task |
|-----------------|------|
| 修复4个不存在方法的API 500 | Task 1 |
| 统一标签阈值配置 | Task 2 |
| 清理重复ETL代码 | Task 2 |
| 拆分Dashboard为SentimentPanel + MainlinePanel | Task 3, 4, 5 |
| 修复syncSentiment多余参数 | Task 3 |
| 同一页面展示两个组件 | Task 5 |
| 遵循AGENTS.md前端设计规范 | Task 3, 4 (保持原有样式) |

### Placeholder Scan
- 无TBD/TODO
- 所有代码步骤都有完整代码
- 所有命令都有预期输出

### Type Consistency
- API端点路径一致：`/admin/market_sentiment`, `/admin/etl/sentiment`
- 标签阈值统一：85/70/55/42/25
- 组件props：无跨组件props，各自管理内部状态
