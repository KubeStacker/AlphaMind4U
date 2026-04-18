# Watchlist Zen Levels Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add action words and key price levels to Watchlist zen mode while keeping the mobile layout simpler than the normal cards.

**Architecture:** Extract the zen-mode level selection into a tiny pure utility so Node tests can verify the behavior without mounting Vue. Then update the zen-mode template in `Watchlist.vue` to render a compact multi-line layout that reuses existing compact watchlist data and degrades cleanly when some fields are missing. Finally sync AGENTS and README docs to describe the new behavior.

**Tech Stack:** Vue 3 `<script setup>`, Node `node:test`, Tailwind utility classes, existing Watchlist compact data helpers

---

### Task 1: Add a regression-tested zen-mode level helper

**Files:**
- Create: `frontend/src/composables/watchlistZen.js`
- Modify: `frontend/tests/kline-units.test.mjs`

- [ ] **Step 1: Write the failing test**

```js
test('zen mode picks primary support resistance and keeps trigger order', async () => {
  const { buildZenFocusTokens } = await import('../src/composables/watchlistZen.js');

  const tokens = buildZenFocusTokens({
    levels: [
      { label: '压力位', shortLabel: '压', priceText: '26.10', isSupport: false },
      { label: '第一支撑', shortLabel: '支', priceText: '24.80', isSupport: true },
      { label: '第二支撑', shortLabel: '支', priceText: '24.10', isSupport: true },
    ],
    triggerText: '26.20放量站稳',
    invalidText: '24.60跌破离场',
  });

  assert.deepEqual(tokens, [
    { key: 'support', label: '支', value: '24.80', tone: 'support' },
    { key: 'resistance', label: '压', value: '26.10', tone: 'resistance' },
    { key: 'trigger', label: '触', value: '26.20放量站稳', tone: 'trigger' },
    { key: 'invalid', label: '失', value: '24.60跌破离场', tone: 'invalid' },
  ]);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test frontend/tests/kline-units.test.mjs`
Expected: FAIL with a module-not-found error for `frontend/src/composables/watchlistZen.js`

- [ ] **Step 3: Write minimal implementation**

```js
const firstLevel = (levels, support) =>
  (Array.isArray(levels) ? levels : []).find((level) => !!level && !!level.isSupport === support) || null;

export const buildZenFocusTokens = ({ levels = [], triggerText = '', invalidText = '' } = {}) => {
  const support = firstLevel(levels, true);
  const resistance = firstLevel(levels, false);
  const tokens = [];

  if (support?.priceText) tokens.push({ key: 'support', label: '支', value: support.priceText, tone: 'support' });
  if (resistance?.priceText) tokens.push({ key: 'resistance', label: '压', value: resistance.priceText, tone: 'resistance' });
  if (String(triggerText || '').trim()) tokens.push({ key: 'trigger', label: '触', value: String(triggerText).trim(), tone: 'trigger' });
  if (String(invalidText || '').trim()) tokens.push({ key: 'invalid', label: '失', value: String(invalidText).trim(), tone: 'invalid' });

  return tokens;
};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test frontend/tests/kline-units.test.mjs`
Expected: PASS for the new zen-mode helper test

### Task 2: Render compact zen-mode action and levels for desktop and mobile

**Files:**
- Modify: `frontend/src/views/Watchlist.vue`

- [ ] **Step 1: Write the failing test**

```js
test('watchlist zen mode renders action and key levels in a mobile-friendly grid', () => {
  const source = readFileSync(watchlistPath, 'utf8');

  assert.match(source, /buildZenFocusTokens/);
  assert.match(source, /getSignalText\(item\)/);
  assert.match(source, /grid-cols-2/);
  assert.match(source, /轻触空白处退出/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test frontend/tests/kline-units.test.mjs`
Expected: FAIL because `Watchlist.vue` does not yet reference the helper or the new zen layout classes

- [ ] **Step 3: Write minimal implementation**

```vue
<div v-if="zenMode" class="fixed inset-0 z-50 bg-obsidian-950/98 px-4 py-6 md:px-8">
  <div class="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4">
    <div v-for="item in holdingRows" :key="item.ts_code">
      <div>{{ getSignalText(item) }}</div>
      <div v-for="token in getZenFocusTokens(item)" :key="token.key">{{ token.label }} {{ token.value }}</div>
    </div>
  </div>
</div>
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test frontend/tests/kline-units.test.mjs`
Expected: PASS for the new zen-mode source assertions

### Task 3: Sync docs and verify the shipped behavior

**Files:**
- Modify: `AGENTS.md`
- Modify: `docs/README.md`
- Modify: `docs/published/trading-system/README.md`

- [ ] **Step 1: Update docs**

```md
- Watchlist 专注模式当前会在全屏持仓视图中直出动作词与主支撑/主压力/触发/失效位，移动端压成更简洁的两列点位块，不再显示长句指导。
```

- [ ] **Step 2: Run verification**

Run: `node --test frontend/tests/kline-units.test.mjs`
Expected: PASS with the zen-mode helper and layout tests green

- [ ] **Step 3: Spot-check API contract**

Run:
```bash
curl -s "http://localhost:8000/admin/watchlist/realtime?analysis_depth=compact&sort_mode=auto&src=sina"
```

Expected: response body includes compact `key_levels` and action-related fields used by the zen-mode UI
