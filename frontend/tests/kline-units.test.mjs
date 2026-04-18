import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

import { useKlineChart } from '../src/composables/useKlineChart.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const watchlistPath = path.resolve(__dirname, '../src/views/Watchlist.vue');

test('watchlist kline popup labels expose units', () => {
  const source = readFileSync(watchlistPath, 'utf8');

  assert.match(source, /成交额\(亿\)/);
  assert.match(source, /成交量\(手\)/);
  assert.match(source, /md:w-\[78vw\]/);
  assert.match(source, /md:max-w-5xl/);
});

test('kline chart legend names volume with explicit lot unit', () => {
  const { createKlineOption } = useKlineChart();
  const option = createKlineOption(
    [
      {
        trade_date: '2026-04-08',
        open: 273.98,
        close: 285.88,
        low: 270.05,
        high: 285.9,
        vol: 457691.27,
        amount: 12781562.886,
        net_mf_amount: 258574.62,
      },
    ],
    { showLegend: true },
  );

  assert.ok(option.legend?.data?.includes('成交量(手)'));
});

test('kline chart allocates more space to lower panels', () => {
  const { createKlineOption } = useKlineChart();
  const option = createKlineOption(
    [
      {
        trade_date: '2026-04-08',
        open: 273.98,
        close: 285.88,
        low: 270.05,
        high: 285.9,
        vol: 457691.27,
        amount: 12781562.886,
        net_mf_amount: 258574.62,
      },
    ],
    { showLegend: true },
  );

  assert.deepEqual(
    option.grid,
    [
      { left: 50, right: 10, top: 35, height: '42%' },
      { left: 50, right: 10, top: '61%', height: '17%' },
      { left: 50, right: 10, top: '80%', height: '17%' },
    ],
  );
});

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

test('holding rows sort by total market value descending', async () => {
  const { sortHoldingRowsByValue } = await import('../src/composables/watchlistHoldings.js');

  const sorted = sortHoldingRowsByValue(
    [
      { ts_code: 'A', price: 20 },
      { ts_code: 'B', price: 10 },
      { ts_code: 'C', price: 30 },
    ],
    {
      A: { shares: 100, avg_cost: 18 },
      B: { shares: 500, avg_cost: 9 },
      C: { shares: 50, avg_cost: 28 },
    },
  );

  assert.deepEqual(sorted.map((item) => item.ts_code), ['B', 'A', 'C']);
});

test('watchlist zen mode renders action and key levels in a mobile-friendly grid', () => {
  const source = readFileSync(watchlistPath, 'utf8');
  const zenBlock = source.match(/<div v-if="zenMode"[\s\S]*?<div ref="klinePopupRef"/)?.[0] || '';

  assert.match(source, /buildZenFocusTokens/);
  assert.match(zenBlock, /getSignalText\(item\)/);
  assert.match(zenBlock, /grid-cols-2/);
  assert.match(zenBlock, /max-w-5xl/);
  assert.match(zenBlock, /rounded-xl/);
  assert.match(zenBlock, /px-2\.5 py-2\.5/);
  assert.match(zenBlock, /text-\[22px\]/);
  assert.match(source, /轻触空白处退出/);
  assert.doesNotMatch(zenBlock, /item\.pct/);
  assert.doesNotMatch(zenBlock, /watchSignalClass\(item\)/);
  assert.doesNotMatch(zenBlock, /zenTokenClass\(token\)/);
  assert.doesNotMatch(zenBlock, /max-w-6xl/);
  assert.doesNotMatch(zenBlock, /rounded-\[22px\]/);
});

test('watchlist image import footer reserves mobile safe area', () => {
  const source = readFileSync(watchlistPath, 'utf8');
  const modalBlock = source.match(/<div v-if="showImageHoldingModal"[\s\S]*?<div v-if="aiAnalysisModal.visible"/)?.[0] || '';

  assert.match(modalBlock, /sticky bottom-0 safe-bottom/);
  assert.match(modalBlock, /flex flex-col items-stretch gap-3/);
  assert.match(modalBlock, /写入 \$\{getHoldingImageMatchedItems\(\)\.length \|\| 0\} 条持仓/);
});

test('watchlist privacy mode persists and masks holding-sensitive values', () => {
  const source = readFileSync(watchlistPath, 'utf8');

  assert.match(source, /WATCHLIST_PRIVACY_KEY = 'jarvis-watchlist-privacy-v1'/);
  assert.match(source, /const privacyMode = ref\(false\)/);
  assert.match(source, /restorePrivacyMode\(\)/);
  assert.match(source, /togglePrivacyMode/);
  assert.match(source, /maskPrivacyText/);
  assert.match(source, /隐私模式/);
  assert.match(source, /maskPrivacyText\(item\.shares\)/);
  assert.match(source, /maskPrivacyText\(item\.avg_cost == null \? '-' : fmt\(item\.avg_cost, 2\)\)/);
});
