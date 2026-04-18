import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const routerPath = path.resolve(__dirname, '../src/router/index.js');
const layoutPath = path.resolve(__dirname, '../src/components/AppLayout.vue');
const viewPath = path.resolve(__dirname, '../src/views/Strategies.vue');

test('strategy plaza helper normalizes rows and summary facts', async () => {
  const { normalizeObservationRows, buildSummaryFacts } = await import('../src/composables/useStrategyPlaza.js');

  const rows = normalizeObservationRows([
    { ts_code: '300308.SZ', name: '中际旭创', reason: '示例观察', ret_3d: 5.12, ret_5d: null, ret_10d: -1.5, backtest_status: 'PARTIAL' },
  ]);
  const facts = buildSummaryFacts({
    observation_count: 12,
    win_rate_3d: 58.3,
    avg_ret_3d: 2.56,
    avg_ret_10d: -0.8,
    summary_text: '近窗共 12 条观察。',
  });

  assert.deepEqual(rows[0], {
    ts_code: '300308.SZ',
    name: '中际旭创',
    reason: '示例观察',
    backtestStatus: 'PARTIAL',
    ret3dText: '+5.12%',
    ret5dText: '-',
    ret10dText: '-1.50%',
  });
  assert.equal(facts[0].label, '样本');
  assert.equal(facts[0].value, '12');
  assert.equal(facts[1].value, '58.3%');
});

test('router and app layout expose a first-level strategies entry', () => {
  const routerSource = readFileSync(routerPath, 'utf8');
  const layoutSource = readFileSync(layoutPath, 'utf8');

  assert.match(routerSource, /const Strategies = \(\) => import\('@\/views\/Strategies\.vue'\)/);
  assert.match(routerSource, /path: 'strategies'/);
  assert.match(routerSource, /name: 'strategies'/);
  assert.match(layoutSource, /策略广场/);
  assert.match(layoutSource, /to=\"\/strategies\"/);
});

test('strategy plaza view keeps a compact toolbar list summary structure', () => {
  const source = readFileSync(viewPath, 'utf8');

  assert.match(source, /type=\"date\"/);
  assert.match(source, /const getLocalDateString = \(\) =>/);
  assert.doesNotMatch(source, /toISOString\(\)\.slice\(0, 10\)/);
  assert.match(source, /新进入观察/);
  assert.match(source, /3日/);
  assert.match(source, /5日/);
  assert.match(source, /10日/);
  assert.match(source, /暂无策略/);
  assert.match(source, /该日暂无新进入观察的标的/);
  assert.match(source, /data-kline-trigger=\"1\"/);
  assert.match(source, /getStockKline/);
  assert.match(source, /useKlineChart/);
  assert.match(source, /v-chart/);
});
