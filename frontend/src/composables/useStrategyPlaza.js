const pct = (value) => {
  if (value === null || value === undefined || value === '') return '-'
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${num >= 0 ? '+' : ''}${num.toFixed(2)}%`
}

export const normalizeObservationRows = (items = []) =>
  (Array.isArray(items) ? items : []).map((item) => ({
    ts_code: item.ts_code || '-',
    name: item.name || item.ts_code || '-',
    reason: item.reason || '-',
    backtestStatus: item.backtest_status || 'PENDING',
    ret3dText: pct(item.ret_3d),
    ret5dText: pct(item.ret_5d),
    ret10dText: pct(item.ret_10d),
  }))

export const buildSummaryFacts = (summary = null) => {
  if (!summary) return []
  return [
    { label: '样本', value: `${Number(summary.observation_count || 0)}` },
    { label: '3日胜率', value: Number.isFinite(Number(summary.win_rate_3d)) ? `${Number(summary.win_rate_3d).toFixed(1)}%` : '-' },
    { label: '3日均值', value: pct(summary.avg_ret_3d) },
    { label: '10日均值', value: pct(summary.avg_ret_10d) },
  ]
}
