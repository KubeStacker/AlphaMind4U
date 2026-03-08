function fmtVol(v) {
  if (!v || !Number.isFinite(v)) return '-';
  if (Math.abs(v) >= 1e8) return `${(v / 1e8).toFixed(2)}亿手`;
  if (Math.abs(v) >= 1e4) return `${(v / 1e4).toFixed(2)}万手`;
  return `${Math.round(v)}手`;
}

function fmtAmount(v) {
  if (!v || !Number.isFinite(v)) return '-';
  return `${v.toFixed(2)}亿`;
}

function fmtRzye(v) {
  if (!v || !Number.isFinite(v)) return '-';
  return `${v.toFixed(2)}亿`;
}

function fmtMf(v) {
  if (!v || !Number.isFinite(v)) return '-';
  const sign = v >= 0 ? '+' : '';
  return `${sign}${v.toFixed(2)}亿`;
}

function fmtPrice(v) {
  if (!v || !Number.isFinite(v)) return '-';
  return v.toFixed(2);
}

export function useKlineChart() {
  const createKlineOption = (data, options = {}) => {
    const { showLegend = false, showDataZoom = false, marginType = 'rzye' } = options;
    
    if (!data || !data.length) return {};
    
    const dates = data.map(item => String(item.trade_date).slice(0, 10));
    const values = data.map(item => [item.open, item.close, item.low, item.high]);
    const vol = data.map(item => Number(item.vol) || 0);
    const ma5 = data.map(item => Number(item.ma5) || null);
    const ma10 = data.map(item => Number(item.ma10) || null);
    const ma20 = data.map(item => Number(item.ma20) || null);
    const rzye = data.map(item => item.rzye != null ? Number(item.rzye) / 1e8 : null);
    const netMfVol = data.map(item => item.net_mf_vol != null ? Number(item.net_mf_vol) / 10 : null);
    const amount = data.map(item => (Number(item.amount) || 0) * 1000 / 1e8);
    
    const latest = data[data.length - 1];
    const latestMa5 = Number(latest?.ma5) || null;
    const latestMa10 = Number(latest?.ma10) || null;
    const latestMa20 = Number(latest?.ma20) || null;
    const latestVol = Number(latest?.vol) || 0;
    const latestRzye = latest?.rzye != null ? Number(latest.rzye) / 1e8 : null;
    const latestNetMf = latest?.net_mf_vol != null ? Number(latest.net_mf_vol) / 10 : null;
    const latestAmount = (Number(latest?.amount) || 0) * 1000 / 1e8;
    
    const series = [];
    
    series.push({ 
      name: 'K线', 
      type: 'candlestick', 
      data: values, 
      itemStyle: { 
        color: '#ef4444', 
        color0: '#22c55e', 
        borderColor: '#ef4444', 
        borderColor0: '#22c55e',
        borderWidth: 1
      } 
    });
    
    if (latestMa5) {
      series.push({ name: 'MA5', type: 'line', data: ma5, showSymbol: false, smooth: true, lineStyle: { width: 1, color: '#f59e0b' } });
    }
    if (latestMa10) {
      series.push({ name: 'MA10', type: 'line', data: ma10, showSymbol: false, smooth: true, lineStyle: { width: 1, color: '#38bdf8' } });
    }
    if (latestMa20) {
      series.push({ name: 'MA20', type: 'line', data: ma20, showSymbol: false, smooth: true, lineStyle: { width: 1, color: '#a78bfa' } });
    }
    
    series.push({ 
      name: '成交量', 
      type: 'bar', 
      xAxisIndex: 1, 
      yAxisIndex: 1, 
      data: vol, 
      barWidth: '70%',
      itemStyle: { 
        color: (params) => { 
          const idx = params.dataIndex; 
          const row = data[idx]; 
          if (!row) return '#666'; 
          return Number(row.close) >= Number(row.open) ? '#ef4444' : '#22c55e'; 
        } 
      } 
    });
    
    const rzyeData = rzye;
    series.push({ 
      name: '融资', 
      type: 'line', 
      xAxisIndex: 2, 
      yAxisIndex: 3, 
      data: rzyeData, 
      smooth: true, 
      showSymbol: false, 
      lineStyle: { width: 1.5, color: '#3b82f6' },
      areaStyle: { color: 'rgba(59, 130, 246, 0.15)' }
    });
    
    const mfData = netMfVol;
    series.push({ 
      name: '主力', 
      type: 'bar', 
      xAxisIndex: 2, 
      yAxisIndex: 2, 
      data: mfData, 
      barWidth: '50%',
      itemStyle: { color: (params) => Number(params.value) >= 0 ? '#0ea5e9' : '#f43f5e' }
    });
    
    const result = {
      backgroundColor: '#1e1e1e',
      animation: false,
      tooltip: { 
        trigger: 'axis', 
        axisPointer: { 
          type: 'cross',
          lineStyle: { color: '#666', type: 'dashed' }
        }, 
        backgroundColor: 'rgba(30, 30, 30, 0.95)', 
        borderColor: '#444', 
        borderWidth: 1,
        textStyle: { color: '#fff', fontSize: 11 },
        formatter: (params) => {
          if (!params || !params.length) return '';
          const date = params[0].axisValue;
          let html = `<div style="font-weight:bold;padding-bottom:5px;">${date}</div>`;
          
          const candleData = params.find(p => p.seriesName === 'K线');
          if (candleData && candleData.value) {
            const [open, close, low, high] = candleData.value;
            const color = close >= open ? '#ef4444' : '#22c55e';
            html += `<div style="color:${color}">${fmtPrice(open)} / ${fmtPrice(close)} / ${fmtPrice(high)} / ${fmtPrice(low)}</div>`;
          }
          
          params.forEach(p => {
            if (!p.value || p.value === '-' || p.value === null) return;
            if (p.seriesName === 'MA5') html += `<div style="color:#f59e0b">MA5 ${fmtPrice(p.value)}</div>`;
            if (p.seriesName === 'MA10') html += `<div style="color:#38bdf8">MA10 ${fmtPrice(p.value)}</div>`;
            if (p.seriesName === 'MA20') html += `<div style="color:#a78bfa">MA20 ${fmtPrice(p.value)}</div>`;
            if (p.seriesName === '成交量') html += `<div style="color:#fff">量 ${fmtVol(p.value)}</div>`;
            if (p.seriesName === '成交额') html += `<div style="color:#fbbf24">额 ${fmtAmount(p.value)}</div>`;
            if (p.seriesName === '融资') html += `<div style="color:#3b82f6">融资 ${fmtRzye(p.value)}</div>`;
            if (p.seriesName === '主力') html += `<div style="color:${p.value >= 0 ? '#0ea5e9' : '#f43f5e'}">主力 ${fmtMf(p.value)}</div>`;
          });
          
          return html;
        }
      },
      legend: { 
        show: showLegend, 
        top: 2, 
        textStyle: { color: '#888', fontSize: 10 }, 
        data: ['K线', 'MA5', 'MA10', 'MA20', '成交量', '融资', '主力']
      },
      axisPointer: {
        link: [{ xAxisIndex: 'all' }]
      },
      grid: [
        { left: 50, right: 10, top: showLegend ? 35 : 15, height: '45%' },
        { left: 50, right: 10, top: '62%', height: '15%' },
        { left: 50, right: 10, top: '79%', height: '10%' },
        { left: 50, right: 10, top: '90%', height: '8%' }
      ],
      xAxis: [
        { type: 'category', data: dates, scale: true, boundaryGap: false, axisLine: { lineStyle: { color: '#444' } }, axisLabel: { show: false }, splitLine: { show: false } },
        { type: 'category', gridIndex: 1, data: dates, axisLine: { lineStyle: { color: '#444' } }, axisLabel: { show: false }, splitLine: { show: false } },
        { type: 'category', gridIndex: 2, data: dates, axisLine: { lineStyle: { color: '#444' } }, axisLabel: { show: false }, splitLine: { show: false } },
        { type: 'category', gridIndex: 3, data: dates, axisLine: { lineStyle: { color: '#444' } }, axisLabel: { color: '#666', fontSize: 9 }, splitLine: { show: false } }
      ],
      yAxis: [
        { scale: true, splitLine: { lineStyle: { color: '#333' } }, axisLabel: { color: '#888', fontSize: 10, formatter: (v) => fmtPrice(v) } },
        { gridIndex: 1, splitNumber: 2, axisLabel: { show: false }, splitLine: { show: false } },
        { gridIndex: 2, scale: true, splitNumber: 2, axisLabel: { show: false }, splitLine: { show: false } },
        { gridIndex: 3, scale: true, splitNumber: 2, axisLabel: { show: false }, splitLine: { show: false } }
      ],
      series
    };
    
    if (showDataZoom) {
      result.dataZoom = [
        { type: 'inside', xAxisIndex: [0, 1, 2, 3], start: 70, end: 100 },
        { type: 'slider', xAxisIndex: [0, 1, 2, 3], bottom: 2, height: 18, borderColor: '#333', backgroundColor: '#222', fillerColor: 'rgba(59, 130, 246, 0.2)', handleStyle: { color: '#3b82f6' }, textStyle: { color: '#888', fontSize: 10 } },
      ];
    }
    
    return result;
  };

  const getLatestKlineData = (data) => {
    if (!data || !data.length) return null;
    const latest = data[data.length - 1];
    return {
      trade_date: latest?.trade_date,
      close: Number(latest?.close) || null,
      open: Number(latest?.open) || null,
      high: Number(latest?.high) || null,
      low: Number(latest?.low) || null,
      vol: Number(latest?.vol) || 0,
      amount: (Number(latest?.amount) || 0) * 1000 / 1e8,
      net_mf_vol: latest?.net_mf_vol != null ? Number(latest.net_mf_vol) / 10 : null,
      rzye: latest?.rzye != null ? Number(latest.rzye) / 1e8 : null,
      ma5: Number(latest?.ma5) || null,
      ma10: Number(latest?.ma10) || null,
      ma20: Number(latest?.ma20) || null,
    };
  };
  
  return { createKlineOption, getLatestKlineData };
}
