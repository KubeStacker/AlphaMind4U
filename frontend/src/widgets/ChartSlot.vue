<script setup lang="ts">
// 图表占位（ECharts 待接入：按需引入 echarts 並在此挂载）。
defineProps<{ title: string; hint?: string }>()
</script>

<template>
  <div class="chart-slot">
    <div class="title"><i data-lucide="bar-chart-3"></i><span>{{ title }}</span></div>
    <div class="body mono">{{ hint || '图表预留位：待接入 ECharts 后在此渲染' }}</div>
  </div>
</template>

<style scoped>
.chart-slot { border: 1px dashed var(--border); border-radius: var(--radius-lg); background: var(--surface); padding: 16px; }
.title { display: flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 600; color: var(--white); margin-bottom: 10px; }
.body { min-height: 120px; display: flex; align-items: center; justify-content: center; color: var(--text-dim); font-size: 12px; background: var(--bg); border-radius: var(--radius); padding: 12px; text-align: center; }
</style>
