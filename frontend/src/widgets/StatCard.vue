<script setup lang="ts">
// 指标小卡（标题 + 数字，数字等宽展示）。
defineProps<{ label: string; value: string | number; icon?: string }>()
</script>

<template>
  <div class="stat">
    <div class="top"><i :data-lucide="icon || 'activity'"></i><span>{{ label }}</span></div>
    <div class="value mono">{{ value }}</div>
  </div>
</template>

<style scoped>
.stat { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-lg); padding: 14px 16px; }
.top { display: flex; align-items: center; gap: 6px; font-size: 11px; color: var(--text-dim); margin-bottom: 8px; }
.value { font-size: 22px; font-weight: 600; color: var(--white); }
</style>
