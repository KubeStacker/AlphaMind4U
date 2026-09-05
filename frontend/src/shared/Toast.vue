<script setup lang="ts">
import { useToastStore } from '@/stores/toast'

// 全局轻提示条（右下角自动消失）。
const toast = useToastStore()
</script>

<template>
  <div class="toasts">
    <div v-for="t in toast.items" :key="t.id" class="toast" :class="t.kind">
      <i v-if="t.kind === 'error'" data-lucide="alert-circle"></i>
      <i v-else-if="t.kind === 'success'" data-lucide="check-circle"></i>
      <i v-else data-lucide="info"></i>
      <span class="mono">{{ t.message }}</span>
    </div>
  </div>
</template>

<style scoped>
.toasts { position: fixed; right: 20px; bottom: 20px; z-index: 60; display: flex; flex-direction: column; gap: 8px; max-width: min(420px, 90vw); }
.toast {
  display: flex; align-items: center; gap: 8px; padding: 9px 14px; font-size: 12px;
  background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
  color: var(--white); box-shadow: 0 8px 24px rgba(0, 0, 0, 0.5); word-break: break-all;
}
.toast.success { border-color: rgba(63, 185, 80, 0.5); }
.toast.error { border-color: rgba(248, 81, 73, 0.5); color: #ff9d97; }
</style>
