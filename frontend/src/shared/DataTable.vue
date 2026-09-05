<script setup lang="ts">
import type { TableColumn } from './table'

// 通用数据表格（只渲染传入行，不做任何假数据）。
defineProps<{ columns: TableColumn[]; rows: Record<string, unknown>[]; loading?: boolean; emptyText?: string }>()
</script>

<template>
  <div class="table-wrap">
    <table class="table">
      <thead>
        <tr>
          <th v-for="c in columns" :key="c.key" :class="c.align === 'right' ? 'r' : 'l'">{{ c.title }}</th>
          <slot name="head-extra" />
        </tr>
      </thead>
      <tbody>
        <tr v-if="loading">
          <td :colspan="columns.length + 1" class="empty-hint">加载中…</td>
        </tr>
        <tr v-else-if="rows.length === 0">
          <td :colspan="columns.length + 1" class="empty-hint">{{ emptyText || '暂无数据' }}</td>
        </tr>
        <template v-else>
          <slot name="body" :rows="rows" />
        </template>
      </tbody>
    </table>
  </div>
</template>

<style scoped>
.table-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: var(--radius); }
.table { width: 100%; border-collapse: collapse; font-size: 12px; }
.table thead tr { background: var(--bg); color: var(--text-dim); border-bottom: 1px solid var(--border); }
.table th { padding: 10px 14px; font-weight: 500; text-align: left; white-space: nowrap; }
.table th.r { text-align: right; }
.table tbody tr { border-bottom: 1px solid var(--surface-subtle); color: var(--text); }
.table tbody tr:last-child { border-bottom: none; }
.table tbody tr:hover { background: rgba(12, 16, 23, 0.6); }
.table td { padding: 10px 14px; }
</style>
