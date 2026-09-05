<script setup lang="ts">
import { onMounted, ref } from 'vue'
import DataTable from '@/shared/DataTable.vue'
import { getCoverage, type TableCoverage } from '@/api/quant'
import { refreshIcons } from '@/shared/icons'

// 质量看板：截至何时、是否完整、缺什么、为何缺、何时补（只渲染真接口数据）。
const tables = ref<TableCoverage[]>([])
const universe = ref(0)
const latestDay = ref('')
const loading = ref(true)
const error = ref('')

async function load(): Promise<void> {
  loading.value = true
  error.value = ''
  try {
    const data = await getCoverage()
    universe.value = data.universe
    latestDay.value = data.latest_trading_day
    tables.value = data.tables
  } catch (e) {
    error.value = e instanceof Error ? e.message : '加载失败'
  } finally {
    loading.value = false
    refreshIcons()
  }
}

onMounted(load)
</script>

<template>
  <section class="card">
    <div class="sec-title">
      <i data-lucide="activity"></i>
      <h3>数据覆盖看板</h3>
      <span class="dim">上市 {{ universe }} 家 · 最新交易日 {{ latestDay || '—' }}</span>
      <button class="btn btn-sm" @click="load">刷新</button>
    </div>
    <p v-if="error" class="err">{{ error }}</p>
    <DataTable
      v-else
      :columns="[
        { key: 'name', title: '表' },
        { key: 'as_of', title: '截至' },
        { key: 'complete', title: '完整' },
        { key: 'total', title: '总量', align: 'right' },
        { key: 'missing', title: '缺口' },
        { key: 'reason', title: '原因' },
        { key: 'next', title: '何时补' },
      ]"
      :rows="tables"
      :loading="loading"
      empty-text="暂无覆盖数据"
    >
      <template #body="{ rows }">
        <tr v-for="t in (rows as unknown as TableCoverage[])" :key="t.name">
          <td class="mono name">{{ t.name }}</td>
          <td class="mono">{{ t.as_of || '—' }}</td>
          <td>
            <span :class="t.complete ? 'tag-ok' : 'tag-bad'">{{ t.complete ? '完整' : '缺口' }}</span>
          </td>
          <td class="mono r">{{ t.total }}</td>
          <td class="mono dim small">{{ t.missing.join(' ') || '—' }}</td>
          <td class="dim small">{{ t.reason || '—' }}</td>
          <td class="dim small">{{ t.next || '—' }}</td>
        </tr>
      </template>
    </DataTable>
  </section>
</template>

<style scoped>
.tag-ok {
  color: var(--ok, #3fb950);
  border: 1px solid var(--ok, #3fb950);
  border-radius: 4px;
  padding: 0 6px;
  font-size: 12px;
}
.tag-bad {
  color: var(--warn, #d29922);
  border: 1px solid var(--warn, #d29922);
  border-radius: 4px;
  padding: 0 6px;
  font-size: 12px;
}
</style>
