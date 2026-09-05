<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import DataTable from '@/shared/DataTable.vue'
import type { TableColumn } from '@/shared/table'
import { listDatasets, describeDataset, type DatasetInfo, type DatasetSchema } from '@/api/quant'
import { parseApiError } from '@/shared/api-error'
import { refreshIcons } from '@/shared/icons'
import StatCard from '@/widgets/StatCard.vue'
import ChartSlot from '@/widgets/ChartSlot.vue'

// 数据看板页：调真接口 GET /admin/datasets 渲染数据表清单。
const datasets = ref<DatasetInfo[]>([])
const loading = ref(false)
const error = ref('')
const schema = ref<DatasetSchema | null>(null)
const schemaLoading = ref(false)
const schemaError = ref('')

// 表格列定义。
const columns: TableColumn[] = [
  { key: 'name', title: '数据表名 (Table)' },
  { key: 'description', title: '业务描述' },
  { key: 'date_column', title: '日期字段', align: 'right' },
]

// 已加载表数量。
const tableCount = computed(() => String(datasets.value.length))

// 加载数据集清单（失败只展示错误，不造假数据）。
async function loadDatasets() {
  loading.value = true
  error.value = ''
  schema.value = null
  try {
    datasets.value = await listDatasets()
  } catch (e) {
    error.value = parseApiError(e)
    datasets.value = []
  } finally {
    loading.value = false
    refreshIcons()
  }
}

// 查看单表结构。
async function showSchema(name: string) {
  schemaLoading.value = true
  schemaError.value = ''
  schema.value = null
  try {
    schema.value = await describeDataset(name)
  } catch (e) {
    schemaError.value = parseApiError(e)
  } finally {
    schemaLoading.value = false
    refreshIcons()
  }
}

onMounted(loadDatasets)
</script>

<template>
  <div class="page">
    <div class="stats">
      <StatCard label="受控数据表总数" :value="tableCount" icon="database" />
      <StatCard label="数据源" value="GET /admin/datasets" icon="server" />
    </div>

    <section class="card">
      <div class="sec-head">
        <div class="sec-title"><i data-lucide="database"></i><h3>数据资产清单（真接口）</h3></div>
        <button class="btn" :disabled="loading" @click="loadDatasets"><i data-lucide="refresh-cw"></i><span>刷新</span></button>
      </div>
      <div v-if="error" class="error-box">{{ error }}<div class="retry"><button class="btn" @click="loadDatasets">重试</button></div></div>
      <DataTable v-else :columns="columns" :rows="datasets" :loading="loading" empty-text="暂无数据表">
        <template #body="{ rows }">
          <tr v-for="d in (rows as unknown as DatasetInfo[])" :key="d.name">
            <td class="mono name">{{ d.name }}</td>
            <td class="dim">{{ d.description }}</td>
            <td class="mono r">{{ d.date_column || '—' }}</td>
            <td class="r"><button class="btn btn-sm" @click="showSchema(d.name)">结构</button></td>
          </tr>
        </template>
        <template #head-extra><th class="r">操作</th></template>
      </DataTable>
    </section>

    <section v-if="schema || schemaLoading || schemaError" class="card">
      <div class="sec-head">
        <div class="sec-title"><i data-lucide="table"></i><h3>表结构{{ schema ? `：${schema.name}` : '' }}</h3></div>
      </div>
      <div v-if="schemaError" class="error-box">{{ schemaError }}</div>
      <div v-else-if="schemaLoading" class="empty-hint">加载中…</div>
      <div v-else-if="schema" class="schema mono">
        <div class="schema-meta">描述：{{ schema.description }} ｜ 日期字段：{{ schema.date_column || '无' }} ｜ 字段数：{{ schema.columns.length }}</div>
        <div class="cols">
          <span v-for="c in schema.columns" :key="c.name" class="col" :class="{ pk: c.primary_key }">{{ c.name }}<em>{{ c.type }}</em></span>
        </div>
      </div>
    </section>

    <ChartSlot title="行数趋势图" hint="图表预留位：待接入 ECharts 后在此渲染各表行数趋势" />
  </div>
</template>

<style scoped>
.page { display: flex; flex-direction: column; gap: 16px; }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
.sec-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding-bottom: 12px; border-bottom: 1px solid var(--surface-subtle); margin-bottom: 14px; }
.sec-title { display: flex; align-items: center; gap: 8px; }
.sec-title h3 { margin: 0; font-size: 13px; color: var(--white); }
.mono.name { color: var(--white); font-weight: 500; }
.dim { color: var(--text-dim); font-size: 11px; }
.r { text-align: right; }
.btn-sm { padding: 4px 10px; font-size: 11px; }
.retry { margin-top: 8px; }
.schema-meta { font-size: 12px; color: var(--text); margin-bottom: 10px; }
.cols { display: flex; flex-wrap: wrap; gap: 6px; }
.col { font-size: 11px; padding: 3px 8px; border-radius: 4px; background: var(--surface-subtle); border: 1px solid var(--border); color: var(--text); }
.col em { font-style: normal; color: var(--text-dim); margin-left: 6px; }
.col.pk { border-color: rgba(56, 139, 253, 0.5); color: var(--accent-blue); }
</style>
