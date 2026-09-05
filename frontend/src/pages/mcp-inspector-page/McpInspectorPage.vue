<script setup lang="ts">
import { ref } from 'vue'
import DataTable from '@/shared/DataTable.vue'
import type { TableColumn } from '@/shared/table'
import { mcpCall } from '@/api/mcp'
import { refreshIcons } from '@/shared/icons'

// MCP 检查页：三个只读工具 list / describe / query 的查询入口。
type ToolKey = 'list' | 'describe' | 'query'
const TOOL_NAMES: Record<ToolKey, string> = {
  list: 'jarvis_list_datasets',
  describe: 'jarvis_describe_dataset',
  query: 'jarvis_query_dataset',
}

const active = ref<ToolKey>('list')
const dataset = ref('stock_basic')
const limit = ref(20)
const loading = ref(false)
const error = ref('')
const resultText = ref('')
const rows = ref<Record<string, unknown>[]>([])
const rowColumns = ref<TableColumn[]>([])
const rowCount = ref(0)

// 切换工具页签。
function switchTool(t: ToolKey) {
  active.value = t
  error.value = ''
  resultText.value = ''
  rows.value = []
  refreshIcons()
}

// 从 MCP 结果中提取 datasets 数组。
function extractDatasets(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[]
  if (result && typeof result === 'object') {
    const r = result as Record<string, unknown>
    if (Array.isArray(r['datasets'])) return r['datasets'] as Record<string, unknown>[]
    if (Array.isArray(r['content'])) return r['content'] as Record<string, unknown>[]
  }
  return []
}

// 执行当前工具调用（失败只展示错误，不造假数据）。
async function runTool() {
  loading.value = true
  error.value = ''
  resultText.value = ''
  rows.value = []
  try {
    let payload: unknown
    if (active.value === 'list') {
      payload = await mcpCall({ tool: TOOL_NAMES.list, args: {} })
      rows.value = extractDatasets(payload)
      rowColumns.value = [
        { key: 'name', title: '表名' },
        { key: 'description', title: '描述' },
        { key: 'date_column', title: '日期字段', align: 'right' },
      ]
    } else if (active.value === 'describe') {
      const name = dataset.value.trim()
      if (!name) throw new Error('请输入数据集名称')
      payload = await mcpCall({ tool: TOOL_NAMES.describe, args: { dataset: name } })
      const cols = (payload as { columns?: Record<string, unknown>[] })?.columns || []
      rows.value = cols
      rowColumns.value = [
        { key: 'name', title: '字段' },
        { key: 'type', title: '类型' },
        { key: 'nullable', title: '可空', align: 'right' },
      ]
    } else {
      const name = dataset.value.trim()
      if (!name) throw new Error('请输入数据集名称')
      payload = await mcpCall({
        tool: TOOL_NAMES.query,
        args: { dataset: name, limit: Math.max(1, Math.min(1000, limit.value || 20)), offset: 0 },
      })
      const r = payload as { rows?: Record<string, unknown>[]; count?: number; columns?: string[] }
      rows.value = r.rows || []
      rowCount.value = r.count ?? rows.value.length
      rowColumns.value = (r.columns || Object.keys(rows.value[0] || {})).map((k) => ({ key: k, title: k }))
    }
    resultText.value = JSON.stringify(payload, null, 2).slice(0, 4000)
  } catch (e) {
    error.value = e instanceof Error ? e.message.slice(0, 500) : '调用失败'
  } finally {
    loading.value = false
    refreshIcons()
  }
}

// 单元格短文本展示（避免全量长字段撑爆表格）。
function cellText(v: unknown): string {
  if (v === null || v === undefined) return '—'
  const s = String(v)
  return s.length > 60 ? `${s.slice(0, 60)}…` : s
}
</script>

<template>
  <div class="page">
    <section class="card">
      <div class="sec-title"><i data-lucide="plug"></i><h3>MCP 只读工具检查（POST /mcp）</h3></div>
      <div class="tabs">
        <button :class="['tab', { on: active === 'list' }]" @click="switchTool('list')"><span class="mono">list</span><em>数据集清单</em></button>
        <button :class="['tab', { on: active === 'describe' }]" @click="switchTool('describe')"><span class="mono">describe</span><em>表结构</em></button>
        <button :class="['tab', { on: active === 'query' }]" @click="switchTool('query')"><span class="mono">query</span><em>行查询</em></button>
      </div>
      <div class="form">
        <div v-if="active !== 'list'" class="field">
          <label class="field-label">数据集名称</label>
          <input v-model="dataset" class="input" placeholder="如 stock_basic">
        </div>
        <div v-if="active === 'query'" class="field short">
          <label class="field-label">行数（≤1000）</label>
          <input v-model.number="limit" type="number" min="1" max="1000" class="input">
        </div>
        <button class="btn btn-primary" :disabled="loading" @click="runTool">
          <i data-lucide="play"></i><span>{{ loading ? '调用中…' : `调用 ${TOOL_NAMES[active]}` }}</span>
        </button>
      </div>
      <div v-if="error" class="error-box">{{ error }}</div>
    </section>

    <section v-if="rows.length > 0" class="card">
      <div class="sec-title"><i data-lucide="table"></i><h3>结果{{ active === 'query' ? `（共 ${rowCount} 行）` : `（共 ${rows.length} 项）` }}</h3></div>
      <DataTable :columns="rowColumns" :rows="rows">
        <template #body="{ rows: list }">
          <tr v-for="(r, i) in (list as unknown as Record<string, unknown>[])" :key="i">
            <td v-for="c in rowColumns" :key="c.key" class="mono cell">{{ cellText(r[c.key]) }}</td>
          </tr>
        </template>
      </DataTable>
    </section>

    <section v-if="resultText" class="card">
      <div class="sec-title"><i data-lucide="braces"></i><h3>原始返回（截断）</h3></div>
      <pre class="raw mono">{{ resultText }}</pre>
    </section>
  </div>
</template>

<style scoped>
.page { display: flex; flex-direction: column; gap: 16px; }
.sec-title { display: flex; align-items: center; gap: 8px; margin-bottom: 14px; }
.sec-title h3 { margin: 0; font-size: 13px; color: var(--white); }
.tabs { display: flex; gap: 8px; margin-bottom: 14px; flex-wrap: wrap; }
.tab { display: flex; align-items: center; gap: 8px; padding: 8px 14px; border-radius: var(--radius); background: var(--bg); border: 1px solid var(--border); color: var(--text); cursor: pointer; font-size: 12px; }
.tab em { font-style: normal; color: var(--text-dim); font-size: 11px; }
.tab.on { border-color: var(--accent-blue); color: var(--white); }
.form { display: flex; gap: 12px; align-items: flex-end; flex-wrap: wrap; margin-bottom: 14px; }
.field { min-width: 220px; }
.field.short { min-width: 130px; }
.cell { font-size: 11px; max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.raw { background: var(--bg); border: 1px solid var(--border); border-radius: var(--radius); padding: 12px; font-size: 11px; line-height: 1.6; overflow: auto; max-height: 320px; white-space: pre-wrap; word-break: break-all; color: var(--text); }
</style>
