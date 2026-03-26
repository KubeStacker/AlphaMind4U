<template>
  <div class="space-y-4 md:space-y-6 animate-in fade-in duration-500 max-w-6xl mx-auto pb-8">
    <!-- Header -->
    <div class="bg-business-dark p-4 rounded-2xl shadow-lg border border-business-light">
      <div class="flex flex-col sm:flex-row sm:items-center justify-between mb-4 gap-3">
        <div class="flex items-center space-x-2">
          <div class="w-1.5 h-4 bg-business-accent rounded-full"></div>
          <h2 class="text-sm font-bold text-slate-400">在线文档</h2>
          <span class="text-[10px] text-slate-500 bg-slate-800 px-2 py-0.5 rounded">{{ docs.length }} 篇</span>
        </div>
        <div class="flex items-center gap-2">
          <button @click="loadDocs" :disabled="loading"
            class="px-2 py-1 rounded border text-[10px] font-bold transition"
            :class="loading ? 'bg-slate-700 text-slate-400 border-slate-600' : 'bg-business-accent/20 text-business-accent border-business-accent/30 hover:bg-business-accent hover:text-white'">
            {{ loading ? '加载中...' : '刷新' }}
          </button>
        </div>
      </div>

      <!-- Category Filter -->
      <div class="flex flex-wrap gap-1.5">
        <button
          v-for="cat in categories" :key="cat.id"
          @click="selectedCategory = cat.id"
          class="px-2.5 py-1 rounded-lg text-[10px] font-bold transition-all border"
          :class="selectedCategory === cat.id
            ? 'bg-business-accent text-white border-business-accent'
            : 'bg-slate-800/50 text-slate-400 border-slate-700 hover:text-white hover:border-slate-500'"
        >
          {{ cat.label }}
          <span v-if="cat.count > 0" class="ml-1 opacity-60">{{ cat.count }}</span>
        </button>
      </div>
    </div>

    <!-- Content Area -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <!-- Doc List -->
      <div class="lg:col-span-1 bg-business-dark rounded-2xl shadow-lg border border-business-light overflow-hidden">
        <div class="p-3 border-b border-business-light">
          <h3 class="text-[10px] font-bold text-slate-500 uppercase tracking-wider">文档列表</h3>
        </div>
        <div class="max-h-[60vh] overflow-y-auto">
          <div v-if="loading" class="p-6 text-center text-slate-500 text-xs">
            <div class="w-6 h-6 border-2 border-business-accent border-t-transparent rounded-full animate-spin mx-auto mb-2"></div>
            加载中...
          </div>
          <div v-else-if="filteredDocs.length === 0" class="p-6 text-center text-slate-500 text-xs">
            暂无文档
          </div>
          <div v-else>
            <button
              v-for="doc in filteredDocs" :key="doc.id"
              @click="selectDoc(doc)"
              class="w-full text-left px-4 py-3 border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors"
              :class="selectedDoc?.id === doc.id ? 'bg-business-accent/10 border-l-2 border-l-business-accent' : ''"
            >
              <div class="flex items-start justify-between gap-2">
                <div class="min-w-0 flex-1">
                  <div class="text-xs font-bold text-slate-200 truncate">{{ doc.title }}</div>
                  <div class="text-[10px] text-slate-500 mt-0.5 truncate">{{ doc.summary || '无摘要' }}</div>
                </div>
                <span class="text-[9px] px-1.5 py-0.5 rounded bg-slate-800 text-slate-400 shrink-0">
                  {{ getCategoryLabel(doc.category) }}
                </span>
              </div>
              <div class="flex items-center gap-2 mt-1.5">
                <span v-for="tag in (doc.tags || []).slice(0, 3)" :key="tag"
                  class="text-[9px] px-1 py-0.5 rounded bg-slate-800/60 text-slate-500">
                  {{ tag }}
                </span>
                <span class="text-[9px] text-slate-600 ml-auto">{{ formatDate(doc.updated_at) }}</span>
              </div>
            </button>
          </div>
        </div>
      </div>

      <!-- Doc Content -->
      <div class="lg:col-span-2 bg-business-dark rounded-2xl shadow-lg border border-business-light overflow-hidden">
        <div v-if="!selectedDoc" class="flex items-center justify-center h-64 text-slate-500 text-xs">
          选择一篇文档查看内容
        </div>
        <div v-else>
          <!-- Doc Header -->
          <div class="p-4 border-b border-business-light">
            <div class="flex items-start justify-between gap-3">
              <div>
                <h2 class="text-sm font-bold text-white">{{ selectedDoc.title }}</h2>
                <p v-if="selectedDoc.summary" class="text-[11px] text-slate-400 mt-1">{{ selectedDoc.summary }}</p>
              </div>
              <button @click="handleDelete" class="text-[10px] px-2 py-1 rounded border border-red-700/30 text-red-400 hover:bg-red-500/10 transition shrink-0">
                删除
              </button>
            </div>
            <div class="flex flex-wrap items-center gap-2 mt-2">
              <span class="text-[9px] px-1.5 py-0.5 rounded bg-business-accent/20 text-business-accent">
                {{ getCategoryLabel(selectedDoc.category) }}
              </span>
              <span v-for="tag in (selectedDoc.tags || [])" :key="tag"
                class="text-[9px] px-1.5 py-0.5 rounded bg-slate-800 text-slate-400">
                {{ tag }}
              </span>
              <span class="text-[9px] text-slate-600 ml-auto">
                {{ formatSize(selectedDoc.size_bytes) }} | {{ formatDate(selectedDoc.updated_at) }}
              </span>
            </div>
          </div>
          <!-- Doc Body -->
          <div class="p-4 max-h-[50vh] overflow-y-auto">
            <div v-if="loadingContent" class="text-center py-8 text-slate-500 text-xs">
              <div class="w-6 h-6 border-2 border-business-accent border-t-transparent rounded-full animate-spin mx-auto mb-2"></div>
              加载内容...
            </div>
            <div v-else-if="contentError" class="text-center py-8 text-red-400 text-xs">
              {{ contentError }}
            </div>
            <div v-else class="prose prose-invert prose-sm max-w-none doc-content" v-html="renderedContent"></div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { getDocsList, getDocContent, deleteDoc } from '@/services/api';

const loading = ref(false);
const loadingContent = ref(false);
const contentError = ref('');
const docs = ref([]);
const selectedDoc = ref(null);
const docContent = ref('');
const selectedCategory = ref('all');

const categories = computed(() => {
  const cats = [
    { id: 'all', label: '全部', count: docs.value.length },
    { id: 'trading-system', label: '交易系统', count: 0 },
    { id: 'research', label: '研究报告', count: 0 },
    { id: 'portfolio', label: '持仓管理', count: 0 },
    { id: 'training', label: '训练记录', count: 0 },
    { id: 'emotion', label: '情绪管理', count: 0 },
    { id: 'daily-log', label: '每日记录', count: 0 },
    { id: 'other', label: '其他', count: 0 },
  ];
  docs.value.forEach(d => {
    const cat = cats.find(c => c.id === d.category);
    if (cat) cat.count++;
  });
  return cats;
});

const filteredDocs = computed(() => {
  if (selectedCategory.value === 'all') return docs.value;
  return docs.value.filter(d => d.category === selectedCategory.value);
});

const renderedContent = computed(() => {
  if (!docContent.value) return '';
  return simpleMarkdown(docContent.value);
});

const getCategoryLabel = (id) => {
  const cat = categories.value.find(c => c.id === id);
  return cat?.label || id;
};

const formatDate = (iso) => {
  if (!iso) return '-';
  return iso.slice(0, 10);
};

const formatSize = (bytes) => {
  if (!bytes) return '-';
  if (bytes < 1024) return `${bytes}B`;
  return `${(bytes / 1024).toFixed(1)}KB`;
};

const simpleMarkdown = (md) => {
  let html = md
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/^### (.+)$/gm, '<h3>$1</h3>')
    .replace(/^## (.+)$/gm, '<h2>$1</h2>')
    .replace(/^# (.+)$/gm, '<h1>$1</h1>')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`(.+?)`/g, '<code>$1</code>')
    .replace(/^- \[x\] (.+)$/gm, '<div>✅ $1</div>')
    .replace(/^- \[ \] (.+)$/gm, '<div>⬜ $1</div>')
    .replace(/^- (.+)$/gm, '<li>$1</li>')
    .replace(/^\d+\. (.+)$/gm, '<li>$1</li>')
    .replace(/\n{2,}/g, '</p><p>')
    .replace(/\n/g, '<br>');
  return `<p>${html}</p>`;
};

const loadDocs = async () => {
  loading.value = true;
  try {
    const res = await getDocsList();
    docs.value = res.data.docs || [];
  } catch (e) {
    console.error('加载文档列表失败', e);
  } finally {
    loading.value = false;
  }
};

const selectDoc = async (doc) => {
  selectedDoc.value = doc;
  loadingContent.value = true;
  contentError.value = '';
  docContent.value = '';
  try {
    const res = await getDocContent(doc.id);
    docContent.value = res.data.content || '';
  } catch (e) {
    contentError.value = '加载文档内容失败';
  } finally {
    loadingContent.value = false;
  }
};

const handleDelete = async () => {
  if (!selectedDoc.value) return;
  if (!confirm(`确定删除文档 "${selectedDoc.value.title}"？`)) return;
  try {
    await deleteDoc(selectedDoc.value.id);
    docs.value = docs.value.filter(d => d.id !== selectedDoc.value.id);
    selectedDoc.value = null;
    docContent.value = '';
  } catch (e) {
    alert('删除失败');
  }
};

onMounted(loadDocs);
</script>

<style scoped>
.doc-content :deep(h1) { font-size: 1.25rem; font-weight: 700; color: #f1f5f9; margin: 1rem 0 0.5rem; }
.doc-content :deep(h2) { font-size: 1.1rem; font-weight: 700; color: #e2e8f0; margin: 0.8rem 0 0.4rem; border-bottom: 1px solid #334155; padding-bottom: 0.3rem; }
.doc-content :deep(h3) { font-size: 0.95rem; font-weight: 600; color: #cbd5e1; margin: 0.6rem 0 0.3rem; }
.doc-content :deep(li) { margin-left: 1rem; list-style: disc; color: #94a3b8; font-size: 0.8rem; line-height: 1.6; }
.doc-content :deep(code) { background: #1e293b; padding: 0.1rem 0.3rem; border-radius: 4px; font-size: 0.75rem; color: #f59e0b; }
.doc-content :deep(strong) { color: #f1f5f9; }
.doc-content :deep(p) { color: #94a3b8; font-size: 0.8rem; line-height: 1.7; }
.doc-content :deep(div) { color: #94a3b8; font-size: 0.8rem; line-height: 1.6; margin: 0.2rem 0; }
</style>
