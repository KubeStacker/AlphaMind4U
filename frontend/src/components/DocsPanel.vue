<template>
  <Teleport to="body">
    <transition name="fade">
      <div v-if="open" class="fixed inset-0 z-[60] bg-black/40" @click="$emit('close')"></div>
    </transition>
    <transition name="slide">
      <div v-if="open"
        class="fixed top-0 right-0 bottom-0 z-[70] bg-business-dark border-l border-business-light shadow-2xl flex flex-col"
        :class="isMobile ? 'w-full' : 'w-[700px]'">
        <!-- Header -->
        <div class="flex items-center justify-between px-4 py-3 border-b border-business-light shrink-0">
          <div class="flex items-center space-x-2">
            <div class="w-1.5 h-4 bg-business-accent rounded-full"></div>
            <h2 class="text-sm font-bold text-slate-200">在线文档</h2>
            <span class="text-[10px] text-slate-500 bg-slate-800 px-1.5 py-0.5 rounded">{{ docs.length }}</span>
          </div>
          <button @click="$emit('close')" class="p-1 rounded hover:bg-slate-700 text-slate-400 hover:text-white transition">
            <XMarkIcon class="w-5 h-5" />
          </button>
        </div>

        <!-- Category Filter -->
        <div class="flex gap-1 px-4 py-2 border-b border-slate-800 shrink-0 overflow-x-auto">
          <button
            v-for="cat in categories" :key="cat.id"
            @click="selectedCategory = cat.id"
            class="px-2.5 py-1 rounded text-[10px] font-bold transition-all border whitespace-nowrap"
            :class="selectedCategory === cat.id
              ? 'bg-business-accent text-white border-business-accent'
              : 'bg-slate-800/50 text-slate-400 border-slate-700 hover:text-white'">
            {{ cat.label }}
            <span v-if="cat.count > 0" class="opacity-60">{{ cat.count }}</span>
          </button>
        </div>

        <!-- Body: desktop side-by-side, mobile stacked -->
        <div class="flex-1 overflow-hidden flex" :class="isMobile ? 'flex-col' : 'flex-row'">
          <!-- Doc List -->
          <div :class="isMobile ? 'flex-1 overflow-y-auto' : 'w-[280px] border-r border-slate-800 overflow-y-auto shrink-0'"
               v-show="isMobile ? !selectedDoc : true">
            <div v-if="loading" class="flex items-center justify-center h-40">
              <div class="w-6 h-6 border-2 border-business-accent border-t-transparent rounded-full animate-spin"></div>
            </div>
            <div v-else-if="filteredDocs.length === 0" class="flex items-center justify-center h-40 text-slate-500 text-xs">
              暂无文档
            </div>
            <div v-else>
              <button
                v-for="doc in filteredDocs" :key="doc.id"
                @click="selectDoc(doc)"
                class="w-full text-left px-3 py-2.5 border-b border-slate-800/50 hover:bg-slate-800/40 transition-colors"
                :class="selectedDoc?.id === doc.id ? 'bg-business-accent/10 border-l-2 border-l-business-accent' : ''">
                <div class="text-xs font-bold text-slate-200 truncate">{{ doc.title }}</div>
                <div class="text-[10px] text-slate-500 mt-0.5 line-clamp-2">{{ doc.summary }}</div>
                <div class="flex items-center gap-1 mt-1">
                  <span class="text-[9px] px-1 py-0.5 rounded bg-slate-800 text-slate-400">{{ getCategoryLabel(doc.category) }}</span>
                  <span class="text-[9px] text-slate-600 ml-auto">{{ formatDate(doc.updated_at) }}</span>
                </div>
              </button>
            </div>
          </div>

          <!-- Doc Content -->
          <div :class="isMobile ? 'flex-1 overflow-y-auto' : 'flex-1 overflow-y-auto'"
               v-show="isMobile ? !!selectedDoc : true">
            <div v-if="!selectedDoc" class="flex items-center justify-center h-full text-slate-500 text-xs">
              选择一篇文档
            </div>
            <div v-else>
              <!-- Mobile back -->
              <button v-if="isMobile" @click="selectedDoc = null; docContent = ''"
                class="flex items-center gap-1 px-4 py-2 text-[10px] text-slate-400 hover:text-white border-b border-slate-800 w-full">
                <ArrowLeftIcon class="w-3 h-3" /> 返回列表
              </button>
              <!-- Header -->
              <div class="px-4 py-3 border-b border-slate-800 sticky top-0 bg-business-dark/95 backdrop-blur-sm z-10">
                <div class="flex items-start justify-between">
                  <div>
                    <h3 class="text-sm font-bold text-white">{{ selectedDoc.title }}</h3>
                    <p v-if="selectedDoc.summary" class="text-[11px] text-slate-400 mt-0.5">{{ selectedDoc.summary }}</p>
                  </div>
                </div>
                <div class="flex flex-wrap items-center gap-1.5 mt-2">
                  <span class="text-[9px] px-1.5 py-0.5 rounded bg-business-accent/20 text-business-accent">
                    {{ getCategoryLabel(selectedDoc.category) }}
                  </span>
                  <span v-for="tag in (selectedDoc.tags || [])" :key="tag"
                    class="text-[9px] px-1 py-0.5 rounded bg-slate-800 text-slate-400">{{ tag }}</span>
                </div>
              </div>
              <!-- Body -->
              <div class="p-4">
                <div v-if="loadingContent" class="flex items-center justify-center py-8">
                  <div class="w-6 h-6 border-2 border-business-accent border-t-transparent rounded-full animate-spin"></div>
                </div>
                <div v-else-if="contentError" class="text-center py-8 text-red-400 text-xs">{{ contentError }}</div>
                <div v-else class="prose prose-invert prose-sm max-w-none doc-content" v-html="renderedContent"></div>
              </div>
            </div>
          </div>
        </div>

        <!-- Footer -->
        <div class="px-4 py-2 border-t border-business-light flex items-center justify-between shrink-0">
          <button @click="loadDocs" class="text-[10px] text-slate-500 hover:text-white transition">
            {{ loading ? '加载中...' : '刷新' }}
          </button>
          <span class="text-[9px] text-slate-600">Jarvis Docs</span>
        </div>
      </div>
    </transition>
  </Teleport>
</template>

<script setup>
import { ref, computed, watch } from 'vue';
import { getDocsList, getDocContent } from '@/services/api';
import { XMarkIcon, ArrowLeftIcon } from '@heroicons/vue/20/solid';

const props = defineProps({ open: Boolean });
const emit = defineEmits(['close']);

const loading = ref(false);
const loadingContent = ref(false);
const contentError = ref('');
const docs = ref([]);
const selectedDoc = ref(null);
const docContent = ref('');
const selectedCategory = ref('all');
const isMobile = ref(window.innerWidth < 768);

window.addEventListener('resize', () => { isMobile.value = window.innerWidth < 768; });

const categories = computed(() => {
  const cats = [
    { id: 'all', label: '全部', count: docs.value.length },
    { id: 'trading-system', label: '交易系统', count: 0 },
    { id: 'research', label: '研究报告', count: 0 },
    { id: 'portfolio', label: '持仓管理', count: 0 },
    { id: 'training', label: '训练记录', count: 0 },
    { id: 'emotion', label: '情绪管理', count: 0 },
    { id: 'other', label: '其他', count: 0 },
  ];
  docs.value.forEach(d => { const c = cats.find(x => x.id === d.category); if (c) c.count++; });
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

const getCategoryLabel = (id) => categories.value.find(c => c.id === id)?.label || id;
const formatDate = (iso) => iso ? iso.slice(0, 10) : '-';

const simpleMarkdown = (md) => {
  // Split into lines for better block handling
  const lines = md.split('\n');
  let html = '';
  let inList = false;
  for (const line of lines) {
    let l = line.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    if (l.match(/^### (.+)$/)) { if (inList) { html += '</ul>'; inList = false; } html += `<h3>${l.slice(4)}</h3>`; }
    else if (l.match(/^## (.+)$/)) { if (inList) { html += '</ul>'; inList = false; } html += `<h2>${l.slice(3)}</h2>`; }
    else if (l.match(/^# (.+)$/)) { if (inList) { html += '</ul>'; inList = false; } html += `<h1>${l.slice(2)}</h1>`; }
    else if (l.match(/^- (.+)$/) || l.match(/^\d+\. (.+)$/)) {
      if (!inList) { html += '<ul>'; inList = true; }
      const content = l.replace(/^- /, '').replace(/^\d+\. /, '');
      html += `<li>${content.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>').replace(/`(.+?)`/g, '<code>$1</code>')}</li>`;
    } else if (l.match(/^- \[x\] (.+)$/)) { html += `<div class="check">✅ ${l.slice(5)}</div>`; }
    else if (l.match(/^- \[ \] (.+)$/)) { html += `<div class="check">⬜ ${l.slice(5)}</div>`; }
    else if (l.trim() === '') { if (inList) { html += '</ul>'; inList = false; } }
    else {
      if (inList) { html += '</ul>'; inList = false; }
      l = l.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>').replace(/`{3}[\s\S]*?`{3}/g, (m) => `<pre>${m.slice(3,-3)}</pre>`).replace(/`(.+?)`/g, '<code>$1</code>');
      html += `<p>${l}</p>`;
    }
  }
  if (inList) html += '</ul>';
  return html;
};

const loadDocs = async () => {
  loading.value = true;
  try { const res = await getDocsList(); docs.value = res.data.docs || []; }
  catch (e) { console.error(e); }
  finally { loading.value = false; }
};

const selectDoc = async (doc) => {
  selectedDoc.value = doc;
  loadingContent.value = true; contentError.value = ''; docContent.value = '';
  try { const res = await getDocContent(doc.id); docContent.value = res.data.content || ''; }
  catch (e) { contentError.value = '加载失败'; }
  finally { loadingContent.value = false; }
};

watch(() => props.open, (v) => { if (v && docs.value.length === 0) loadDocs(); });
</script>

<style scoped>
.fade-enter-active, .fade-leave-active { transition: opacity 0.2s ease; }
.fade-enter-from, .fade-leave-to { opacity: 0; }
.slide-enter-active, .slide-leave-active { transition: transform 0.25s ease; }
.slide-enter-from, .slide-leave-to { transform: translateX(100%); }
.line-clamp-2 { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }

.doc-content :deep(h1) { font-size: 1.2rem; font-weight: 700; color: #f1f5f9; margin: 1rem 0 0.5rem; }
.doc-content :deep(h2) { font-size: 1.05rem; font-weight: 700; color: #e2e8f0; margin: 0.9rem 0 0.4rem; border-bottom: 1px solid #334155; padding-bottom: 0.3rem; }
.doc-content :deep(h3) { font-size: 0.92rem; font-weight: 600; color: #cbd5e1; margin: 0.6rem 0 0.25rem; }
.doc-content :deep(ul) { margin: 0.3rem 0; padding-left: 1.2rem; }
.doc-content :deep(li) { list-style: disc; color: #94a3b8; font-size: 0.82rem; line-height: 1.65; margin: 0.15rem 0; }
.doc-content :deep(code) { background: #1e293b; padding: 0.1rem 0.35rem; border-radius: 4px; font-size: 0.76rem; color: #f59e0b; }
.doc-content :deep(pre) { background: #0f172a; padding: 0.8rem; border-radius: 6px; overflow-x: auto; font-size: 0.72rem; color: #94a3b8; border: 1px solid #1e293b; margin: 0.5rem 0; }
.doc-content :deep(strong) { color: #f1f5f9; }
.doc-content :deep(p) { color: #94a3b8; font-size: 0.82rem; line-height: 1.7; margin: 0.3rem 0; }
.doc-content :deep(.check) { color: #94a3b8; font-size: 0.82rem; line-height: 1.6; margin: 0.2rem 0; }
</style>
