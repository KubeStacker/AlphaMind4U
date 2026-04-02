<template>
  <div class="space-y-4 md:space-y-6 animate-in fade-in duration-500 max-w-7xl mx-auto pb-8">
    <!-- Header -->
    <div class="bg-business-dark p-4 rounded-2xl shadow-lg border border-business-light">
      <div class="flex flex-col sm:flex-row sm:items-center justify-between mb-4 gap-3">
        <div class="flex items-center space-x-2">
          <div class="w-1.5 h-4 bg-business-accent rounded-full"></div>
          <h2 class="text-sm font-bold text-slate-400">在线文档</h2>
          <span class="text-[10px] text-slate-500 bg-slate-800 px-2 py-0.5 rounded">{{ allDocs.length }} 篇</span>
        </div>
        <div class="flex items-center gap-2">
          <button @click="showNotesPanel = true" class="px-2 py-1 rounded border text-[10px] font-bold transition bg-slate-800/50 text-slate-400 border-slate-700 hover:text-white hover:border-slate-500">
            📝 笔记汇总
          </button>
          <button @click="showTagsManager = true" class="px-2 py-1 rounded border text-[10px] font-bold transition bg-slate-800/50 text-slate-400 border-slate-700 hover:text-white hover:border-slate-500">
            🏷️ 标签管理
          </button>
          <button @click="loadDocs" :disabled="loading"
            class="px-2 py-1 rounded border text-[10px] font-bold transition"
            :class="loading ? 'bg-slate-700 text-slate-400 border-slate-600' : 'bg-business-accent/20 text-business-accent border-business-accent/30 hover:bg-business-accent hover:text-white'">
            {{ loading ? '加载中...' : '刷新' }}
          </button>
        </div>
      </div>

      <!-- Source Filter + Category Filter -->
      <div class="flex flex-wrap gap-2 mb-3">
        <button
          v-for="src in sources" :key="src.id"
          @click="selectedSource = src.id"
          class="px-2.5 py-1 rounded-lg text-[10px] font-bold transition-all border"
          :class="selectedSource === src.id
            ? 'bg-business-accent text-white border-business-accent'
            : 'bg-slate-800/50 text-slate-400 border-slate-700 hover:text-white hover:border-slate-500'"
        >
          {{ src.label }}
        </button>
      </div>
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
    <div class="grid grid-cols-1 lg:grid-cols-4 gap-4">
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
                  <div class="text-xs font-bold text-slate-200 truncate flex items-center gap-1">
                    <span v-if="doc.is_published" class="text-[9px] px-1 py-0.5 rounded bg-green-900/30 text-green-400">已发布</span>
                    {{ doc.title }}
                  </div>
                  <div class="text-[10px] text-slate-500 mt-0.5 truncate">{{ doc.summary || '无摘要' }}</div>
                </div>
              </div>
              <div class="flex items-center gap-2 mt-1.5">
                <span v-for="tag in (doc.display_tags || []).slice(0, 3)" :key="tag"
                  class="text-[9px] px-1 py-0.5 rounded bg-slate-800/60 text-slate-500"
                  :style="tag.color ? { borderLeft: `2px solid ${tag.color}` } : {}">
                  {{ tag.tag_name || tag }}
                </span>
                <span class="text-[9px] text-slate-600 ml-auto">{{ formatDate(doc.updated_at) }}</span>
              </div>
            </button>
          </div>
        </div>
      </div>

      <!-- Doc Content -->
      <div class="lg:col-span-3 bg-business-dark rounded-2xl shadow-lg border border-business-light overflow-hidden">
        <div v-if="!selectedDoc" class="flex items-center justify-center h-64 text-slate-500 text-xs">
          选择一篇文档查看内容
        </div>
        <div v-else class="flex flex-col h-full" style="max-height: 70vh;">
          <!-- Doc Header -->
          <div class="p-4 border-b border-business-light shrink-0">
            <div class="flex items-start justify-between gap-3">
              <div>
                <h2 class="text-sm font-bold text-white flex items-center gap-2">
                  <span v-if="selectedDoc.is_published" class="text-[9px] px-1.5 py-0.5 rounded bg-green-900/30 text-green-400">已发布</span>
                  {{ selectedDoc.title }}
                </h2>
                <p v-if="selectedDoc.summary" class="text-[11px] text-slate-400 mt-1">{{ selectedDoc.summary }}</p>
              </div>
              <div class="flex items-center gap-2">
                <button @click="openNoteEditor(null)" class="text-[10px] px-2 py-1 rounded border border-blue-700/30 text-blue-400 hover:bg-blue-500/10 transition">
                  + 笔记
                </button>
                <button @click="openTagSelector" class="text-[10px] px-2 py-1 rounded border border-slate-700/30 text-slate-400 hover:bg-slate-700/30 transition">
                  🏷️ 标签
                </button>
                <button v-if="!selectedDoc.is_published" @click="handleDelete" class="text-[10px] px-2 py-1 rounded border border-red-700/30 text-red-400 hover:bg-red-500/10 transition">
                  删除
                </button>
              </div>
            </div>
            <div class="flex flex-wrap items-center gap-2 mt-2">
              <span class="text-[9px] px-1.5 py-0.5 rounded bg-business-accent/20 text-business-accent">
                {{ getCategoryLabel(selectedDoc.category) }}
              </span>
              <span v-for="tag in selectedDocTags" :key="tag.id"
                class="text-[9px] px-1.5 py-0.5 rounded bg-slate-800 text-slate-400"
                :style="{ borderLeft: `2px solid ${tag.color}` }">
                {{ tag.tag_name }}
              </span>
              <span class="text-[9px] text-slate-600 ml-auto">
                {{ formatSize(selectedDoc.size_bytes) }} | {{ formatDate(selectedDoc.updated_at) }}
              </span>
            </div>
          </div>
          <!-- Doc Body -->
          <div ref="contentRef" class="flex-1 p-4 overflow-y-auto doc-content" v-html="renderedContent" @scroll="onScroll"></div>
          <!-- Notes Section -->
          <div v-if="docNotes.length > 0" class="p-4 border-t border-slate-800 shrink-0">
            <div class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">📝 笔记 ({{ docNotes.length }})</div>
            <div class="space-y-2 max-h-32 overflow-y-auto">
              <div v-for="note in docNotes" :key="note.id" class="p-2 bg-slate-800/50 rounded text-xs">
                <div class="flex items-start justify-between gap-2">
                  <span v-if="note.line_number > 0" class="text-[9px] px-1 py-0.5 rounded bg-blue-900/30 text-blue-400">行 {{ note.line_number }}</span>
                  <span class="text-[9px] px-1 py-0.5 rounded" :class="note.note_type === 'highlight' ? 'bg-yellow-900/30 text-yellow-400' : 'bg-purple-900/30 text-purple-400'">
                    {{ note.note_type === 'highlight' ? '高亮' : '笔记' }}
                  </span>
                  <div class="flex gap-1">
                    <button @click="openNoteEditor(note)" class="text-[9px] text-slate-500 hover:text-white">编辑</button>
                    <button @click="deleteNote(note.id)" class="text-[9px] text-red-500 hover:text-red-400">删除</button>
                  </div>
                </div>
                <div class="text-slate-300 mt-1">{{ note.note_content }}</div>
                <div class="text-[9px] text-slate-600 mt-1">{{ formatDate(note.created_at) }}</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Notes Summary Panel -->
    <Teleport to="body">
      <transition name="fade">
        <div v-if="showNotesPanel" class="fixed inset-0 z-[60] bg-black/40" @click="showNotesPanel = false"></div>
      </transition>
      <transition name="slide">
        <div v-if="showNotesPanel" class="fixed top-0 right-0 bottom-0 z-[70] bg-business-dark border-l border-business-light shadow-2xl w-full max-w-2xl flex flex-col">
          <div class="flex items-center justify-between px-4 py-3 border-b border-business-light shrink-0">
            <div class="flex items-center space-x-2">
              <h2 class="text-sm font-bold text-slate-200">📝 笔记汇总</h2>
            </div>
            <button @click="showNotesPanel = false" class="p-1 rounded hover:bg-slate-700 text-slate-400 hover:text-white">
              <XMarkIcon class="w-5 h-5" />
            </button>
          </div>
          <div class="flex-1 overflow-y-auto p-4">
            <div v-if="loadingNotes" class="text-center py-8 text-slate-500">
              <div class="w-6 h-6 border-2 border-business-accent border-t-transparent rounded-full animate-spin mx-auto"></div>
            </div>
            <div v-else-if="allNotes.length === 0" class="text-center py-8 text-slate-500 text-xs">
              暂无笔记，开始记录你的心得吧
            </div>
            <div v-else class="space-y-3">
              <div v-for="note in allNotes" :key="note.id" class="p-3 bg-slate-800/50 rounded-lg border border-slate-700">
                <div class="flex items-center justify-between mb-2">
                  <span class="text-xs font-bold text-slate-200">{{ note.doc_title }}</span>
                  <span class="text-[9px] text-slate-500">{{ formatDate(note.created_at) }}</span>
                </div>
                <div class="text-xs text-slate-300 mb-1">{{ note.note_content }}</div>
                <div class="flex items-center gap-2">
                  <span v-if="note.line_number > 0" class="text-[9px] px-1.5 py-0.5 rounded bg-blue-900/30 text-blue-400">行 {{ note.line_number }}</span>
                  <span class="text-[9px] px-1.5 py-0.5 rounded" :class="note.note_type === 'highlight' ? 'bg-yellow-900/30 text-yellow-400' : 'bg-purple-900/30 text-purple-400'">
                    {{ note.note_type === 'highlight' ? '高亮' : '笔记' }}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </transition>
    </Teleport>

    <!-- Tags Manager Panel -->
    <Teleport to="body">
      <transition name="fade">
        <div v-if="showTagsManager" class="fixed inset-0 z-[60] bg-black/40" @click="showTagsManager = false"></div>
      </transition>
      <transition name="slide">
        <div v-if="showTagsManager" class="fixed top-0 right-0 bottom-0 z-[70] bg-business-dark border-l border-business-light shadow-2xl w-full max-w-md flex flex-col">
          <div class="flex items-center justify-between px-4 py-3 border-b border-business-light shrink-0">
            <div class="flex items-center space-x-2">
              <h2 class="text-sm font-bold text-slate-200">🏷️ 标签管理</h2>
            </div>
            <button @click="showTagsManager = false" class="p-1 rounded hover:bg-slate-700 text-slate-400 hover:text-white">
              <XMarkIcon class="w-5 h-5" />
            </button>
          </div>
          <div class="p-4 border-b border-slate-800 shrink-0">
            <div class="flex gap-2">
              <input v-model="newTagName" placeholder="新标签名称" class="flex-1 px-2 py-1.5 rounded bg-slate-800 border border-slate-700 text-xs text-slate-200 placeholder-slate-500" />
              <input v-model="newTagColor" type="color" class="w-8 h-8 rounded cursor-pointer" />
              <button @click="createTag" class="px-3 py-1.5 rounded bg-business-accent text-white text-xs font-bold">添加</button>
            </div>
          </div>
          <div class="flex-1 overflow-y-auto p-4">
            <div class="space-y-2">
              <div v-for="tag in userTags" :key="tag.id" class="flex items-center justify-between p-2 bg-slate-800/50 rounded">
                <span class="text-xs text-slate-200 flex items-center gap-2">
                  <span class="w-3 h-3 rounded-full" :style="{ backgroundColor: tag.color }"></span>
                  {{ tag.tag_name }}
                </span>
                <button @click="deleteTag(tag.id)" class="text-[9px] text-red-500 hover:text-red-400">删除</button>
              </div>
            </div>
          </div>
        </div>
      </transition>
    </Teleport>

    <!-- Note Editor Modal -->
    <Teleport to="body">
      <transition name="fade">
        <div v-if="showNoteEditor" class="fixed inset-0 z-[60] bg-black/40" @click="showNoteEditor = false"></div>
      </transition>
      <div v-if="showNoteEditor" class="fixed inset-0 z-[70] flex items-center justify-center">
        <div class="bg-business-dark border border-business-light rounded-xl p-4 w-full max-w-md shadow-2xl">
          <h3 class="text-sm font-bold text-slate-200 mb-3">{{ editingNote?.id ? '编辑笔记' : '添加笔记' }}</h3>
          <textarea v-model="noteContent" rows="4" placeholder="记录你的心得..." class="w-full px-3 py-2 rounded bg-slate-800 border border-slate-700 text-xs text-slate-200 placeholder-slate-500 mb-3"></textarea>
          <div class="flex gap-2 mb-3">
            <select v-model="noteType" class="px-2 py-1 rounded bg-slate-800 border border-slate-700 text-xs text-slate-200">
              <option value="note">笔记</option>
              <option value="highlight">高亮</option>
            </select>
            <input v-model.number="noteLineNumber" type="number" placeholder="行号(可选)" class="flex-1 px-2 py-1 rounded bg-slate-800 border border-slate-700 text-xs text-slate-200" />
          </div>
          <div class="flex justify-end gap-2">
            <button @click="showNoteEditor = false" class="px-3 py-1.5 rounded border border-slate-700 text-slate-400 text-xs">取消</button>
            <button @click="saveNote" class="px-3 py-1.5 rounded bg-business-accent text-white text-xs font-bold">保存</button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- Tag Selector Modal -->
    <Teleport to="body">
      <transition name="fade">
        <div v-if="showTagSelector" class="fixed inset-0 z-[60] bg-black/40" @click="showTagSelector = false"></div>
      </transition>
      <div v-if="showTagSelector" class="fixed inset-0 z-[70] flex items-center justify-center">
        <div class="bg-business-dark border border-business-light rounded-xl p-4 w-full max-w-sm shadow-2xl">
          <h3 class="text-sm font-bold text-slate-200 mb-3">选择标签</h3>
          <div class="space-y-2 max-h-60 overflow-y-auto mb-3">
            <label v-for="tag in userTags" :key="tag.id" class="flex items-center gap-2 p-2 rounded hover:bg-slate-800/50 cursor-pointer">
              <input type="checkbox" :value="tag.id" v-model="selectedTagIds" class="rounded" />
              <span class="w-3 h-3 rounded-full" :style="{ backgroundColor: tag.color }"></span>
              <span class="text-xs text-slate-200">{{ tag.tag_name }}</span>
            </label>
          </div>
          <div class="flex justify-end gap-2">
            <button @click="showTagSelector = false" class="px-3 py-1.5 rounded border border-slate-700 text-slate-400 text-xs">取消</button>
            <button @click="saveDocTags" class="px-3 py-1.5 rounded bg-business-accent text-white text-xs font-bold">保存</button>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, nextTick } from 'vue';
import { getDocsList, getPublishedDocsList, getDocContent, deleteDoc, getDocProgress, updateDocProgress, getUserTags, createUserTag, deleteUserTag, getDocTags, setDocTags, getDocNotes, createDocNote, updateDocNote, deleteDocNote, getAllNotes } from '@/services/api';
import { XMarkIcon } from '@heroicons/vue/20/solid';

const loading = ref(false);
const loadingNotes = ref(false);
const allDocs = ref([]);
const selectedDoc = ref(null);
const docContent = ref('');
const selectedSource = ref('all');
const selectedCategory = ref('all');
const contentRef = ref(null);

// Tags & Notes
const showTagsManager = ref(false);
const showNotesPanel = ref(false);
const showNoteEditor = ref(false);
const showTagSelector = ref(false);
const userTags = ref([]);
const selectedTagIds = ref([]);
const newTagName = ref('');
const newTagColor = ref('#64748b');
const docNotes = ref([]);
const selectedDocTags = ref([]);
const allNotes = ref([]);

// Note editor
const editingNote = ref(null);
const noteContent = ref('');
const noteType = ref('note');
const noteLineNumber = ref(0);

const sources = computed(() => [
  { id: 'all', label: '全部' },
  { id: 'published', label: '已发布' },
  { id: 'custom', label: '我的文档' },
]);

const categories = computed(() => {
  const cats = [
    { id: 'all', label: '全部', count: 0 },
    { id: 'trading-system', label: '交易系统', count: 0 },
    { id: 'research', label: '研究报告', count: 0 },
    { id: 'portfolio', label: '持仓管理', count: 0 },
    { id: 'training', label: '训练记录', count: 0 },
    { id: 'emotion', label: '情绪管理', count: 0 },
    { id: 'daily-log', label: '每日记录', count: 0 },
    { id: 'kline-patterns', label: 'K线形态', count: 0 },
    { id: 'other', label: '其他', count: 0 },
  ];
  filteredDocs.value.forEach(d => {
    const cat = cats.find(c => c.id === d.category);
    if (cat) cat.count++;
  });
  return cats;
});

const filteredDocs = computed(() => {
  let docs = allDocs.value;
  if (selectedSource.value === 'published') {
    docs = docs.filter(d => d.is_published);
  } else if (selectedSource.value === 'custom') {
    docs = docs.filter(d => !d.is_published);
  }
  if (selectedCategory.value !== 'all') {
    docs = docs.filter(d => d.category === selectedCategory.value);
  }
  return docs;
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
  const lines = md.split('\n');
  let html = '';
  let inList = false;
  let inCode = false;
  for (const line of lines) {
    let l = line.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    if (l.match(/^```.*$/)) { if (!inCode) { html += '<pre>'; inCode = true; } else { html += '</pre>'; inCode = false; } continue; }
    if (inCode) { html += l + '\n'; continue; }
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
  try {
    const res = await getDocsList({ include_published: true });
    allDocs.value = res.data.docs || [];
  } catch (e) {
    console.error('加载文档列表失败', e);
  } finally {
    loading.value = false;
  }
};

const selectDoc = async (doc) => {
  selectedDoc.value = doc;
  docContent.value = '';
  docNotes.value = [];
  selectedDocTags.value = [];
  
  try {
    const res = await getDocContent(doc.id);
    docContent.value = res.data.content || '';
    
    const progress = await getDocProgress(doc.id);
    if (progress.data.last_line > 0 && contentRef.value) {
      nextTick(() => {
        const lineHeight = 20;
        contentRef.value.scrollTop = progress.data.last_line * lineHeight;
      });
    }
    
    const notesRes = await getDocNotes(doc.id);
    docNotes.value = notesRes.data.notes || [];
    
    const tagsRes = await getDocTags(doc.id);
    selectedDocTags.value = tagsRes.data.tags || [];
  } catch (e) {
    console.error('加载文档内容失败', e);
  }
};

const onScroll = async () => {
  if (!selectedDoc.value || !contentRef.value) return;
  const scrollTop = contentRef.value.scrollTop;
  const lineNumber = Math.floor(scrollTop / 20);
  if (lineNumber > 0) {
    try {
      await updateDocProgress(selectedDoc.value.id, { scroll_position: scrollTop, last_line: lineNumber });
    } catch (e) {
      console.error('保存阅读进度失败', e);
    }
  }
};

const handleDelete = async () => {
  if (!selectedDoc.value) return;
  if (!confirm(`确定删除文档 "${selectedDoc.value.title}"？`)) return;
  try {
    await deleteDoc(selectedDoc.value.id);
    allDocs.value = allDocs.value.filter(d => d.id !== selectedDoc.value.id);
    selectedDoc.value = null;
    docContent.value = '';
  } catch (e) {
    alert('删除失败');
  }
};

// Tags Manager
const loadUserTags = async () => {
  try {
    const res = await getUserTags();
    userTags.value = res.data.tags || [];
  } catch (e) {
    console.error('加载标签失败', e);
  }
};

const createTag = async () => {
  if (!newTagName.value.trim()) return;
  try {
    await createUserTag({ tag_name: newTagName.value.trim(), color: newTagColor.value });
    newTagName.value = '';
    await loadUserTags();
  } catch (e) {
    console.error('创建标签失败', e);
  }
};

const deleteTag = async (tagId) => {
  if (!confirm('确定删除此标签？')) return;
  try {
    await deleteUserTag(tagId);
    await loadUserTags();
  } catch (e) {
    console.error('删除标签失败', e);
  }
};

// Notes
const loadAllNotes = async () => {
  loadingNotes.value = true;
  try {
    const res = await getAllNotes({ limit: 50 });
    allNotes.value = res.data.notes || [];
  } catch (e) {
    console.error('加载笔记失败', e);
  } finally {
    loadingNotes.value = false;
  }
};

const openNoteEditor = (note) => {
  editingNote.value = note;
  if (note) {
    noteContent.value = note.note_content;
    noteType.value = note.note_type;
    noteLineNumber.value = note.line_number;
  } else {
    noteContent.value = '';
    noteType.value = 'note';
    noteLineNumber.value = 0;
  }
  showNoteEditor.value = true;
};

const saveNote = async () => {
  if (!selectedDoc.value || !noteContent.value.trim()) return;
  try {
    if (editingNote.value?.id) {
      await updateDocNote(editingNote.value.id, { note_content: noteContent.value, note_type: noteType.value });
    } else {
      await createDocNote(selectedDoc.value.id, { note_content: noteContent.value, note_type: noteType.value, line_number: noteLineNumber.value });
    }
    showNoteEditor.value = false;
    const notesRes = await getDocNotes(selectedDoc.value.id);
    docNotes.value = notesRes.data.notes || [];
    await loadAllNotes();
  } catch (e) {
    console.error('保存笔记失败', e);
  }
};

const deleteNote = async (noteId) => {
  if (!confirm('确定删除此笔记？')) return;
  try {
    await deleteDocNote(noteId);
    if (selectedDoc.value) {
      const notesRes = await getDocNotes(selectedDoc.value.id);
      docNotes.value = notesRes.data.notes || [];
    }
    await loadAllNotes();
  } catch (e) {
    console.error('删除笔记失败', e);
  }
};

// Doc Tags
const openTagSelector = async () => {
  await loadUserTags();
  selectedTagIds.value = selectedDocTags.value.map(t => t.id);
  showTagSelector.value = true;
};

const saveDocTags = async () => {
  if (!selectedDoc.value) return;
  try {
    await setDocTags(selectedDoc.value.id, selectedTagIds.value);
    showTagSelector.value = false;
    const tagsRes = await getDocTags(selectedDoc.value.id);
    selectedDocTags.value = tagsRes.data.tags || [];
  } catch (e) {
    console.error('保存标签失败', e);
  }
};

onMounted(async () => {
  await loadDocs();
  await loadUserTags();
  await loadAllNotes();
});
</script>

<style scoped>
.doc-content :deep(h1) { font-size: 1.25rem; font-weight: 700; color: #f1f5f9; margin: 1rem 0 0.5rem; }
.doc-content :deep(h2) { font-size: 1.1rem; font-weight: 700; color: #e2e8f0; margin: 0.8rem 0 0.4rem; border-bottom: 1px solid #334155; padding-bottom: 0.3rem; }
.doc-content :deep(h3) { font-size: 0.95rem; font-weight: 600; color: #cbd5e1; margin: 0.6rem 0 0.3rem; }
.doc-content :deep(ul) { margin: 0.3rem 0; padding-left: 1.2rem; }
.doc-content :deep(li) { list-style: disc; color: #94a3b8; font-size: 0.8rem; line-height: 1.65; margin: 0.15rem 0; }
.doc-content :deep(code) { background: #1e293b; padding: 0.1rem 0.3rem; border-radius: 4px; font-size: 0.75rem; color: #f59e0b; }
.doc-content :deep(pre) { background: #0f172a; padding: 0.8rem; border-radius: 6px; overflow-x: auto; font-size: 0.72rem; color: #94a3b8; border: 1px solid #1e293b; margin: 0.5rem 0; }
.doc-content :deep(strong) { color: #f1f5f9; }
.doc-content :deep(p) { color: #94a3b8; font-size: 0.8rem; line-height: 1.7; margin: 0.3rem 0; }
.doc-content :deep(.check) { color: #94a3b8; font-size: 0.8rem; line-height: 1.6; margin: 0.2rem 0; }

.fade-enter-active, .fade-leave-active { transition: opacity 0.2s ease; }
.fade-enter-from, .fade-leave-to { opacity: 0; }
.slide-enter-active, .slide-leave-active { transition: transform 0.25s ease; }
.slide-enter-from, .slide-leave-to { transform: translateX(100%); }
</style>