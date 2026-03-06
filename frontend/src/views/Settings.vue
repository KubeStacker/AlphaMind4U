<template>
  <div class="max-w-5xl mx-auto space-y-6 pb-20 md:pb-0 animate-in fade-in duration-500">
    <div class="flex flex-col md:flex-row md:items-center justify-between gap-4">
      <h1 class="text-xl font-bold text-white tracking-tight">系统管理控制台</h1>
      
      <!-- 紧凑型标签切换 -->
      <div v-if="visibleTabs.length > 0" class="flex bg-business-dark p-1 rounded-xl border border-business-light shadow-lg self-start">
        <button v-for="tab in visibleTabs" :key="tab.id" @click="handleTabClick(tab.id)"
          :class="[activeTab === tab.id ? 'bg-business-accent text-white shadow-md' : 'text-slate-500 hover:text-slate-300', 'px-4 py-1.5 rounded-lg text-[11px] font-bold transition-all whitespace-nowrap']">
          {{ tab.name }}
        </button>
      </div>
    </div>

    <div v-if="visibleTabs.length === 0" class="bg-business-dark rounded-2xl border border-business-light p-6 text-center text-slate-400 text-sm">
      当前账号无控制台访问权限
    </div>

    <!-- 用户管理 -->
    <div v-if="activeTab === 'users'" class="space-y-4">
      <div class="bg-business-dark rounded-2xl border border-business-light overflow-hidden shadow-business">
        <div class="p-4 border-b border-business-light flex justify-between items-center bg-slate-900/30">
          <div class="flex items-center space-x-2">
            <UserGroupIcon class="w-4 h-4 text-business-accent" />
            <h2 class="text-sm font-bold text-white">用户访问控制</h2>
          </div>
          <button @click="showAddUserModal = true" class="h-8 px-4 bg-business-accent text-white rounded-lg text-[10px] font-bold uppercase transition-all active:scale-95 shadow-lg">
            新建节点
          </button>
        </div>

        <div class="overflow-x-auto">
          <table class="w-full text-left border-collapse">
            <thead class="bg-business-darker/50 text-slate-500 text-[9px] font-bold uppercase tracking-wider">
              <tr>
                <th class="px-5 py-3">节点标识</th>
                <th class="px-5 py-3">权限等级</th>
                <th class="hidden md:table-cell px-5 py-3">创建时间</th>
                <th class="px-5 py-3 text-right">管理</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-business-light/20">
              <tr v-for="user in users" :key="user.id" class="hover:bg-white/5 transition-colors">
                <td class="px-5 py-3 font-bold text-white text-sm">{{ user.username }}</td>
                <td class="px-5 py-3">
                  <span :class="[user.role === 'admin' ? 'text-business-highlight border-business-highlight/30' : 'text-business-accent border-business-accent/30', 'px-2 py-0.5 rounded border text-[9px] font-bold']">
                    {{ user.role === 'admin' ? '超级管理员' : '标准用户' }}
                  </span>
                </td>
                <td class="hidden md:table-cell px-5 py-3 text-xs text-slate-500 font-medium">
                  {{ new Date(user.created_at).toLocaleDateString() }}
                </td>
                <td class="px-5 py-3 text-right">
                  <div class="flex justify-end space-x-2">
                    <button @click="openPasswordModal(user)" class="p-1.5 text-slate-500 hover:text-white" title="重置"><KeyIcon class="w-4 h-4" /></button>
                    <button @click="handleDeleteUser(user.id)" class="p-1.5 text-slate-500 hover:text-business-success" title="注销"><TrashIcon class="w-4 h-4" /></button>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
          <div v-if="users.length === 0" class="p-10 text-center text-slate-600 text-xs italic">
            正在获取节点列表...
          </div>
        </div>
      </div>
    </div>

    <!-- 数据同步 -->
    <div v-if="activeTab === 'data' && authStore.isAdmin" class="space-y-6">
      <!-- 任务执行监控 -->
      <div v-if="tasksStatus.current_task || tasksStatus.history.length > 0" class="bg-business-dark p-4 rounded-2xl border border-business-light shadow-business">
        <div class="flex items-center justify-between mb-4">
           <div class="flex items-center space-x-2">
              <div class="w-1 h-3 bg-business-accent rounded-full"></div>
              <h3 class="text-[11px] font-bold text-slate-400 uppercase tracking-widest">后台任务中心</h3>
           </div>
           <div class="text-[9px] font-bold text-slate-500 bg-slate-900 px-2 py-0.5 rounded border border-white/5">
              等待队列: {{ tasksStatus.queue_size }}
           </div>
        </div>

        <div class="space-y-2 max-h-[200px] overflow-y-auto custom-scrollbar pr-1">
           <!-- 当前任务 -->
           <div v-if="tasksStatus.current_task" class="flex items-center justify-between p-2 bg-business-accent/5 rounded-lg border border-business-accent/20 animate-pulse">
              <div class="flex items-center space-x-3">
                 <PlayIcon class="w-3 h-3 text-business-accent" />
                 <span class="text-[10px] font-bold text-white">{{ tasksStatus.current_task.name }}</span>
              </div>
              <span class="text-[8px] font-black text-business-accent uppercase tracking-tighter">执行中...</span>
           </div>

           <!-- 历史任务 -->
           <div v-for="(h, idx) in tasksStatus.history" :key="idx" class="flex items-center justify-between p-2 bg-slate-900/40 rounded-lg border border-white/5">
              <div class="flex items-center space-x-3">
                 <CheckCircleIcon v-if="h.status === 'COMPLETED'" class="w-3 h-3 text-business-success" />
                 <XCircleIcon v-else-if="h.status === 'FAILED'" class="w-3 h-3 text-business-danger" />
                 <ClockIcon v-else class="w-3 h-3 text-slate-500" />
                 <span class="text-[10px] font-medium text-slate-300">{{ h.name }}</span>
              </div>
              <div class="flex items-center space-x-2">
                 <span v-if="h.error" class="text-[8px] text-business-danger truncate max-w-[100px]">{{ h.error }}</span>
                 <span class="text-[8px] text-slate-500 font-mono">{{ h.finished_at ? new Date(h.finished_at * 1000).toLocaleTimeString() : '' }}</span>
              </div>
           </div>
        </div>
      </div>

      <!-- 财务数据表统计与同步 -->
      <div class="bg-business-dark p-5 rounded-2xl border border-business-light shadow-business">
        <div class="flex items-center justify-between mb-4">
          <div class="flex items-center space-x-3">
            <div class="w-1 h-4 bg-business-warning rounded-full shadow-[0_0_8px_#f59e0b]"></div>
            <h3 class="text-sm font-bold text-white tracking-tight">财务数据表</h3>
          </div>
          <button @click="fetchTableStats()" :disabled="statsLoading" class="px-3 py-1 bg-slate-800 hover:bg-business-accent text-slate-300 hover:text-white rounded-lg text-[9px] font-bold border border-business-light transition-all flex items-center space-x-1">
            <ArrowPathIcon :class="{'animate-spin': statsLoading}" class="w-3 h-3" />
            <span>刷新</span>
          </button>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <!-- 财务指标 -->
          <div class="bg-business-darker/50 p-4 rounded-xl border border-business-light/30">
            <div class="flex items-center justify-between mb-3">
              <div>
                <h4 class="text-xs font-bold text-white">财务指标</h4>
                <p class="text-[9px] text-slate-500">stock_fina_indicator</p>
              </div>
              <span class="text-lg font-bold text-business-success">{{ tableStats['stock_fina_indicator'] || 0 }}</span>
            </div>
            <button @click="handleSyncFinaIndicator()" :disabled="taskLoading.fina_indicator" class="w-full px-3 py-1.5 bg-business-success/10 hover:bg-business-success text-business-success hover:text-white rounded-lg text-[9px] font-bold border border-business-success/30 transition-all flex items-center justify-center space-x-1">
              <ArrowPathIcon :class="{'animate-spin': taskLoading.fina_indicator}" class="w-3 h-3" />
              <span>{{ taskLoading.fina_indicator ? '同步中...' : '同步数据' }}</span>
            </button>
          </div>

          <!-- 季度利润表 -->
          <div class="bg-business-darker/50 p-4 rounded-xl border border-business-light/30">
            <div class="flex items-center justify-between mb-3">
              <div>
                <h4 class="text-xs font-bold text-white">季度利润表</h4>
                <p class="text-[9px] text-slate-500">stock_income</p>
              </div>
              <span class="text-lg font-bold text-business-danger">{{ tableStats['stock_income'] || 0 }}</span>
            </div>
            <button @click="handleSyncQuarterlyIncome()" :disabled="taskLoading.quarterly_income" class="w-full px-3 py-1.5 bg-business-danger/10 hover:bg-business-danger text-business-danger hover:text-white rounded-lg text-[9px] font-bold border border-business-danger/30 transition-all flex items-center justify-center space-x-1">
              <ArrowPathIcon :class="{'animate-spin': taskLoading.quarterly_income}" class="w-3 h-3" />
              <span>{{ taskLoading.quarterly_income ? '同步中...' : '同步数据' }}</span>
            </button>
          </div>
        </div>
      </div>

      <!-- 数据准确性校验 -->
      <div class="bg-business-dark p-5 rounded-2xl border border-business-light shadow-business">
        <div class="flex flex-col md:flex-row md:items-center justify-between mb-4 gap-3">
          <div class="flex items-center space-x-3">
            <div class="w-1 h-4 bg-purple-500 rounded-full shadow-[0_0_8px_#a855f7]"></div>
            <h3 class="text-sm font-bold text-white tracking-tight">数据准确性校验</h3>
          </div>
          <div class="flex items-center gap-2">
            <input v-model="verifyTsCode" type="text" placeholder="股票代码" class="w-24 bg-business-darker border border-business-light rounded-lg px-2 py-1.5 text-xs font-bold text-white outline-none focus:border-purple-500 uppercase" />
            <button @click="handleVerifyData" :disabled="verifyLoading" class="px-3 py-1.5 bg-purple-600 hover:bg-purple-500 text-white rounded-lg text-[10px] font-bold transition-all flex items-center space-x-1">
              <ArrowPathIcon :class="{'animate-spin': verifyLoading}" class="w-3 h-3" />
              <span>{{ verifyLoading ? '校验中...' : '校验' }}</span>
            </button>
          </div>
        </div>
        
        <div v-if="verifyResult" class="grid grid-cols-2 md:grid-cols-5 gap-3">
          <div v-for="(val, key) in verifyResult" :key="key" class="bg-slate-900/50 p-3 rounded-xl border border-business-light/30">
            <div class="text-[9px] text-slate-500 font-bold uppercase mb-1">{{ key }}</div>
            <div class="flex items-center justify-between">
              <span class="text-xs font-bold text-slate-300">{{ val.api || 0 }} / {{ val.db || 0 }}</span>
              <span v-if="val.match !== undefined" :class="val.match ? 'text-green-400' : 'text-red-400'" class="text-[10px] font-bold">
                {{ val.match ? '✓' : '✗' }}
              </span>
            </div>
          </div>
        </div>
      </div>

      <!-- 数据表完整性日历 -->
      <div v-for="(report, key) in integrityReports" :key="key" class="bg-business-dark p-5 rounded-2xl border border-business-light shadow-business overflow-hidden">
        <div class="flex flex-col md:flex-row md:items-center justify-between mb-6 gap-4">
          <div class="flex items-center space-x-3">
            <div class="w-1 h-4 bg-business-highlight rounded-full shadow-[0_0_8px_#06b6d4]"></div>
            <h3 class="text-sm font-bold text-white tracking-tight italic">{{ tableConfigs[key]?.label || key }}</h3>
            
            <!-- 针对特定表的同步按钮 -->
            <button 
              v-if="tableConfigs[key]"
              @click="handleRunTableTask(key)"
              :disabled="taskLoading[key]"
              class="ml-4 px-3 py-1 bg-business-accent/10 hover:bg-business-accent text-business-accent hover:text-white rounded-md text-[9px] font-bold border border-business-accent/30 transition-all flex items-center space-x-1"
            >
              <ArrowPathIcon :class="{'animate-spin': taskLoading[key]}" class="w-3 h-3" />
              <span>{{ taskLoading[key] ? '执行中' : tableConfigs[key].batchLabel }}</span>
            </button>
          </div>
          <div class="flex space-x-3 bg-business-darker p-2 rounded-lg border border-business-light self-start md:self-auto shadow-inner">
            <span class="flex items-center text-[8px] font-bold text-slate-400 uppercase"><span class="w-1.5 h-1.5 bg-business-danger rounded-full mr-1.5 shadow-[0_0_6px_#10b981]"></span> 完整</span>
            <span class="flex items-center text-[8px] font-bold text-slate-400 uppercase"><span class="w-1.5 h-1.5 bg-business-warning rounded-full mr-1.5 shadow-[0_0_6px_#f59e0b]"></span> 部分</span>
            <span class="flex items-center text-[8px] font-bold text-slate-400 uppercase"><span class="w-1.5 h-1.5 bg-business-success rounded-full mr-1.5 shadow-[0_0_6px_#f43f5e]"></span> 缺失</span>
          </div>
        </div>
        <v-chart :option="chartOptions[key]" autoresize class="h-40 w-full" @click="(p) => handleDataMapClick(p, key)" />
      </div>
    </div>

    <!-- SQL 终端 -->
    <div v-if="activeTab === 'db' && authStore.isAdmin" class="space-y-4">
      <div class="bg-business-dark p-5 rounded-2xl border border-business-light shadow-business">
        <textarea v-model="sqlQuery" rows="4" class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-3 text-xs font-mono text-business-highlight focus:outline-none focus:border-business-accent transition-all" placeholder="输入 SQL 指令..."></textarea>
        <div class="flex justify-between items-center mt-4">
          <p class="text-[9px] font-bold text-slate-600 uppercase tracking-widest">Safe Read-Only Console</p>
          <button @click="runQuery" :disabled="queryLoading" class="h-9 px-6 bg-business-accent text-white rounded-lg text-[10px] font-bold uppercase transition-all active:scale-95 shadow-lg">
            执行指令
          </button>
        </div>
      </div>

      <div v-if="queryResult" class="bg-business-dark rounded-2xl border border-business-light overflow-hidden shadow-business">
        <div class="overflow-x-auto max-h-[400px]">
          <table class="w-full text-left border-collapse">
            <thead class="bg-slate-900 text-slate-500 text-[9px] font-bold uppercase sticky top-0 z-10">
              <tr><th v-for="col in queryResult.columns" :key="col" class="px-4 py-3 border-b border-business-light">{{ col }}</th></tr>
            </thead>
            <tbody class="divide-y divide-business-light/20">
              <tr v-for="(row, idx) in queryResult.data" :key="idx" class="hover:bg-white/5">
                <td v-for="col in queryResult.columns" :key="col" class="px-4 py-2.5 text-[10px] font-medium text-slate-400 whitespace-nowrap">{{ row[col] }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Modals (紧凑型) -->
    <Dialog :open="showAddUserModal" @close="showAddUserModal = false" class="relative z-50">
      <div class="fixed inset-0 bg-business-darker/80 backdrop-blur-sm" />
      <div class="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel class="w-full max-w-sm rounded-2xl bg-business-dark border border-business-light p-6 shadow-2xl">
          <DialogTitle class="text-lg font-bold text-white mb-6 tracking-tight">新增用户节点</DialogTitle>
          <form @submit.prevent="handleAddUser" class="space-y-4">
            <div class="space-y-1">
              <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1">用户名标识</label>
              <input v-model="newUser.username" type="text" required class="w-full h-10 bg-business-darker border border-business-light rounded-lg px-4 text-white text-sm focus:outline-none focus:border-business-accent transition-all" />
            </div>
            <div class="space-y-1">
              <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1">访问凭证</label>
              <input v-model="newUser.password" type="password" required class="w-full h-10 bg-business-darker border border-business-light rounded-lg px-4 text-white text-sm focus:outline-none focus:border-business-accent transition-all" />
            </div>
            <div class="space-y-1">
              <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1">权限协议</label>
              <select v-model="newUser.role" class="w-full h-10 bg-business-darker border border-business-light rounded-lg px-4 text-white text-xs outline-none">
                <option value="viewer">VIEWER (只读)</option>
                <option value="admin">ADMIN (最高权限)</option>
              </select>
            </div>
            <div class="flex justify-end gap-3 mt-8">
              <button type="button" @click="showAddUserModal = false" class="text-xs font-bold text-slate-500 px-4">取消</button>
              <button type="submit" class="h-10 bg-business-accent text-white px-6 rounded-lg font-bold text-[11px] shadow-lg">确认部署</button>
            </div>
          </form>
        </DialogPanel>
      </div>
    </Dialog>

    <Dialog :open="showPasswordModal" @close="showPasswordModal = false" class="relative z-50">
      <div class="fixed inset-0 bg-business-darker/80 backdrop-blur-sm" />
      <div class="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel class="w-full max-w-sm rounded-2xl bg-business-dark border border-business-light p-6 shadow-2xl">
          <DialogTitle class="text-lg font-bold text-white mb-2">更新凭证</DialogTitle>
          <p class="text-slate-500 text-[10px] font-bold uppercase mb-6 tracking-widest">目标: {{ selectedUser?.username }}</p>
          <form @submit.prevent="handleChangePassword" class="space-y-4">
            <input v-model="newPassword" type="password" required minlength="6" class="w-full h-10 bg-business-darker border border-business-light rounded-lg px-4 text-white text-sm focus:outline-none focus:border-business-accent transition-all" placeholder="输入新密码" />
            <div class="flex justify-end gap-3 mt-8">
              <button type="button" @click="showPasswordModal = false" class="text-xs font-bold text-slate-500 px-4">取消</button>
              <button type="submit" class="h-10 bg-business-accent text-white px-6 rounded-lg font-bold text-[11px] shadow-lg">提交更新</button>
            </div>
          </form>
        </DialogPanel>
      </div>
    </Dialog>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch, reactive, computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { getIntegrityReport, listUsers, deleteUser, createUser, updatePassword, executeDBQuery, syncDailyDate, syncFinancials, syncMoneyflow, syncMarketIndex, syncMargin, getTasksStatus, getTableStats, syncFinaIndicator, syncQuarterlyIncome, verifyDataAccuracy } from '@/services/api';
import { useAuthStore } from '@/stores/auth';
import { Dialog, DialogPanel, DialogTitle } from '@headlessui/vue'
import { 
  UserGroupIcon, TrashIcon, KeyIcon, ArrowPathIcon, ChartBarIcon, ArrowUpTrayIcon,
  CheckCircleIcon, XCircleIcon, ClockIcon, PlayIcon
} from '@heroicons/vue/20/solid'

const route = useRoute();
const router = useRouter();
const authStore = useAuthStore();
const tabs = [
  { id: 'users', name: '用户管理', admin: true },
  { id: 'data', name: '数据管理', admin: true },
  { id: 'db', name: 'SQL控制台', admin: true }
];
const visibleTabs = computed(() => tabs.filter(tab => !tab.admin || authStore.isAdmin));

const resolveActiveTab = (requestedTab = null) => {
  const requested = requestedTab || route.query.tab;
  const ids = visibleTabs.value.map(t => t.id);
  if (requested && ids.includes(requested)) return requested;
  return ids[0] || '';
};

const activeTab = ref(resolveActiveTab());

watch(() => route.query.tab, (newTab) => {
  const resolved = resolveActiveTab(newTab);
  if (activeTab.value !== resolved) {
    activeTab.value = resolved;
  }
});

watch(activeTab, (newTab) => {
  if (route.query.tab !== newTab) {
    const nextQuery = { ...route.query, tab: newTab };
    router.replace({ query: nextQuery });
  }
  if (newTab === 'data') {
    fetchTasksStatus();
    fetchTableStats();
  } else if (newTab === 'db') {
    fetchDataIntegrity();
  }
});

watch(visibleTabs, () => {
  if (!visibleTabs.value.some(t => t.id === activeTab.value)) {
    activeTab.value = resolveActiveTab();
  }
});

const handleTabClick = (tabId) => {
  if (!visibleTabs.value.some((t) => t.id === tabId)) return;
  activeTab.value = tabId;
};

const users = ref([]);
const integrityReports = ref({});
const queryLoading = ref(false);
const loading = ref(false);
const taskLoading = reactive({});
const tasksStatus = ref({ current_task: null, history: [], queue_size: 0 });
const statsLoading = ref(false);
const tableStats = ref({});
const verifyLoading = ref(false);
const verifyResult = ref(null);
const verifyTsCode = ref('688256.SH');
let statusTimer = null;

const fetchTasksStatus = async () => {
  if (activeTab.value !== 'data') return;
  try {
    const res = await getTasksStatus();
    tasksStatus.value = res.data;
  } catch (e) {}
};

const handleVerifyData = async () => {
  verifyLoading.value = true;
  try {
    const res = await verifyDataAccuracy(verifyTsCode.value);
    if (res.data.status === 'success') {
      verifyResult.value = res.data.data;
    }
  } catch (e) { console.error('验证失败', e); }
  finally { verifyLoading.value = false; }
};

onMounted(() => { 
  if (authStore.isAdmin) {
    fetchUsers();
    if (activeTab.value === 'data') {
      fetchTableStats();
      fetchTasksStatus();
    } else if (activeTab.value === 'db') {
      fetchDataIntegrity();
    }
  }
  statusTimer = setInterval(fetchTasksStatus, 3000);
});

onUnmounted(() => {
  if (statusTimer) clearInterval(statusTimer);
});

// 表名映射到可读名称及同步任务
const tableConfigs = {
  'daily_price': { 
    label: '股票行情数据 (daily_price)', 
    action: (date) => syncDailyDate(date),
    batchAction: () => syncDailyDate(null), 
    batchLabel: '同步日线行情'
  },
  'stock_moneyflow': { 
    label: '资金流向数据 (stock_moneyflow)', 
    action: (date) => syncMoneyflow(1), 
    batchAction: () => syncMoneyflow(1),
    batchLabel: '同步资金流数据'
  },
  'market_index': { 
    label: '指数行情数据 (market_index)', 
    action: (date) => syncMarketIndex('000001.SH', 1),
    batchAction: () => syncMarketIndex('000001.SH', 1),
    batchLabel: '同步上证指数'
  },
  'stock_margin': { 
    label: '融资融券数据 (stock_margin)', 
    action: (date) => syncMargin(30),
    batchAction: () => syncMargin(90),
    batchLabel: '同步融资融券'
  }
};

// 优化：将图表配置计算移出模板，防止响应式更新导致的闪烁
const chartOptions = computed(() => {
  const options = {};
  for (const key in integrityReports.value) {
    options[key] = generateChartOption(key, integrityReports.value[key]);
  }
  return options;
});

const handleRunTableTask = async (tableName) => {
  const config = tableConfigs[tableName];
  if (!config || !config.batchAction) return;
  
  if (!confirm(`确认启动 [${config.label}] 的同步任务？`)) return;
  
  taskLoading[tableName] = true;
  try {
    const res = await config.batchAction();
    alert(res.data.message || "任务已启动");
  } catch (e) {
    alert("启动失败: " + (e.response?.data?.detail || e.message));
  } finally {
    taskLoading[tableName] = false;
  }
};

const showAddUserModal = ref(false);
const newUser = reactive({ username: '', password: '', role: 'viewer' });
const showPasswordModal = ref(false);
const selectedUser = ref(null);
const newPassword = ref('');
const sqlQuery = ref(`SELECT * FROM daily_price WHERE factors != '{}' LIMIT 10`);
const queryResult = ref(null);

const fetchUsers = async () => {
  try {
    const res = await listUsers();
    users.value = res.data;
  } catch (e) { console.error("API Error", e); }
};

const handleDataMapClick = async (params, tableName) => {
  if (!params.data || !params.data[0]) return;
  const dateStr = params.data[0];
  const statusVal = params.data[1];
  const count = params.data[2];
  
  const statusNames = { 0: '休市', 1: '缺失', 2: '部分', 3: '完整' };
  const statusName = statusNames[statusVal] || '未知';
  const config = tableConfigs[tableName];

  if (confirm(`日期: ${dateStr}\n表名: ${tableName}\n当前状态: ${statusName} (${count} 条)\n是否触发该日数据补全任务？`)) {
    try {
      // 针对行情数据，我们可以精确补全某一天
      if (tableName === 'daily_price') {
        await syncDailyDate(dateStr);
      } else {
        // 其他表目前可能只支持范围同步，提示用户
        await config.action(dateStr);
      }
      alert(`${dateStr} 相关同步任务已启动。`);
    } catch (e) {
      alert("启动失败: " + (e.response?.data?.detail || e.message));
    }
  }
};

const handleAddUser = async () => {
  try {
    await createUser(newUser);
    showAddUserModal.value = false;
    Object.assign(newUser, { username: '', password: '', role: 'viewer' });
    await fetchUsers();
  } catch (e) { alert("创建失败"); }
};

const openPasswordModal = (user) => {
  selectedUser.value = user;
  newPassword.value = '';
  showPasswordModal.value = true;
};

const handleChangePassword = async () => {
  try {
    await updatePassword(selectedUser.value.id, newPassword.value);
    showPasswordModal.value = false;
    alert("密码已更新");
  } catch (e) { alert("更新失败"); }
};

const handleDeleteUser = async (id) => {
  if (id === authStore.user?.id) return alert("严禁注销当前节点");
  if (!confirm('确认永久注销该节点？')) return;
  try {
    await deleteUser(id);
    await fetchUsers();
  } catch (e) { alert("注销失败"); }
};

const runQuery = async () => {
  if (!sqlQuery.value.trim()) return;
  queryLoading.value = true;
  try {
    const res = await executeDBQuery(sqlQuery.value);
    queryResult.value = res.data;
  } catch (e) { alert("执行错误"); } finally { queryLoading.value = false; }
};

const fetchDataIntegrity = async () => {
  if (!authStore.isAdmin) return;
  loading.value = true;
  try {
    const today = new Date();
    const threeMonthsAgo = new Date();
    threeMonthsAgo.setMonth(today.getMonth() - 3);
    const response = await getIntegrityReport(threeMonthsAgo.toISOString().split('T')[0], today.toISOString().split('T')[0]);
    integrityReports.value = response.data;
  } catch (e) {} finally { loading.value = false; }
};

const fetchTableStats = async () => {
  statsLoading.value = true;
  try {
    const res = await getTableStats();
    const stats = {};
    res.data.data.forEach(row => {
      stats[row.tbl] = row.cnt;
    });
    tableStats.value = stats;
  } catch (e) { console.error('Failed to fetch table stats', e); }
  finally { statsLoading.value = false; }
};

const handleSyncFinaIndicator = async () => {
  if (!confirm('确认同步财务指标数据？')) return;
  taskLoading.fina_indicator = true;
  try {
    const res = await syncFinaIndicator(500);
    alert(res.data.message || '同步已启动');
    setTimeout(fetchTableStats, 2000);
  } catch (e) { alert('同步失败: ' + (e.response?.data?.detail || e.message)); }
  finally { taskLoading.fina_indicator = false; }
};

const handleSyncQuarterlyIncome = async () => {
  if (!confirm('确认同步季度利润表数据？')) return;
  taskLoading.quarterly_income = true;
  try {
    const res = await syncQuarterlyIncome(null, 2020);
    alert(res.data.message || '同步已启动');
    setTimeout(fetchTableStats, 2000);
  } catch (e) { alert('同步失败: ' + (e.response?.data?.detail || e.message)); }
  finally { taskLoading.quarterly_income = false; }
};

const generateChartOption = (title, data) => {
  if (!data || data.length === 0) return {};
  const statusMap = { 'FULL': 3, 'PARTIAL': 2, 'MISSING': 1, 'HOLIDAY': 0 };
  const seriesData = data.map(item => [item.date, statusMap[item.status] ?? -1, item.count]);
  const dateRange = [seriesData[0][0], seriesData[seriesData.length-1][0]];
  return {
    backgroundColor: 'transparent',
    tooltip: {
      formatter: (p) => {
        const val = p.data;
        return `<div class="p-2 font-mono text-[10px]"><div class="font-bold border-b border-slate-700 mb-1 pb-1">${val[0]}</div>数据量: <span class="text-business-highlight">${val[2]}</span></div>`;
      }
    },
    visualMap: { 
      show: false, 
      min: 0, 
      max: 3, 
      dimension: 1, 
      type: 'piecewise', 
      pieces: [
        { value: 3, color: '#10b981' }, // FULL -> 绿
        { value: 2, color: '#f59e0b' }, // PARTIAL -> 黄
        { value: 1, color: '#f43f5e' }, // MISSING -> 红
        { value: 0, color: '#1e293b' }  // HOLIDAY -> 深色
      ] 
    },
    calendar: { 
      top: 25, 
      bottom: 5, 
      left: 25, 
      right: 10, 
      range: dateRange, 
      cellSize: ['auto', 15], // 增加高度到 15px
      splitLine: { show: false }, 
      itemStyle: { borderWidth: 2, borderColor: '#0f172a' }, 
      dayLabel: { show: false }, 
      monthLabel: { nameMap: 'cn', color: '#64748b', fontSize: 9, margin: 10 }, 
      yearLabel: { show: false } 
    },
    series: [
      { 
        type: 'heatmap', 
        coordinateSystem: 'calendar', 
        data: seriesData,
        label: {
          show: true,
          formatter: (p) => p.data[1] === 0 ? 'X' : '', // 状态为0（休市）时显示 X
          color: '#cbd5e1', // 高亮白灰色
          fontSize: 10,
          fontWeight: 'bold'
        },
        emphasis: {
          itemStyle: {
            borderWidth: 2,
            borderColor: '#3b82f6'
          }
        }
      }
    ]
  };
};

</script>
