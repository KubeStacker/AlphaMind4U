<template>
  <div class="max-w-5xl mx-auto space-y-6 pb-20 md:pb-0 animate-in fade-in duration-500">
    <div class="flex flex-col md:flex-row md:items-center justify-between gap-4">
      <h1 class="text-xl font-bold text-white tracking-tight">系统管理控制台</h1>
      
      <!-- 紧凑型标签切换 -->
      <div class="flex bg-business-dark p-1 rounded-xl border border-business-light shadow-lg self-start">
        <button v-for="tab in visibleTabs" :key="tab.id" @click="activeTab = tab.id"
          :class="[activeTab === tab.id ? 'bg-business-accent text-white shadow-md' : 'text-slate-500 hover:text-slate-300', 'px-4 py-1.5 rounded-lg text-[11px] font-bold transition-all whitespace-nowrap']">
          {{ tab.name }}
        </button>
      </div>
    </div>

    <!-- 用户管理 -->
    <div v-show="activeTab === 'users'" class="space-y-4">
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
    <div v-show="activeTab === 'data' && authStore.isAdmin" class="space-y-4">
      <div v-for="(report, key) in integrityReports" :key="key" class="bg-business-dark p-5 rounded-2xl border border-business-light shadow-business overflow-hidden">
        <div class="flex flex-col md:flex-row md:items-center justify-between mb-6 gap-4">
          <div class="flex items-center space-x-3">
            <div class="w-1 h-4 bg-business-highlight rounded-full shadow-[0_0_8px_#06b6d4]"></div>
            <h3 class="text-sm font-bold text-white tracking-tight italic">{{ key }} 状态图</h3>
          </div>
          <div class="flex space-x-3 bg-business-darker p-2 rounded-lg border border-business-light self-start md:self-auto shadow-inner">
            <span class="flex items-center text-[8px] font-bold text-slate-400 uppercase"><span class="w-1.5 h-1.5 bg-business-success rounded-full mr-1.5 shadow-[0_0_6px_#f43f5e]"></span> 补全</span>
            <span class="flex items-center text-[8px] font-bold text-slate-400 uppercase"><span class="w-1.5 h-1.5 bg-business-warning rounded-full mr-1.5 shadow-[0_0_6px_#f59e0b]"></span> 部分</span>
            <span class="flex items-center text-[8px] font-bold text-slate-400 uppercase"><span class="w-1.5 h-1.5 bg-business-danger rounded-full mr-1.5 shadow-[0_0_6px_#10b981]"></span> 缺失</span>
          </div>
        </div>
        <v-chart :option="generateChartOption(key, report)" autoresize class="h-40 w-full" />
      </div>
    </div>

    <!-- SQL 终端 -->
    <div v-show="activeTab === 'db' && authStore.isAdmin" class="space-y-4">
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
import { ref, onMounted, watch, reactive, computed } from 'vue';
import { useRoute } from 'vue-router';
import { getIntegrityReport, listUsers, deleteUser, createUser, updatePassword, executeDBQuery } from '@/services/api';
import { useAuthStore } from '@/stores/auth';
import { Dialog, DialogPanel, DialogTitle } from '@headlessui/vue'
import { 
  UserGroupIcon, TrashIcon, KeyIcon
} from '@heroicons/vue/20/solid'

const route = useRoute();
const authStore = useAuthStore();
const activeTab = ref(route.query.tab || 'users');

watch(() => route.query.tab, (newTab) => {
  activeTab.value = newTab || 'users';
});

const users = ref([]);
const integrityReports = ref({});
const queryLoading = ref(false);
const loading = ref(false);

const tabs = [
  { id: 'users', name: '用户管理', admin: false },
  { id: 'data', name: '数据管理', admin: true },
  { id: 'db', name: 'SQL控制台', admin: true }
];

const visibleTabs = computed(() => tabs.filter(tab => !tab.admin || authStore.isAdmin));

const showAddUserModal = ref(false);
const newUser = reactive({ username: '', password: '', role: 'viewer' });
const showPasswordModal = ref(false);
const selectedUser = ref(null);
const newPassword = ref('');
const sqlQuery = ref(`SELECT * FROM daily_price WHERE factors != '{}' LIMIT 10`);
const queryResult = ref(null);

const tableLabels = { 'daily_price': 'Market Data' };

const fetchUsers = async () => {
  try {
    const res = await listUsers();
    users.value = res.data;
  } catch (e) { console.error("API Error", e); }
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
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(today.getFullYear() - 1);
    const response = await getIntegrityReport(oneYearAgo.toISOString().split('T')[0], today.toISOString().split('T')[0]);
    integrityReports.value = response.data;
  } catch (e) {} finally { loading.value = false; }
};

const generateChartOption = (title, data) => {
  if (!data || data.length === 0) return {};
  const statusMap = { 'FULL': 3, 'PARTIAL': 2, 'MISSING': 1, 'HOLIDAY': 0 };
  const seriesData = data.map(item => [item.date, statusMap[item.status] ?? -1, item.count]);
  const dateRange = [seriesData[0][0], seriesData[seriesData.length-1][0]];
  return {
    backgroundColor: 'transparent',
    visualMap: { show: false, min: 0, max: 3, type: 'piecewise', pieces: [{ value: 3, color: '#10b981' }, { value: 2, color: '#f59e0b' }, { value: 1, color: '#f43f5e' }, { value: 0, color: '#1e293b' }] },
    calendar: { top: 15, bottom: 0, left: 20, right: 10, range: dateRange, cellSize: ['auto', 10], splitLine: { show: false }, itemStyle: { borderWidth: 1, borderColor: '#0f172a' }, dayLabel: { show: false }, monthLabel: { nameMap: 'cn', color: '#475569', fontSize: 8 }, yearLabel: { show: false } },
    series: { type: 'heatmap', coordinateSystem: 'calendar', data: seriesData }
  };
};

onMounted(() => { 
  fetchUsers(); 
  fetchDataIntegrity(); 
});
</script>
