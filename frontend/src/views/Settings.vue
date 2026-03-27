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
    <div v-if="activeTab === 'data' && authStore.isAdmin" class="space-y-5">

      <!-- 顶部状态栏：一句话告诉你数据是否正常 -->
      <div class="bg-business-dark p-4 rounded-2xl border border-business-light shadow-business">
        <div class="flex flex-col md:flex-row md:items-center justify-between gap-3">
          <div class="flex items-center gap-3">
            <div :class="overallHealth === 'good' ? 'bg-green-500' : overallHealth === 'warn' ? 'bg-yellow-500' : 'bg-red-500'" class="w-2.5 h-2.5 rounded-full shadow-lg"></div>
            <span class="text-xs font-bold text-white">{{ healthSummary }}</span>
          </div>
          <div class="flex items-center gap-2">
            <button @click="refreshAll" :disabled="refreshing" class="px-3 py-1.5 bg-slate-800 hover:bg-business-accent text-slate-300 hover:text-white rounded-lg text-[9px] font-bold border border-business-light transition-all flex items-center gap-1">
              <ArrowPathIcon :class="{'animate-spin': refreshing}" class="w-3 h-3" />
              <span>刷新</span>
            </button>
          </div>
        </div>
      </div>

      <!-- 正在运行的任务（只显示运行中，不显示历史） -->
      <div v-if="runningTasks.length > 0" class="bg-business-accent/5 p-3 rounded-xl border border-business-accent/20 animate-pulse">
        <div v-for="task in runningTasks" :key="task.task_id" class="flex items-center justify-between">
          <div class="flex items-center gap-2">
            <PlayIcon class="w-3 h-3 text-business-accent" />
            <span class="text-[10px] font-bold text-white">{{ task.task_type }}</span>
            <span v-if="task.progress > 0" class="text-[9px] text-business-accent">{{ Math.round(task.progress) }}%</span>
          </div>
          <span class="text-[8px] text-slate-400">执行中</span>
        </div>
      </div>

      <!-- 核心数据卡片（4个日频表） -->
      <div class="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div v-for="table in coreTables" :key="table.key" class="bg-business-dark p-4 rounded-xl border border-business-light shadow-business">
          <div class="flex items-center justify-between mb-2">
            <span class="text-[10px] font-bold text-slate-400">{{ table.label }}</span>
            <span :class="table.statusColor" class="w-2 h-2 rounded-full"></span>
          </div>
          <div class="text-sm font-bold text-white mb-1">{{ table.countStr }}</div>
          <div class="text-[9px] text-slate-500 font-mono mb-3">{{ table.dateRange }}</div>
          <!-- 90天完整性迷你条 -->
          <div class="flex gap-px h-1.5 rounded-full overflow-hidden mb-2">
            <div v-for="(day, i) in table.miniBar" :key="i" 
                 :class="day === 'full' ? 'bg-green-500' : day === 'partial' ? 'bg-yellow-500' : day === 'missing' ? 'bg-red-500' : 'bg-slate-800'"
                 class="flex-1 rounded-sm"></div>
          </div>
          <div class="flex items-center justify-between">
            <span class="text-[8px] text-slate-500">{{ table.completeness }}% 完整</span>
            <button @click="handleRunTableTask(table.key)" :disabled="taskLoading[table.key]"
              class="text-[8px] font-bold text-business-accent hover:text-white transition-colors">
              {{ taskLoading[table.key] ? '同步中' : '同步' }}
            </button>
          </div>
        </div>
      </div>

      <!-- 可展开区域 -->
      <div class="space-y-3">
        <!-- 热力图（折叠） -->
        <div class="bg-business-dark rounded-xl border border-business-light overflow-hidden">
          <button @click="showHeatmap = !showHeatmap" class="w-full flex items-center justify-between p-4 hover:bg-white/5 transition-colors">
            <div class="flex items-center gap-2">
              <div class="w-1 h-3 bg-business-warning rounded-full"></div>
              <span class="text-[11px] font-bold text-white">完整性日历</span>
              <span class="text-[9px] text-slate-500">近90天详情，点击日期补数据</span>
            </div>
            <svg :class="showHeatmap ? 'rotate-180' : ''" class="w-4 h-4 text-slate-500 transition-transform" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
          </button>
          <div v-if="showHeatmap" class="border-t border-business-light p-4 space-y-4">
            <div v-for="(report, key) in integrityDailyData" :key="key">
              <div class="flex items-center justify-between mb-1">
                <span class="text-[9px] font-bold text-slate-400">{{ getTableLabel(key) }}</span>
              </div>
              <v-chart v-if="chartOptions[key]" :option="chartOptions[key]" autoresize class="h-28 w-full" @click="(p) => handleDataMapClick(p, key)" />
            </div>
          </div>
        </div>

        <!-- 数据校验（折叠） -->
        <div class="bg-business-dark rounded-xl border border-business-light overflow-hidden">
          <button @click="showVerify = !showVerify" class="w-full flex items-center justify-between p-4 hover:bg-white/5 transition-colors">
            <div class="flex items-center gap-2">
              <div class="w-1 h-3 bg-purple-500 rounded-full"></div>
              <span class="text-[11px] font-bold text-white">数据校验</span>
              <span class="text-[9px] text-slate-500">API返回 vs DB记录对比</span>
            </div>
            <svg :class="showVerify ? 'rotate-180' : ''" class="w-4 h-4 text-slate-500 transition-transform" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
          </button>
          <div v-if="showVerify" class="border-t border-business-light p-4">
            <div class="flex items-center gap-2 mb-3">
              <input v-model="verifyTsCode" type="text" placeholder="股票代码" class="w-28 bg-business-darker border border-business-light rounded-lg px-2 py-1.5 text-xs font-bold text-white outline-none focus:border-purple-500 uppercase" />
              <button @click="handleVerifyData" :disabled="verifyLoading" class="px-3 py-1.5 bg-purple-600 hover:bg-purple-500 text-white rounded-lg text-[10px] font-bold transition-all flex items-center space-x-1">
                <ArrowPathIcon :class="{'animate-spin': verifyLoading}" class="w-3 h-3" />
                <span>{{ verifyLoading ? '校验中' : '校验' }}</span>
              </button>
            </div>
            <div v-if="verifyResult" class="grid grid-cols-2 md:grid-cols-5 gap-2">
              <div v-for="(val, key) in verifyResult" :key="key" class="bg-slate-900/50 p-2.5 rounded-lg border" :class="val.error ? 'border-red-500/30' : (val.match ? 'border-green-500/30' : 'border-red-500/30')">
                <div class="text-[8px] text-slate-500 font-bold uppercase mb-1">{{ key }}</div>
                <div v-if="val.error" class="text-[8px] text-red-400 truncate">{{ val.error.slice(0, 30) }}</div>
                <div v-else class="flex items-center justify-between">
                  <span class="text-[10px] font-bold text-slate-300">{{ val.api || 0 }}/{{ val.db || 0 }}</span>
                  <span :class="val.match ? 'text-green-400' : 'text-red-400'" class="text-[9px] font-bold">{{ val.match ? '一致' : '不一致' }}</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- 财务数据（折叠） -->
        <div class="bg-business-dark rounded-xl border border-business-light overflow-hidden">
          <button @click="showFinancial = !showFinancial" class="w-full flex items-center justify-between p-4 hover:bg-white/5 transition-colors">
            <div class="flex items-center gap-2">
              <div class="w-1 h-3 bg-business-success rounded-full"></div>
              <span class="text-[11px] font-bold text-white">财务数据</span>
              <span class="text-[9px] text-slate-500">低频数据，非每日同步</span>
            </div>
            <svg :class="showFinancial ? 'rotate-180' : ''" class="w-4 h-4 text-slate-500 transition-transform" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
          </button>
          <div v-if="showFinancial" class="border-t border-business-light p-4 grid grid-cols-1 md:grid-cols-2 gap-3">
            <div class="bg-slate-900/50 p-3 rounded-lg border border-business-light/30">
              <div class="flex items-center justify-between mb-2">
                <span class="text-[10px] font-bold text-white">财务指标</span>
                <span class="text-sm font-bold text-slate-300">{{ formatNumber(dashboard?.tables?.stock_fina_indicator?.count || 0) }}</span>
              </div>
              <button @click="handleSyncFinaIndicator()" :disabled="taskLoading.fina_indicator" class="w-full py-1 bg-business-success/10 hover:bg-business-success text-business-success hover:text-white rounded text-[8px] font-bold border border-business-success/20 transition-all">
                {{ taskLoading.fina_indicator ? '同步中' : '同步' }}
              </button>
            </div>
            <div class="bg-slate-900/50 p-3 rounded-lg border border-business-light/30">
              <div class="flex items-center justify-between mb-2">
                <span class="text-[10px] font-bold text-white">季度利润表</span>
                <span class="text-sm font-bold text-slate-300">{{ formatNumber(dashboard?.tables?.stock_income?.count || 0) }}</span>
              </div>
              <button @click="handleSyncQuarterlyIncome()" :disabled="taskLoading.quarterly_income" class="w-full py-1 bg-business-success/10 hover:bg-business-success text-business-success hover:text-white rounded text-[8px] font-bold border border-business-success/20 transition-all">
                {{ taskLoading.quarterly_income ? '同步中' : '同步' }}
              </button>
            </div>
          </div>
        </div>
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

    <!-- AI 模型配置 -->
    <div v-if="activeTab === 'ai'" class="space-y-4">
      <div class="bg-business-dark rounded-2xl border border-business-light overflow-hidden shadow-business">
        <div class="p-4 border-b border-business-light flex justify-between items-center bg-slate-900/30">
          <div class="flex items-center space-x-2">
            <SparklesIcon class="w-4 h-4 text-purple-400" />
            <h2 class="text-sm font-bold text-white">AI 模型配置</h2>
          </div>
          <button @click="saveAIConfig" :disabled="aiConfigLoading" class="h-8 px-4 bg-purple-600 hover:bg-purple-500 text-white rounded-lg text-[10px] font-bold uppercase transition-all shadow-lg flex items-center space-x-1">
            <ArrowPathIcon :class="{'animate-spin': aiConfigLoading}" class="w-3 h-3" />
            <span>保存配置</span>
          </button>
        </div>

        <div class="p-5 space-y-5">
          <!-- 模型提供商 -->
          <div class="space-y-2">
            <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1 tracking-wider">模型提供商</label>
            <div class="grid grid-cols-2 gap-2">
              <button 
                v-for="provider in modelProviders" 
                :key="provider.value"
                @click="aiConfig.model_provider = provider.value"
                :class="aiConfig.model_provider === provider.value ? 'bg-purple-600 border-purple-400 text-white' : 'bg-business-darker border-business-light text-slate-400 hover:border-purple-500'"
                class="px-4 py-2.5 rounded-xl border text-[11px] font-bold transition-all"
              >
                {{ provider.label }}
              </button>
            </div>
          </div>

          <!-- 模型名称 -->
          <div class="space-y-2">
            <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1 tracking-wider">模型名称</label>
            <input v-model="aiConfig.model_name" type="text" class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-2.5 text-xs font-medium text-white focus:outline-none focus:border-purple-500 transition-all" placeholder="DeepSeek: deepseek-chat, OpenAI: gpt-4o" />
            <p class="text-[9px] text-slate-600 ml-1">留空将使用提供商默认模型</p>
          </div>

          <!-- API 地址 -->
          <div class="space-y-2">
            <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1 tracking-wider">API 地址</label>
            <input v-model="aiConfig.base_url" type="text" class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-2.5 text-xs font-medium text-white focus:outline-none focus:border-purple-500 transition-all" placeholder="留空使用默认地址" />
          </div>

          <!-- API Key -->
          <div class="space-y-2">
            <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1 tracking-wider">API Key</label>
            <div class="relative">
              <input v-model="aiConfig.api_key" :type="showApiKey ? 'text' : 'password'" class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-2.5 text-xs font-medium text-white focus:outline-none focus:border-purple-500 transition-all pr-10" placeholder="sk-..." />
              <button type="button" @click="showApiKey = !showApiKey" class="absolute right-3 top-1/2 -translate-y-1/2 text-slate-500 hover:text-white">
                <EyeIcon v-if="!showApiKey" class="w-4 h-4" />
                <EyeSlashIcon v-else class="w-4 h-4" />
              </button>
            </div>
          </div>

          <!-- 生成参数 -->
          <div class="grid grid-cols-2 gap-4">
            <div class="space-y-2">
              <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1 tracking-wider">最大 Tokens</label>
              <input v-model.number="aiConfig.max_tokens" type="number" min="100" max="128000" class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-2.5 text-xs font-medium text-white focus:outline-none focus:border-purple-500 transition-all" />
            </div>
            <div class="space-y-2">
              <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1 tracking-wider">Temperature</label>
              <input v-model.number="aiConfig.temperature" type="number" min="0" max="2" step="0.1" class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-2.5 text-xs font-medium text-white focus:outline-none focus:border-purple-500 transition-all" />
            </div>
          </div>

          <!-- System Prompt -->
          <div class="space-y-2">
            <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1 tracking-wider">System Prompt</label>
            <textarea v-model="aiConfig.system_prompt" rows="4" class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-3 text-xs font-medium text-white focus:outline-none focus:border-purple-500 transition-all resize-none" placeholder="输入系统提示词，用于定义AI助手的行为和角色..."></textarea>
          </div>
        </div>
      </div>

      <!-- 提示词模板管理 -->
      <div class="bg-business-dark rounded-2xl border border-business-light overflow-hidden shadow-business">
        <div class="p-4 border-b border-business-light flex justify-between items-center bg-slate-900/30">
          <div class="flex items-center space-x-2">
            <SparklesIcon class="w-4 h-4 text-purple-400" />
            <h2 class="text-sm font-bold text-white">提示词模板</h2>
          </div>
          <button @click="showTemplateModal = true" class="h-8 px-4 bg-purple-600 hover:bg-purple-500 text-white rounded-lg text-[10px] font-bold uppercase transition-all shadow-lg flex items-center space-x-1">
            <span>新建模板</span>
          </button>
        </div>

        <div class="p-4 space-y-3">
          <div v-if="promptTemplates.length === 0" class="text-center text-slate-500 text-xs py-4">
            暂无模板，点击新建创建一个
          </div>
          <div v-for="tpl in promptTemplates" :key="tpl.id" class="bg-business-darker rounded-xl border border-business-light p-3">
            <div class="flex items-center justify-between mb-2">
              <div class="flex items-center gap-2">
                <span class="text-xs font-bold text-white">{{ tpl.name }}</span>
                <span v-if="tpl.is_default" class="px-1.5 py-0.5 rounded bg-purple-600/30 text-purple-300 text-[9px] font-bold">默认</span>
                <span v-if="selectedTemplateId === tpl.id" class="px-1.5 py-0.5 rounded bg-green-600/30 text-green-300 text-[9px] font-bold">已选中</span>
              </div>
              <div class="flex gap-2">
                <button v-if="selectedTemplateId !== tpl.id" @click="selectPromptTemplate(tpl.id)" class="text-[10px] text-purple-400 hover:text-purple-300 font-bold">选用</button>
                <button @click="deletePromptTemplate(tpl.id)" class="text-[10px] text-red-400 hover:text-red-300 font-bold">删除</button>
              </div>
            </div>
            <p class="text-[10px] text-slate-500 line-clamp-2">{{ tpl.content }}</p>
          </div>
        </div>
      </div>
    </div>

    <!-- 模板创建弹窗 -->
    <Dialog :open="showTemplateModal" @close="showTemplateModal = false" class="relative z-50">
      <div class="fixed inset-0 bg-business-darker/80 backdrop-blur-sm" />
      <div class="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel class="w-full max-w-lg rounded-2xl bg-business-dark border border-business-light p-6 shadow-2xl max-h-[80vh] overflow-y-auto">
          <DialogTitle class="text-lg font-bold text-white mb-4">新建提示词模板</DialogTitle>
          <form @submit.prevent="savePromptTemplate" class="space-y-4">
            <div class="space-y-2">
              <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1 tracking-wider">模板名称</label>
              <input v-model="newTemplate.name" type="text" required class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-2.5 text-xs font-medium text-white focus:outline-none focus:border-purple-500 transition-all" placeholder="例如：A股分析模板" />
            </div>
            <div class="space-y-2">
              <label class="block text-[10px] font-bold text-slate-500 uppercase ml-1 tracking-wider">模板内容</label>
              <p class="text-[9px] text-slate-600 mb-2">可用变量：{stock_snapshot} {capital_flow_snapshot} {market_context} {holding_context} {commentary_snapshot} {analysis_snapshot}（兼容旧变量：{stock_basic} {price_data} {money_flow} {holding} {market_sentiment} {mainline}）</p>
              <textarea v-model="newTemplate.content" rows="10" required class="w-full bg-business-darker border border-business-light rounded-xl px-4 py-3 text-xs font-medium text-white focus:outline-none focus:border-purple-500 transition-all resize-none font-mono" placeholder="输入提示词模板..."></textarea>
            </div>
            <div class="flex items-center gap-2">
              <input v-model="newTemplate.is_default" type="checkbox" id="isDefault" class="w-4 h-4 rounded" />
              <label for="isDefault" class="text-[10px] text-slate-400 font-bold">设为默认模板</label>
            </div>
            <div class="flex justify-end gap-3 mt-6">
              <button type="button" @click="showTemplateModal = false" class="text-xs font-bold text-slate-500 px-4">取消</button>
              <button type="submit" class="h-10 bg-purple-600 text-white px-6 rounded-lg font-bold text-[11px] shadow-lg">创建模板</button>
            </div>
          </form>
        </DialogPanel>
      </div>
    </Dialog>

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
import { getIntegrityReport, listUsers, deleteUser, createUser, updatePassword, executeDBQuery, syncDailyDate, syncFinancials, syncMoneyflow, syncMarketIndex, syncMargin, getTasksStatus, syncFinaIndicator, syncQuarterlyIncome, verifyDataAccuracy, getUserAIConfig, updateUserAIConfig, getPromptTemplates, createPromptTemplate, updatePromptTemplate, deletePromptTemplate as deletePromptTemplateApi, getSelectedTemplate, selectTemplate, getDataDashboard, getDayDataStatus, syncSpecificDate } from '@/services/api';
import { useAuthStore } from '@/stores/auth';
import { Dialog, DialogPanel, DialogTitle } from '@headlessui/vue'
import { 
  UserGroupIcon, TrashIcon, KeyIcon, ArrowPathIcon, ChartBarIcon, ArrowUpTrayIcon,
  CheckCircleIcon, XCircleIcon, ClockIcon, PlayIcon, SparklesIcon,
  EyeIcon, EyeSlashIcon
} from '@heroicons/vue/20/solid'

const route = useRoute();
const router = useRouter();
const authStore = useAuthStore();
const tabs = [
  { id: 'users', name: '用户管理', admin: true },
  { id: 'data', name: '数据管理', admin: true },
  { id: 'ai', name: 'AI配置', admin: false },
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
    fetchDashboard();
    fetchIntegrity();
  } else if (newTab === 'ai') {
    fetchAIConfig();
    fetchPromptTemplates();
    fetchSelectedTemplate();
  }
});

const fetchSelectedTemplate = async () => {
  try {
    const res = await getSelectedTemplate();
    selectedTemplateId.value = res.data.selected_template_id;
  } catch (e) {
    console.error('获取选中模板失败', e);
  }
};

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
const integrityDailyData = ref({});
const integritySummaries = ref(null);
const integrityLoading = ref(false);
const queryLoading = ref(false);
const taskLoading = reactive({});
const tasksStatus = ref({ tasks: [] });
const dashboard = ref(null);
const dashboardLoading = ref(false);
const verifyLoading = ref(false);
const verifyResult = ref(null);
const verifyTsCode = ref('688256.SH');
const showHeatmap = ref(false);
const showVerify = ref(false);
const showFinancial = ref(false);
const refreshing = ref(false);
let statusTimer = null;

// 运行中的任务
const runningTasks = computed(() => {
  return (tasksStatus.value.tasks || []).filter(t => t.status === 'RUNNING');
});

// 4个核心日频表
const coreTables = computed(() => {
  const keys = ['daily_price', 'stock_moneyflow', 'market_index', 'stock_margin'];
  const labels = { daily_price: '日线行情', stock_moneyflow: '资金流向', market_index: '市场指数', stock_margin: '融资融券' };
  return keys.map(key => {
    const info = dashboard.value?.tables?.[key] || {};
    const summary = integritySummaries.value?.[key] || {};
    const completeness = summary.completeness ?? (info.count > 0 ? 100 : 0);
    
    // 生成迷你条数据（从完整性数据采样约30个点）
    const dailyData = integrityDailyData.value?.[key] || [];
    const step = Math.max(1, Math.floor(dailyData.length / 30));
    const miniBar = [];
    for (let i = 0; i < dailyData.length; i += step) {
      const s = dailyData[i]?.status;
      if (s === 'FULL') miniBar.push('full');
      else if (s === 'PARTIAL') miniBar.push('partial');
      else if (s === 'MISSING') miniBar.push('missing');
      else miniBar.push('holiday');
    }
    
    const countStr = info.count >= 10000 ? (info.count / 10000).toFixed(0) + '万' : (info.count || 0).toLocaleString();
    const dateRange = info.last_date ? (info.last_date || '').slice(0,10).replace(/-/g,'/') : '无数据';
    
    let statusColor = 'bg-green-500 shadow-[0_0_6px_#4ade80]';
    if (completeness < 80) statusColor = 'bg-yellow-500 shadow-[0_0_6px_#facc15]';
    if (completeness < 50) statusColor = 'bg-red-500 shadow-[0_0_6px_#f87171]';
    if (info.count === 0) statusColor = 'bg-slate-600';
    
    return { key, label: labels[key], countStr, dateRange, completeness, miniBar, statusColor };
  });
});

// 整体健康状态
const overallHealth = computed(() => {
  if (!integritySummaries.value) return 'warn';
  const vals = Object.values(integritySummaries.value);
  if (vals.length === 0) return 'warn';
  const avg = vals.reduce((s, v) => s + (v.completeness || 0), 0) / vals.length;
  if (avg >= 95) return 'good';
  if (avg >= 80) return 'warn';
  return 'bad';
});

const healthSummary = computed(() => {
  if (refreshing.value || dashboardLoading.value) return '加载中...';
  if (!dashboard.value) return '点击刷新获取数据状态';
  const d = dashboard.value.tables || {};
  const lastDates = Object.values(d).map(t => t.last_date).filter(Boolean).sort();
  const latest = lastDates.length > 0 ? lastDates[lastDates.length - 1] : '未知';
  
  if (!integritySummaries.value) return `最近同步: ${latest}`;
  const vals = Object.values(integritySummaries.value);
  const avg = vals.reduce((s, v) => s + (v.completeness || 0), 0) / vals.length;
  const missing = vals.reduce((s, v) => s + (v.missing_days || 0), 0);
  
  if (missing > 0) return `最近同步: ${latest} · 近90天完整度 ${avg.toFixed(0)}% · ${missing}天数据缺失`;
  return `最近同步: ${latest} · 近90天完整度 ${avg.toFixed(0)}% · 数据正常`;
});

// 刷新全部
const refreshAll = async () => {
  refreshing.value = true;
  try {
    await Promise.all([fetchDashboard(), fetchIntegrity(), fetchTasksStatus()]);
  } finally {
    refreshing.value = false;
  }
};

// AI 配置
const aiConfigLoading = ref(false);
const showApiKey = ref(false);
const aiConfig = reactive({
  model_provider: 'deepseek',
  model_name: '',
  api_key: '',
  base_url: '',
  system_prompt: '',
  max_tokens: 1200,
  temperature: 0.35
});

const modelProviders = [
  { value: 'deepseek', label: 'DeepSeek' },
  { value: 'openai', label: 'OpenAI' }
];

const promptTemplates = ref([]);
const selectedTemplateId = ref(null);
const showTemplateModal = ref(false);
const newTemplate = reactive({ name: '', content: '', is_default: false });

const defaultTemplateContent = `你是A股交易分析助手，偏短中线执行，不写成研究报告。

请只基于以下资料输出简洁 Markdown，总字数控制在 260-420 字。

输出格式固定为：
## 结论
- 观点：偏多 / 中性 / 偏空
- 建议：买入试错 / 持有 / 观望 / 减仓 / 回避
- 时效：1-3日 或 1-2周
- 置信度：高 / 中 / 低

## 核心依据
- 最多 3 条，只写最关键证据

## 关键价位与动作
- 支撑位：
- 压力位：
- 触发条件：
- 失效条件：

## 持仓应对
- 空仓：
- 持仓：

## 风险
- 最多 2 条，直接写风险触发点

要求：
1. 结论先行，不复述大段原始数据。
2. 不做关联个股推荐，不写“可关注某某板块/同行”。
3. 只有在数据支持时才给出价位和动作；不确定就写“等待确认”。
4. 避免空话、套话和长免责声明。

标的概览：
{stock_snapshot}

资金与杠杆：
{capital_flow_snapshot}

市场环境：
{market_context}

持仓信息：
{holding_context}

量化点评：
{commentary_snapshot}`;

const fetchPromptTemplates = async () => {
  if (activeTab.value !== 'ai') return;
  try {
    const res = await getPromptTemplates();
    promptTemplates.value = res.data || [];
  } catch (e) {
    console.error('获取提示词模板失败', e);
  }
};

const savePromptTemplate = async () => {
  if (!newTemplate.name || !newTemplate.content) {
    alert('请填写模板名称和内容');
    return;
  }
  try {
    await createPromptTemplate(newTemplate);
    showTemplateModal.value = false;
    Object.assign(newTemplate, { name: '', content: '', is_default: false });
    await fetchPromptTemplates();
    alert('模板创建成功');
  } catch (e) {
    alert('创建失败: ' + (e.response?.data?.detail || e.message));
  }
};

const deletePromptTemplate = async (id) => {
  if (!confirm('确认删除该模板？')) return;
  try {
    await deletePromptTemplateApi(id);
    await fetchPromptTemplates();
  } catch (e) {
    alert('删除失败');
  }
};

const selectPromptTemplate = async (id) => {
  try {
    await selectTemplate(id);
    selectedTemplateId.value = id;
    alert('模板已选中');
  } catch (e) {
    alert('选择失败');
  }
};

const fetchAIConfig = async () => {
  if (activeTab.value !== 'ai') return;
  try {
    const res = await getUserAIConfig();
    Object.assign(aiConfig, res.data);
  } catch (e) {
    console.error('获取AI配置失败', e);
  }
};

const saveAIConfig = async () => {
  aiConfigLoading.value = true;
  try {
    await updateUserAIConfig(aiConfig);
    alert('AI配置已保存');
  } catch (e) {
    alert('保存失败: ' + (e.response?.data?.detail || e.message));
  } finally {
    aiConfigLoading.value = false;
  }
};

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
      fetchDashboard();
      fetchTasksStatus();
      fetchIntegrity();
    }
  }
  statusTimer = setInterval(fetchTasksStatus, 5000);
});

onUnmounted(() => {
  if (statusTimer) clearInterval(statusTimer);
});

// 表名映射到可读名称及同步任务
const tableConfigs = {
  'daily_price': { 
    label: '日线行情', 
    action: (date) => syncDailyDate(date),
    batchAction: () => syncDailyDate(null), 
    batchLabel: '全量同步'
  },
  'stock_moneyflow': { 
    label: '资金流向', 
    action: (date) => syncSpecificDate(date, 'stock_moneyflow'),
    batchAction: () => syncMoneyflow(1),
    batchLabel: '全量同步'
  },
  'market_index': { 
    label: '市场指数', 
    action: (date) => syncSpecificDate(date, 'market_index'),
    batchAction: () => syncMarketIndex('000001.SH', 1),
    batchLabel: '全量同步'
  },
  'stock_margin': { 
    label: '融资融券', 
    action: (date) => syncSpecificDate(date, 'stock_margin'),
    batchAction: () => syncMargin(90),
    batchLabel: '全量同步'
  }
};

const getTableLabel = (tableName) => {
  return tableConfigs[tableName]?.label || dashboard.value?.tables?.[tableName]?.label || tableName;
};

const formatNumber = (n) => {
  if (!n) return '0';
  if (n >= 1000000) return (n / 10000).toFixed(0) + '万';
  if (n >= 10000) return (n / 10000).toFixed(1) + '万';
  return n.toLocaleString();
};

const formatTaskTime = (ts) => {
  if (!ts) return '';
  try {
    const d = new Date(ts);
    if (isNaN(d.getTime())) return ts.slice(0, 16);
    return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
  } catch { return ''; }
};

// 图表配置计算
const chartOptions = computed(() => {
  const options = {};
  for (const key in integrityDailyData.value) {
    options[key] = generateChartOption(key, integrityDailyData.value[key]);
  }
  return options;
});

const handleRunTableTask = async (tableName) => {
  const config = tableConfigs[tableName];
  if (!config || !config.batchAction) return;
  
  if (!confirm(`确认启动 [${config.label}] 的全量同步任务？`)) return;
  
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
  
  if (statusVal === 0) return;
  
  if (confirm(`日期: ${dateStr}\n表: ${getTableLabel(tableName)}\n状态: ${statusName} (${count} 条)\n\n是否触发该日数据刷新？`)) {
    try {
      const res = await syncSpecificDate(dateStr, tableName);
      alert(res.data.message || `${dateStr} 同步任务已启动`);
      setTimeout(fetchTasksStatus, 1000);
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

const fetchIntegrity = async () => {
  if (!authStore.isAdmin) return;
  integrityLoading.value = true;
  try {
    const today = new Date();
    const threeMonthsAgo = new Date();
    threeMonthsAgo.setMonth(today.getMonth() - 3);
    const startDate = threeMonthsAgo.toISOString().split('T')[0];
    const endDate = today.toISOString().split('T')[0];
    const response = await getIntegrityReport(startDate, endDate);
    integrityDailyData.value = response.data.daily_data || {};
    integritySummaries.value = response.data.summaries || {};
  } catch (e) {
    console.error('获取完整性报告失败:', e);
  } finally { integrityLoading.value = false; }
};

const fetchDashboard = async () => {
  if (!authStore.isAdmin) return;
  dashboardLoading.value = true;
  try {
    const res = await getDataDashboard();
    dashboard.value = res.data;
  } catch (e) { console.error('获取数据仪表盘失败', e); }
  finally { dashboardLoading.value = false; }
};

const handleSyncFinaIndicator = async () => {
  if (!confirm('确认同步财务指标数据？')) return;
  taskLoading.fina_indicator = true;
  try {
    const res = await syncFinaIndicator(500);
    alert(res.data.message || '同步已启动');
    setTimeout(fetchDashboard, 2000);
  } catch (e) { alert('同步失败: ' + (e.response?.data?.detail || e.message)); }
  finally { taskLoading.fina_indicator = false; }
};

const handleSyncQuarterlyIncome = async () => {
  if (!confirm('确认同步季度利润表数据？')) return;
  taskLoading.quarterly_income = true;
  try {
    const res = await syncQuarterlyIncome(null, 2020);
    alert(res.data.message || '同步已启动');
    setTimeout(fetchDashboard, 2000);
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
