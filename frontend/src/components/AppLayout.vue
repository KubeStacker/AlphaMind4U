<template>
  <div :class="[isSettingsPage ? 'bg-transparent' : 'min-h-screen bg-obsidian-950']" class="text-slate-200 font-sans pb-16 md:pb-0">
    <div class="fixed inset-0 pointer-events-none z-0">
      <div class="absolute inset-0 bg-[radial-gradient(ellipse_80%_50%_at_50%_-20%,rgba(99,102,241,0.06),transparent)]"></div>
    </div>

    <header class="relative z-50 bg-obsidian-900/80 backdrop-blur-xl border-b border-white/[0.04] sticky top-0 safe-top">
      <nav class="max-w-6xl mx-auto px-4 flex justify-between items-center h-14">
        <div class="flex items-center gap-6">
          <router-link to="/" class="flex items-center gap-2.5 group">
            <div class="w-7 h-7 bg-gradient-to-br from-signal-accent to-signal-data rounded-lg flex items-center justify-center shadow-lg shadow-signal-accent/10 group-hover:shadow-signal-accent/20 transition-shadow">
              <span class="text-white font-black text-[10px] italic tracking-tighter">J</span>
            </div>
            <span class="text-sm font-bold tracking-tight text-white/90">Jarvis</span>
          </router-link>
          <div class="hidden md:flex items-center gap-1">
            <router-link
              to="/"
              class="px-3 py-1.5 rounded-lg text-[11px] font-semibold transition-all duration-200"
              :class="isWatchlistPage ? 'bg-white/[0.06] text-white' : 'text-slate-500 hover:text-slate-300 hover:bg-white/[0.03]'"
            >
              盯盘
            </router-link>
            <router-link
              to="/dashboard"
              class="px-3 py-1.5 rounded-lg text-[11px] font-semibold transition-all duration-200"
              :class="isDashboardPage ? 'bg-white/[0.06] text-white' : 'text-slate-500 hover:text-slate-300 hover:bg-white/[0.03]'"
            >
              仪表盘
            </router-link>
          </div>
        </div>

        <div class="flex items-center gap-1.5">
          <button @click="showDocs = !showDocs"
            class="hidden md:flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[11px] font-semibold transition-all duration-200"
            :class="showDocs ? 'bg-white/[0.06] text-white' : 'text-slate-500 hover:text-slate-300 hover:bg-white/[0.03]'">
            <DocumentTextIcon class="w-3.5 h-3.5" />
            <span>文档</span>
          </button>
          <div class="hidden md:block">
            <Menu as="div" class="relative inline-block text-left">
              <MenuButton class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[11px] font-semibold text-slate-500 hover:text-slate-300 hover:bg-white/[0.03] transition-all duration-200">
                <Cog6ToothIcon class="w-3.5 h-3.5" />
                <span>控制台</span>
                <ChevronDownIcon class="w-3 h-3 opacity-40" />
              </MenuButton>
              <transition enter-active-class="transition duration-100 ease-out" enter-from-class="transform scale-95 opacity-0" enter-to-class="transform scale-100 opacity-100" leave-active-class="transition duration-75 ease-in" leave-from-class="transform scale-100 opacity-100" leave-to-class="transform scale-95 opacity-0">
                <MenuItems class="absolute right-0 z-[140] mt-2 w-48 origin-top-right overflow-hidden rounded-xl border border-white/[0.08] bg-[linear-gradient(180deg,rgba(35,40,64,0.98),rgba(18,21,31,0.98))] shadow-[0_18px_38px_rgba(0,0,0,0.5)] focus:outline-none backdrop-blur-xl">
                  <div class="space-y-1 p-1.5">
                    <MenuItem v-slot="{ active }">
                      <router-link :to="{ path: '/settings', query: { tab: 'users' } }" :class="[active ? 'border-white/[0.12] bg-obsidian-700 text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]' : 'border-white/[0.08] bg-obsidian-900/95 text-slate-200 hover:bg-obsidian-700/95 hover:text-white', 'group flex w-full items-center rounded-lg border px-3 py-2 text-[11px] font-semibold transition-colors']">
                        <UserGroupIcon class="mr-2.5 h-3.5 w-3.5 opacity-60" /> 用户管理
                      </router-link>
                    </MenuItem>
                    <MenuItem v-slot="{ active }">
                      <router-link :to="{ path: '/settings', query: { tab: 'ai' } }" :class="[active ? 'border-white/[0.12] bg-obsidian-700 text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]' : 'border-white/[0.08] bg-obsidian-900/95 text-slate-200 hover:bg-obsidian-700/95 hover:text-white', 'group flex w-full items-center rounded-lg border px-3 py-2 text-[11px] font-semibold transition-colors']">
                        <SparklesIcon class="mr-2.5 h-3.5 w-3.5 opacity-60" /> AI 配置
                      </router-link>
                    </MenuItem>
                    <MenuItem v-if="authStore.isAdmin" v-slot="{ active }">
                      <router-link :to="{ path: '/settings', query: { tab: 'data' } }" :class="[active ? 'border-white/[0.12] bg-obsidian-700 text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]' : 'border-white/[0.08] bg-obsidian-900/95 text-slate-200 hover:bg-obsidian-700/95 hover:text-white', 'group flex w-full items-center rounded-lg border px-3 py-2 text-[11px] font-semibold transition-colors']">
                        <CircleStackIcon class="mr-2.5 h-3.5 w-3.5 opacity-60" /> 数据管理
                      </router-link>
                    </MenuItem>
                    <MenuItem v-if="authStore.isAdmin" v-slot="{ active }">
                      <router-link :to="{ path: '/settings', query: { tab: 'db' } }" :class="[active ? 'border-white/[0.12] bg-obsidian-700 text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]' : 'border-white/[0.08] bg-obsidian-900/95 text-slate-200 hover:bg-obsidian-700/95 hover:text-white', 'group flex w-full items-center rounded-lg border px-3 py-2 text-[11px] font-semibold transition-colors']">
                        <CommandLineIcon class="mr-2.5 h-3.5 w-3.5 opacity-60" /> SQL 控制台
                      </router-link>
                    </MenuItem>
                  </div>
                  <div class="border-t border-white/[0.06] p-1.5">
                    <MenuItem v-slot="{ active }">
                      <button @click="handleLogout" :class="[active ? 'bg-signal-bear/10 text-signal-bear' : 'text-slate-600', 'group flex w-full items-center rounded-lg px-3 py-2 text-[11px] font-semibold transition-colors']">
                        <ArrowRightOnRectangleIcon class="mr-2.5 h-3.5 w-3.5" /> 安全退出
                      </button>
                    </MenuItem>
                  </div>
                </MenuItems>
              </transition>
            </Menu>
          </div>
          <button @click="handleLogout" class="md:hidden p-2 text-slate-500 hover:text-slate-300 transition-colors"><ArrowRightOnRectangleIcon class="w-5 h-5" /></button>
        </div>
      </nav>
    </header>

    <main class="relative z-10 max-w-6xl mx-auto px-4 py-4 md:py-6">
      <router-view />
    </main>

    <div class="md:hidden fixed bottom-0 left-0 right-0 bg-obsidian-900/90 backdrop-blur-xl border-t border-white/[0.04] safe-bottom z-50">
      <div class="flex justify-around items-center h-16">
        <router-link to="/" class="flex flex-col items-center gap-0.5 transition-all duration-200 py-1 px-3 rounded-xl" :class="isWatchlistPage ? 'text-signal-bull' : 'text-slate-600'">
          <EyeIcon class="w-5 h-5" />
          <span class="text-[9px] font-semibold">盯盘</span>
        </router-link>
        <router-link to="/dashboard" class="flex flex-col items-center gap-0.5 transition-all duration-200 py-1 px-3 rounded-xl" :class="isDashboardPage ? 'text-signal-bull' : 'text-slate-600'">
          <ChartBarIcon class="w-5 h-5" />
          <span class="text-[9px] font-semibold">仪表盘</span>
        </router-link>
        <button @click="showDocs = !showDocs" class="flex flex-col items-center gap-0.5 transition-all duration-200 py-1 px-3 rounded-xl" :class="showDocs ? 'text-signal-bull' : 'text-slate-600'">
          <DocumentTextIcon class="w-5 h-5" />
          <span class="text-[9px] font-semibold">文档</span>
        </button>
        <router-link to="/settings" class="flex flex-col items-center gap-0.5 transition-all duration-200 py-1 px-3 rounded-xl" :class="isSettingsPage ? 'text-signal-bull' : 'text-slate-600'">
          <Cog6ToothIcon class="w-5 h-5" />
          <span class="text-[9px] font-semibold">设置</span>
        </router-link>
      </div>
    </div>

    <DocsPanel :open="showDocs" @close="showDocs = false" />
  </div>
</template>

<script setup>
import { ref, computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { useAuthStore } from '@/stores/auth';
import { Menu, MenuButton, MenuItems, MenuItem } from '@headlessui/vue'
import { 
  ChevronDownIcon, Cog6ToothIcon, UserGroupIcon, 
  CircleStackIcon, ArrowRightOnRectangleIcon, ChartBarIcon, CommandLineIcon,
  EyeIcon, SparklesIcon, DocumentTextIcon
} from '@heroicons/vue/20/solid'
import DocsPanel from './DocsPanel.vue';

const route = useRoute();
const router = useRouter();
const authStore = useAuthStore();
const showDocs = ref(false);

const isSettingsPage = computed(() => route.path.startsWith('/settings'));
const isWatchlistPage = computed(() => route.name === 'watchlist');
const isDashboardPage = computed(() => route.name === 'dashboard');

const handleLogout = () => { authStore.logout(); router.push({ name: 'login' }); };
</script>
