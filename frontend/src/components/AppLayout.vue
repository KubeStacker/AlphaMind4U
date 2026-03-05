<template>
  <div :class="[isSettingsPage ? 'bg-transparent' : 'min-h-screen bg-business-darker']" class="text-slate-200 font-sans pb-16 md:pb-0">
    <!-- 顶部导航栏 - 紧凑型 -->
    <header class="bg-business-dark/80 backdrop-blur-md border-b border-business-light sticky top-0 z-50 safe-top shadow-md">
      <nav class="container mx-auto px-4 flex justify-between items-center h-14">
        <div class="flex items-center">
          <router-link to="/" class="flex items-center space-x-2 group">
            <div class="w-7 h-7 bg-gradient-to-br from-business-accent to-business-highlight rounded-lg flex items-center justify-center shadow-md">
              <span class="text-white font-black text-xs italic">J</span>
            </div>
            <span class="text-sm font-bold tracking-tight text-white uppercase">Jarvis</span>
          </router-link>
          <div class="hidden md:flex ml-8 items-center space-x-1">
            <router-link
              to="/"
              class="px-3 py-1.5 rounded-lg text-xs font-bold transition-all"
              :class="isWatchlistPage ? 'bg-business-accent text-white' : 'text-slate-400 hover:text-white'"
            >
              盯盘
            </router-link>
            <router-link
              to="/dashboard"
              class="px-3 py-1.5 rounded-lg text-xs font-bold transition-all"
              :class="isDashboardPage ? 'bg-business-accent text-white' : 'text-slate-400 hover:text-white'"
            >
              仪表盘
            </router-link>
            <router-link
              to="/falcon"
              class="px-3 py-1.5 rounded-lg text-xs font-bold transition-all"
              :class="isFalconPage ? 'bg-business-accent text-white' : 'text-slate-400 hover:text-white'"
            >
              猎鹰
            </router-link>
          </div>
        </div>

        <div class="flex items-center space-x-2">
          <div class="hidden md:block">
            <Menu as="div" class="relative inline-block text-left">
              <MenuButton class="flex items-center space-x-1 px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400 hover:bg-business-light transition-all">
                <Cog6ToothIcon class="w-4 h-4" />
                <span>控制台</span>
                <ChevronDownIcon class="w-3 h-3 opacity-40" />
              </MenuButton>
              <transition enter-active-class="transition duration-100" enter-from-class="transform scale-95 opacity-0" enter-to-class="transform scale-100 opacity-100" leave-active-class="transition duration-75" leave-from-class="transform scale-100 opacity-100" leave-to-class="transform scale-95 opacity-0">
                <MenuItems class="absolute right-0 mt-2 w-44 origin-top-right divide-y divide-business-light rounded-xl bg-business-dark border border-business-light shadow-2xl focus:outline-none overflow-hidden">
                  <div class="p-1.5">
                    <MenuItem v-slot="{ active }">
                      <router-link to="/settings?tab=users" :class="[active ? 'bg-business-accent text-white' : 'text-slate-300', 'group flex w-full items-center rounded-lg px-2.5 py-2 text-xs font-bold transition-colors']">
                        <UserGroupIcon class="mr-2 h-4 w-4 opacity-70" /> 用户管理
                      </router-link>
                    </MenuItem>
                    <MenuItem v-if="authStore.isAdmin" v-slot="{ active }">
                      <router-link to="/settings?tab=data" :class="[active ? 'bg-business-accent text-white' : 'text-slate-300', 'group flex w-full items-center rounded-lg px-2.5 py-2 text-xs font-bold transition-colors']">
                        <CircleStackIcon class="mr-2 h-4 w-4 opacity-70" /> 数据管理
                      </router-link>
                    </MenuItem>
                    <MenuItem v-if="authStore.isAdmin" v-slot="{ active }">
                      <router-link to="/settings?tab=db" :class="[active ? 'bg-business-accent text-white' : 'text-slate-300', 'group flex w-full items-center rounded-lg px-2.5 py-2 text-xs font-bold transition-colors']">
                        <CommandLineIcon class="mr-2 h-4 w-4 opacity-70" /> SQL 控制台
                      </router-link>
                    </MenuItem>
                  </div>
                  <div class="p-1.5">
                    <MenuItem v-slot="{ active }">
                      <button @click="handleLogout" :class="[active ? 'bg-business-success/10 text-business-success' : 'text-slate-500', 'group flex w-full items-center rounded-lg px-2.5 py-2 text-xs font-bold']">
                        <ArrowRightOnRectangleIcon class="mr-2 h-4 w-4" /> 安全退出
                      </button>
                    </MenuItem>
                  </div>
                </MenuItems>
              </transition>
            </Menu>
          </div>
          <button @click="handleLogout" class="md:hidden p-2 text-slate-400"><ArrowRightOnRectangleIcon class="w-5 h-5" /></button>
        </div>
      </nav>
    </header>

    <main class="container mx-auto px-4 py-4 md:py-6">
      <router-view />
    </main>

    <!-- 底部导航 (移动端) - 极简 -->
    <div class="md:hidden fixed bottom-0 left-0 right-0 bg-business-dark/95 backdrop-blur-md border-t border-business-light safe-bottom z-50">
      <div class="flex justify-around items-center h-14">
        <router-link to="/" class="flex flex-col items-center transition-all" :class="isWatchlistPage ? 'text-business-highlight' : 'text-slate-500'">
          <EyeIcon class="w-5 h-5" />
          <span class="text-[9px] font-bold mt-0.5">盯盘</span>
        </router-link>
        <router-link to="/dashboard" class="flex flex-col items-center transition-all" :class="isDashboardPage ? 'text-business-highlight' : 'text-slate-500'">
          <ChartBarIcon class="w-5 h-5" />
          <span class="text-[9px] font-bold mt-0.5">仪表盘</span>
        </router-link>
        <router-link to="/falcon" class="flex flex-col items-center transition-all" :class="isFalconPage ? 'text-business-highlight' : 'text-slate-500'">
          <CommandLineIcon class="w-5 h-5" />
          <span class="text-[9px] font-bold mt-0.5">猎鹰</span>
        </router-link>
        <router-link to="/settings" class="flex flex-col items-center transition-all" active-class="text-business-highlight" inactive-class="text-slate-500">
          <Cog6ToothIcon class="w-5 h-5" />
          <span class="text-[9px] font-bold mt-0.5">设置</span>
        </router-link>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { useAuthStore } from '@/stores/auth';
import { Menu, MenuButton, MenuItems, MenuItem } from '@headlessui/vue'
import { 
  ChevronDownIcon, Cog6ToothIcon, UserGroupIcon, 
  CircleStackIcon, ArrowRightOnRectangleIcon, ChartBarIcon, CommandLineIcon,
  EyeIcon
} from '@heroicons/vue/20/solid'

const route = useRoute();
const router = useRouter();
const authStore = useAuthStore();

const isSettingsPage = computed(() => route.path.startsWith('/settings'));
const isWatchlistPage = computed(() => route.name === 'watchlist');
const isDashboardPage = computed(() => route.name === 'dashboard');
const isFalconPage = computed(() => route.name === 'falcon');

const handleLogout = () => { authStore.logout(); router.push({ name: 'login' }); };
</script>
