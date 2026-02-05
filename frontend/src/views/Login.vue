<template>
  <div class="flex items-center justify-center min-h-screen bg-business-darker px-4">
    <div class="w-full max-w-[340px] space-y-8 p-8 bg-business-dark rounded-3xl shadow-2xl border border-business-light animate-in zoom-in-95 duration-500">
      <div class="text-center">
        <div class="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-gradient-to-br from-business-accent to-blue-700 shadow-xl mb-6">
          <span class="text-3xl font-black text-white italic">J</span>
        </div>
        <h2 class="text-xl font-bold text-white tracking-tight">智能终端访问</h2>
      </div>
      
      <form class="space-y-5" @submit.prevent="handleLogin">
        <div class="space-y-1.5">
          <label class="text-[10px] font-bold text-slate-500 uppercase ml-1">身份标识</label>
          <input v-model="username" type="text" required class="w-full h-11 px-4 bg-business-darker border border-business-light rounded-xl text-white font-bold text-sm focus:outline-none focus:ring-1 focus:ring-business-accent/50 transition-all shadow-inner" />
        </div>
        
        <div class="space-y-1.5">
          <label class="text-[10px] font-bold text-slate-500 uppercase ml-1">访问凭证</label>
          <input v-model="password" type="password" required class="w-full h-11 px-4 bg-business-darker border border-business-light rounded-xl text-white font-bold text-sm focus:outline-none focus:ring-1 focus:ring-business-accent/50 transition-all shadow-inner" />
        </div>

        <div v-if="error" class="bg-business-success/10 border border-business-success/20 text-business-success text-[10px] p-3 rounded-lg flex items-center font-bold">
          <ExclamationCircleIcon class="w-4 h-4 mr-2" />
          {{ error }}
        </div>

        <div class="pt-2">
          <button type="submit" :disabled="loading" class="w-full h-12 flex justify-center items-center bg-gradient-to-r from-business-accent to-blue-600 rounded-xl text-xs font-bold text-white active:scale-95 transition-all shadow-lg">
            <span v-if="loading">验证中...</span>
            <span v-else>建立连接</span>
          </button>
        </div>
      </form>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import { useRouter } from 'vue-router';
import { useAuthStore } from '@/stores/auth';
import { ExclamationCircleIcon } from '@heroicons/vue/20/solid'

const username = ref('admin');
const password = ref('admin');
const error = ref('');
const loading = ref(false);
const router = useRouter();
const authStore = useAuthStore();

const handleLogin = async () => {
  loading.value = true;
  error.value = '';
  try {
    await authStore.login(username.value, password.value);
    router.push({ name: 'dashboard' });
  } catch (err) {
    error.value = '授权失败，请检查凭据';
  } finally {
    loading.value = false;
  }
};
</script>
