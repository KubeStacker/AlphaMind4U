<template>
  <div id="main-app" class="dark">
    <div class="bg-business-darker min-h-screen text-slate-200 selection:bg-business-accent/30 selection:text-white">
      <router-view />
    </div>
  </div>
</template>

<script setup>
import { onMounted } from 'vue';

onMounted(() => {
  document.documentElement.classList.add('dark');
});
</script>

<style>
body {
  margin: 0;
  padding: 0;
  font-family: 'Inter', 'PingFang SC', sans-serif;
}
/* 自定义滚动条 */
::-webkit-scrollbar {
  width: 5px;
}
::-webkit-scrollbar-track {
  background: #0f172a;
}
::-webkit-scrollbar-thumb {
  background: #334155;
  border-radius: 10px;
}
::-webkit-scrollbar-thumb:hover {
  background: #3b82f6;
}
</style>