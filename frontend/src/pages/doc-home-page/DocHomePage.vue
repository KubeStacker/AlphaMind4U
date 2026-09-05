<script setup lang="ts">
// 文档首页占位：docreader :8002 未就绪，固定展示 502。
const DOC_PORT = 8002
</script>

<template>
  <div class="page">
    <section class="card center">
      <i data-lucide="book-x" class="big"></i>
      <h3>文档服务未就绪</h3>
      <p class="mono code">HTTP 502 ｜ docreader-backend :{{ DOC_PORT }} 预留中</p>
      <p class="dim">文档后端接入后，此页将展示文档检索入口；当前不发起任何请求。</p>
      <router-link to="/data" class="btn"><i data-lucide="arrow-left"></i><span>返回数据看板</span></router-link>
    </section>
  </div>
</template>

<style scoped>
.page { display: flex; flex-direction: column; gap: 16px; }
.center { text-align: center; padding: 48px 20px; display: flex; flex-direction: column; align-items: center; gap: 10px; }
.big { width: 36px; height: 36px; color: var(--text-dim); }
.center h3 { margin: 0; font-size: 14px; color: var(--white); }
.code { color: var(--accent-amber); font-size: 12px; margin: 0; }
.dim { color: var(--text-dim); font-size: 12px; margin: 0 0 8px; }
</style>
