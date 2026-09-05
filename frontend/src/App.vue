<script setup lang="ts">
import { computed } from 'vue'
import Topbar from '@/shared/Topbar.vue'
import AuthGuard from '@/shared/AuthGuard.vue'
import ConfirmModal from '@/shared/ConfirmModal.vue'
import Toast from '@/shared/Toast.vue'
import { useAuthStore } from '@/stores/auth'

// 应用根组件（顶栏 + 路由视图 + 全局弹窗）。
const auth = useAuthStore()

// 是否展示顶栏（已登录才展示）。
const showTopbar = computed(() => auth.isAuthed)
</script>

<template>
  <Topbar v-if="showTopbar" />
  <main class="main">
    <AuthGuard>
      <router-view />
    </AuthGuard>
  </main>
  <ConfirmModal />
  <Toast />
</template>

<style scoped>
.main { flex: 1; width: 100%; max-width: 1120px; margin: 0 auto; padding: 20px 16px 40px; }
</style>
