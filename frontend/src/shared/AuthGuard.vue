<script setup lang="ts">
import { computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

// 登录门禁守卫：未登录时渲染插槽前先跳登录页。
// 注意登录页本身必须放行，否则守卫与路由重定向互锁导致全白屏。
const auth = useAuthStore()
const router = useRouter()
const route = useRoute()

// 是否放行当前路由（已登录，或正在访问登录页）。
const visible = computed(() => auth.isAuthed || route.path === '/login')

watch(
  () => auth.isAuthed,
  (ok) => { if (!ok && route.path !== '/login') router.push('/login') },
  { immediate: true },
)
</script>

<template>
  <slot v-if="visible" />
</template>
