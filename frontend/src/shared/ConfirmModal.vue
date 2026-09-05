<script setup lang="ts">
import { useConfirmStore } from '@/stores/confirm'

// 全局二次确认弹窗（由 stores/confirm 驱动）。
const confirm = useConfirmStore()

// 用户点击确认。
function onOk() {
  confirm.answer(true)
}

// 用户点击取消。
function onCancel() {
  confirm.answer(false)
}
</script>

<template>
  <div v-if="confirm.pending" class="mask" @click.self="onCancel">
    <div class="modal">
      <div class="head">
        <i data-lucide="alert-triangle" class="warn"></i>
        <h3>{{ confirm.pending.title }}</h3>
      </div>
      <p class="msg mono">{{ confirm.pending.message }}</p>
      <div class="actions">
        <button class="btn" @click="onCancel">取消</button>
        <button class="btn btn-primary" @click="onOk">{{ confirm.pending.okText }}</button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.mask {
  position: fixed; inset: 0; z-index: 50; background: rgba(0, 0, 0, 0.6);
  display: flex; align-items: center; justify-content: center; padding: 16px;
}
.modal { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-lg); padding: 20px; width: 100%; max-width: 380px; }
.head { display: flex; align-items: center; gap: 8px; }
.head h3 { margin: 0; font-size: 13px; color: var(--white); }
.warn { color: var(--accent-rose); width: 16px; height: 16px; }
.msg { font-size: 12px; color: var(--text); line-height: 1.7; }
.actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 16px; }
</style>
