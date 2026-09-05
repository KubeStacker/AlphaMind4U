import { defineStore } from 'pinia'

// 单条提示内容。
export interface ToastItem {
  id: number
  message: string
  kind: 'info' | 'success' | 'error'
}

let nextId = 1

// 全局轻提示管理。
export const useToastStore = defineStore('toast', {
  state: () => ({ items: [] as ToastItem[] }),
  actions: {
    // 推入一条提示（默认 2.5 秒消失）。
    push(message: string, kind: ToastItem['kind'] = 'info', duration = 2500) {
      const id = nextId++
      this.items.push({ id, message, kind })
      window.setTimeout(() => this.dismiss(id), duration)
    },
    // 关闭指定提示。
    dismiss(id: number) {
      this.items = this.items.filter((t) => t.id !== id)
    },
  },
})
