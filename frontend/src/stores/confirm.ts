import { defineStore } from 'pinia'

// 二次确认请求内容。
export interface ConfirmRequest {
  title: string
  message: string
  okText: string
}

// 全局二次确认管理（Promise 风格调用）。
export const useConfirmStore = defineStore('confirm', {
  state: () => ({
    pending: null as ConfirmRequest | null,
    resolver: null as ((ok: boolean) => void) | null,
  }),
  actions: {
    // 弹出确认框并等待用户选择。
    ask(req: ConfirmRequest): Promise<boolean> {
      this.pending = req
      return new Promise<boolean>((resolve) => {
        this.resolver = resolve
      })
    },
    // 用户确认或取消。
    answer(ok: boolean) {
      this.resolver?.(ok)
      this.pending = null
      this.resolver = null
    },
  },
})
