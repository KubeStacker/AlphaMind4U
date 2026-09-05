// 后端错误统一转中文提示（不输出全量数据到 console）。
export function parseApiError(err: unknown): string {
  if (err instanceof ApiHttpError) {
    if (err.status === 401) return '认证失败：令牌无效或缺失，请重新登录'
    if (err.status === 404) return `接口不存在：${err.path}`
    if (err.status === 502) return '上游服务未就绪，请稍后重试'
    return `请求失败（${err.status}）：${err.body || '未知错误'}`
  }
  if (err instanceof TypeError) return '网络连接失败：请确认后端服务已启动'
  if (err instanceof Error) return `请求异常：${err.message.slice(0, 200)}`
  return '未知错误，请稍后重试'
}

// 带状态码与路径的 HTTP 错误。
export class ApiHttpError extends Error {
  status: number
  path: string
  body: string
  constructor(status: number, path: string, body: string) {
    super(`HTTP ${status} ${path}`)
    this.status = status
    this.path = path
    this.body = body
  }
}
