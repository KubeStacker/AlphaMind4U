// 文档后端占位 client：docreader :8002 未就绪，调用直接抛 502。
export class DocNotReadyError extends Error {
  status = 502
  constructor() {
    super('文档服务未就绪（HTTP 502 占位）')
  }
}

// 文档健康检查占位（后端就绪前直接抛错，不发请求）。
export async function docHealth(): Promise<never> {
  throw new DocNotReadyError()
}
