import { getToken } from './http'

// MCP JSON-RPC 调用参数。
export interface McpCallParams {
  tool: string
  args: Record<string, unknown>
}

// 解析 streamable-http 响应（SSE 或纯 JSON，不打全量数据到 console）。
function parseMcpBody(contentType: string, text: string): unknown {
  let payload = text.trim()
  if (contentType.includes('text/event-stream')) {
    const lines = payload.split('\n').filter((l) => l.startsWith('data:'))
    if (lines.length === 0) throw new Error('MCP 返回为空')
    payload = lines.map((l) => l.slice(5).trim()).join('\n')
  }
  let msg: { result?: unknown; error?: { message?: string } }
  try {
    msg = JSON.parse(payload)
  } catch {
    throw new Error('MCP 响应不是合法 JSON')
  }
  if (msg.error) throw new Error(`MCP 工具错误：${String(msg.error.message || '未知错误').slice(0, 300)}`)
  return msg.result ?? msg
}

// 调用 MCP 只读工具（POST /mcp，JSON-RPC）。
export async function mcpCall(params: McpCallParams): Promise<unknown> {
  const token = getToken()
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
  }
  if (token) headers['Authorization'] = `Bearer ${token}`
  let resp: Response
  try {
    resp = await fetch('/mcp', {
      method: 'POST',
      headers,
      body: JSON.stringify({ jsonrpc: '2.0', id: Date.now(), method: 'tools/call', params: { name: params.tool, arguments: params.args } }),
    })
  } catch {
    throw new TypeError('网络连接失败')
  }
  const text = await resp.text()
  if (!resp.ok) {
    if (resp.status === 401) throw new Error('MCP 认证失败：令牌无效或缺失')
    throw new Error(`MCP 请求失败（${resp.status}）：${text.slice(0, 300)}`)
  }
  return parseMcpBody(resp.headers.get('content-type') || '', text)
}

// 查询 MCP 工具列表（只读自检用）。
export async function mcpListTools(): Promise<unknown> {
  const token = getToken()
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
  }
  if (token) headers['Authorization'] = `Bearer ${token}`
  const resp = await fetch('/mcp', {
    method: 'POST',
    headers,
    body: JSON.stringify({ jsonrpc: '2.0', id: Date.now(), method: 'tools/list', params: {} }),
  })
  const text = await resp.text()
  if (!resp.ok) throw new Error(`MCP 请求失败（${resp.status}）`)
  return parseMcpBody(resp.headers.get('content-type') || '', text)
}
