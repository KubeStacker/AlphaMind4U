import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path'

// Vite 前端工程配置（反代 / 预览端口），入口为根 index.html。
export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: { '@': resolve(__dirname, 'src') },
  },
  server: {
    port: 5173,
    proxy: {
      // 量化后端只读与任务接口。
      '/admin': { target: 'http://127.0.0.1:8000', changeOrigin: true },
      // MCP 只读代理（streamable-http）。
      '/mcp': { target: 'http://127.0.0.1:8081', changeOrigin: true },
      // 文档后端预留（未就绪，前端仅展示 502 占位）。
      '/doc': { target: 'http://127.0.0.1:8002', changeOrigin: true },
    },
  },
  preview: {
    port: 8080,
    host: '0.0.0.0',
  },
  build: {
    outDir: 'dist',
  },
})
