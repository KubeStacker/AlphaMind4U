# AI 智能体工作技巧

## 节省 Token 消耗

**原则：** 减少不必要的索引和搜索操作。

**具体方法：**
- ❌ 不要多次搜索同一文件
- ❌ 不要重复读取已读过的文件
- ✅ 一次读取足够上下文（用 limit 参数）
- ✅ 批量执行独立的工具调用
- ✅ 直接定位文件路径，避免全局搜索

## 高效文件操作

**读取：**
- 使用 Read 工具，指定合理的 offset 和 limit
- 避免 tiny repeated slices（30 行块）
- 需要更多上下文时，读取更大的窗口

**搜索：**
- 使用 Glob 工具查找文件（NOT find/ls）
- 使用 Grep 工具搜索内容（NOT grep/rg）
- 支持并行调用多个工具

**编辑：**
- 使用 Edit 工具进行精确替换
- 使用 Write 工具创建新文件
- 避免 cat/head/tail/sed/awk

## 验证方式

**后端修改：**
```bash
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1"}'
```

**前端修改：**
- 通过浏览器访问容器暴露的端口验证
- 较大改动建议使用无头浏览器
- 用户名：`yuanpeng`，密码：`1qaz2wsx`

**禁止：**
- 使用 `npm run build` 验证前端
- 使用 `pip install` 重新安装依赖验证后端

## 日志和调试

**查看日志：**
```bash
docker-compose logs -f backend
docker-compose logs -f frontend
```

**进入容器：**
```bash
docker-compose exec backend bash
```

## 日期处理

**库：** 使用 `arrow` 库处理日期

**存储：** 在 DuckDB 中存储为 DATE 类型

**时区：** 使用上海时区（Asia/Shanghai）
