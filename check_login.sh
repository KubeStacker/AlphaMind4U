#!/bin/bash
echo "=== 登录页面诊断 ==="
echo ""
echo "1. 检查前端服务状态："
docker-compose ps frontend | grep frontend
echo ""
echo "2. 检查前端是否可以访问："
curl -s -o /dev/null -w "HTTP状态码: %{http_code}\n" http://localhost:80
echo ""
echo "3. 检查登录API："
curl -s -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' | python3 -c "import sys, json; d=json.load(sys.stdin); print('✅ 登录API正常' if 'access_token' in d else '❌ 登录API异常')" 2>/dev/null || echo "❌ 登录API异常"
echo ""
echo "4. 检查前端构建文件："
docker exec stock-frontend ls -lh /usr/share/nginx/html/assets/ | head -3
echo ""
echo "=== 访问建议 ==="
echo "1. 直接访问: http://$(hostname -I | awk '{print $1}')/login"
echo "2. 或访问: http://$(hostname -I | awk '{print $1}')/"
echo "3. 清除浏览器缓存后重试（Ctrl+F5 或 Cmd+Shift+R）"
