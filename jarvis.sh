#!/bin/sh
# jarvis.sh — 全仓唯一服务管理入口（原生进程，无 docker）。
# 用法：./jarvis.sh start|stop|restart|status|logs [quant|mcp|frontend|all]
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RUN_DIR="$ROOT/data/run"
LOG_DIR="$ROOT/data/logs"
mkdir -p "$RUN_DIR" "$LOG_DIR"

# shellcheck disable=SC1091
[ -f "$ROOT/.env" ] && set -a && . "$ROOT/.env" && set +a

QUANT_PORT="${QUANT_PORT:-8000}"
MCP_PORT="${MCP_PORT:-8081}"
FRONTEND_PORT="${FRONTEND_PORT:-8080}"

QUANT_BIN="$ROOT/services/quant-backend/bin/quant-backend"
MCP_BIN="$ROOT/services/mcp-server/bin/mcp-server"
FRONTEND_DIR="$ROOT/frontend"

cmd="${1:-status}"
target="${2:-all}"

is_up() { # pid 文件存活即 up
  [ -f "$RUN_DIR/$1.pid" ] && kill -0 "$(cat "$RUN_DIR/$1.pid")" 2>/dev/null
}

health() { # 健康检查 ./jarvis.sh health <svc>
  case "$1" in
    quant) curl -sf "http://127.0.0.1:${QUANT_PORT}/" >/dev/null 2>&1 ;;
    mcp) curl -sf "http://127.0.0.1:${MCP_PORT}/health" >/dev/null 2>&1 ;;
    frontend) curl -sf "http://127.0.0.1:${FRONTEND_PORT}/" >/dev/null 2>&1 ;;
  esac
}

wait_healthy() { # 等待健康，最多 30s
  i=0
  while [ "$i" -lt 30 ]; do
    if health "$1"; then return 0; fi
    sleep 1; i=$((i+1))
  done
  echo "WARN: $1 健康检查超时" >&2
  return 1
}

check_disk() { # 磁盘水位 ≥80% 告警
  use=$(df -P "$ROOT/data" | awk 'NR==2{print $5}' | tr -d '%')
  if [ "${use:-0}" -ge 90 ]; then
    echo "WARN: data 磁盘 ${use}% ≥90%，已拒绝全量任务，只许增量" >&2
  elif [ "${use:-0}" -ge 80 ]; then
    echo "WARN: data 磁盘 ${use}% ≥80%，请清理备份/日志" >&2
  fi
}

start_one() {
  case "$1" in
    quant)
      is_up quant && { echo "quant 已运行"; return 0; }
      kill_stale quant
      check_disk
      (cd "$ROOT/services/quant-backend" && go build -o bin/quant-backend ./cmd/server)
      QUANT_LISTEN_ADDR="0.0.0.0:${QUANT_PORT}" QUANT_DB_PATH="$ROOT/data/quant.db" \
        setsid "$QUANT_BIN" </dev/null >>"$LOG_DIR/quant.log" 2>&1 &
      echo $! >"$RUN_DIR/quant.pid"
      if wait_healthy quant && own_pid_alive quant; then echo "quant 已启动 :${QUANT_PORT}"
      else echo "quant 启动失败（查 $LOG_DIR/quant.log）" >&2; return 1; fi
      ;;
    mcp)
      is_up mcp && { echo "mcp 已运行"; return 0; }
      kill_stale mcp
      (cd "$ROOT/services/mcp-server" && go build -o bin/mcp-server ./cmd/server)
      MCP_PORT="${MCP_PORT}" JARVIS_API_BASE_URL="http://127.0.0.1:${QUANT_PORT}/admin" \
        setsid "$MCP_BIN" >>"$LOG_DIR/mcp.log" 2>&1 &
      echo $! >"$RUN_DIR/mcp.pid"
      if wait_healthy mcp && own_pid_alive mcp; then echo "mcp 已启动 :${MCP_PORT}"
      else echo "mcp 启动失败（查 $LOG_DIR/mcp.log）" >&2; return 1; fi
      ;;
    frontend)
      is_up frontend && { echo "frontend 已运行"; return 0; }
      kill_stale frontend
      # 直起 vite 本体（不用 npm 包裹，使 PID 即服务进程，stop 可靠）。
      # 注意：必须在 frontend 目录下启动，否则 vite 根目录错误导致 404。
      (cd "$FRONTEND_DIR" && setsid ./node_modules/.bin/vite --port "$FRONTEND_PORT" --host \
        >>"$LOG_DIR/frontend.log" 2>&1 & echo $! >"$RUN_DIR/frontend.pid")
      if wait_healthy frontend && own_pid_alive frontend; then echo "frontend 已启动 :${FRONTEND_PORT}"
      else echo "frontend 启动失败（查 $LOG_DIR/frontend.log）" >&2; return 1; fi
      ;;
  esac
}

stop_one() {
  if ! is_up "$1"; then echo "$1 未运行"; rm -f "$RUN_DIR/$1.pid"; return 0; fi
  pid=$(cat "$RUN_DIR/$1.pid")
  kill "$pid" 2>/dev/null || true
  i=0
  while kill -0 "$pid" 2>/dev/null && [ "$i" -lt 10 ]; do sleep 1; i=$((i+1)); done
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$RUN_DIR/$1.pid"
  echo "$1 已停止"
}

kill_stale() { # 按二进制名清理残留进程，防端口被旧实例占用导致假启动
  case "$1" in
    quant) pkill -f "services/quant-backend/bin/quant-backend" 2>/dev/null || true ;;
    mcp) pkill -f "services/mcp-server/bin/mcp-server" 2>/dev/null || true ;;
    frontend) pkill -f "frontend/node_modules/.bin/vite" 2>/dev/null || true ;;
  esac
  sleep 1
}

own_pid_alive() { # pid 文件进程必须存活，否则健康成功也算假启动
  [ -f "$RUN_DIR/$1.pid" ] && kill -0 "$(cat "$RUN_DIR/$1.pid")" 2>/dev/null
}

for_each() { # 按依赖顺序 quant → mcp → frontend
  for s in quant mcp frontend; do
    if [ "$target" = "all" ] || [ "$target" = "$s" ]; then "$1" "$s"; fi
  done
}

case "$cmd" in
  start) for_each start_one ;;
  stop)
    if [ "$target" = "all" ]; then
      for s in frontend mcp quant; do stop_one "$s"; done
    else stop_one "$target"; fi ;;
  restart)
    if [ "$target" = "all" ]; then
      for s in frontend mcp quant; do stop_one "$s"; done
      for s in quant mcp frontend; do start_one "$s"; done
    else stop_one "$target"; start_one "$target"; fi ;;
  status)
    for s in quant mcp frontend; do
      if [ "$target" = "all" ] || [ "$target" = "$s" ]; then
        if is_up "$s"; then
          if health "$s"; then echo "$s 运行中 pid=$(cat "$RUN_DIR/$s.pid") 健康OK"
          else echo "$s 运行中 pid=$(cat "$RUN_DIR/$s.pid") 健康失败"; fi
        else echo "$s 未运行"; fi
      fi
    done
    check_disk ;;
  logs) tail -f "$LOG_DIR/${target:-all}.log" 2>/dev/null || ls "$LOG_DIR" ;;
  *) echo "用法：$0 start|stop|restart|status|logs [quant|mcp|frontend|all]" >&2; exit 1 ;;
esac
