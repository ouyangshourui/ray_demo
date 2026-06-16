#!/bin/bash

# Ray Data Demo 启动脚本
# 用法：
#   ./start.sh           # 启动（占用前台）
#   ./start.sh restart   # 强制杀端口后重启
#   ./start.sh stop      # 仅停止
#   ./start.sh status    # 查看进程 + 端口

set -u

PORT=15556
APP_FILE="app.py"
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

color_ok()  { printf "\033[32m%s\033[0m\n" "$*"; }
color_warn(){ printf "\033[33m%s\033[0m\n" "$*"; }
color_err() { printf "\033[31m%s\033[0m\n" "$*"; }

kill_port() {
    local pids
    pids=$(lsof -ti :"$PORT" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        color_warn "端口 $PORT 被占用，PID: $pids，强制终止..."
        # shellcheck disable=SC2086
        kill -9 $pids 2>/dev/null || true
        sleep 1
        # 二次确认
        local left
        left=$(lsof -ti :"$PORT" 2>/dev/null || true)
        if [ -n "$left" ]; then
            color_err "端口 $PORT 仍被占用：$left，请手动处理"
            return 1
        fi
        color_ok "端口 $PORT 已释放"
    else
        color_ok "端口 $PORT 空闲"
    fi
    # 兜底：杀掉残留的同名 app.py 进程（同目录）
    local app_pids
    app_pids=$(pgrep -f "python.* $ROOT_DIR/$APP_FILE" 2>/dev/null || true)
    if [ -n "$app_pids" ]; then
        color_warn "发现残留 $APP_FILE 进程：$app_pids，一并清理"
        # shellcheck disable=SC2086
        kill -9 $app_pids 2>/dev/null || true
        sleep 1
    fi
}

show_status() {
    echo "----- 端口 $PORT -----"
    lsof -i :"$PORT" 2>/dev/null || echo "（无监听）"
    echo "----- $APP_FILE 进程 -----"
    pgrep -fl "$APP_FILE" || echo "（无进程）"
}

check_env() {
    if ! command -v python3 &> /dev/null; then
        color_err "Python3 未安装，请先安装 Python3"
        exit 1
    fi
    color_ok "Python 版本: $(python3 --version)"

    if ! python3 -c "import ray" &> /dev/null; then
        color_warn "Ray 未安装，正在安装依赖..."
        pip install -r "$ROOT_DIR/requirements.txt"
        if [ $? -ne 0 ]; then
            color_err "依赖安装失败"
            exit 1
        fi
    fi
    color_ok "依赖检查完成"
}

start_server() {
    cd "$ROOT_DIR" || exit 1
    echo ""
    echo "=========================================="
    echo "  Ray Data Demo Server"
    echo "=========================================="
    check_env
    kill_port
    echo ""
    echo "📡 访问地址: http://localhost:$PORT"
    echo "📋 场景实验室: http://localhost:$PORT/scenarios"
    echo ""
    echo "按 Ctrl+C 停止服务"
    echo ""
    exec python3 "$APP_FILE"
}

case "${1:-start}" in
    start)
        start_server
        ;;
    restart)
        # 与 start 等价（start 已自带杀端口逻辑），保留命令使语义更直观
        start_server
        ;;
    stop)
        kill_port
        color_ok "已停止"
        ;;
    status)
        show_status
        ;;
    *)
        echo "用法: $0 [start|restart|stop|status]"
        exit 1
        ;;
esac
