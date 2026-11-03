#!/usr/bin/env bash
# ============================================================
#  大宗商品多周期预测看板 — 一键部署脚本
#  用法: bash deploy.sh [端口]      默认端口 8099
#  说明: 脚本从自己所在目录读取 gold_predictor.py + echarts.min.js
#        就地部署并后台运行, 自动做健康检查
# ============================================================
set -e

PORT="${1:-8099}"
SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="${DEPLOY_DIR:-$SRC_DIR}"

echo "=============================================="
echo " 大宗商品多周期预测看板 · 一键部署"
echo " 代码目录 : $SRC_DIR"
echo " 部署目录 : $DEPLOY_DIR"
echo " 端口     : $PORT"
echo "=============================================="

echo ""
echo "[1/6] 检查 python3 ..."
if ! command -v python3 >/dev/null 2>&1; then
  echo "错误: 未检测到 python3。请先安装 Python 3 (>=3.8)。"
  exit 1
fi
python3 --version

echo ""
echo "[2/6] 检查核心文件 ..."
for f in gold_predictor.py echarts.min.js; do
  if [ ! -f "$SRC_DIR/$f" ]; then
    echo "错误: 缺少核心文件 $SRC_DIR/$f"
    exit 1
  fi
done
echo "核心文件齐全。"

echo ""
echo "[3/6] 检查是否已在运行 ..."
RUNNING_PID=$(pgrep -f "gold_predictor.py --port $PORT" | head -1 || true)
if [ -n "$RUNNING_PID" ]; then
  echo "检测到端口 $PORT 上已有看板在运行 (PID $RUNNING_PID)。"
  echo "如需重启, 先运行: pkill -f 'gold_predictor.py --port $PORT'"
else
  echo "未运行, 准备启动。"
fi

echo ""
echo "[4/6] 准备数据目录 ..."
mkdir -p "$DEPLOY_DIR/commodity_state"

if [ -n "$RUNNING_PID" ]; then
  NEW_PID="$RUNNING_PID"
else
  echo ""
  echo "[5/6] 后台启动服务 ..."
  cd "$DEPLOY_DIR"
  setsid nohup python3 -u "$DEPLOY_DIR/gold_predictor.py" --port "$PORT" \
    > "$DEPLOY_DIR/gold_predictor.log" 2>&1 < /dev/null &
  sleep 8
  NEW_PID=$(pgrep -f "gold_predictor.py --port $PORT" | head -1 || true)
  if [ -z "$NEW_PID" ]; then
    echo "错误: 服务启动失败, 查看日志: tail -f $DEPLOY_DIR/gold_predictor.log"
    exit 1
  fi
  echo "已启动, PID: $NEW_PID"
fi

echo ""
echo "[6/6] 健康检查 (等待服务就绪) ..."
code=000
for i in 1 2 3 4 5 6 7 8 9 10 11 12; do
  code=$(curl -s -m 3 -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT/" || echo 000)
  if [ "$code" = "200" ]; then
    break
  fi
  echo "  等待中... ($i/12, HTTP $code)"
  sleep 5
done

if [ "$code" != "200" ]; then
  echo "警告: 健康检查未通过 (HTTP $code)。"
  echo "日志: tail -f $DEPLOY_DIR/gold_predictor.log"
  exit 1
fi

echo ""
echo "=============================================="
echo " 部署完成 ✅  服务已正常运行"
echo "=============================================="
echo " 访问地址    : http://127.0.0.1:$PORT"
echo " 局域网访问  : http://<本机IP>:$PORT  (需防火墙放行 $PORT)"
echo " 进程 PID    : $NEW_PID"
echo " 日志文件    : $DEPLOY_DIR/gold_predictor.log"
echo ""
echo " 常用命令:"
echo "   查看日志  : tail -f $DEPLOY_DIR/gold_predictor.log"
echo "   停止服务  : pkill -f 'gold_predictor.py --port $PORT'"
echo "   重启服务  : pkill -f 'gold_predictor.py --port $PORT'; sleep 2; bash deploy.sh $PORT"
echo "=============================================="
