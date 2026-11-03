#!/usr/bin/env bash
# ============================================================
#  大宗商品看板 · 一键「启动 + 自动出 Word 日报」
#  用法: bash 启动出日报.sh [端口]       默认 8099
#  效果: 服务没跑则后台启动 → 等就绪 → 自动生成 Word 日报
#        (日报_YYYY-MM-DD.docx, 当天重复运行会覆盖当天那份)
# ============================================================
set -e
PORT="${1:-8099}"
BASE="$(cd "$(dirname "$0")" && pwd)"
LOG="$BASE/gold_predictor.log"

echo "=============================================="
echo " 大宗商品看板 · 一键启动 + 自动出日报"
echo " 目录 : $BASE"
echo " 端口 : $PORT"
echo "=============================================="

# 1. 检查 python3
if ! command -v python3 >/dev/null 2>&1; then
  echo "错误: 未找到 python3"
  exit 1
fi

# 2. 检测服务是否已在运行
RUNNING=$(curl -s -m 3 -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT/" 2>/dev/null || echo 000)
if [ "$RUNNING" = "200" ]; then
  echo "[1/4] 服务已在运行, 跳过启动。"
else
  echo "[1/4] 启动服务 (后台)..."
  cd "$BASE"
  setsid nohup python3 -u "$BASE/gold_predictor.py" --port "$PORT" \
    > "$LOG" 2>&1 < /dev/null &
fi

# 3. 等待就绪
echo "[2/4] 等待服务就绪..."
code=000
for i in $(seq 1 18); do
  code=$(curl -s -m 3 -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT/" 2>/dev/null || echo 000)
  if [ "$code" = "200" ]; then
    break
  fi
  echo "  等待中... ($i/18, HTTP $code)"
  sleep 5
done
if [ "$code" != "200" ]; then
  echo "错误: 服务未就绪, 查看日志: tail -f $LOG"
  exit 1
fi
PID=$(pgrep -f "gold_predictor.py --port $PORT" | head -1 || echo "?")
echo "  服务就绪, PID: $PID"

# 4. 生成 Word 日报
echo "[3/4] 生成 Word 日报..."
cd "$BASE"
python3 "$BASE/生成日报.py"

DOCX="$BASE/日报_$(date +%Y-%m-%d).docx"
echo "[4/4] 完成 ✅"
echo ""
echo "=============================================="
echo " Word 日报 : $DOCX"
echo " 网页地址  : http://127.0.0.1:$PORT"
echo " 日志      : $LOG"
echo " 停止服务  : pkill -f 'gold_predictor.py --port $PORT'"
echo "=============================================="
