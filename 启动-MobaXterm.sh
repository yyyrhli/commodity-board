#!/usr/bin/env bash
# ============================================================
#  大宗商品多周期预测看板 · MobaXterm 常驻启动脚本
#  用法: 在 MobaXterm 本地终端进入本文件夹后运行
#        bash 启动-MobaXterm.sh
#  效果: 在独立的 Windows 命令行窗口里后台运行,
#        关闭 MobaXterm 终端也不影响服务。
#  停止: 关闭弹出的那个命令行窗口, 或运行 停止-MobaXterm.bat
# ============================================================
set -e

cd "$(dirname "$0")" || exit 1

# 检查核心文件
for f in gold_predictor.py echarts.min.js; do
  if [ ! -f "$f" ]; then
    echo "错误: 缺少核心文件 $f"
    exit 1
  fi
done

# 检查 python
if ! command -v python >/dev/null 2>&1; then
  echo "错误: 找不到 python。请先安装 Python 3 并勾选 Add python.exe to PATH。"
  exit 1
fi

# 检查端口是否已被占用
if netstat -ano 2>/dev/null | grep -q ":8099 .*LISTENING"; then
  echo "端口 8099 已在运行(可能已经启动过)。"
  echo "如果页面打不开, 请先运行 停止-MobaXterm.bat 再重新启动。"
else
  # 转成 Windows 路径, 在独立 cmd 窗口里后台运行
  WINPATH="$(cygpath -w "$PWD" 2>/dev/null || echo "$PWD")"
  cmd //c "start \"commodity-dashboard\" /D \"$WINPATH\" cmd /k python gold_predictor.py --port 8099"
  sleep 4
fi

echo ""
echo "=============================================="
echo " 已启动 ✅"
echo " 浏览器打开: http://127.0.0.1:8099"
echo ""
echo " 说明:"
echo "  - 会弹出一个独立的命令行窗口在跑服务(有日志),"
echo "    关闭 MobaXterm 终端不影响它运行。"
echo "  - 停止服务: 关闭那个命令行窗口,"
echo "    或双击 停止-MobaXterm.bat"
echo "=============================================="
