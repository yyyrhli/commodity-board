@echo off
chcp 65001 >nul
cd /d %~dp0
echo ============================================
echo   大宗商品多周期预测看板 正在启动...
echo   启动后请用浏览器打开: http://127.0.0.1:8099
echo   关闭本窗口即停止服务
echo ============================================
python gold_predictor.py --port 8099
echo.
echo 服务已停止。
pause
