@echo off
chcp 65001 >nul
echo ==============================================
echo   大宗商品多周期预测看板 · 停止服务
echo ==============================================
set FOUND=0
for /f "tokens=5" %%p in ('netstat -ano ^| findstr :8099 ^| findstr LISTENING') do (
  taskkill /f /pid %%p >nul 2>&1
  set FOUND=1
)
if "%FOUND%"=="1" (
  echo 服务已停止, 端口 8099 已释放。
) else (
  echo 未检测到运行中的服务(端口 8099 没有监听)。
)
pause
