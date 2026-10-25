@echo off
chcp 65001 >nul
rem fleetdash 电源操作 token —— 一键复制到剪贴板（2026-10-08 建）
rem 场景：面板 dgx.angineer.cn/fleet 点 关机/重启/唤醒 时要手输，不必背诵或翻文件
setlocal
set "T=%~dp0power.token"
if not exist "%T%" (
  echo [x] 找不到 "%T%"
  pause
  exit /b 1
)
type "%T%" | clip
echo.
echo [ok] 电源 token 已复制到剪贴板，去面板 Ctrl+V 粘贴：
echo.
type "%T%"
echo.
echo 看完按任意键关闭本窗口。
pause
