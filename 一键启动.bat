@echo off
setlocal
title CEMS one-click start

REM ============================================================
REM  Double-click this file to start everything.
REM  Default: Docker mode -> start all services, wait until the
REM  web dashboard really answers, then open the browser.
REM
REM  Local mode (4 python windows, per-layer logs):
REM      this file + argument:  -Mode Local
REM  Start without opening browser:  -NoBrowser
REM  Stop all containers:            -Stop
REM
REM  Real logic lives in start.ps1 (same folder). This is a wrapper.
REM  ASCII only on purpose: keep cmd from mis-parsing CJK comments.
REM ============================================================

set "PS=powershell"
where pwsh >nul 2>&1 && set "PS=pwsh"

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %*
set "RC=%ERRORLEVEL%"

if not "%RC%"=="0" (
  echo.
  echo [ERROR] Startup failed, exit code %RC%. See messages above.
  echo         Press any key to close this window.
  pause >nul
)

exit /b %RC%
