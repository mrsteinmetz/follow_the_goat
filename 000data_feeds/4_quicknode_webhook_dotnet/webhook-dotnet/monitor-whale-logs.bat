@echo off
REM Monitor Whale Activity Endpoint Logs
REM Double-click this file to watch logs in real-time

echo.
echo ========================================
echo Whale Activity Endpoint Log Monitor
echo ========================================
echo.

powershell -ExecutionPolicy Bypass -File "%~dp0monitor-whale-logs.ps1"

pause

