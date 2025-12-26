@echo off
REM Test Whale Tracking Endpoint
REM This script runs PowerShell test script for the whale webhook

echo.
echo Testing Whale Tracking Webhook...
echo.

powershell.exe -ExecutionPolicy Bypass -File "%~dp0test_whale_endpoint.ps1"

if errorlevel 1 (
    echo.
    echo Test failed!
    pause
    exit /b 1
)

echo.
echo Press any key to exit...
pause >nul

