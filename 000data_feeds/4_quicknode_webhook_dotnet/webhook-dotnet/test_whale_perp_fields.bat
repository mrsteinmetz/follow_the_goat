@echo off
REM Test whale webhook endpoint with perpetual position fields

echo.
echo ============================================
echo Testing Whale Webhook with Perp Position Data
echo ============================================
echo.

powershell.exe -ExecutionPolicy Bypass -File test_whale_perp_fields.ps1

pause

