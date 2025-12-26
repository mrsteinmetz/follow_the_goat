@echo off
REM Test webhook endpoint with perpetual position fields

echo.
echo ========================================
echo Testing Webhook with Perp Position Data
echo ========================================
echo.

powershell.exe -ExecutionPolicy Bypass -File test_perp_fields.ps1

pause

