@echo off
REM Test the raw_instructions_data field integration

echo ======================================
echo Testing Raw Instructions Data Field
echo ======================================
echo.

REM Set the webhook URL (update if different)
set WEBHOOK_URL=http://localhost:8080

echo 1. Testing Health Check...
echo.
powershell -Command "Invoke-RestMethod -Uri '%WEBHOOK_URL%/health' -Method Get | ConvertTo-Json -Depth 3"
echo.
echo.

echo 2. Sending Test Payload with Raw Instructions Data...
echo.
powershell -Command "$payload = Get-Content 'test_raw_instructions.json' | ConvertFrom-Json | ConvertTo-Json -Depth 10 -Compress; Invoke-RestMethod -Uri '%WEBHOOK_URL%' -Method Post -Body $payload -ContentType 'application/json' | ConvertTo-Json -Depth 3"
echo.
echo.

echo 3. Waiting 2 seconds for async processing...
timeout /t 2 /nobreak > nul
echo.

echo ======================================
echo Test Complete!
echo ======================================
echo.
echo Next Steps:
echo 1. Check database to verify data was saved:
echo    SELECT signature, perp_platform, LEFT(raw_instructions_data, 100) as preview 
echo    FROM sol_stablecoin_trades WHERE raw_instructions_data IS NOT NULL ORDER BY id DESC LIMIT 3;
echo.
echo 2. Verify the raw_instructions_data column contains JSON:
echo    SELECT JSON_VALID(raw_instructions_data) FROM sol_stablecoin_trades WHERE raw_instructions_data IS NOT NULL LIMIT 1;
echo.

pause

