@echo off
REM Test script for raw_instructions_data with MISSING fields (Compute Budget, System instructions)
REM This tests that the nullable Base58Data and Accounts properties handle missing keys correctly

echo ========================================
echo Testing Raw Instructions with MISSING FIELDS
echo ========================================
echo.
echo This test includes instructions with:
echo - Missing base58_data (Compute Budget)
echo - Missing accounts (Compute Budget)
echo - Missing both fields
echo.

REM Read the test payload
set "payload_file=test_raw_instructions_missing_fields.json"

if not exist "%payload_file%" (
    echo ERROR: Test file not found: %payload_file%
    exit /b 1
)

echo Sending test payload to webhook...
echo.

REM Send POST request using PowerShell
powershell -Command "$payload = Get-Content '%payload_file%' -Raw; $response = Invoke-WebRequest -Uri 'http://localhost:5000/' -Method POST -Body $payload -ContentType 'application/json'; Write-Host 'Status Code:' $response.StatusCode; Write-Host 'Response:'; $response.Content"

echo.
echo ========================================
echo Test Complete!
echo ========================================
echo.
echo Next: Check the database to verify raw_instructions_data was saved correctly
echo.
echo SQL Query:
echo SELECT signature, 
echo        JSON_LENGTH(raw_instructions_data) AS instruction_count,
echo        raw_instructions_data,
echo        created_at 
echo FROM sol_stablecoin_trades 
echo WHERE signature = 'TestSignature1WithMissingFields123456789';
echo.

pause

