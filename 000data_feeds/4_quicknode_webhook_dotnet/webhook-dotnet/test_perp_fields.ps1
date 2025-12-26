# Test webhook endpoint with perpetual position fields
# This script tests the webhook with the new perp fields

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Testing Webhook with Perp Position Data" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$webhookUrl = "http://localhost:5000"
$testPayloadFile = "test_payload_with_perp.json"

# Check if payload file exists
if (-not (Test-Path $testPayloadFile)) {
    Write-Host "ERROR: Test payload file not found: $testPayloadFile" -ForegroundColor Red
    Write-Host "Please ensure the file exists in the current directory." -ForegroundColor Yellow
    exit 1
}

# Read test payload
Write-Host "Reading test payload from: $testPayloadFile" -ForegroundColor Yellow
$payload = Get-Content $testPayloadFile -Raw

Write-Host "Payload loaded successfully" -ForegroundColor Green
Write-Host ""

# Test 1: Send test payload
Write-Host "Test 1: Sending test payload with perp position data..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri $webhookUrl -Method Post -Body $payload -ContentType "application/json"
    Write-Host "SUCCESS: Test payload accepted" -ForegroundColor Green
    Write-Host "Response:" -ForegroundColor Cyan
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor White
} catch {
    Write-Host "FAILED: Could not send test payload" -ForegroundColor Red
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Test 2: Check health endpoint
Write-Host "Test 2: Checking health endpoint..." -ForegroundColor Yellow
try {
    $healthResponse = Invoke-RestMethod -Uri "$webhookUrl/health" -Method Get
    Write-Host "SUCCESS: Health check passed" -ForegroundColor Green
    Write-Host "Recent trades: $($healthResponse.recent_trades)" -ForegroundColor Cyan
    Write-Host "Recent whale movements: $($healthResponse.recent_whale_movements)" -ForegroundColor Cyan
} catch {
    Write-Host "FAILED: Health check failed" -ForegroundColor Red
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""

# Test 3: Verify data format
Write-Host "Test 3: Verifying payload structure..." -ForegroundColor Yellow
$payloadObj = $payload | ConvertFrom-Json
$firstTrade = $payloadObj.matchedTransactions[0]

Write-Host "Checking perp fields in first trade..." -ForegroundColor Cyan
Write-Host "  - has_perp_position: $($firstTrade.has_perp_position)" -ForegroundColor White
Write-Host "  - perp_platform: $($firstTrade.perp_platform)" -ForegroundColor White
Write-Host "  - perp_direction: $($firstTrade.perp_direction)" -ForegroundColor White
Write-Host "  - perp_size: $($firstTrade.perp_size)" -ForegroundColor White
Write-Host "  - perp_leverage: $($firstTrade.perp_leverage)" -ForegroundColor White
Write-Host "  - perp_entry_price: $($firstTrade.perp_entry_price)" -ForegroundColor White

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "All Tests Completed!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Check your database to verify the perp fields were inserted" -ForegroundColor White
Write-Host "2. Query: SELECT signature, wallet_address, direction, has_perp_position, perp_platform, perp_direction, perp_size FROM sol_stablecoin_trades ORDER BY id DESC LIMIT 5;" -ForegroundColor Gray
Write-Host "3. Verify that trades with perp positions show the correct data" -ForegroundColor White
Write-Host ""

