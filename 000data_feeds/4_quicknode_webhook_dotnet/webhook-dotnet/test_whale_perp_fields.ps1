# Test whale webhook endpoint with perpetual position fields
# This script tests the whale webhook with the new perp fields

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Testing Whale Webhook with Perp Position Data" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$webhookUrl = "http://localhost:5000/webhooks/whale-activity"
$testPayloadFile = "test_whale_payload_with_perp.json"

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
Write-Host "Test 1: Sending whale movements with perp position data..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri $webhookUrl -Method Post -Body $payload -ContentType "application/json"
    Write-Host "SUCCESS: Whale movements accepted" -ForegroundColor Green
    Write-Host "Response:" -ForegroundColor Cyan
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor White
} catch {
    Write-Host "FAILED: Could not send whale movements" -ForegroundColor Red
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Test 2: Check health endpoint
Write-Host "Test 2: Checking health endpoint..." -ForegroundColor Yellow
try {
    $healthResponse = Invoke-RestMethod -Uri "http://localhost:5000/health" -Method Get
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
$firstMovement = $payloadObj.whaleMovements[0]

Write-Host "Checking perp fields in first whale movement..." -ForegroundColor Cyan
Write-Host "  - whale_type: $($firstMovement.whale_type)" -ForegroundColor White
Write-Host "  - direction: $($firstMovement.direction)" -ForegroundColor White
Write-Host "  - abs_change: $($firstMovement.abs_change) SOL" -ForegroundColor White
Write-Host "  - has_perp_position: $($firstMovement.has_perp_position)" -ForegroundColor White
Write-Host "  - perp_platform: $($firstMovement.perp_platform)" -ForegroundColor White
Write-Host "  - perp_direction: $($firstMovement.perp_direction)" -ForegroundColor White
Write-Host "  - perp_size: $($firstMovement.perp_size)" -ForegroundColor White
Write-Host "  - perp_leverage: $($firstMovement.perp_leverage)" -ForegroundColor White
Write-Host "  - perp_entry_price: $($firstMovement.perp_entry_price)" -ForegroundColor White

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "All Tests Completed!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Check your database to verify the perp fields were inserted" -ForegroundColor White
Write-Host "2. Query: SELECT signature, wallet_address, whale_type, direction, has_perp_position, perp_platform, perp_direction, perp_size FROM whale_movements ORDER BY id DESC LIMIT 5;" -ForegroundColor Gray
Write-Host "3. Verify that whale movements with perp positions show the correct data" -ForegroundColor White
Write-Host ""

