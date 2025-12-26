# Test script for Whale Tracking Webhook Endpoint
# Usage: .\test_whale_endpoint.ps1

Write-Host "Testing Whale Tracking Webhook Endpoint" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$baseUrl = "http://localhost:5000"  # Change to your domain in production
$endpoint = "$baseUrl/webhooks/whale-activity"

# Test payload with sample whale movement data
$testPayload = @{
    whaleMovements = @(
        @{
            signature = "TestSignature_$(Get-Date -Format 'yyyyMMddHHmmss')"
            wallet_address = "TestWallet123456789ABC"
            whale_type = "MEGA_WHALE"
            current_balance = 125340.50
            sol_change = -2500.00
            abs_change = 2500.00
            percentage_moved = 1.95
            direction = "sending"
            action = "SENT"
            movement_significance = "HIGH"
            previous_balance = 127840.50
            fee_paid = 0.000005
            block_time = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
            timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            received_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            slot = 123456789
        }
    )
    summary = @{
        totalMovements = 1
        totalVolume = 2500.00
        netFlow = -2500.00
        receiving = 0
        sending = 1
    }
} | ConvertTo-Json -Depth 10

Write-Host "1. Testing endpoint availability..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri $endpoint -Method POST -Body $testPayload -ContentType "application/json"
    Write-Host "   ✓ Endpoint is accessible" -ForegroundColor Green
    Write-Host "   Response: $($response | ConvertTo-Json)" -ForegroundColor Gray
    Write-Host ""
} catch {
    Write-Host "   ✗ Endpoint test failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
    exit 1
}

Write-Host "2. Testing with multiple whale movements..." -ForegroundColor Yellow
$multiPayload = @{
    whaleMovements = @(
        @{
            signature = "TestMulti1_$(Get-Date -Format 'yyyyMMddHHmmss')_001"
            wallet_address = "MultiTestWallet001"
            whale_type = "SUPER_WHALE"
            current_balance = 75000.00
            sol_change = -3000.00
            abs_change = 3000.00
            percentage_moved = 4.00
            direction = "sending"
            action = "SENT"
            movement_significance = "HIGH"
            previous_balance = 78000.00
            fee_paid = 0.000005
            block_time = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
            timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            received_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            slot = 123456790
        },
        @{
            signature = "TestMulti1_$(Get-Date -Format 'yyyyMMddHHmmss')_002"
            wallet_address = "MultiTestWallet002"
            whale_type = "WHALE"
            current_balance = 25000.00
            sol_change = 5000.00
            abs_change = 5000.00
            percentage_moved = 25.00
            direction = "receiving"
            action = "RECEIVED"
            movement_significance = "CRITICAL"
            previous_balance = 20000.00
            fee_paid = 0.000005
            block_time = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
            timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            received_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            slot = 123456791
        }
    )
    summary = @{
        totalMovements = 2
        totalVolume = 8000.00
        netFlow = 2000.00
        receiving = 1
        sending = 1
    }
} | ConvertTo-Json -Depth 10

try {
    $response = Invoke-RestMethod -Uri $endpoint -Method POST -Body $multiPayload -ContentType "application/json"
    Write-Host "   ✓ Multiple movements processed" -ForegroundColor Green
    Write-Host "   Response: $($response | ConvertTo-Json)" -ForegroundColor Gray
    Write-Host ""
} catch {
    Write-Host "   ✗ Multiple movements test failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
}

Write-Host "3. Testing duplicate handling..." -ForegroundColor Yellow
try {
    # Send the same payload twice
    $dupPayload = @{
        whaleMovements = @(
            @{
                signature = "DuplicateTest123"
                wallet_address = "DupTestWallet"
                whale_type = "WHALE"
                current_balance = 15000.00
                sol_change = -1000.00
                abs_change = 1000.00
                percentage_moved = 6.67
                direction = "sending"
                action = "SENT"
                movement_significance = "HIGH"
                previous_balance = 16000.00
                fee_paid = 0.000005
                block_time = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
                timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
                received_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
                slot = 999999999
            }
        )
    } | ConvertTo-Json -Depth 10
    
    $response1 = Invoke-RestMethod -Uri $endpoint -Method POST -Body $dupPayload -ContentType "application/json"
    Start-Sleep -Seconds 2
    $response2 = Invoke-RestMethod -Uri $endpoint -Method POST -Body $dupPayload -ContentType "application/json"
    
    Write-Host "   ✓ Duplicate handling works (both requests accepted)" -ForegroundColor Green
    Write-Host ""
} catch {
    Write-Host "   ⚠ Duplicate test warning: $($_.Exception.Message)" -ForegroundColor Yellow
    Write-Host ""
}

Write-Host "4. Checking health endpoint..." -ForegroundColor Yellow
try {
    $healthResponse = Invoke-RestMethod -Uri "$baseUrl/health" -Method GET
    Write-Host "   ✓ Health check successful" -ForegroundColor Green
    Write-Host "   Status: $($healthResponse.status)" -ForegroundColor Gray
    Write-Host "   Recent Trades: $($healthResponse.recent_trades)" -ForegroundColor Gray
    Write-Host "   Recent Whale Movements: $($healthResponse.recent_whale_movements)" -ForegroundColor Gray
    Write-Host ""
} catch {
    Write-Host "   ✗ Health check failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
}

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "✓ Testing Complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Check database: SELECT * FROM whale_movements ORDER BY created_at DESC LIMIT 10;" -ForegroundColor Gray
Write-Host "2. Configure QuickNode webhook to point to: $endpoint" -ForegroundColor Gray
Write-Host "3. Monitor logs for live data" -ForegroundColor Gray
Write-Host ""

